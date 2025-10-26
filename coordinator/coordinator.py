#!/usr/bin/env python3
"""
Coordination Service implementing:
- Hot Topic Detection using LoOP (Local Outlier Probability)
- Topic-Aware Load Balancing (Algorithm 1 from paper)
- Trie data structure for topic management
"""

import numpy as np
import paho.mqtt.client as mqtt
import json
import time
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Set
from scipy.special import erf
from scipy.spatial.distance import euclidean
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BrokerInfo:
    """Information about a broker"""
    host: str
    port: int
    capacity_mbps: float  # Network capacity in Mbps
    data_rate: float  # d_i: Data transmission rate
    utilization: float = 0.0
    topics: Set[str] = None
    
    def __post_init__(self):
        if self.topics is None:
            self.topics = set()


class TrieNode:
    """Trie node for hierarchical topic storage"""
    def __init__(self):
        self.children = {}
        self.is_topic = False
        self.subscribers = set()
        self.request_rate = 0.0
        self.message_count = 0


class TopicTrie:
    """Trie data structure for efficient topic matching and management"""
    def __init__(self):
        self.root = TrieNode()
    
    def insert(self, topic: str, subscriber_id: str = None):
        """Insert a topic into the Trie"""
        node = self.root
        for level in topic.split('/'):
            if level not in node.children:
                node.children[level] = TrieNode()
            node = node.children[level]
        node.is_topic = True
        if subscriber_id:
            node.subscribers.add(subscriber_id)
    
    def search(self, topic: str) -> TrieNode:
        """Search for a topic in the Trie"""
        node = self.root
        for level in topic.split('/'):
            if level not in node.children:
                return None
            node = node.children[level]
        return node if node.is_topic else None
    
    def update_stats(self, topic: str, request_rate: float):
        """Update topic statistics"""
        node = self.search(topic)
        if node:
            node.request_rate = request_rate
            node.message_count += 1


class HotTopicDetector:
    """
    Hot Topic Detection using Local Outlier Probability (LoOP)
    Based on Section III-A of the paper
    """
    def __init__(self, k_neighbors: int = 5, lambda_param: float = 3.0):
        self.k_neighbors = k_neighbors
        self.lambda_param = lambda_param
    
    def compute_pdist(self, point: float, context: List[float]) -> float:
        """Compute probabilistic distance"""
        if not context:
            return 0.0
        distances = [abs(point - c) for c in context]
        return np.mean(distances) if distances else 0.0
    
    def compute_plof(self, request_rates: Dict[str, float]) -> Dict[str, float]:
        """
        Compute Probabilistic Local Outlier Factor (PLOF)
        Equation (1) from the paper
        """
        if len(request_rates) < 2:
            return {topic: 0.0 for topic in request_rates}
        
        topics = list(request_rates.keys())
        rates = list(request_rates.values())
        plof_scores = {}
        
        for topic, rate in request_rates.items():
            # Get k-nearest neighbors based on request rate
            distances = [(other_topic, abs(rate - other_rate)) 
                        for other_topic, other_rate in request_rates.items() 
                        if other_topic != topic]
            distances.sort(key=lambda x: x[1])
            context = [request_rates[t] for t, _ in distances[:self.k_neighbors]]
            
            # Compute PLOF
            pdist_k = self.compute_pdist(rate, context)
            expected_pdist = np.mean([self.compute_pdist(c, context) for c in context])
            
            if expected_pdist > 0:
                plof = (pdist_k / expected_pdist) - 1
            else:
                plof = 0.0
            
            plof_scores[topic] = max(0.0, plof)
        
        return plof_scores
    
    def compute_loop(self, request_rates: Dict[str, float], threshold: float = 0.8) -> Set[str]:
        """
        Compute Local Outlier Probability (LoOP)
        Equations (2) and (3) from the paper
        Returns set of hot topics
        """
        plof_scores = self.compute_plof(request_rates)
        
        if not plof_scores:
            return set()
        
        # Compute nPLOF (normalized PLOF)
        plof_values = list(plof_scores.values())
        nplof = self.lambda_param * np.sqrt(np.mean([p**2 for p in plof_values]))
        
        # Compute LoOP for each topic
        loop_scores = {}
        hot_topics = set()
        
        for topic, plof in plof_scores.items():
            if nplof > 0:
                loop_score = max(0, erf(plof / (nplof * np.sqrt(2))))
            else:
                loop_score = 0.0
            
            loop_scores[topic] = loop_score
            
            # Mark as hot topic if LoOP score is high
            if loop_score >= threshold:
                hot_topics.add(topic)
        
        logger.info(f"LoOP Scores: {loop_scores}")
        logger.info(f"Hot Topics detected: {hot_topics}")
        
        return hot_topics


class LoadBalancer:
    """
    Topic-Aware Load Balancing Algorithm
    Implements Algorithm 1 from the paper (Section III-D)
    """
    def __init__(self, brokers: List[BrokerInfo]):
        self.brokers = brokers
        self.trie = TopicTrie()
        self.hot_detector = HotTopicDetector()
        self.topic_stats = defaultdict(lambda: {'rate': 0.0, 'subscribers': 0})
        self.client_broker_map = {}  # X matrix: client -> broker assignment
    
    def calculate_optimal_utilization(self, total_arrival_rate: float) -> Dict[int, float]:
        """
        Calculate optimal broker utilization Q*
        Equation (15) from the paper
        """
        total_service_rate = sum(b.data_rate for b in self.brokers)
        num_brokers = len(self.brokers)
        
        optimal_util = {}
        for i, broker in enumerate(self.brokers):
            Q_optimal = 1 - (total_service_rate - total_arrival_rate) / (num_brokers * broker.data_rate)
            optimal_util[i] = max(0.0, min(0.99, Q_optimal))  # Keep stable (0 < Q < 1)
        
        logger.info(f"Optimal Utilizations: {optimal_util}")
        return optimal_util
    
    def calculate_cost_function(self, broker_idx: int, current_util: float, optimal_util: float) -> float:
        """
        Calculate cost function ν_i
        Equation (17) from the paper
        """
        return abs(optimal_util - current_util)
    
    def allocation_matrix(self, hot_topics: Set[str], optimal_utils: Dict[int, float]) -> Dict[str, int]:
        """
        Algorithm 1: Topic-Aware Load Balancing
        Returns allocation: topic -> broker_index
        """
        allocation = {}
        
        # Sort topics by popularity (request rate) in descending order
        sorted_topics = sorted(hot_topics, 
                             key=lambda t: self.topic_stats[t]['rate'], 
                             reverse=True)
        
        logger.info(f"Allocating {len(sorted_topics)} hot topics to brokers")
        
        for topic in sorted_topics:
            topic_rate = self.topic_stats[topic]['rate']
            best_broker = None
            best_cost = float('inf')
            
            # Find available broker with best cost function
            for i, broker in enumerate(self.brokers):
                # Check availability: R_i + R_k < μ_i
                current_load = sum(self.topic_stats[t]['rate'] 
                                 for t in broker.topics)
                
                if current_load + topic_rate < broker.data_rate:
                    # Calculate new utilization if topic is assigned
                    new_util = (current_load + topic_rate) / broker.data_rate
                    cost = self.calculate_cost_function(i, new_util, optimal_utils[i])
                    
                    if cost < best_cost:
                        best_cost = cost
                        best_broker = i
            
            if best_broker is not None:
                allocation[topic] = best_broker
                self.brokers[best_broker].topics.add(topic)
                logger.info(f"Allocated topic '{topic}' (rate={topic_rate:.2f}) to broker {best_broker}")
            else:
                # Fallback: assign to least loaded broker
                least_loaded = min(range(len(self.brokers)), 
                                 key=lambda i: len(self.brokers[i].topics))
                allocation[topic] = least_loaded
                self.brokers[least_loaded].topics.add(topic)
                logger.warning(f"Fallback allocation: topic '{topic}' to broker {least_loaded}")
        
        return allocation
    
    def update_topic_stats(self, topic: str, rate: float, num_subscribers: int):
        """Update topic statistics for hot topic detection"""
        self.topic_stats[topic]['rate'] = rate
        self.topic_stats[topic]['subscribers'] = num_subscribers
        self.trie.update_stats(topic, rate)
    
    def detect_and_balance(self):
        """Main load balancing routine"""
        # Get all topics with their request rates
        request_rates = {topic: stats['rate'] 
                        for topic, stats in self.topic_stats.items()}
        
        if not request_rates:
            logger.info("No topics to balance")
            return {}
        
        # Step 1: Detect hot topics using LoOP
        hot_topics = self.hot_detector.compute_loop(request_rates)
        
        if not hot_topics:
            logger.info("No hot topics detected")
            return {}
        
        # Step 2: Calculate optimal utilization
        total_rate = sum(request_rates.values())
        optimal_utils = self.calculate_optimal_utilization(total_rate)
        
        # Step 3: Run allocation algorithm
        allocation = self.allocation_matrix(hot_topics, optimal_utils)
        
        # Step 4: Update broker utilizations
        for i, broker in enumerate(self.brokers):
            load = sum(self.topic_stats[t]['rate'] for t in broker.topics)
            broker.utilization = load / broker.data_rate if broker.data_rate > 0 else 0
            logger.info(f"Broker {i} utilization: {broker.utilization:.2%}")
        
        return allocation


class CoordinationService:
    """Main coordination service"""
    def __init__(self):
        self.brokers = self._init_brokers()
        self.load_balancer = LoadBalancer(self.brokers)
        self.clients = {}
        self.lock = threading.Lock()
        
        # MQTT clients for each broker
        self.broker_clients = {}
        self._connect_to_brokers()
        
        # Central coordination broker for client registration
        self.coord_client = mqtt.Client("coordinator_main")
        self.coord_client.on_connect = self._on_coord_connect
        self.coord_client.on_message = self._on_coord_message
        self._connect_coordination_broker()
    
    def _init_brokers(self) -> List[BrokerInfo]:
        """Initialize broker information"""
        broker_configs = [
            ("broker1", 1883, 350),
            ("broker2", 1883, 450),
            ("broker3", 1883, 550),
            ("broker4", 1883, 600),
        ]
        
        brokers = []
        for host, port, capacity in broker_configs:
            # Convert capacity (Mbps) to effective data rate
            # Assuming average message size of 550KB
            data_rate = capacity * 1024 / (550 * 8)  # messages per second
            brokers.append(BrokerInfo(host, port, capacity, data_rate))
        
        return brokers
    
    def _connect_to_brokers(self):
        """Connect to all brokers"""
        for i, broker in enumerate(self.brokers):
            client = mqtt.Client(f"coordinator_broker{i}")
            client.on_connect = lambda c, u, f, rc, idx=i: self._on_broker_connect(c, u, f, rc, idx)
            client.on_message = self._on_broker_message
            
            try:
                client.connect(broker.host, broker.port, 60)
                client.loop_start()
                self.broker_clients[i] = client
                logger.info(f"Connected to broker {i} at {broker.host}:{broker.port}")
            except Exception as e:
                logger.error(f"Failed to connect to broker {i}: {e}")
    
    def _connect_coordination_broker(self):
        """Connect to broker1 for coordination messages"""
        try:
            self.coord_client.connect("broker1", 1883, 60)
            self.coord_client.loop_start()
            logger.info("Coordination client connected to broker1")
        except Exception as e:
            logger.error(f"Failed to connect coordination client: {e}")
    
    def _on_coord_connect(self, client, userdata, flags, rc):
        """Callback when coordination client connects"""
        logger.info(f"Coordinator connected with result code {rc}")
        # Subscribe to registration and statistics topics
        client.subscribe("coordinator/register/+")
        client.subscribe("coordinator/stats/+")
        logger.info("Subscribed to coordinator topics")
    
    def _on_coord_message(self, client, userdata, msg):
        """Handle coordination messages from clients"""
        try:
            topic_parts = msg.topic.split('/')
            
            if topic_parts[1] == 'register':
                # Client registration: coordinator/register/{publisher|subscriber}
                payload = json.loads(msg.payload.decode())
                client_id = payload['client_id']
                client_type = payload['type']
                topics = payload['topics']
                
                if client_type == 'publisher':
                    rates = payload.get('rates', {})
                    self.register_publisher(client_id, topics, rates)
                    logger.info(f"Registered publisher {client_id} with {len(topics)} topics")
                elif client_type == 'subscriber':
                    self.register_subscriber(client_id, topics)
                    logger.info(f"Registered subscriber {client_id} with {len(topics)} topics")
            
            elif topic_parts[1] == 'stats':
                # Topic statistics: coordinator/stats/{topic}
                payload = json.loads(msg.payload.decode())
                topic = payload['topic']
                rate = payload.get('rate', 0.0)
                num_subs = payload.get('subscribers', 0)
                
                with self.lock:
                    self.load_balancer.update_topic_stats(topic, rate, num_subs)
                
        except Exception as e:
            logger.error(f"Error processing coordination message: {e}")
    
    def send_migration_command(self, client_id: str, new_broker_host: str, new_broker_port: int):
        """Send migration command to a client"""
        try:
            command = {
                'client_id': client_id,
                'broker_host': new_broker_host,
                'broker_port': new_broker_port,
                'timestamp': time.time()
            }
            
            self.coord_client.publish(
                f"coordinator/migrate/{client_id}",
                json.dumps(command),
                qos=1
            )
            logger.info(f"Sent migration command to {client_id}: {new_broker_host}:{new_broker_port}")
            
        except Exception as e:
            logger.error(f"Failed to send migration command: {e}")
    
    def register_publisher(self, client_id: str, topics: List[str], rates: Dict[str, float]):
        """Register a publisher with its topics and rates"""
        with self.lock:
            self.clients[client_id] = {'type': 'publisher', 'topics': topics}
            
            for topic in topics:
                rate = rates.get(topic, 1.0)
                self.load_balancer.update_topic_stats(topic, rate, 0)
                self.load_balancer.trie.insert(topic)
    
    def register_subscriber(self, client_id: str, topics: List[str]):
        """Register a subscriber with its topic subscriptions"""
        with self.lock:
            self.clients[client_id] = {'type': 'subscriber', 'topics': topics}
            
            for topic in topics:
                current_subs = self.load_balancer.topic_stats[topic]['subscribers']
                self.load_balancer.topic_stats[topic]['subscribers'] = current_subs + 1
                self.load_balancer.trie.insert(topic, client_id)
    
    def run_balancing_cycle(self):
        """Run load balancing cycle periodically"""
        # Wait for initial registrations
        time.sleep(15)
        
        while True:
            time.sleep(10)  # Balance every 10 seconds
            
            try:
                with self.lock:
                    allocation = self.load_balancer.detect_and_balance()
                    
                    if allocation:
                        logger.info(f"Load balancing complete. Allocation: {allocation}")
                        
                        # Migrate clients based on new allocation
                        for topic, broker_idx in allocation.items():
                            # Find clients using this topic
                            for client_id, client_info in self.clients.items():
                                if topic in client_info['topics']:
                                    # Get target broker info
                                    target_broker = self.brokers[broker_idx]
                                    
                                    # Check if client needs migration
                                    current_broker_host = client_info.get('broker_host')
                                    if current_broker_host != target_broker.host:
                                        logger.info(f"Migrating {client_id} to {target_broker.host}")
                                        self.send_migration_command(
                                            client_id,
                                            target_broker.host,
                                            target_broker.port
                                        )
                                        # Update client's broker assignment
                                        client_info['broker_host'] = target_broker.host
                                        client_info['broker_port'] = target_broker.port
                    
            except Exception as e:
                logger.error(f"Error in balancing cycle: {e}")
                import traceback
                traceback.print_exc()


def main():
    """Main entry point"""
    logger.info("Starting Coordination Service...")
    
    coordinator = CoordinationService()
    
    # Start balancing thread
    balance_thread = threading.Thread(target=coordinator.run_balancing_cycle, daemon=True)
    balance_thread.start()
    
    # Keep service running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down coordination service...")


if __name__ == "__main__":
    main()
