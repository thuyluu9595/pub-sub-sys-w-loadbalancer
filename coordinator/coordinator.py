#!/usr/bin/env python3
"""
Enhanced Coordination Service with Overhead Tracking
Integrates overhead measurement as described in paper Section III-D6
"""

import sys

sys.path.append('/app')

import numpy as np
import paho.mqtt.client as mqtt
import json
import time
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Set
from scipy.special import erf
import logging
import os

# Import overhead tracker
try:
    from overhead_tracker import OverheadTracker, calculate_message_size, estimate_trie_update_overhead
except ImportError:
    logging.warning("overhead_tracker not found, overhead tracking disabled")
    OverheadTracker = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BrokerInfo:
    """Information about a broker"""
    host: str
    port: int
    capacity_mbps: float
    data_rate: float
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
    """Hot Topic Detection using LoOP"""

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
        """Compute PLOF"""
        if len(request_rates) < 2:
            return {topic: 0.0 for topic in request_rates}

        plof_scores = {}

        for topic, rate in request_rates.items():
            distances = [(other_topic, abs(rate - other_rate))
                         for other_topic, other_rate in request_rates.items()
                         if other_topic != topic]
            distances.sort(key=lambda x: x[1])
            context = [request_rates[t] for t, _ in distances[:self.k_neighbors]]

            pdist_k = self.compute_pdist(rate, context)
            expected_pdist = np.mean([self.compute_pdist(c, context) for c in context])

            if expected_pdist > 0:
                plof = (pdist_k / expected_pdist) - 1
            else:
                plof = 0.0

            plof_scores[topic] = max(0.0, plof)

        return plof_scores

    def compute_loop(self, request_rates: Dict[str, float], threshold: float = 0.8) -> Set[str]:
        """Compute LoOP"""
        plof_scores = self.compute_plof(request_rates)

        if not plof_scores:
            return set()

        plof_values = list(plof_scores.values())
        nplof = self.lambda_param * np.sqrt(np.mean([p ** 2 for p in plof_values]))

        loop_scores = {}
        hot_topics = set()

        for topic, plof in plof_scores.items():
            if nplof > 0:
                loop_score = max(0, erf(plof / (nplof * np.sqrt(2))))
            else:
                loop_score = 0.0

            loop_scores[topic] = loop_score

            if loop_score >= threshold:
                hot_topics.add(topic)

        logger.info(f"LoOP Scores: {loop_scores}")
        logger.info(f"Hot Topics detected: {hot_topics}")

        return hot_topics


class LoadBalancer:
    """Topic-Aware Load Balancing with Overhead Tracking"""

    def __init__(self, brokers: List[BrokerInfo], overhead_tracker=None):
        self.brokers = brokers
        self.trie = TopicTrie()
        self.hot_detector = HotTopicDetector()
        self.topic_stats = defaultdict(lambda: {'rate': 0.0, 'subscribers': 0})
        self.client_broker_map = {}
        self.overhead_tracker = overhead_tracker

    def calculate_optimal_utilization(self, total_arrival_rate: float) -> Dict[int, float]:
        """Calculate optimal Q*"""
        total_service_rate = sum(b.data_rate for b in self.brokers)
        num_brokers = len(self.brokers)

        optimal_util = {}
        for i, broker in enumerate(self.brokers):
            Q_optimal = 1 - (total_service_rate - total_arrival_rate) / (num_brokers * broker.data_rate)
            optimal_util[i] = max(0.0, min(0.99, Q_optimal))

        logger.info(f"Optimal Utilizations: {optimal_util}")
        return optimal_util

    def calculate_cost_function(self, broker_idx: int, current_util: float, optimal_util: float) -> float:
        """Calculate cost function ν_i"""
        return abs(optimal_util - current_util)

    def allocation_matrix(self, hot_topics: Set[str], optimal_utils: Dict[int, float]) -> Dict[str, int]:
        """Algorithm 1: Topic-Aware Load Balancing"""
        allocation = {}

        sorted_topics = sorted(hot_topics,
                               key=lambda t: self.topic_stats[t]['rate'],
                               reverse=True)

        logger.info(f"Allocating {len(sorted_topics)} hot topics to brokers")

        for topic in sorted_topics:
            topic_rate = self.topic_stats[topic]['rate'] * max(1, self.topic_stats[topic]['subscribers'])
            best_broker = None
            best_cost = float('inf')

            for i, broker in enumerate(self.brokers):
                current_load = sum(
                    self.topic_stats[t]['rate'] * max(1, self.topic_stats[t]['subscribers'])
                    for t in broker.topics
                )

                if current_load + topic_rate < broker.data_rate:
                    new_util = (current_load + topic_rate) / broker.data_rate
                    cost = self.calculate_cost_function(i, new_util, optimal_utils[i])

                    if cost < best_cost:
                        best_cost = cost
                        best_broker = i

            if best_broker is not None:
                allocation[topic] = best_broker
                self.brokers[best_broker].topics.add(topic)
                logger.info(f"Allocated topic '{topic}' (rate={topic_rate:.2f}) to broker {best_broker}")

                # Track mapping overhead (Ω₂)
                if self.overhead_tracker:
                    mapping_msg = {
                        'topic': topic,
                        'broker': best_broker,
                        'rate': topic_rate
                    }
                    msg_size = calculate_message_size(mapping_msg)
                    self.overhead_tracker.track_mapping(f"system", topic, msg_size)
            else:
                least_loaded = min(range(len(self.brokers)),
                                   key=lambda i: len(self.brokers[i].topics))
                allocation[topic] = least_loaded
                self.brokers[least_loaded].topics.add(topic)
                logger.warning(f"Fallback allocation: topic '{topic}' to broker {least_loaded}")

        return allocation

    def update_topic_stats(self, topic: str, rate: float, num_subscribers: int):
        """Update topic statistics"""
        self.topic_stats[topic]['rate'] = rate
        self.topic_stats[topic]['subscribers'] = num_subscribers
        self.trie.update_stats(topic, rate)

    def detect_and_balance(self):
        """Main load balancing routine with overhead tracking"""
        request_rates = {}
        for topic, stats in self.topic_stats.items():
            load = stats.get('rate', 0.0) * stats.get('subscribers', 0)
            if load > 0:
                request_rates[topic] = load

        if not request_rates:
            logger.info("No topics to balance")
            return {}

        # Start tracking migration cycle
        if self.overhead_tracker:
            self.overhead_tracker.start_migration_cycle()

        hot_topics = self.hot_detector.compute_loop(request_rates)

        if not hot_topics:
            logger.info("No hot topics detected")
            if self.overhead_tracker:
                self.overhead_tracker.end_migration_cycle(0)
            return {}

        total_rate = sum(request_rates.values())
        optimal_utils = self.calculate_optimal_utilization(total_rate)

        for b in self.brokers:
            b.topics.clear()

        allocation = self.allocation_matrix(hot_topics, optimal_utils)

        # Update broker utilizations and performance metrics
        utilizations = []
        for i, broker in enumerate(self.brokers):
            load = sum(
                self.topic_stats[t]['rate'] * max(1, self.topic_stats[t]['subscribers'])
                for t in broker.topics
            )
            broker.utilization = load / broker.data_rate if broker.data_rate > 0 else 0.0
            utilizations.append(broker.utilization)
            logger.info(f"Broker {i} utilization: {broker.utilization:.2%}")

        # Calculate load variance
        variance = np.var(utilizations) if len(utilizations) > 1 else 0.0

        # Update performance metrics
        if self.overhead_tracker:
            self.overhead_tracker.update_performance_metrics(
                utilizations=utilizations,
                variance=variance
            )
            self.overhead_tracker.end_migration_cycle(len(allocation))

        return allocation


class CoordinationService:
    """Enhanced Coordination Service with Overhead Tracking"""

    def __init__(self):
        self.brokers = self._init_brokers()

        # Initialize overhead tracker
        self.overhead_tracker = None
        if OverheadTracker:
            self.overhead_tracker = OverheadTracker(
                num_brokers=len(self.brokers),
                avg_topic_size_kb=550
            )
            logger.info("Overhead tracking ENABLED")

        self.load_balancer = LoadBalancer(self.brokers, self.overhead_tracker)
        self.clients = {}
        self.lock = threading.Lock()

        self.broker_clients = {}
        self._connect_to_brokers()

        self.coord_client = mqtt.Client("coordinator_main")
        self.ctrl_host = os.getenv("CTRL_HOST", 'broker1')
        self.ctrl_port = int(os.getenv("CTRL_PORT", '1883'))
        self.coord_client.on_connect = self._on_coord_connect
        self.coord_client.on_message = self._on_coord_message
        self._connect_coordination_broker()

        self._last_ack_ts = {}
        self._cmd_sent_ts = {}

        # Start periodic overhead reporting
        if self.overhead_tracker:
            self._start_overhead_reporting()

    def _init_brokers(self) -> List[BrokerInfo]:
        """Initialize broker information"""
        broker_configs = [
            ("broker2", 1883, 450),
            ("broker3", 1883, 550),
            ("broker4", 1883, 600),
        ]

        brokers = []
        for host, port, capacity in broker_configs:
            data_rate = capacity * 1024 / (550 * 8)
            brokers.append(BrokerInfo(host, port, capacity, data_rate))

        return brokers

    def _on_broker_connect(self, client, userdata, flags, rc, broker_idx):
        """Callback when connected to a broker"""
        logger.info(f"Coordinator connected to broker {broker_idx} with result code {rc}")

    def _on_broker_message(self, client, userdata, msg):
        """Handle messages from brokers"""
        pass

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
        """Connect to control broker for coordination"""
        try:
            self.coord_client.connect(self.ctrl_host, self.ctrl_port, 60)
            self.coord_client.loop_start()
            logger.info(f"Coordination client connected to {self.ctrl_host}")
        except Exception as e:
            logger.error(f"Failed to connect coordination client: {e}")

    def _on_coord_connect(self, client, userdata, flags, rc):
        """Callback when coordination client connects"""
        logger.info(f"Coordinator connected with result code {rc}")
        client.subscribe("coordinator/register/+")
        client.subscribe("coordinator/stats/+")
        client.subscribe("coordinator/ack")
        logger.info("Subscribed to coordinator topics")

    def _on_coord_message(self, client, userdata, msg):
        """Handle coordination messages with overhead tracking"""
        try:
            topic_parts = msg.topic.split('/')
            msg_size = len(msg.payload)

            if topic_parts[1] == 'register':
                payload = json.loads(msg.payload.decode())
                client_id = payload['client_id']
                client_type = payload['type']
                topics = payload['topics']
                broker_host = payload.get('data_broker_host')
                broker_port = payload.get('data_broker_port')

                # Track registration overhead (Ω₁)
                if self.overhead_tracker:
                    self.overhead_tracker.track_registration(client_id, len(topics), msg_size)

                if client_type == 'publisher':
                    rates = payload.get('rates', {})
                    self.register_publisher(client_id, topics, rates, broker_host, broker_port)
                    logger.info(f"Registered publisher {client_id} with {len(topics)} topics")
                elif client_type == 'subscriber':
                    self.register_subscriber(client_id, topics, broker_host, broker_port)
                    logger.info(f"Registered subscriber {client_id} with {len(topics)} topics")

            elif topic_parts[1] == 'stats':
                payload = json.loads(msg.payload.decode())
                topic = payload['topic']
                rate = payload.get('rate', 0.0)

                # Track stats overhead
                if self.overhead_tracker:
                    self.overhead_tracker.track_stats_message(topic, msg_size)

                with self.lock:
                    current_subs = self.load_balancer.topic_stats[topic].get('subscribers', 0)
                    self.load_balancer.update_topic_stats(topic, rate, current_subs)

                    # Update performance metrics
                    if self.overhead_tracker:
                        self.overhead_tracker.update_performance_metrics(messages_delivered=1)

            elif topic_parts[1] == 'ack':
                ack = json.loads(msg.payload.decode())
                client_id = ack.get('client_id')

                # Track ACK overhead
                if self.overhead_tracker and client_id:
                    self.overhead_tracker.track_ack_message(client_id, msg_size)

                if not client_id:
                    return

                info = self.clients.get(client_id)
                if info is not None:
                    info['broker_host'] = ack.get('new_broker_host', info.get('broker_host'))
                    if 'new_broker_port' in ack:
                        try:
                            info['broker_port'] = int(ack['new_broker_port'])
                        except Exception:
                            info['broker_port'] = ack['new_broker_port']

                self._last_ack_ts[client_id] = time.monotonic()

        except Exception as e:
            logger.error(f"Error processing coordination message: {e}")
            import traceback
            traceback.print_exc()

    def _wait_for_acks(self, client_ids, timeout=10.0):
        """Wait for ACKs from clients"""
        deadline = time.monotonic() + timeout
        pending = set(client_ids)

        while pending and time.monotonic() < deadline:
            done = []
            for cid in list(pending):
                sent_at = self._cmd_sent_ts.get(cid, 0.0)
                ack_at = self._last_ack_ts.get(cid, 0.0)
                if ack_at >= sent_at and sent_at > 0.0:
                    done.append(cid)
            for cid in done:
                pending.discard(cid)
            time.sleep(0.05)

        if pending:
            logger.warning(f"Timed out waiting for ACKs from: {sorted(pending)}")
        else:
            logger.info("All ACKs received.")
        return list(pending)

    def send_migration_command(self, client_id: str, new_broker_host: str, new_broker_port: int,
                               topic: str = None, from_broker: str = None):
        """Send migration command with overhead tracking"""
        try:
            command = {
                'client_id': client_id,
                'broker_host': new_broker_host,
                'broker_port': new_broker_port,
                'timestamp': time.time()
            }

            msg_size = calculate_message_size(command)
            self._cmd_sent_ts[client_id] = time.monotonic()

            self.coord_client.publish(
                f"coordinator/migrate/{client_id}",
                json.dumps(command),
                qos=1
            )

            # Track migration overhead (Ω₃)
            if self.overhead_tracker and topic:
                # Estimate Trie update overhead
                num_subs = self.load_balancer.topic_stats[topic].get('subscribers', 0)
                trie_overhead = estimate_trie_update_overhead(topic, num_subs)

                self.overhead_tracker.track_migration(
                    client_id, topic, from_broker or "unknown",
                    new_broker_host, msg_size, trie_overhead
                )

            logger.info(f"Sent migration command to {client_id}: {new_broker_host}:{new_broker_port}")

        except Exception as e:
            logger.error(f"Failed to send migration command: {e}")

    def register_publisher(self, client_id: str, topics: List[str], rates: Dict[str, float],
                           broker_host: str, broker_port: int):
        """Register a publisher"""
        with self.lock:
            self.clients[client_id] = {
                'type': 'publisher',
                'topics': topics,
                'broker_host': broker_host,
                'broker_port': broker_port
            }

            for topic in topics:
                rate = rates.get(topic, 1.0)
                self.load_balancer.update_topic_stats(topic, rate, 0)
                self.load_balancer.trie.insert(topic)

    def register_subscriber(self, client_id: str, topics: List[str],
                            broker_host: str, broker_port: int):
        """Register a subscriber"""
        with self.lock:
            self.clients[client_id] = {
                'type': 'subscriber',
                'topics': topics,
                'broker_host': broker_host,
                'broker_port': broker_port
            }

            for topic in topics:
                current_subs = self.load_balancer.topic_stats[topic]['subscribers']
                self.load_balancer.topic_stats[topic]['subscribers'] = current_subs + 1
                self.load_balancer.trie.insert(topic, client_id)

    def _start_overhead_reporting(self):
        """Start periodic overhead reporting"""

        def report_overhead():
            while True:
                time.sleep(30)  # Report every 30 seconds
                if self.overhead_tracker:
                    report = self.overhead_tracker.get_summary_report()
                    logger.info(report)

                    # Export to JSON
                    self.overhead_tracker.export_json('/app/overhead_data.json')

        thread = threading.Thread(target=report_overhead, daemon=True)
        thread.start()
        logger.info("Overhead reporting thread started")

    def run_balancing_cycle(self):
        """Run load balancing cycle with overhead tracking"""
        time.sleep(10)

        while True:
            time.sleep(2)

            try:
                with self.lock:
                    allocation = self.load_balancer.detect_and_balance()

                    if allocation:
                        logger.info(f"Load balancing complete. Allocation: {allocation}")

                        for topic, broker_idx in allocation.items():
                            target = self.brokers[broker_idx]

                            # Get current broker for clients
                            subs = [cid for cid, info in self.clients.items()
                                    if info['type'] == 'subscriber' and topic in info['topics']
                                    and info.get('broker_host') != target.host]
                            pubs = [cid for cid, info in self.clients.items()
                                    if info['type'] == 'publisher' and topic in info['topics']
                                    and info.get('broker_host') != target.host]

                            # Move subscribers first
                            for cid in subs:
                                from_broker = self.clients[cid].get('broker_host', 'unknown')
                                self.send_migration_command(cid, target.host, target.port,
                                                            topic, from_broker)
                            self._wait_for_acks(subs, timeout=8.0)

                            # Then move publishers
                            for cid in pubs:
                                from_broker = self.clients[cid].get('broker_host', 'unknown')
                                self.send_migration_command(cid, target.host, target.port,
                                                            topic, from_broker)
                            self._wait_for_acks(pubs, timeout=8.0)

            except Exception as e:
                logger.error(f"Error in balancing cycle: {e}")
                import traceback
                traceback.print_exc()


def main():
    """Main entry point"""
    logger.info("Starting Enhanced Coordination Service with Overhead Tracking...")

    coordinator = CoordinationService()

    balance_thread = threading.Thread(target=coordinator.run_balancing_cycle, daemon=True)
    balance_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down coordination service...")

        # Final overhead report
        if coordinator.overhead_tracker:
            final_report = coordinator.overhead_tracker.get_summary_report()
            logger.info("\nFINAL OVERHEAD REPORT:")
            logger.info(final_report)


if __name__ == "__main__":
    main()