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
import os
import csv

logging.basicConfig(level=logging.INFO)
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
    
    def compute_loop(self, request_rates: Dict[str, float], threshold: float = 0.4) -> Set[str]:
        """
        Compute Local Outlier Probability (LoOP)
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
            # topic_rate = self.topic_stats[topic]['rate']
            topic_rate = self.topic_stats[topic]['rate'] * max(1, self.topic_stats[topic]['subscribers'])
            best_broker = None
            best_cost = float('inf')
            
            # Find available broker with best cost function
            for i, broker in enumerate(self.brokers):
                current_load = sum(
                    self.topic_stats[t]['rate'] * max(1, self.topic_stats[t]['subscribers'])
                    for t in broker.topics
                )

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
        request_rates = {}
        for topic, stats in self.topic_stats.items():
            # This is the change: load = rate * subscribers
            # This better reflects the paper's model
            load = stats.get('rate', 0.0) * stats.get('subscribers', 0)
            if load > 0:
                request_rates[topic] = load
        
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

        # Reset per-cycle topic allocations to avoid accumulation
        for b in self.brokers:
            b.topics.clear()

        # Step 3: Run allocation algorithm
        allocation = self.allocation_matrix(hot_topics, optimal_utils)
        
        # Step 4: Update broker utilizations
        for i, broker in enumerate(self.brokers):
            # load = sum(self.topic_stats[t]['rate'] for t in broker.topics
            load = sum(
                self.topic_stats[t]['rate'] * max(1, self.topic_stats[t]['subscribers'])
                for t in broker.topics
            )
            broker.utilization = load / broker.data_rate if broker.data_rate > 0 else 0.0
            logger.info(f"Broker {i} utilization: {broker.utilization:.2%}")
        
        return allocation


class OverheadMonitor:
    """
    Tracks control-plane overhead in bytes/messages by category and per topic.
    Categories we track:
      - IN:  'register_in', 'stats_in', 'ack_in', 'other_in'
      - OUT: 'migrate_out', 'other_out'
    Per-topic we attribute:
      - stats_in (incoming stats)
      - migrate_out (outgoing migration cmds), if caller provides ctx_topic
    """
    def __init__(self, window_s: float = 10.0):
        self.window_s = max(1.0, float(window_s))
        self.lock = threading.Lock()
        self._reset_all()

    def _reset_all(self):
        self.cum_in = {"bytes": 0, "msgs": 0}
        self.cum_out = {"bytes": 0, "msgs": 0}
        self.cum_by_cat_in = defaultdict(lambda: {"bytes": 0, "msgs": 0})
        self.cum_by_cat_out = defaultdict(lambda: {"bytes": 0, "msgs": 0})
        self.cum_per_topic = defaultdict(lambda: {
            "stats_in_bytes": 0, "stats_in_msgs": 0,
            "migrate_out_bytes": 0, "migrate_out_msgs": 0
        })

        self.win_start = time.time()
        self.win_in = {"bytes": 0, "msgs": 0}
        self.win_out = {"bytes": 0, "msgs": 0}
        self.win_by_cat_in = defaultdict(lambda: {"bytes": 0, "msgs": 0})
        self.win_by_cat_out = defaultdict(lambda: {"bytes": 0, "msgs": 0})
        self.win_per_topic = defaultdict(lambda: {
            "stats_in_bytes": 0, "stats_in_msgs": 0,
            "migrate_out_bytes": 0, "migrate_out_msgs": 0
        })

    def _add(self, bucket: dict, key: str, byte_count: int, msgs: int = 1):
        e = bucket[key]
        e["bytes"] += int(byte_count)
        e["msgs"] += int(msgs)

    def record_in(self, category: str, topic_str: str, payload_len: int, *, per_topic: str | None = None):
        """Count incoming message. We include topic bytes + payload bytes."""
        byte_count = int(payload_len) + len(topic_str or "")
        with self.lock:
            self.cum_in["bytes"] += byte_count; self.cum_in["msgs"] += 1
            self.win_in["bytes"] += byte_count; self.win_in["msgs"] += 1
            self._add(self.cum_by_cat_in, category, byte_count)
            self._add(self.win_by_cat_in, category, byte_count)
            if per_topic and category == "stats_in":
                t = self.cum_per_topic[per_topic]; t["stats_in_bytes"] += byte_count; t["stats_in_msgs"] += 1
                w = self.win_per_topic[per_topic]; w["stats_in_bytes"] += byte_count; w["stats_in_msgs"] += 1

    def record_out(self, category: str, topic_str: str, payload_len: int, *, per_topic: str | None = None):
        """Count outgoing message. We include topic bytes + payload bytes."""
        # Note: caller should avoid counting internal metrics publications to prevent feedback.
        byte_count = int(payload_len) + len(topic_str or "")
        with self.lock:
            self.cum_out["bytes"] += byte_count; self.cum_out["msgs"] += 1
            self.win_out["bytes"] += byte_count; self.win_out["msgs"] += 1
            self._add(self.cum_by_cat_out, category, byte_count)
            self._add(self.win_by_cat_out, category, byte_count)
            if per_topic and category == "migrate_out":
                t = self.cum_per_topic[per_topic]; t["migrate_out_bytes"] += byte_count; t["migrate_out_msgs"] += 1
                w = self.win_per_topic[per_topic]; w["migrate_out_bytes"] += byte_count; w["migrate_out_msgs"] += 1

    def snapshot_and_reset_window(self) -> dict:
        """Return a snapshot for the last window and reset window counters."""
        with self.lock:
            now = time.time()
            elapsed = max(1e-6, now - self.win_start)
            snap = {
                "window_seconds": elapsed,
                "inbound": dict(self.win_in),
                "outbound": dict(self.win_out),
                "by_category_in": {k: dict(v) for k, v in self.win_by_cat_in.items()},
                "by_category_out": {k: dict(v) for k, v in self.win_by_cat_out.items()},
                "per_topic": {k: dict(v) for k, v in self.win_per_topic.items()},
                "cumulative": {
                    "inbound": dict(self.cum_in),
                    "outbound": dict(self.cum_out),
                }
            }
            # reset window
            self.win_start = now
            self.win_in = {"bytes": 0, "msgs": 0}
            self.win_out = {"bytes": 0, "msgs": 0}
            self.win_by_cat_in = defaultdict(lambda: {"bytes": 0, "msgs": 0})
            self.win_by_cat_out = defaultdict(lambda: {"bytes": 0, "msgs": 0})
            self.win_per_topic = defaultdict(lambda: {
                "stats_in_bytes": 0, "stats_in_msgs": 0,
                "migrate_out_bytes": 0, "migrate_out_msgs": 0
            })
            return snap


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
        self.ctrl_host = os.getenv("CTRL_HOST", 'broker1')
        self.ctrl_port = int(os.getenv("CTRL_PORT", '1883'))
        self.coord_client.on_connect = self._on_coord_connect
        self.coord_client.on_message = self._on_coord_message
        self._connect_coordination_broker()

        self._last_ack_ts = {}  # cid -> monotonic() time of last ACK seen
        self._cmd_sent_ts = {}  # cid -> monotonic() time of last migrate command sent

        # --- Overhead monitor ---
        window_s = float(os.getenv("OVERHEAD_WINDOW_S", "5"))
        self._publish_metrics = os.getenv("OVERHEAD_PUBLISH", "1") == "1"
        self.overhead = OverheadMonitor(window_s=window_s)

        # CSV config
        self._csv_path = os.getenv("OVERHEAD_CSV_PATH", "/app/overhead_metrics.csv")
        # fixed schema so you can analyze easily later
        self._csv_fields = [
            "ts_iso", "window_seconds",
            "in_bytes", "in_msgs", "out_bytes", "out_msgs",
            "register_in_bytes", "stats_in_bytes", "ack_in_bytes", "other_in_bytes",
            "migrate_out_bytes", "other_out_bytes",
            # paper-style buckets for convenience:
            "init_bytes", "mapping_bytes", "reassignment_bytes"
        ]
        self._ensure_csv_header()

        # Start reporter thread
        self._oh_thread = threading.Thread(target=self._overhead_reporter, daemon=True)
        self._oh_thread.start()

    def _overhead_reporter(self):
        """Periodically log and (optionally) publish control-plane overhead snapshots."""
        topic_metrics = "coordinator/metrics/overhead"
        while True:
            time.sleep(self.overhead.window_s)
            snap = self.overhead.snapshot_and_reset_window()

            # Pretty log
            in_b = snap["inbound"]["bytes"];
            out_b = snap["outbound"]["bytes"]
            in_m = snap["inbound"]["msgs"];
            out_m = snap["outbound"]["msgs"]
            win = snap["window_seconds"]
            in_rate = in_b / win if win > 0 else 0.0
            out_rate = out_b / win if win > 0 else 0.0

            logger.info(
                "[OVERHEAD] window=%.2fs  IN: %d bytes (%d msgs, %.1f B/s)  "
                "OUT: %d bytes (%d msgs, %.1f B/s)  cats_in=%s  cats_out=%s",
                win, in_b, in_m, in_rate, out_b, out_m, out_rate,
                {k: v["bytes"] for k, v in snap["by_category_in"].items()},
                {k: v["bytes"] for k, v in snap["by_category_out"].items()},
            )

            # Optionally publish a JSON snapshot (excluded from overhead counting)
            if self._publish_metrics:
                try:
                    self.coord_client.publish(topic_metrics, json.dumps(snap), qos=0)
                except Exception as e:
                    logger.warning(f"Failed to publish overhead metrics: {e}")

    def _init_brokers(self) -> List[BrokerInfo]:
        """Initialize broker information"""
        broker_configs = [
            # ("broker1", 1883, 350),
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

    def _on_broker_connect(self, client, userdata, flags, rc, broker_idx):
        """Callback when connected to a broker"""
        logger.info(f"Coordinator connected to broker {broker_idx} with result code {rc}")
        # Subscribe to stats topic
        client.subscribe(f"$SYS/broker{broker_idx}/stats/#")

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
        """Connect to broker1 for coordination messages"""
        try:
            self.coord_client.connect(self.ctrl_host, self.ctrl_port, 60)
            self.coord_client.loop_start()
            logger.info(f"Coordination client connected to control broker {self.ctrl_host}")
        except Exception as e:
            logger.error(f"Failed to connect coordination client: {e}")
    
    def _on_coord_connect(self, client, userdata, flags, rc):
        """Callback when coordination client connects"""
        logger.info(f"Coordinator connected with result code {rc}")
        # Subscribe to registration and statistics topics
        client.subscribe("coordinator/register/+")
        client.subscribe("coordinator/stats/#")
        client.subscribe("coordinator/ack")
        logger.info("Subscribed to coordinator topics")

    def _on_coord_message(self, client, userdata, msg):
        """Handle coordination messages from clients + record control-plane overhead."""
        try:
            topic_parts = msg.topic.split('/')

            # Do not count metrics we publish ourselves (avoid feedback loops)
            if msg.topic.startswith("coordinator/metrics/"):
                return

            if topic_parts[1] == 'register':
                # INCOMING control-plane: registration
                self.overhead.record_in('register_in', msg.topic, len(msg.payload))

                payload = json.loads(msg.payload.decode())
                client_id = payload['client_id']
                client_type = payload['type']
                topics = payload['topics']
                broker_host = payload.get('data_broker_host')
                broker_port = payload.get('data_broker_port')

                if client_type == 'publisher':
                    rates = payload.get('rates', {})
                    self.register_publisher(client_id, topics, rates, broker_host, broker_port)
                    logger.info(f"Registered publisher {client_id} with {len(topics)} topics")
                elif client_type == 'subscriber':
                    self.register_subscriber(client_id, topics, broker_host, broker_port)
                    logger.info(f"Registered subscriber {client_id} with {len(topics)} topics")

            elif topic_parts[1] == 'stats':
                # INCOMING control-plane: stats (attribute per-topic)
                full_topic = "/".join(topic_parts[2:]) if len(topic_parts) > 2 else None
                self.overhead.record_in('stats_in', msg.topic, len(msg.payload), per_topic = full_topic)

                payload = json.loads(msg.payload.decode())
                topic = payload.get('topic', full_topic)
                rate = payload.get('rate', 0.0)
                with self.lock:
                    current_subs = self.load_balancer.topic_stats[topic].get('subscribers', 0)
                    self.load_balancer.update_topic_stats(topic, rate, current_subs)

            elif topic_parts[1] == 'ack':
                # INCOMING control-plane: ack
                self.overhead.record_in('ack_in', msg.topic, len(msg.payload))

                ack = json.loads(msg.payload.decode())
                client_id = ack.get('client_id')
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

                # mark that we saw an ACK now
                self._last_ack_ts[client_id] = time.monotonic()

            else:
                # INCOMING control-plane: other
                self.overhead.record_in('other_in', msg.topic, len(msg.payload))

        except Exception as e:
            logger.error(f"Error processing coordination message: {e}")

    def _wait_for_acks(self, client_ids, timeout=10.0):
        """Wait until each client in client_ids has ACKed AFTER its last command was sent,
        or until timeout elapses. Returns list of missing cids (empty if all good)."""
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
            time.sleep(0.05)  # tiny poll interval

        if pending:
            logger.warning(f"Timed out waiting for ACKs from: {sorted(pending)}")
        else:
            logger.info("All ACKs received.")
        return list(pending)

    def _ensure_csv_header(self):
        """Create CSV file with header if it doesn't exist or is empty."""
        try:
            need_header = True
            if os.path.exists(self._csv_path):
                need_header = os.path.getsize(self._csv_path) == 0
            else:
                # ensure directory exists
                os.makedirs(os.path.dirname(self._csv_path), exist_ok=True)

            if need_header:
                with open(self._csv_path, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(self._csv_fields)
                logger.info(f"Created overhead CSV with header at {self._csv_path}")
        except Exception as e:
            logger.warning(f"Could not prepare CSV header at {self._csv_path}: {e}")

    # def _overhead_reporter(self):
    #     """Periodically log and (optionally) publish control-plane overhead snapshots."""
    #     topic_metrics = "coordinator/metrics/overhead"
    #     while True:
    #         time.sleep(self.overhead.window_s)
    #         snap = self.overhead.snapshot_and_reset_window()
    #
    #         # Pretty log
    #         in_b = snap["inbound"]["bytes"]
    #         out_b = snap["outbound"]["bytes"]
    #         in_m = snap["inbound"]["msgs"]
    #         out_m = snap["outbound"]["msgs"]
    #         win = snap["window_seconds"]
    #         in_rate = in_b / win if win > 0 else 0.0
    #         out_rate = out_b / win if win > 0 else 0.0
    #
    #         logger.info(
    #             "[OVERHEAD] window=%.2fs  IN: %d bytes (%d msgs, %.1f B/s)  "
    #             "OUT: %d bytes (%d msgs, %.1f B/s)  cats_in=%s  cats_out=%s",
    #             win, in_b, in_m, in_rate, out_b, out_m, out_rate,
    #             {k: v["bytes"] for k, v in snap["by_category_in"].items()},
    #             {k: v["bytes"] for k, v in snap["by_category_out"].items()},
    #         )
    #
    #         # Optionally publish a JSON snapshot (excluded from overhead counting)
    #         if self._publish_metrics:
    #             try:
    #                 self.coord_client.publish(topic_metrics, json.dumps(snap), qos=0)
    #             except Exception as e:
    #                 logger.warning(f"Failed to publish overhead metrics: {e}")
    def _overhead_reporter(self):
        """Every window (default 5s), log + append one CSV row with overhead metrics."""
        topic_metrics = "coordinator/metrics/overhead"
        while True:
            time.sleep(self.overhead.window_s)
            snap = self.overhead.snapshot_and_reset_window()

            # Basic aggregates
            in_b = snap["inbound"]["bytes"];
            out_b = snap["outbound"]["bytes"]
            in_m = snap["inbound"]["msgs"];
            out_m = snap["outbound"]["msgs"]
            win = snap["window_seconds"]
            cats_in = snap["by_category_in"]
            cats_out = snap["by_category_out"]

            # Category bytes (missing => 0)
            def bcat(d, k):
                return d.get(k, {"bytes": 0})["bytes"]

            reg_b = bcat(cats_in, "register_in")
            sts_b = bcat(cats_in, "stats_in")
            ack_b = bcat(cats_in, "ack_in")
            oth_in = bcat(cats_in, "other_in")
            mig_b = bcat(cats_out, "migrate_out")
            oth_out = bcat(cats_out, "other_out")

            # Paper buckets (derived):
            init_bytes = reg_b  # you can add first-window stats if you want
            mapping_bytes = mig_b + ack_b
            reassignment_bytes = mapping_bytes  # in this implementation, reassignment is migrate+ack

            # Log summary
            logger.info(
                "[OVERHEAD] window=%.2fs  IN: %d bytes (%d msgs)  OUT: %d bytes (%d msgs) "
                "cats_in=%s  cats_out=%s",
                win, in_b, in_m, out_b, out_m,
                {k: v['bytes'] for k, v in cats_in.items()},
                {k: v['bytes'] for k, v in cats_out.items()},
            )
            logger.info("[OVERHEAD.breakdown] init=%dB map=%dB reassign=%dB",
                        init_bytes, mapping_bytes, reassignment_bytes)

            # Optional JSON publish (not counted by the monitor)
            if self._publish_metrics:
                try:
                    self.coord_client.publish(topic_metrics, json.dumps(snap), qos=0)
                except Exception as e:
                    logger.warning(f"Failed to publish overhead metrics: {e}")

            # Append one CSV row
            try:
                with open(self._csv_path, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([
                        time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),  # ts_iso (UTC)
                        f"{win:.2f}",
                        in_b, in_m, out_b, out_m,
                        reg_b, sts_b, ack_b, oth_in,
                        mig_b, oth_out,
                        init_bytes, mapping_bytes, reassignment_bytes
                    ])
            except Exception as e:
                logger.warning(f"Failed to write CSV row to {self._csv_path}: {e}")

    def send_migration_command(self, client_id: str, new_broker_host: str, new_broker_port: int,
                               ctx_topic: str | None = None):
        """Send migration command to a client and record OUT overhead (attribute to ctx_topic if provided)."""
        try:
            command = {
                'client_id': client_id,
                'broker_host': new_broker_host,
                'broker_port': new_broker_port,
                'timestamp': time.time()
            }

            # record when we told this client to move
            self._cmd_sent_ts[client_id] = time.monotonic()

            topic = f"coordinator/migrate/{client_id}"
            payload = json.dumps(command)

            # OUTGOING control-plane: migration
            self.overhead.record_out('migrate_out', topic, len(payload), per_topic=ctx_topic)

            self.coord_client.publish(topic, payload, qos=1)
            logger.info(f"Sent migration command to {client_id}: {new_broker_host}:{new_broker_port}")

        except Exception as e:
            logger.error(f"Failed to send migration command: {e}")

    def register_publisher(self, client_id: str, topics: List[str], rates: Dict[str, float], broker_host: str, broker_port: int):
        """Register a publisher with its topics and rates"""
        with self.lock:
            self.clients[client_id] = {'type': 'publisher', 'topics': topics, 'broker_host': broker_host, 'broker_port': broker_port}
            
            for topic in topics:
                rate = rates.get(topic, 1.0)
                self.load_balancer.update_topic_stats(topic, rate, 0)
                self.load_balancer.trie.insert(topic)
    
    def register_subscriber(self, client_id: str, topics: List[str], broker_host: str, broker_port: int):
        """Register a subscriber with its topic subscriptions"""
        with self.lock:
            self.clients[client_id] = {'type': 'subscriber', 'topics': topics, 'broker_host': broker_host, 'broker_port': broker_port}
            
            for topic in topics:
                current_subs = self.load_balancer.topic_stats[topic]['subscribers']
                self.load_balancer.topic_stats[topic]['subscribers'] = current_subs + 1
                self.load_balancer.trie.insert(topic, client_id)

    def run_balancing_cycle(self):
        """Run load balancing cycle periodically"""
        # Wait for initial registrations
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
                            subs = [cid for cid, info in self.clients.items()
                                    if info['type'] == 'subscriber' and topic in info['topics']
                                    and info.get('broker_host') != target.host]
                            pubs = [cid for cid, info in self.clients.items()
                                    if info['type'] == 'publisher' and topic in info['topics']
                                    and info.get('broker_host') != target.host]

                            # (1) move subscribers, wait for ACKs
                            for cid in subs:
                                self.send_migration_command(cid, target.host, target.port, ctx_topic=topic)
                            self._wait_for_acks(subs, timeout=8.0)

                            # (2) then move publishers
                            for cid in pubs:
                                self.send_migration_command(cid, target.host, target.port, ctx_topic=topic)
                            self._wait_for_acks(pubs, timeout=8.0)

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
