#!/usr/bin/env python3
"""
Overhead Tracker Module - FIXED VERSION (no deadlock)
Implements overhead measurement as described in Section III-D6 of the paper
"""

import time
import json
import threading
from dataclasses import dataclass, field
from typing import Dict, List
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class MessageOverhead:
    """Track overhead for a single message type"""
    count: int = 0
    total_bytes: int = 0
    total_latency: float = 0.0
    timestamps: List[float] = field(default_factory=list)

    def add_message(self, size_bytes: int, latency: float = 0.0):
        """Record a message"""
        self.count += 1
        self.total_bytes += size_bytes
        self.total_latency += latency
        self.timestamps.append(time.time())

    @property
    def avg_size(self) -> float:
        """Average message size in bytes"""
        return self.total_bytes / self.count if self.count > 0 else 0.0

    @property
    def avg_latency(self) -> float:
        """Average latency in seconds"""
        return self.total_latency / self.count if self.count > 0 else 0.0

    @property
    def messages_per_second(self) -> float:
        """Calculate message rate"""
        if len(self.timestamps) < 2:
            return 0.0
        duration = self.timestamps[-1] - self.timestamps[0]
        return self.count / duration if duration > 0 else 0.0


class OverheadTracker:
    """
    Track all coordination overhead as per paper Section III-D6
    FIXED: Uses threading.RLock() for reentrant locking to prevent deadlocks
    """

    def __init__(self, num_brokers: int, avg_topic_size_kb: int = 550):
        self.num_brokers = num_brokers
        self.avg_topic_size_kb = avg_topic_size_kb

        # κ coefficient: topic_size × num_brokers
        self.kappa = avg_topic_size_kb * num_brokers

        # Track different overhead types
        self.registration_overhead = MessageOverhead()
        self.mapping_overhead = MessageOverhead()
        self.migration_overhead = MessageOverhead()
        self.stats_overhead = MessageOverhead()
        self.ack_overhead = MessageOverhead()

        # Track by topic
        self.topic_overheads: Dict[str, MessageOverhead] = defaultdict(MessageOverhead)

        # Track migration cycles
        self.migration_cycles = []
        self.current_cycle_start = None

        # Use RLock instead of Lock to allow reentrant locking (prevents deadlock)
        self.lock = threading.RLock()

        # Performance metrics
        self.performance_metrics = {
            'total_messages_delivered': 0,
            'total_latency': 0.0,
            'broker_utilizations': [],
            'load_imbalance_variance': []
        }

    def track_registration(self, client_id: str, num_topics: int, message_size: int):
        """Track registration overhead (Ω₁ = rₖ * yₖⁱ)"""
        with self.lock:
            self.registration_overhead.add_message(message_size)
            logger.debug(f"Registration overhead: {client_id}, {num_topics} topics, {message_size} bytes")

    def track_mapping(self, client_id: str, topic: str, message_size: int):
        """Track client-broker mapping overhead (Ω₂ = yₖⁱ)"""
        with self.lock:
            self.mapping_overhead.add_message(message_size)
            self.topic_overheads[topic].add_message(message_size)
            logger.debug(f"Mapping overhead: {client_id} -> {topic}, {message_size} bytes")

    def start_migration_cycle(self):
        """Mark the start of a migration cycle"""
        with self.lock:
            self.current_cycle_start = time.time()

    def track_migration(self, client_id: str, topic: str, from_broker: str,
                        to_broker: str, message_size: int, trie_update_size: int = 0):
        """Track topic re-assignment overhead (Ω₃ = ∇)"""
        with self.lock:
            total_size = message_size + trie_update_size
            self.migration_overhead.add_message(total_size)
            self.topic_overheads[topic].add_message(total_size)
            logger.debug(f"Migration overhead: {client_id} ({topic}): {from_broker} -> {to_broker}, {total_size} bytes")

    def end_migration_cycle(self, num_migrations: int):
        """Mark the end of a migration cycle"""
        with self.lock:
            if self.current_cycle_start:
                duration = time.time() - self.current_cycle_start
                self.migration_cycles.append({
                    'timestamp': self.current_cycle_start,
                    'duration': duration,
                    'num_migrations': num_migrations
                })
                self.current_cycle_start = None

    def track_stats_message(self, topic: str, message_size: int):
        """Track periodic statistics messages from publishers"""
        with self.lock:
            self.stats_overhead.add_message(message_size)
            logger.debug(f"Stats overhead: {topic}, {message_size} bytes")

    def track_ack_message(self, client_id: str, message_size: int):
        """Track ACK messages from clients after migration"""
        with self.lock:
            self.ack_overhead.add_message(message_size)
            logger.debug(f"ACK overhead: {client_id}, {message_size} bytes")

    def get_total_overhead_bytes(self) -> int:
        """Calculate total communication overhead"""
        with self.lock:
            omega_1 = self.registration_overhead.total_bytes
            omega_2 = self.mapping_overhead.total_bytes
            omega_3 = self.migration_overhead.total_bytes

            # Apply κ coefficient to coordination overhead
            coordination_overhead = self.kappa * (omega_1 + omega_2 + omega_3) / 1000

            # Add periodic overhead (not multiplied by κ)
            periodic_overhead = self.stats_overhead.total_bytes + self.ack_overhead.total_bytes

            return int(coordination_overhead + periodic_overhead)

    def get_overhead_breakdown(self) -> Dict:
        """Get detailed breakdown of overhead components"""
        with self.lock:
            total_overhead = self.get_total_overhead_bytes()

            return {
                'total_overhead_bytes': total_overhead,
                'total_overhead_mb': total_overhead / (1024 * 1024),
                'components': {
                    'registration': {
                        'count': self.registration_overhead.count,
                        'total_bytes': self.registration_overhead.total_bytes,
                        'avg_size_bytes': self.registration_overhead.avg_size,
                        'percentage': (self.registration_overhead.total_bytes / total_overhead * 100)
                        if total_overhead > 0 else 0
                    },
                    'mapping': {
                        'count': self.mapping_overhead.count,
                        'total_bytes': self.mapping_overhead.total_bytes,
                        'avg_size_bytes': self.mapping_overhead.avg_size,
                        'percentage': (self.mapping_overhead.total_bytes / total_overhead * 100)
                        if total_overhead > 0 else 0
                    },
                    'migration': {
                        'count': self.migration_overhead.count,
                        'total_bytes': self.migration_overhead.total_bytes,
                        'avg_size_bytes': self.migration_overhead.avg_size,
                        'percentage': (self.migration_overhead.total_bytes / total_overhead * 100)
                        if total_overhead > 0 else 0
                    },
                    'stats': {
                        'count': self.stats_overhead.count,
                        'total_bytes': self.stats_overhead.total_bytes,
                        'avg_size_bytes': self.stats_overhead.avg_size,
                        'percentage': (self.stats_overhead.total_bytes / total_overhead * 100)
                        if total_overhead > 0 else 0
                    },
                    'ack': {
                        'count': self.ack_overhead.count,
                        'total_bytes': self.ack_overhead.total_bytes,
                        'avg_size_bytes': self.ack_overhead.avg_size,
                        'percentage': (self.ack_overhead.total_bytes / total_overhead * 100)
                        if total_overhead > 0 else 0
                    }
                },
                'migration_cycles': len(self.migration_cycles),
                'avg_migrations_per_cycle': (
                    sum(c['num_migrations'] for c in self.migration_cycles) / len(self.migration_cycles)
                    if self.migration_cycles else 0
                ),
                'kappa_coefficient': self.kappa
            }

    def update_performance_metrics(self, messages_delivered: int = 0, latency: float = 0.0,
                                   utilizations: List[float] = None, variance: float = 0.0):
        """Track performance metrics to compare against overhead"""
        with self.lock:
            if messages_delivered > 0:
                self.performance_metrics['total_messages_delivered'] += messages_delivered
            if latency > 0:
                self.performance_metrics['total_latency'] += latency
            if utilizations:
                self.performance_metrics['broker_utilizations'].append(utilizations)
            if variance > 0:
                self.performance_metrics['load_imbalance_variance'].append(variance)

    def get_efficiency_ratio(self) -> float:
        """Calculate efficiency ratio: useful_work / overhead"""
        # Use RLock so we can call get_total_overhead_bytes() which also needs the lock
        with self.lock:
            overhead = self.get_total_overhead_bytes()
            useful_work = self.performance_metrics['total_messages_delivered']

            if overhead == 0:
                return float('inf')

            return useful_work / (overhead / 1024)  # Normalize overhead to KB

    def get_summary_report(self) -> str:
        """Generate a human-readable summary report"""
        breakdown = self.get_overhead_breakdown()
        efficiency = self.get_efficiency_ratio()

        report = []
        report.append("\n" + "=" * 70)
        report.append("COORDINATION OVERHEAD ANALYSIS")
        report.append("=" * 70)

        report.append(f"\nTotal Overhead: {breakdown['total_overhead_mb']:.2f} MB")
        report.append(f"κ Coefficient: {breakdown['kappa_coefficient']}")
        report.append(f"Migration Cycles: {breakdown['migration_cycles']}")
        report.append(f"Avg Migrations/Cycle: {breakdown['avg_migrations_per_cycle']:.1f}")

        report.append("\nOverhead Breakdown:")
        report.append("-" * 70)

        for component, data in breakdown['components'].items():
            report.append(f"\n{component.upper()}:")
            report.append(f"  Messages: {data['count']}")
            report.append(f"  Total: {data['total_bytes'] / 1024:.2f} KB")
            report.append(f"  Average: {data['avg_size_bytes']:.0f} bytes/msg")
            report.append(f"  Percentage: {data['percentage']:.1f}%")

        report.append("\nPerformance vs Overhead:")
        report.append("-" * 70)
        report.append(f"Messages Delivered: {self.performance_metrics['total_messages_delivered']}")
        report.append(f"Efficiency Ratio: {efficiency:.2f} (messages per KB overhead)")

        if self.performance_metrics['broker_utilizations']:
            avg_util = sum(sum(u) / len(u) for u in self.performance_metrics['broker_utilizations']) / \
                       len(self.performance_metrics['broker_utilizations'])
            report.append(f"Avg Broker Utilization: {avg_util:.2%}")

        if self.performance_metrics['load_imbalance_variance']:
            avg_variance = sum(self.performance_metrics['load_imbalance_variance']) / \
                           len(self.performance_metrics['load_imbalance_variance'])
            report.append(f"Avg Load Variance: {avg_variance:.4f}")

        report.append("=" * 70 + "\n")

        return "\n".join(report)

    def export_json(self, filepath: str):
        """
        Export overhead data to JSON for analysis
        FIXED: Create a copy of data while holding lock, then write without lock
        """
        # Get data snapshot while holding lock
        with self.lock:
            data = {
                'timestamp': time.time(),
                'breakdown': self.get_overhead_breakdown(),
                'efficiency_ratio': self.get_efficiency_ratio(),
                'performance_metrics': {
                    'total_messages_delivered': self.performance_metrics['total_messages_delivered'],
                    'total_latency': self.performance_metrics['total_latency'],
                    'broker_utilizations': self.performance_metrics['broker_utilizations'][-10:] if
                    self.performance_metrics['broker_utilizations'] else [],
                    'load_imbalance_variance': self.performance_metrics['load_imbalance_variance'][-10:] if
                    self.performance_metrics['load_imbalance_variance'] else []
                },
                'migration_cycles': self.migration_cycles[-10:] if self.migration_cycles else []
            }

        # Write to file WITHOUT holding the lock (this prevents deadlock)
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Overhead data exported to {filepath}")
        except Exception as e:
            logger.error(f"Failed to export overhead data: {e}")


def calculate_message_size(payload: dict) -> int:
    """Calculate the size of a JSON message in bytes"""
    return len(json.dumps(payload).encode('utf-8'))


def estimate_trie_update_overhead(topic: str, num_subscribers: int) -> int:
    """Estimate overhead for Trie data structure update"""
    topic_levels = len(topic.split('/'))
    base_overhead = topic_levels * 64
    subscriber_overhead = num_subscribers * 16
    return base_overhead + subscriber_overhead