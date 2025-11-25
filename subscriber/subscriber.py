#!/usr/bin/env python3
"""
Subscriber Application (plan-driven)
- Reads the same plan (CSV/JSON) used by the publishers
- Subscribes deterministically to those topics so "rate × subscribers" is non-zero,
  helping hot-topic detection trigger reliably (with lowered LoOP threshold)

Environment variables:
  CONTROL_HOST=broker1
  CONTROL_PORT=1883
  PLAN_FILE=/app/plan.csv          # path to CSV or JSON plan
  PLAN_FORMAT=csv                  # csv | json
  NUM_SUBSCRIBERS=20
  SUBSCRIBE_MODE=all               # all | targeted
  HOT_RATE_THRESHOLD=0.8           # infer "hot" topics if SUBSCRIBE_MODE=targeted
  MIN_SUBS_PER_HOT=6               # targeted mode: guaranteed subscribers for each hot topic
  MAX_TOPICS_PER_SUB=8             # targeted mode: cap per-subscriber topic list size

Notes:
- "all" mode: every subscriber subscribes to all plan topics (simplest, ensures
  subscribers > 0 for all topics; detection then depends on rate differences).
- "targeted" mode: infer hot topics by max rate across phases >= HOT_RATE_THRESHOLD,
  ensure at least MIN_SUBS_PER_HOT subscribers per hot topic; fill remaining slots
  with non-hot topics up to MAX_TOPICS_PER_SUB.
"""

import paho.mqtt.client as mqtt
import json
import time
import logging
from typing import List, Dict, Tuple
from collections import defaultdict
import threading
import os
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Phase:
    __slots__ = ("start_s", "end_s", "rate_mps", "payload_bytes")
    def __init__(self, start_s: float, duration_s: float, rate_mps: float, payload_kb: int):
        self.start_s = float(start_s)
        self.end_s = float(start_s) + float(duration_s)
        self.rate_mps = float(rate_mps)
        self.payload_bytes = int(payload_kb) * 1024

class Plan:
    def __init__(self):
        self.topic_phases: Dict[str, List[Phase]] = {}

    def load_csv(self, path: str):
        with open(path, "r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or row[0].strip().startswith("#"):
                    continue
                # topic,phase,start_s,duration_s,rate_mps,payload_kb
                if len(row) < 6:
                    raise ValueError(f"CSV row has <6 columns: {row}")
                topic = row[0].strip()
                start_s = float(row[2])
                duration_s = float(row[3])
                rate_mps = float(row[4])
                payload_kb = int(row[5])
                self.topic_phases.setdefault(topic, []).append(
                    Phase(start_s, duration_s, rate_mps, payload_kb)
                )
        for t, phases in self.topic_phases.items():
            phases.sort(key=lambda p: p.start_s)

    def load_json(self, path: str):
        with open(path, "r") as f:
            data = json.load(f)
        for ph in data.get("phases", []):
            topic = ph["topic"]
            start_s = float(ph["start_s"])
            duration_s = float(ph["duration_s"])
            rate_mps = float(ph["rate_mps"])
            payload_kb = int(ph["payload_kb"])
            self.topic_phases.setdefault(topic, []).append(
                Phase(start_s, duration_s, rate_mps, payload_kb)
            )
        for t, phases in self.topic_phases.items():
            phases.sort(key=lambda p: p.start_s)

    def topics(self) -> List[str]:
        return sorted(self.topic_phases.keys())

    def peak_rate(self, topic: str) -> float:
        phases = self.topic_phases.get(topic, [])
        return max((p.rate_mps for p in phases), default=0.0)


class Subscriber:
    """MQTT Subscriber that listens to multiple topics"""
    def __init__(self, broker_host: str, broker_port: int, client_id: str):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.client = mqtt.Client(client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.topics = []
        self.message_stats = defaultdict(lambda: {
            'count': 0,
            'total_latency': 0.0,
            'last_received': 0
        })
        self.lock = threading.Lock()
        self.migration_lock = threading.Lock()
        self.registered = False

        # Control broker
        self.control_host = os.getenv("CONTROL_HOST", "broker1")
        self.control_port = int(os.getenv("CONTROL_PORT", "1883"))
        self.ctrl_client = mqtt.Client(f"{client_id}-ctrl")
        self.ctrl_client.on_connect = self.on_ctrl_connect
        self.ctrl_client.on_message = self.on_message

    def on_ctrl_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Subscriber {self.client_id} connected to control broker")
            self.ctrl_client.subscribe(f"coordinator/migrate/{self.client_id}")
            if not self.registered and self.topics:
                self.register_with_coordinator()
        else:
            logger.error(f"Connection to control broker failed with code {rc}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Subscriber {self.client_id} connected to {self.broker_host}:{self.broker_port}")
            for topic in self.topics:
                self.client.subscribe(topic, qos=1)
        else:
            logger.error(f"Connection failed with code {rc}")

    def register_with_coordinator(self):
        try:
            self.ctrl_client.publish(
                "coordinator/register/subscriber",
                json.dumps({
                    'client_id': self.client_id,
                    'type': 'subscriber',
                    'topics': self.topics,
                    'data_broker_host': self.broker_host,
                    'data_broker_port': self.broker_port
                }),
                qos=1,
            )
            self.registered = True
            logger.info(f"Registered with coordinator: {len(self.topics)} topics")
        except Exception as e:
            logger.error(f"Failed to register with coordinator: {e}")

    def migrate_to_broker(self, new_host: str, new_port: int):
        with self.migration_lock:
            ack = {
                "client_id": self.client_id,
                "role": "subscriber",
                "new_broker_host": new_host,
                "new_broker_port": new_port,
                "t": time.time()
            }
            if new_host == self.broker_host and new_port == self.broker_port:
                self.ctrl_client.publish("coordinator/ack", json.dumps(ack), qos=1)
                logger.info("Already connected to target broker")
                return
            logger.info(f"Migrating from {self.broker_host}:{self.broker_port} to {new_host}:{new_port}")
            self.client.loop_stop(); self.client.disconnect()
            self.broker_host = new_host; self.broker_port = new_port
            self.client = mqtt.Client(self.client_id)
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            try:
                self.client.connect(new_host, new_port, 60)
                self.client.loop_start()
                logger.info(f"Successfully migrated client {self.client_id} to {new_host}:{new_port}")
                self.ctrl_client.publish("coordinator/ack", json.dumps(ack), qos=1)
            except Exception as e:
                logger.error(f"Failed to migrate: {e}")

    def on_message(self, client, userdata, msg):
        if msg.topic == f"coordinator/migrate/{self.client_id}":
            try:
                command = json.loads(msg.payload.decode())
                self.migrate_to_broker(command['broker_host'], command['broker_port'])
                return
            except Exception as e:
                logger.error(f"Error processing migration command: {e}")
                return
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            send_time = payload.get('timestamp', time.time())
            receive_time = time.time()
            latency = receive_time - send_time
            with self.lock:
                stats = self.message_stats[topic]
                stats['count'] += 1
                stats['total_latency'] += latency
                stats['last_received'] = receive_time
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def connect(self) -> bool:
        try:
            self.ctrl_client.connect(self.control_host, self.control_port, 60)
            self.ctrl_client.loop_start(); time.sleep(0.5)
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start(); time.sleep(1)
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False

    def subscribe_to_topics(self, topics: List[str]):
        self.topics = list(topics)
        if self.client.is_connected():
            for topic in self.topics:
                self.client.subscribe(topic, qos=1)

    def stop(self):
        self.client.loop_stop(); self.client.disconnect()
        self.ctrl_client.loop_stop(); self.ctrl_client.disconnect()

class SubscriberManager:
    def __init__(self, plan: Plan, num_subscribers: int = 20):
        self.plan = plan
        self.num_subscribers = num_subscribers
        self.subscribers: List[Subscriber] = []
        # Keep your existing broker pool
        self.brokers: List[Tuple[str,int]] = [
            ("broker2", 1883), ("broker3", 1883), ("broker4", 1883)
        ]
        self.all_topics: List[str] = self.plan.topics()

        # Mode & knobs
        self.mode = os.getenv("SUBSCRIBE_MODE", "all").lower()  # all | targeted
        self.hot_rate_threshold = float(os.getenv("HOT_RATE_THRESHOLD", "0.8"))
        self.min_subs_per_hot = int(os.getenv("MIN_SUBS_PER_HOT", "6"))
        self.max_topics_per_sub = int(os.getenv("MAX_TOPICS_PER_SUB", "8"))

    def _infer_hot_topics(self) -> List[str]:
        hot = []
        for t in self.all_topics:
            if self.plan.peak_rate(t) >= self.hot_rate_threshold:
                hot.append(t)
        return sorted(hot)

    def _build_assignments_all(self) -> List[List[str]]:
        # every subscriber subscribes to all plan topics
        return [list(self.all_topics) for _ in range(self.num_subscribers)]

    def _build_assignments_targeted(self) -> List[List[str]]:
        hot = self._infer_hot_topics()
        non_hot = [t for t in self.all_topics if t not in hot]
        logger.info(f"Inferred hot topics (threshold={self.hot_rate_threshold}): {hot}")

        # initialize empty lists
        assignments: List[List[str]] = [[] for _ in range(self.num_subscribers)]

        # ensure each hot topic has at least min_subs_per_hot subscribers
        si = 0
        for t in hot:
            for _ in range(self.min_subs_per_hot):
                assignments[si % self.num_subscribers].append(t)
                si += 1

        # fill remaining slots with non-hot topics up to cap per subscriber
        ni = 0
        for idx in range(self.num_subscribers):
            while len(assignments[idx]) < self.max_topics_per_sub and non_hot:
                assignments[idx].append(non_hot[ni % len(non_hot)])
                ni += 1

        # make topics unique per subscriber
        for idx in range(self.num_subscribers):
            assignments[idx] = sorted(list(dict.fromkeys(assignments[idx])))
        return assignments

    def start(self):
        if not self.all_topics:
            raise RuntimeError("Plan has no topics to subscribe to")

        if self.mode == "all":
            assignments = self._build_assignments_all()
        else:
            assignments = self._build_assignments_targeted()

        for i in range(self.num_subscribers):
            host, port = self.brokers[i % len(self.brokers)]
            sub = Subscriber(host, port, f"sub_{i}")
            sub_topics = assignments[i]
            sub.subscribe_to_topics(sub_topics)
            if sub.connect():
                self.subscribers.append(sub)
                logger.info(f"Started subscriber {i} with {len(sub_topics)} subscriptions")
        logger.info(f"All {len(self.subscribers)} subscribers started (mode={self.mode})")

        # stats reporter
        threading.Thread(target=self._report_stats, daemon=True).start()

    def _report_stats(self):
        while True:
            time.sleep(30)
            all_stats = defaultdict(lambda: {
                'total_messages': 0,
                'total_latency': 0.0,
                'subscriber_count': 0
            })
            for s in self.subscribers:
                # lightweight snapshot
                for topic, info in list(s.message_stats.items()):
                    all_stats[topic]['total_messages'] += info['count']
                    all_stats[topic]['total_latency'] += info['total_latency']
                    if info['count'] > 0:
                        all_stats[topic]['subscriber_count'] += 1
            logger.info("\n" + "="*60)
            logger.info("AGGREGATED STATISTICS")
            logger.info("="*60)
            for topic in sorted(all_stats.keys()):
                stats = all_stats[topic]
                if stats['total_messages'] > 0:
                    avg_latency = stats['total_latency'] / stats['total_messages']
                    logger.info(f"Topic: {topic}")
                    logger.info(f"  Total Messages: {stats['total_messages']}")
                    logger.info(f"  Avg Latency: {avg_latency:.3f}s")
                    logger.info(f"  Subscriber Count: {stats['subscriber_count']}")
            logger.info("="*60 + "\n")

    def stop_all(self):
        for s in self.subscribers:
            s.stop()

def load_plan_from_env() -> Plan:
    path = os.getenv("PLAN_FILE", "/app/plan.csv")
    fmt = os.getenv("PLAN_FORMAT", "csv").lower()
    plan = Plan()
    if fmt == "csv":
        plan.load_csv(path)
    elif fmt == "json":
        plan.load_json(path)
    else:
        raise ValueError(f"Unsupported PLAN_FORMAT: {fmt}")
    return plan


def main():
    logger.info("Starting Subscriber Manager...")
    time.sleep(5)

    plan = load_plan_from_env()
    num_subscribers = int(os.getenv("NUM_SUBSCRIBERS", "20"))
    mgr = SubscriberManager(plan, num_subscribers=num_subscribers)
    mgr.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping subscribers...")
        mgr.stop_all()


if __name__ == "__main__":
    main()
