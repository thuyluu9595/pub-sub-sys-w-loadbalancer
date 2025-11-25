#!/usr/bin/env python3
"""
Deterministic, plan-driven Publisher
- No randomness
- Reads a plan file (CSV or JSON) that defines per-topic phases over time
- Produces repeatable traffic so coordinator overhead metrics are comparable
"""

import paho.mqtt.client as mqtt
import json
import time
import threading
import logging
from typing import Dict, List, Tuple
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
    """Holds per-topic phase schedules, parsed from CSV/JSON."""
    def __init__(self):
        self.topic_phases: Dict[str, List[Phase]] = {}

    def load_csv(self, path: str):
        with open(path, "r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or row[0].strip().startswith("#"):
                    continue
                # topic, phase, start_s, duration_s, rate_mps, payload_kb
                if len(row) < 6:
                    raise ValueError(f"CSV row has <6 columns: {row}")
                topic = row[0].strip()
                # phase index is ignored for logic but kept in file for readability
                start_s = float(row[2])
                duration_s = float(row[3])
                rate_mps = float(row[4])
                payload_kb = int(row[5])
                self.topic_phases.setdefault(topic, []).append(
                    Phase(start_s, duration_s, rate_mps, payload_kb)
                )
        # sort phases per topic by start time
        for t, phases in self.topic_phases.items():
            phases.sort(key=lambda p: p.start_s)

    def load_json(self, path: str):
        with open(path, "r") as f:
            data = json.load(f)
        phases = data.get("phases", [])
        for ph in phases:
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
        return list(self.topic_phases.keys())

class Publisher:
    """MQTT Publisher that generates traffic on multiple topics deterministically."""
    def __init__(self, broker_host: str, broker_port: int, client_id: str):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.client = mqtt.Client(client_id)
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message
        self.running = False

        self.topics: List[str] = []
        # current rate per topic (for stats)
        self.publish_rates: Dict[str, float] = {}
        # phases per topic
        self.phases: Dict[str, List[Phase]] = {}
        # thread handles
        self._threads: List[threading.Thread] = []
        # start time reference
        self._t0 = None

        self.migration_lock = threading.Lock()
        self.registered = False

        # Control-broker client
        self.control_host = os.getenv("CTRL_HOST", "broker1")
        self.control_port = int(os.getenv("CTRL_PORT", "1883"))
        self.ctrl_client = mqtt.Client(f"{client_id}-ctrl")
        self.ctrl_client.on_connect = self.on_ctrl_connect
        self.ctrl_client.on_message = self.on_message
        self.ctrl_client.on_publish = self.on_publish

    def on_ctrl_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to control broker")
            client.subscribe(f"coordinator/migrate/{self.client_id}")
            if not self.registered and self.topics:
                self.register_with_coordinator()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Publisher {self.client_id} connected to {self.broker_host}:{self.broker_port}")
            if not self.registered and self.topics:
                self.register_with_coordinator()
        else:
            logger.error(f"Connection failed with code {rc}")

    def on_message(self, client, userdata, msg):
        if msg.topic == f"coordinator/migrate/{self.client_id}":
            try:
                command = json.loads(msg.payload.decode())
                new_host = command['broker_host']
                new_port = command['broker_port']
                logger.info(f"Received migration command: {new_host}:{new_port}")
                time.sleep(0.2)
                self.migrate_to_broker(new_host, new_port)
            except Exception as e:
                logger.error(f"Error processing migration command: {e}")

    def on_publish(self, client, userdata, mid):
        pass

    def register_with_coordinator(self):
        try:
            self.ctrl_client.publish("coordinator/register/publisher", json.dumps({
                "client_id": self.client_id,
                "type": "publisher",
                "topics": self.topics,
                "rates": self.publish_rates,
                "data_broker_host": self.broker_host,
                "data_broker_port": self.broker_port
            }), qos=1)
            self.registered = True
            logger.info(f"Registered with coordinator: {len(self.topics)} topics")
        except Exception as e:
            logger.error(f"Failed to register with coordinator: {e}")

    def _send_statistics(self):
        while self.running:
            time.sleep(5)
            for topic in self.topics:
                try:
                    self.ctrl_client.publish(
                        f"coordinator/stats/{topic}",
                        json.dumps({
                            "topic": topic,
                            "rate": self.publish_rates.get(topic, 0.0),
                            "publisher_id": self.client_id
                        }),
                        qos=0,
                    )
                except Exception as e:
                    logger.error(f"Failed to send statistics: {e}")

    def migrate_to_broker(self, new_host: str, new_port: int):
        with self.migration_lock:
            ack = {
                "client_id": self.client_id,
                "role": "publisher",
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
            self.client.on_publish = self.on_publish
            self.client.on_message = self.on_message
            try:
                self.client.connect(new_host, new_port, 60)
                self.client.loop_start()
                logger.info(f"Successfully migrated publisher {self.client_id} to {new_host}:{new_port}")
                self.ctrl_client.publish("coordinator/ack", json.dumps(ack), qos=1)
            except Exception as e:
                logger.error(f"Failed to migrate: {e}")

    def connect(self) -> bool:
        try:
            self.ctrl_client.connect(self.control_host, self.control_port, 60)
            self.ctrl_client.loop_start()
            time.sleep(0.5)
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            time.sleep(1)
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False

    def set_plan(self, plan: Plan):
        self.phases = plan.topic_phases
        self.topics = sorted(plan.topics())
        # initialize current rates as the first active phase rate (or 0)
        for t in self.topics:
            self.publish_rates[t] = 0.0

    def publish_message(self, topic: str, payload: dict) -> bool:
        try:
            msg_json = json.dumps(payload)
            result = self.client.publish(topic, msg_json, qos=1)
            return result.is_published()
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            return False

    def start_publishing(self):
        self.running = True
        self._t0 = time.monotonic()
        threading.Thread(target=self._send_statistics, daemon=True).start()
        # one thread per topic
        for topic in self.topics:
            th = threading.Thread(target=self._publish_loop, args=(topic,), daemon=True)
            th.start(); self._threads.append(th)
        logger.info(f"Started deterministic publishing on {len(self.topics)} topics")

    def _current_phase(self, topic: str, now_s: float) -> Phase | None:
        phases = self.phases.get(topic, [])
        for p in phases:
            if p.start_s <= now_s < p.end_s:
                return p
        return None

    def _publish_loop(self, topic: str):
        msg_id = 0
        next_due = time.monotonic()
        while self.running:
            now_s = time.monotonic() - self._t0
            ph = self._current_phase(topic, now_s)
            if ph is None or ph.rate_mps <= 0.0:
                # idle until next phase
                self.publish_rates[topic] = 0.0
                time.sleep(0.05)
                continue

            # fixed rate scheduling (no jitter)
            interval = 1.0 / ph.rate_mps
            if time.monotonic() >= next_due:
                # exact-length payload (deterministic)
                payload = {
                    'publisher_id': self.client_id,
                    'topic': topic,
                    'message_id': msg_id,
                    'timestamp': time.time(),
                    'data': 'X' * ph.payload_bytes
                }
                if self.publish_message(topic, payload):
                    msg_id += 1
                next_due += interval
                self.publish_rates[topic] = ph.rate_mps
            else:
                time.sleep(min(0.01, max(0.0, next_due - time.monotonic())))

    def stop(self):
        self.running = False
        self.client.loop_stop(); self.client.disconnect()
        self.ctrl_client.loop_stop(); self.ctrl_client.disconnect()

class PublisherManager:
    """Starts N publisher instances deterministically using a shared plan.
    Each publisher gets a disjoint subset of topics (round-robin over sorted topics).
    """
    def __init__(self, plan: Plan, num_publishers: int = 5):
        self.plan = plan
        self.num_publishers = num_publishers
        self.publishers: List[Publisher] = []
        self.brokers: List[Tuple[str,int]] = [
            ("broker2", 1883), ("broker3", 1883), ("broker4", 1883)
        ]

    def start(self):
        topics = sorted(self.plan.topics())
        if not topics:
            raise RuntimeError("Plan has no topics")

        for i in range(self.num_publishers):
            host, port = self.brokers[i % len(self.brokers)]
            pub = Publisher(host, port, f"pub_{i}")

            # slice topics deterministically
            start_idx = i * len(topics) // self.num_publishers
            end_idx = (i + 1) * len(topics) // self.num_publishers
            sub_topics = topics[start_idx:end_idx]

            # subset plan for this publisher
            sub_plan = Plan()
            for t in sub_topics:
                sub_plan.topic_phases[t] = list(self.plan.topic_phases[t])

            pub.set_plan(sub_plan)

            if pub.connect():
                # initialize publish_rates from first active phase (0 at t=0 if future)
                for t in sub_topics:
                    ph0 = None
                    for p in sub_plan.topic_phases[t]:
                        if p.start_s <= 0 < p.end_s:
                            ph0 = p; break
                    pub.publish_rates[t] = ph0.rate_mps if ph0 else 0.0
                pub.register_with_coordinator()
                pub.start_publishing()
                self.publishers.append(pub)
                logger.info(f"Started publisher {i} with {len(sub_topics)} topics")
        logger.info(f"All {len(self.publishers)} deterministic publishers started")

    def stop_all(self):
        for pub in self.publishers:
            pub.stop()


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
    logger.info("Starting Deterministic Publisher Manager...")
    time.sleep(5)  # wait for brokers/coordinator

    plan = load_plan_from_env()
    num_publishers = int(os.getenv("NUM_PUBLISHERS", "5"))
    mgr = PublisherManager(plan, num_publishers=num_publishers)
    mgr.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping publishers...")
        mgr.stop_all()


if __name__ == "__main__":
    main()