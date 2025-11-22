#!/usr/bin/env python3
"""
Publisher Application
Publishes messages to various topics with different rates
"""

import paho.mqtt.client as mqtt
import json
import time
import random
import threading
import logging
from typing import Dict, List
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Publisher:
    """MQTT Publisher that generates traffic on multiple topics"""
    
    def __init__(self, broker_host: str, broker_port: int, client_id: str):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.client = mqtt.Client(client_id)
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message
        self.running = False
        self.topics = []
        self.publish_rates = {}  # topic -> messages per second
        self.migration_lock = threading.Lock()
        self.registered = False

        # Attributes for control broker
        self.control_host = os.getenv("CONTROL_HOST", "broker1")
        self.control_port = int(os.getenv("CONTROL_PORT", "1883"))
        self.ctrl_client = mqtt.Client(f"{client_id}-ctrl")
        self.ctrl_client.on_connect = self.on_ctrl_connect
        self.ctrl_client.on_message = self.on_message
        self.ctrl_client.on_publish = self.on_publish

    def on_ctrl_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to control broker")
            client.subscribe(f"coordinator/migrate/{self.client_id}")

            # Register with coordinator if not already registered
            if not self.registered and self.topics:
                self.register_with_coordinator()


    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker"""
        if rc == 0:
            logger.info(f"Publisher {self.client_id} connected to {self.broker_host}:{self.broker_port}")

            # Register with coordinator if not already registered
            if not self.registered and self.topics:
                self.register_with_coordinator()
        else:
            logger.error(f"Connection failed with code {rc}")
    
    def on_message(self, client, userdata, msg):
        """Handle migration commands from coordinator"""
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
    
    def register_with_coordinator(self):
        """Register this publisher with the coordinator"""
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
        """Periodically send topic statistics to coordinator"""
        while self.running:
            time.sleep(5)  # Send stats every 5 seconds
            
            for topic in self.topics:
                try:
                    self.ctrl_client.publish(f"coordinator/stats/{topic}", json.dumps({
                        "topic": topic,
                        "rate": self.publish_rates.get(topic, 0.0),
                        "publisher_id": self.client_id
                    }), qos=0)

                except Exception as e:
                    logger.error(f"Failed to send statistics: {e}")
    
    def migrate_to_broker(self, new_host: str, new_port: int):
        """Migrate to a new broker"""
        with (self.migration_lock):
            ack = {
                "client_id": self.client_id,
                "role": "publisher",
                "new_broker_host": new_host,
                "new_broker_port": new_port,
                "t": time.time()
            }
            if new_host == self.broker_host and new_port == self.broker_port:
                # send ACK anyway so coordinator considers this client "migrated"
                self.ctrl_client.publish("coordinator/ack", json.dumps(ack), qos=1)
                logger.info("Already connected to target broker")
                return
            
            logger.info(f"Migrating from {self.broker_host}:{self.broker_port} to {new_host}:{new_port}")
            
            # Disconnect from current broker
            self.client.loop_stop()
            self.client.disconnect()
            
            # Update broker info
            self.broker_host = new_host
            self.broker_port = new_port
            
            # Create new client with same ID
            self.client = mqtt.Client(self.client_id)
            self.client.on_connect = self.on_connect
            self.client.on_publish = self.on_publish
            self.client.on_message = self.on_message
            
            # Reconnect to new broker
            try:
                self.client.connect(new_host, new_port, 60)
                self.client.loop_start()
                logger.info(f"Successfully migrated to {new_host}:{new_port}")

                # Send ACK when succeeded migration
                self.ctrl_client.publish("coordinator/ack", json.dumps(ack), qos=1)
            except Exception as e:
                logger.error(f"Failed to migrate: {e}")

    
    def on_publish(self, client, userdata, mid):
        """Callback for when a message is published"""
        pass
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            # Connect to control broker
            self.ctrl_client.connect(self.control_host, self.control_port, 60)
            self.ctrl_client.loop_start()
            time.sleep(0.5)  # Wait for connection

            # Connect to data broker
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            time.sleep(1)  # Wait for connection

            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False
    
    def set_topics(self, topics: List[str], rates: Dict[str, float]):
        """Set topics to publish to with their rates"""
        self.topics = topics
        self.publish_rates = rates
        logger.info(f"Publisher {self.client_id} configured with {len(topics)} topics")
    
    def publish_message(self, topic: str, payload: dict):
        """Publish a single message to a topic"""
        try:
            msg_json = json.dumps(payload)
            result = self.client.publish(topic, msg_json, qos=1)
            return result.is_published()
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            return False
    
    def start_publishing(self):
        """Start publishing messages on all topics"""
        self.running = True

        # Start sending statistics
        threading.Thread(target=self._send_statistics, daemon=True).start()

        threads = []
        
        for topic in self.topics:
            rate = self.publish_rates.get(topic, 1.0)
            thread = threading.Thread(
                target=self._publish_loop,
                args=(topic, rate),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        logger.info(f"Started publishing on {len(threads)} topics")
        return threads
    
    def _publish_loop(self, topic: str, rate: float):
        """Publish messages to a topic at a given rate"""
        interval = 1.0 / rate if rate > 0 else 1.0
        msg_count = 0
        
        while self.running:
            payload = {
                'publisher_id': self.client_id,
                'topic': topic,
                'message_id': msg_count,
                'timestamp': time.time(),
                'data': self._generate_payload(50, 100)
            }
            
            if self.publish_message(topic, payload):
                msg_count += 1
                if msg_count % 10 == 0:
                    logger.debug(f"Published {msg_count} messages to {topic}")
            
            time.sleep(interval + random.uniform(-0.1, 0.1))  # Add jitter
    
    def _generate_payload(self, min_kb: int, max_kb: int) -> str:
        """Generate payload of specified size"""
        size_bytes = random.randint(min_kb * 1024, max_kb * 1024)
        return 'X' * size_bytes
    
    def stop(self):
        """Stop publishing"""
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()

        self.ctrl_client.loop_stop()
        self.ctrl_client.disconnect()


class PublisherManager:
    """Manages multiple publishers"""
    
    def __init__(self, num_topics: int = 20):
        self.num_topics = num_topics
        self.publishers = []
        self.topics = self._generate_topics()
        self.brokers = [
            ("broker2", 1883),
            ("broker3", 1883),
            ("broker4", 1883),
        ]
    
    def _generate_topics(self) -> List[str]:
        """Generate hierarchical topics (like smart home scenario)"""
        topics = []
        
        # Generate topics with hierarchical structure
        locations = ['home', 'office', 'warehouse']
        rooms = ['livingroom', 'bedroom', 'kitchen', 'bathroom']
        sensors = ['temperature', 'humidity', 'motion', 'light', 'pressure']
        
        for location in locations:
            for room in rooms:
                for sensor in sensors[:3]:  # Limit combinations
                    topic = f"{location}/{room}/{sensor}"
                    topics.append(topic)
                    if len(topics) >= self.num_topics:
                        return topics
        
        return topics[:self.num_topics]
    
    def _generate_publish_rates(self) -> Dict[str, float]:
        """
        Generate publish rates following a power-law distribution
        to create hot topics (high rate) and normal topics (low rate)
        """
        rates = {}
        
        # Create a few hot topics with high rates
        num_hot_topics = max(1, self.num_topics // 5)
        hot_topics = random.sample(self.topics, num_hot_topics)
        
        for topic in self.topics:
            if topic in hot_topics:
                # Hot topics: 1.0 - 2.0 messages/sec
                rates[topic] = random.uniform(1.0, 2.0)
            else:
                # Normal topics: 0.1 - 0.5 messages/sec
                rates[topic] = random.uniform(0.1, 0.5)
        
        logger.info(f"Hot topics: {hot_topics}")
        return rates
    
    def start(self, num_publishers: int = 5):
        """Start multiple publishers"""
        rates = self._generate_publish_rates()
        
        for i in range(num_publishers):
            # Round-robin broker assignment initially
            broker_host, broker_port = self.brokers[i % len(self.brokers)]
            
            # Each publisher handles a subset of topics
            start_idx = i * len(self.topics) // num_publishers
            end_idx = (i + 1) * len(self.topics) // num_publishers
            pub_topics = self.topics[start_idx:end_idx]
            
            publisher = Publisher(broker_host, broker_port, f"pub_{i}")

            pub_rates = {t: rates[t] for t in pub_topics}
            publisher.set_topics(pub_topics, pub_rates)
            
            if publisher.connect():
                publisher.start_publishing()
                self.publishers.append(publisher)
                
                logger.info(f"Started publisher {i} with {len(pub_topics)} topics")
        
        logger.info(f"All {len(self.publishers)} publishers started")
    
    def stop_all(self):
        """Stop all publishers"""
        for pub in self.publishers:
            pub.stop()


def main():
    """Main entry point"""
    logger.info("Starting Publisher Manager...")
    
    # Wait for brokers to be ready
    time.sleep(5)
    
    manager = PublisherManager(num_topics=20)
    manager.start(num_publishers=5)
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping publishers...")
        manager.stop_all()


if __name__ == "__main__":
    main()
