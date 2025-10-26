#!/usr/bin/env python3
"""
Subscriber Application
Subscribes to various topics and measures delivery metrics
"""

import paho.mqtt.client as mqtt
import json
import time
import random
import logging
from typing import List, Dict
from collections import defaultdict
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker"""
        if rc == 0:
            logger.info(f"Subscriber {self.client_id} connected to {self.broker_host}:{self.broker_port}")
            
            # Subscribe to migration commands
            client.subscribe(f"coordinator/migrate/{self.client_id}")
            
            # Subscribe to all topics
            for topic in self.topics:
                self.client.subscribe(topic, qos=1)
                logger.debug(f"Subscribed to {topic}")
            
            # Register with coordinator if not already registered
            if not self.registered and self.topics:
                self.register_with_coordinator()
        else:
            logger.error(f"Connection failed with code {rc}")
    
    def register_with_coordinator(self):
        """Register this subscriber with the coordinator"""
        try:
            registration = {
                'client_id': self.client_id,
                'type': 'subscriber',
                'topics': self.topics
            }
            
            self.client.publish(
                "coordinator/register/subscriber",
                json.dumps(registration),
                qos=1
            )
            self.registered = True
            logger.info(f"Registered with coordinator: {len(self.topics)} topics")
            
        except Exception as e:
            logger.error(f"Failed to register with coordinator: {e}")
    
    def migrate_to_broker(self, new_host: str, new_port: int):
        """Migrate to a new broker"""
        with self.migration_lock:
            if new_host == self.broker_host and new_port == self.broker_port:
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
            self.client.on_message = self.on_message
            
            # Reconnect to new broker
            try:
                self.client.connect(new_host, new_port, 60)
                self.client.loop_start()
                logger.info(f"Successfully migrated to {new_host}:{new_port}")
            except Exception as e:
                logger.error(f"Failed to migrate: {e}")
    
    def on_message(self, client, userdata, msg):
        """Callback for when a message is received"""
        # Handle migration commands
        if msg.topic == f"coordinator/migrate/{self.client_id}":
            try:
                command = json.loads(msg.payload.decode())
                new_host = command['broker_host']
                new_port = command['broker_port']
                
                logger.info(f"Received migration command: {new_host}:{new_port}")
                self.migrate_to_broker(new_host, new_port)
                return
            except Exception as e:
                logger.error(f"Error processing migration command: {e}")
                return
        
        # Handle regular messages
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            
            # Calculate latency
            send_time = payload.get('timestamp', time.time())
            receive_time = time.time()
            latency = receive_time - send_time
            
            with self.lock:
                stats = self.message_stats[topic]
                stats['count'] += 1
                stats['total_latency'] += latency
                stats['last_received'] = receive_time
            
            if stats['count'] % 10 == 0:
                avg_latency = stats['total_latency'] / stats['count']
                logger.debug(f"Topic {topic}: received {stats['count']} msgs, "
                           f"avg latency: {avg_latency:.3f}s")
        
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            time.sleep(2)  # Wait for connection
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False
    
    def subscribe_to_topics(self, topics: List[str]):
        """Set topics to subscribe to"""
        self.topics = topics
        # If already connected, subscribe now
        if self.client.is_connected():
            for topic in topics:
                self.client.subscribe(topic, qos=1)
    
    def get_stats(self) -> Dict:
        """Get statistics for all topics"""
        with self.lock:
            stats_copy = {}
            for topic, stats in self.message_stats.items():
                if stats['count'] > 0:
                    stats_copy[topic] = {
                        'count': stats['count'],
                        'avg_latency': stats['total_latency'] / stats['count'],
                        'last_received': stats['last_received']
                    }
            return stats_copy
    
    def stop(self):
        """Stop subscriber"""
        self.client.loop_stop()
        self.client.disconnect()


class SubscriberManager:
    """Manages multiple subscribers"""
    
    def __init__(self, num_subscribers: int = 50):
        self.num_subscribers = num_subscribers
        self.subscribers = []
        self.brokers = [
            ("broker1", 1883),
            ("broker2", 1883),
            ("broker3", 1883),
            ("broker4", 1883),
        ]
        self.topics = self._generate_topics()
    
    def _generate_topics(self) -> List[str]:
        """Generate hierarchical topics matching publisher topics"""
        topics = []
        
        locations = ['home', 'office', 'warehouse']
        rooms = ['livingroom', 'bedroom', 'kitchen', 'bathroom']
        sensors = ['temperature', 'humidity', 'motion', 'light', 'pressure']
        
        for location in locations:
            for room in rooms:
                for sensor in sensors[:3]:
                    topic = f"{location}/{room}/{sensor}"
                    topics.append(topic)
                    if len(topics) >= 20:
                        return topics
        
        return topics[:20]
    
    def start(self):
        """Start multiple subscribers"""
        for i in range(self.num_subscribers):
            # Round-robin broker assignment initially
            broker_host, broker_port = self.brokers[i % len(self.brokers)]
            
            subscriber = Subscriber(broker_host, broker_port, f"sub_{i}")
            
            if subscriber.connect():
                # Each subscriber subscribes to a random subset of topics
                # to simulate varying subscription patterns
                num_subscriptions = random.randint(1, min(5, len(self.topics)))
                sub_topics = random.sample(self.topics, num_subscriptions)
                
                subscriber.subscribe_to_topics(sub_topics)
                self.subscribers.append(subscriber)
                
                logger.info(f"Started subscriber {i} with {len(sub_topics)} subscriptions")
        
        logger.info(f"All {len(self.subscribers)} subscribers started")
        
        # Start stats reporting thread
        stats_thread = threading.Thread(target=self._report_stats, daemon=True)
        stats_thread.start()
    
    def _report_stats(self):
        """Periodically report aggregated statistics"""
        while True:
            time.sleep(30)  # Report every 30 seconds
            
            all_stats = defaultdict(lambda: {
                'total_messages': 0,
                'total_latency': 0.0,
                'subscriber_count': 0
            })
            
            for subscriber in self.subscribers:
                stats = subscriber.get_stats()
                for topic, topic_stats in stats.items():
                    all_stats[topic]['total_messages'] += topic_stats['count']
                    all_stats[topic]['total_latency'] += (
                        topic_stats['avg_latency'] * topic_stats['count']
                    )
                    all_stats[topic]['subscriber_count'] += 1
            
            # Log aggregated stats
            logger.info("\n" + "="*60)
            logger.info("AGGREGATED STATISTICS")
            logger.info("="*60)
            
            for topic, stats in sorted(all_stats.items()):
                if stats['total_messages'] > 0:
                    avg_latency = stats['total_latency'] / stats['total_messages']
                    logger.info(f"Topic: {topic}")
                    logger.info(f"  Total Messages: {stats['total_messages']}")
                    logger.info(f"  Avg Latency: {avg_latency:.3f}s")
                    logger.info(f"  Subscriber Count: {stats['subscriber_count']}")
            
            logger.info("="*60 + "\n")
    
    def stop_all(self):
        """Stop all subscribers"""
        for sub in self.subscribers:
            sub.stop()


def main():
    """Main entry point"""
    logger.info("Starting Subscriber Manager...")
    
    # Wait for brokers and publishers to be ready
    time.sleep(10)
    
    manager = SubscriberManager(num_subscribers=50)
    manager.start()
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping subscribers...")
        manager.stop_all()


if __name__ == "__main__":
    main()
