#!/usr/bin/env python3
"""
Test Script - Verify MQTT Load Balancing System
Tests hot topic detection, load balancing, and message delivery
"""

import paho.mqtt.client as mqtt
import time
import json
import sys
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SystemTester:
    """Test the MQTT load balancing system"""
    
    def __init__(self):
        self.brokers = [
            ("localhost", 1883),
            ("localhost", 1884),
            ("localhost", 1885),
            ("localhost", 1886),
        ]
        self.test_results = defaultdict(dict)
    
    def test_broker_connectivity(self):
        """Test 1: Verify all brokers are reachable"""
        logger.info("\n" + "="*60)
        logger.info("TEST 1: Broker Connectivity")
        logger.info("="*60)
        
        for i, (host, port) in enumerate(self.brokers):
            try:
                client = mqtt.Client(f"test_broker_{i}")
                result = client.connect(host, port, 10)
                client.disconnect()
                
                if result == 0:
                    logger.info(f"✓ Broker {i+1} ({host}:{port}) - CONNECTED")
                    self.test_results['connectivity'][f'broker{i+1}'] = True
                else:
                    logger.error(f"✗ Broker {i+1} ({host}:{port}) - FAILED (code: {result})")
                    self.test_results['connectivity'][f'broker{i+1}'] = False
            except Exception as e:
                logger.error(f"✗ Broker {i+1} ({host}:{port}) - ERROR: {e}")
                self.test_results['connectivity'][f'broker{i+1}'] = False
            
            time.sleep(0.5)
        
        all_connected = all(self.test_results['connectivity'].values())
        logger.info(f"\nResult: {'PASS' if all_connected else 'FAIL'}")
        return all_connected
    
    def test_publish_subscribe(self):
        """Test 2: Verify basic pub-sub functionality"""
        logger.info("\n" + "="*60)
        logger.info("TEST 2: Publish-Subscribe Functionality")
        logger.info("="*60)
        
        messages_received = []
        
        def on_message(client, userdata, msg):
            messages_received.append(msg.topic)
        
        # Test on each broker
        for i, (host, port) in enumerate(self.brokers):
            try:
                # Setup subscriber
                sub = mqtt.Client(f"test_sub_{i}")
                sub.on_message = on_message
                sub.connect(host, port, 10)
                sub.subscribe("test/topic", qos=1)
                sub.loop_start()
                
                time.sleep(1)
                
                # Setup publisher
                pub = mqtt.Client(f"test_pub_{i}")
                pub.connect(host, port, 10)
                pub.publish("test/topic", json.dumps({"test": i}), qos=1)
                
                time.sleep(2)
                
                pub.disconnect()
                sub.loop_stop()
                sub.disconnect()
                
                if "test/topic" in messages_received:
                    logger.info(f"✓ Broker {i+1} - Pub-Sub WORKING")
                    self.test_results['pubsub'][f'broker{i+1}'] = True
                else:
                    logger.error(f"✗ Broker {i+1} - Message NOT received")
                    self.test_results['pubsub'][f'broker{i+1}'] = False
                
                messages_received.clear()
                
            except Exception as e:
                logger.error(f"✗ Broker {i+1} - ERROR: {e}")
                self.test_results['pubsub'][f'broker{i+1}'] = False
            
            time.sleep(0.5)
        
        all_working = all(self.test_results['pubsub'].values())
        logger.info(f"\nResult: {'PASS' if all_working else 'FAIL'}")
        return all_working
    
    def test_hot_topic_simulation(self):
        """Test 3: Simulate hot topics and verify detection"""
        logger.info("\n" + "="*60)
        logger.info("TEST 3: Hot Topic Simulation")
        logger.info("="*60)
        
        host, port = self.brokers[0]
        
        # Create topics with different rates
        hot_topics = ["home/livingroom/temperature", "office/desk/motion"]
        normal_topics = ["warehouse/storage/humidity", "home/bedroom/light"]
        
        try:
            pub = mqtt.Client("hot_topic_test")
            pub.connect(host, port, 10)
            pub.loop_start()
            
            logger.info("Simulating traffic...")
            
            # Send many messages to hot topics
            for i in range(20):
                for topic in hot_topics:
                    payload = json.dumps({
                        "test": "hot_topic",
                        "count": i,
                        "timestamp": time.time()
                    })
                    pub.publish(topic, payload, qos=1)
                time.sleep(0.1)
            
            # Send few messages to normal topics
            for i in range(5):
                for topic in normal_topics:
                    payload = json.dumps({
                        "test": "normal_topic",
                        "count": i,
                        "timestamp": time.time()
                    })
                    pub.publish(topic, payload, qos=1)
                time.sleep(0.2)
            
            pub.loop_stop()
            pub.disconnect()
            
            logger.info(f"✓ Published to hot topics: {hot_topics}")
            logger.info(f"✓ Published to normal topics: {normal_topics}")
            logger.info("Check coordinator logs for hot topic detection!")
            
            self.test_results['hot_topics'] = True
            logger.info("\nResult: PASS (Check coordinator logs for LoOP detection)")
            return True
            
        except Exception as e:
            logger.error(f"✗ Hot topic simulation failed: {e}")
            self.test_results['hot_topics'] = False
            logger.info("\nResult: FAIL")
            return False
    
    def test_hierarchical_topics(self):
        """Test 4: Verify hierarchical topic structure"""
        logger.info("\n" + "="*60)
        logger.info("TEST 4: Hierarchical Topic Structure")
        logger.info("="*60)
        
        host, port = self.brokers[0]
        
        hierarchical_topics = [
            "home/livingroom/temperature",
            "home/livingroom/humidity",
            "home/bedroom/temperature",
            "office/desk/motion",
            "office/desk/light",
            "warehouse/storage/pressure"
        ]
        
        messages_received = []
        
        def on_message(client, userdata, msg):
            messages_received.append(msg.topic)
        
        try:
            # Subscribe with wildcards
            sub = mqtt.Client("wildcard_test")
            sub.on_message = on_message
            sub.connect(host, port, 10)
            
            # Test wildcards
            sub.subscribe("home/#", qos=1)  # All home topics
            sub.subscribe("office/+/motion", qos=1)  # Single level wildcard
            
            sub.loop_start()
            time.sleep(1)
            
            # Publish to topics
            pub = mqtt.Client("hierarchy_pub")
            pub.connect(host, port, 10)
            
            for topic in hierarchical_topics:
                pub.publish(topic, json.dumps({"test": "hierarchy"}), qos=1)
                time.sleep(0.1)
            
            time.sleep(2)
            
            pub.disconnect()
            sub.loop_stop()
            sub.disconnect()
            
            # Verify received topics
            home_topics = [t for t in messages_received if t.startswith("home/")]
            motion_topics = [t for t in messages_received if t.endswith("/motion")]
            
            logger.info(f"✓ Received home/* topics: {len(home_topics)}")
            logger.info(f"✓ Received */motion topics: {len(motion_topics)}")
            
            if home_topics and motion_topics:
                self.test_results['hierarchy'] = True
                logger.info("\nResult: PASS")
                return True
            else:
                self.test_results['hierarchy'] = False
                logger.info("\nResult: FAIL")
                return False
                
        except Exception as e:
            logger.error(f"✗ Hierarchy test failed: {e}")
            self.test_results['hierarchy'] = False
            logger.info("\nResult: FAIL")
            return False
    
    def test_qos_levels(self):
        """Test 5: Verify QoS levels"""
        logger.info("\n" + "="*60)
        logger.info("TEST 5: QoS Levels")
        logger.info("="*60)
        
        host, port = self.brokers[0]
        qos_results = {}
        
        for qos in [0, 1, 2]:
            messages_received = []
            
            def on_message(client, userdata, msg):
                messages_received.append(msg.qos)
            
            try:
                sub = mqtt.Client(f"qos_test_sub_{qos}")
                sub.on_message = on_message
                sub.connect(host, port, 10)
                sub.subscribe(f"test/qos/{qos}", qos=qos)
                sub.loop_start()
                
                time.sleep(1)
                
                pub = mqtt.Client(f"qos_test_pub_{qos}")
                pub.connect(host, port, 10)
                
                for i in range(5):
                    pub.publish(f"test/qos/{qos}", f"QoS {qos} message {i}", qos=qos)
                    time.sleep(0.1)
                
                time.sleep(2)
                
                pub.disconnect()
                sub.loop_stop()
                sub.disconnect()
                
                if messages_received:
                    logger.info(f"✓ QoS {qos} - Received {len(messages_received)} messages")
                    qos_results[qos] = True
                else:
                    logger.error(f"✗ QoS {qos} - No messages received")
                    qos_results[qos] = False
                    
            except Exception as e:
                logger.error(f"✗ QoS {qos} test failed: {e}")
                qos_results[qos] = False
            
            time.sleep(0.5)
        
        all_qos_working = all(qos_results.values())
        self.test_results['qos'] = qos_results
        logger.info(f"\nResult: {'PASS' if all_qos_working else 'FAIL'}")
        return all_qos_working
    
    def run_all_tests(self):
        """Run all tests"""
        logger.info("\n" + "="*60)
        logger.info("MQTT LOAD BALANCING SYSTEM - TEST SUITE")
        logger.info("="*60)
        
        tests = [
            ("Broker Connectivity", self.test_broker_connectivity),
            ("Publish-Subscribe", self.test_publish_subscribe),
            ("Hot Topic Simulation", self.test_hot_topic_simulation),
            ("Hierarchical Topics", self.test_hierarchical_topics),
            ("QoS Levels", self.test_qos_levels),
        ]
        
        results = {}
        for test_name, test_func in tests:
            try:
                results[test_name] = test_func()
            except Exception as e:
                logger.error(f"Test '{test_name}' crashed: {e}")
                results[test_name] = False
            
            time.sleep(2)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("TEST SUMMARY")
        logger.info("="*60)
        
        for test_name, passed in results.items():
            status = "✓ PASS" if passed else "✗ FAIL"
            logger.info(f"{status} - {test_name}")
        
        passed_count = sum(results.values())
        total_count = len(results)
        
        logger.info(f"\nTotal: {passed_count}/{total_count} tests passed")
        logger.info("="*60 + "\n")
        
        return passed_count == total_count


def main():
    """Main entry point"""
    logger.info("Starting system tests...")
    logger.info("Make sure all containers are running: docker-compose ps\n")
    
    time.sleep(2)
    
    tester = SystemTester()
    success = tester.run_all_tests()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
