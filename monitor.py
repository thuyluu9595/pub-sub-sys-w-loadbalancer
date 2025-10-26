#!/usr/bin/env python3
"""
Real-time Monitoring Dashboard
Monitors broker loads, topic distribution, and system metrics
"""

import paho.mqtt.client as mqtt
import time
import json
from collections import defaultdict
from datetime import datetime
import threading
import sys

try:
    import curses
    CURSES_AVAILABLE = True
except ImportError:
    CURSES_AVAILABLE = False
    print("Warning: curses not available, using simple text output")


class MetricsCollector:
    """Collect metrics from the MQTT system"""
    
    def __init__(self):
        self.brokers = [
            ("localhost", 1883, "Broker 1 (350Mbps)"),
            ("localhost", 1884, "Broker 2 (450Mbps)"),
            ("localhost", 1885, "Broker 3 (550Mbps)"),
            ("localhost", 1886, "Broker 4 (600Mbps)"),
        ]
        
        self.metrics = {
            'broker_messages': defaultdict(int),
            'topic_messages': defaultdict(int),
            'broker_bytes': defaultdict(int),
            'topic_rates': defaultdict(list),
            'latencies': [],
            'last_update': time.time()
        }
        
        self.clients = []
        self.running = True
        self.lock = threading.Lock()
    
    def on_message(self, client, userdata, msg):
        """Callback for message reception"""
        broker_id = userdata['broker_id']
        
        with self.lock:
            # Update metrics
            self.metrics['broker_messages'][broker_id] += 1
            self.metrics['topic_messages'][msg.topic] += 1
            self.metrics['broker_bytes'][broker_id] += len(msg.payload)
            
            # Calculate latency if timestamp in message
            try:
                payload = json.loads(msg.payload.decode())
                if 'timestamp' in payload:
                    latency = time.time() - payload['timestamp']
                    self.metrics['latencies'].append(latency)
                    
                    # Keep only last 100 latencies
                    if len(self.metrics['latencies']) > 100:
                        self.metrics['latencies'] = self.metrics['latencies'][-100:]
            except:
                pass
    
    def connect_to_brokers(self):
        """Connect to all brokers for monitoring"""
        for i, (host, port, name) in enumerate(self.brokers):
            try:
                client = mqtt.Client(f"monitor_{i}")
                client.user_data_set({'broker_id': i, 'name': name})
                client.on_message = self.on_message
                
                client.connect(host, port, 60)
                client.subscribe("#", qos=0)  # Subscribe to all topics
                client.loop_start()
                
                self.clients.append(client)
                print(f"Connected to {name}")
            except Exception as e:
                print(f"Failed to connect to {name}: {e}")
    
    def get_metrics(self):
        """Get current metrics snapshot"""
        with self.lock:
            return {
                'broker_messages': dict(self.metrics['broker_messages']),
                'topic_messages': dict(self.metrics['topic_messages']),
                'broker_bytes': dict(self.metrics['broker_bytes']),
                'avg_latency': sum(self.metrics['latencies']) / len(self.metrics['latencies']) 
                               if self.metrics['latencies'] else 0,
                'total_messages': sum(self.metrics['broker_messages'].values()),
                'total_topics': len(self.metrics['topic_messages']),
            }
    
    def stop(self):
        """Stop monitoring"""
        self.running = False
        for client in self.clients:
            client.loop_stop()
            client.disconnect()


class SimpleTextDashboard:
    """Simple text-based dashboard for systems without curses"""
    
    def __init__(self, collector):
        self.collector = collector
        self.running = True
    
    def display(self):
        """Display metrics in simple text format"""
        while self.running:
            metrics = self.collector.get_metrics()
            
            # Clear screen (works on most terminals)
            print("\033[2J\033[H")
            
            print("="*70)
            print("MQTT LOAD BALANCING - MONITORING DASHBOARD".center(70))
            print("="*70)
            print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # Broker statistics
            print("BROKER STATISTICS:")
            print("-" * 70)
            for i, (_, _, name) in enumerate(self.collector.brokers):
                messages = metrics['broker_messages'].get(i, 0)
                bytes_transferred = metrics['broker_bytes'].get(i, 0) / 1024 / 1024  # MB
                
                # Calculate load percentage (relative)
                total_msgs = metrics['total_messages']
                load_pct = (messages / total_msgs * 100) if total_msgs > 0 else 0
                
                bar_length = int(load_pct / 2)  # 50 chars = 100%
                bar = "█" * bar_length + "░" * (50 - bar_length)
                
                print(f"{name:25} | {messages:6} msgs | {bytes_transferred:8.2f} MB | {load_pct:5.1f}%")
                print(f"                          [{bar}]")
            
            print()
            
            # Top topics
            print("TOP 10 ACTIVE TOPICS:")
            print("-" * 70)
            sorted_topics = sorted(metrics['topic_messages'].items(), 
                                 key=lambda x: x[1], reverse=True)[:10]
            
            for topic, count in sorted_topics:
                print(f"  {topic:50} {count:6} msgs")
            
            print()
            
            # System metrics
            print("SYSTEM METRICS:")
            print("-" * 70)
            print(f"  Total Messages:    {metrics['total_messages']:,}")
            print(f"  Active Topics:     {metrics['total_topics']}")
            print(f"  Avg Latency:       {metrics['avg_latency']*1000:.2f} ms")
            
            print()
            print("="*70)
            print("Press Ctrl+C to exit")
            
            time.sleep(2)
    
    def start(self):
        """Start the dashboard"""
        try:
            self.display()
        except KeyboardInterrupt:
            self.running = False
            print("\nStopping dashboard...")


class CursesDashboard:
    """Curses-based dashboard with better visuals"""
    
    def __init__(self, collector):
        self.collector = collector
        self.running = True
    
    def draw_bar(self, stdscr, y, x, width, percentage, label):
        """Draw a progress bar"""
        filled = int(width * percentage / 100)
        bar = "█" * filled + "░" * (width - filled)
        
        stdscr.addstr(y, x, f"{label:20} [{bar}] {percentage:5.1f}%")
    
    def display(self, stdscr):
        """Display dashboard with curses"""
        curses.curs_set(0)  # Hide cursor
        stdscr.nodelay(1)   # Non-blocking input
        
        while self.running:
            try:
                stdscr.clear()
                height, width = stdscr.getmaxyx()
                
                metrics = self.collector.get_metrics()
                
                # Header
                title = "MQTT LOAD BALANCING - MONITORING DASHBOARD"
                stdscr.addstr(0, (width - len(title)) // 2, title, curses.A_BOLD)
                stdscr.addstr(1, 0, "=" * width)
                stdscr.addstr(2, 0, f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Broker statistics
                y = 4
                stdscr.addstr(y, 0, "BROKER STATISTICS:", curses.A_BOLD)
                y += 1
                
                total_msgs = metrics['total_messages']
                for i, (_, _, name) in enumerate(self.collector.brokers):
                    messages = metrics['broker_messages'].get(i, 0)
                    load_pct = (messages / total_msgs * 100) if total_msgs > 0 else 0
                    
                    self.draw_bar(stdscr, y, 2, 40, load_pct, name)
                    stdscr.addstr(y, 70, f"{messages:6} msgs")
                    y += 1
                
                # Top topics
                y += 2
                stdscr.addstr(y, 0, "TOP ACTIVE TOPICS:", curses.A_BOLD)
                y += 1
                
                sorted_topics = sorted(metrics['topic_messages'].items(), 
                                     key=lambda x: x[1], reverse=True)[:8]
                
                for topic, count in sorted_topics:
                    stdscr.addstr(y, 2, f"{topic[:50]:50} {count:6} msgs")
                    y += 1
                
                # System metrics
                y += 2
                stdscr.addstr(y, 0, "SYSTEM METRICS:", curses.A_BOLD)
                y += 1
                stdscr.addstr(y, 2, f"Total Messages:    {metrics['total_messages']:,}")
                y += 1
                stdscr.addstr(y, 2, f"Active Topics:     {metrics['total_topics']}")
                y += 1
                stdscr.addstr(y, 2, f"Avg Latency:       {metrics['avg_latency']*1000:.2f} ms")
                
                # Footer
                stdscr.addstr(height-2, 0, "=" * width)
                stdscr.addstr(height-1, 0, "Press 'q' to exit")
                
                stdscr.refresh()
                
                # Check for quit
                key = stdscr.getch()
                if key == ord('q'):
                    self.running = False
                    break
                
                time.sleep(1)
                
            except KeyboardInterrupt:
                self.running = False
                break
    
    def start(self):
        """Start the dashboard"""
        try:
            curses.wrapper(self.display)
        except Exception as e:
            print(f"Dashboard error: {e}")


def main():
    """Main entry point"""
    print("Starting monitoring dashboard...")
    print("Connecting to brokers...\n")
    
    collector = MetricsCollector()
    collector.connect_to_brokers()
    
    time.sleep(2)
    
    # Choose dashboard type
    if CURSES_AVAILABLE:
        print("Starting curses dashboard...")
        dashboard = CursesDashboard(collector)
    else:
        print("Starting simple text dashboard...")
        dashboard = SimpleTextDashboard(collector)
    
    try:
        dashboard.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        collector.stop()


if __name__ == "__main__":
    main()
