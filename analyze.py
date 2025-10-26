#!/usr/bin/env python3
"""
Performance Analysis Script
Analyzes system performance metrics and generates reports
"""

import re
import json
from collections import defaultdict
from datetime import datetime
import statistics
import sys

class PerformanceAnalyzer:
    """Analyze performance from log files"""
    
    def __init__(self):
        self.metrics = {
            'broker_utilization': defaultdict(list),
            'hot_topics': [],
            'loop_scores': [],
            'topic_allocation': defaultdict(list),
            'latencies': [],
            'message_counts': defaultdict(int),
            'timestamps': []
        }
    
    def parse_coordinator_logs(self, log_file):
        """Parse coordinator logs for load balancing metrics"""
        print("\n" + "="*70)
        print("PARSING COORDINATOR LOGS")
        print("="*70)
        
        try:
            with open(log_file, 'r') as f:
                logs = f.readlines()
        except FileNotFoundError:
            print(f"Error: Log file '{log_file}' not found")
            print("Run: docker-compose logs coordinator > coordinator_logs.txt")
            return
        
        for line in logs:
            # Parse broker utilization
            util_match = re.search(r'Broker (\d+) utilization: ([\d.]+)%', line)
            if util_match:
                broker_id = int(util_match.group(1))
                utilization = float(util_match.group(2))
                self.metrics['broker_utilization'][broker_id].append(utilization)
            
            # Parse hot topics
            hot_match = re.search(r"Hot Topics detected: \{([^}]*)\}", line)
            if hot_match:
                topics_str = hot_match.group(1)
                if topics_str:
                    topics = [t.strip().strip("'") for t in topics_str.split(',')]
                    self.metrics['hot_topics'].append(topics)
            
            # Parse LoOP scores
            loop_match = re.search(r"LoOP Scores: (\{[^}]+\})", line)
            if loop_match:
                try:
                    scores = eval(loop_match.group(1))
                    self.metrics['loop_scores'].append(scores)
                except:
                    pass
            
            # Parse topic allocation
            alloc_match = re.search(r"Allocated topic '([^']+)'.*to broker (\d+)", line)
            if alloc_match:
                topic = alloc_match.group(1)
                broker = int(alloc_match.group(2))
                self.metrics['topic_allocation'][topic].append(broker)
        
        print(f"✓ Parsed {len(logs)} log lines")
        print(f"✓ Found {len(self.metrics['hot_topics'])} hot topic detections")
        print(f"✓ Tracked {len(self.metrics['broker_utilization'])} brokers")
    
    def parse_subscriber_logs(self, log_file):
        """Parse subscriber logs for latency metrics"""
        print("\n" + "="*70)
        print("PARSING SUBSCRIBER LOGS")
        print("="*70)
        
        try:
            with open(log_file, 'r') as f:
                logs = f.readlines()
        except FileNotFoundError:
            print(f"Error: Log file '{log_file}' not found")
            print("Run: docker-compose logs subscriber > subscriber_logs.txt")
            return
        
        for line in logs:
            # Parse latency
            latency_match = re.search(r'avg latency: ([\d.]+)s', line)
            if latency_match:
                latency = float(latency_match.group(1))
                self.metrics['latencies'].append(latency)
            
            # Parse message counts
            count_match = re.search(r'Total Messages: (\d+)', line)
            if count_match:
                count = int(count_match.group(1))
                self.metrics['message_counts']['total'] = max(
                    self.metrics['message_counts']['total'], count
                )
        
        print(f"✓ Parsed {len(logs)} log lines")
        print(f"✓ Collected {len(self.metrics['latencies'])} latency measurements")
    
    def calculate_statistics(self):
        """Calculate statistical metrics"""
        print("\n" + "="*70)
        print("PERFORMANCE STATISTICS")
        print("="*70)
        
        # Broker utilization statistics
        print("\n1. BROKER UTILIZATION:")
        print("-" * 70)
        
        for broker_id in sorted(self.metrics['broker_utilization'].keys()):
            utils = self.metrics['broker_utilization'][broker_id]
            if utils:
                avg_util = statistics.mean(utils)
                std_util = statistics.stdev(utils) if len(utils) > 1 else 0
                min_util = min(utils)
                max_util = max(utils)
                
                print(f"  Broker {broker_id}:")
                print(f"    Average:  {avg_util:6.2f}%")
                print(f"    Std Dev:  {std_util:6.2f}%")
                print(f"    Min:      {min_util:6.2f}%")
                print(f"    Max:      {max_util:6.2f}%")
        
        # Calculate variance across brokers (load balance metric)
        all_utils = []
        for utils in self.metrics['broker_utilization'].values():
            if utils:
                all_utils.extend(utils)
        
        if all_utils:
            variance = statistics.variance(all_utils) if len(all_utils) > 1 else 0
            print(f"\n  Overall Utilization Variance: {variance:.2f}")
            print(f"  (Lower is better - indicates more even distribution)")
        
        # Hot topics analysis
        print("\n2. HOT TOPICS:")
        print("-" * 70)
        
        if self.metrics['hot_topics']:
            all_hot_topics = []
            for topics in self.metrics['hot_topics']:
                all_hot_topics.extend(topics)
            
            # Count frequency
            topic_freq = defaultdict(int)
            for topic in all_hot_topics:
                topic_freq[topic] += 1
            
            print(f"  Total hot topic detections: {len(self.metrics['hot_topics'])}")
            print(f"  Unique hot topics: {len(topic_freq)}")
            print(f"\n  Most frequently hot topics:")
            
            for topic, count in sorted(topic_freq.items(), 
                                      key=lambda x: x[1], 
                                      reverse=True)[:10]:
                percentage = (count / len(self.metrics['hot_topics'])) * 100
                print(f"    {topic:40} {count:3} times ({percentage:5.1f}%)")
        else:
            print("  No hot topics detected")
        
        # Latency statistics
        print("\n3. LATENCY METRICS:")
        print("-" * 70)
        
        if self.metrics['latencies']:
            avg_latency = statistics.mean(self.metrics['latencies'])
            median_latency = statistics.median(self.metrics['latencies'])
            std_latency = statistics.stdev(self.metrics['latencies']) if len(self.metrics['latencies']) > 1 else 0
            min_latency = min(self.metrics['latencies'])
            max_latency = max(self.metrics['latencies'])
            
            print(f"  Average Latency:  {avg_latency*1000:8.2f} ms")
            print(f"  Median Latency:   {median_latency*1000:8.2f} ms")
            print(f"  Std Deviation:    {std_latency*1000:8.2f} ms")
            print(f"  Min Latency:      {min_latency*1000:8.2f} ms")
            print(f"  Max Latency:      {max_latency*1000:8.2f} ms")
            
            # Percentiles
            sorted_latencies = sorted(self.metrics['latencies'])
            p50 = sorted_latencies[int(len(sorted_latencies) * 0.50)]
            p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
            
            print(f"\n  Latency Percentiles:")
            print(f"    P50 (median):  {p50*1000:8.2f} ms")
            print(f"    P95:           {p95*1000:8.2f} ms")
            print(f"    P99:           {p99*1000:8.2f} ms")
        else:
            print("  No latency data available")
        
        # Topic allocation analysis
        print("\n4. TOPIC ALLOCATION:")
        print("-" * 70)
        
        if self.metrics['topic_allocation']:
            print(f"  Topics allocated: {len(self.metrics['topic_allocation'])}")
            
            # Count reallocations
            reallocations = sum(1 for brokers in self.metrics['topic_allocation'].values() 
                              if len(set(brokers)) > 1)
            
            print(f"  Topics reallocated: {reallocations}")
            print(f"  Reallocation rate: {(reallocations/len(self.metrics['topic_allocation'])*100):.1f}%")
            
            # Broker load distribution
            broker_topic_counts = defaultdict(int)
            for topic, brokers in self.metrics['topic_allocation'].items():
                # Count final allocation
                broker_topic_counts[brokers[-1]] += 1
            
            print(f"\n  Topics per broker:")
            for broker_id in sorted(broker_topic_counts.keys()):
                count = broker_topic_counts[broker_id]
                print(f"    Broker {broker_id}: {count} topics")
    
    def generate_report(self, output_file='performance_report.txt'):
        """Generate a comprehensive performance report"""
        print("\n" + "="*70)
        print("GENERATING REPORT")
        print("="*70)
        
        with open(output_file, 'w') as f:
            f.write("="*70 + "\n")
            f.write("MQTT LOAD BALANCING - PERFORMANCE REPORT\n")
            f.write("="*70 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Summary
            f.write("EXECUTIVE SUMMARY\n")
            f.write("-"*70 + "\n")
            
            if self.metrics['broker_utilization']:
                all_utils = []
                for utils in self.metrics['broker_utilization'].values():
                    all_utils.extend(utils)
                avg_util = statistics.mean(all_utils) if all_utils else 0
                variance = statistics.variance(all_utils) if len(all_utils) > 1 else 0
                
                f.write(f"Average Server Utilization: {avg_util:.2f}%\n")
                f.write(f"Load Distribution Variance: {variance:.2f}\n")
            
            if self.metrics['latencies']:
                avg_latency = statistics.mean(self.metrics['latencies'])
                f.write(f"Average Message Latency: {avg_latency*1000:.2f} ms\n")
            
            if self.metrics['hot_topics']:
                total_detections = len(self.metrics['hot_topics'])
                f.write(f"Hot Topic Detections: {total_detections}\n")
            
            f.write("\n" + "="*70 + "\n")
            f.write("DETAILED METRICS\n")
            f.write("="*70 + "\n\n")
            
            # Detailed metrics would go here
            f.write("See console output for detailed statistics.\n")
        
        print(f"✓ Report saved to: {output_file}")
    
    def compare_with_baseline(self):
        """Compare with expected performance from paper"""
        print("\n" + "="*70)
        print("COMPARISON WITH PAPER RESULTS")
        print("="*70)
        
        print("\nExpected results from paper:")
        print("  - 11% reduction in average waiting time")
        print("  - 20% more even load distribution")
        print("  - ~22% average server utilization (close to optimal)")
        
        if self.metrics['broker_utilization']:
            all_utils = []
            for utils in self.metrics['broker_utilization'].values():
                all_utils.extend(utils)
            
            if all_utils:
                avg_util = statistics.mean(all_utils)
                variance = statistics.variance(all_utils) if len(all_utils) > 1 else 0
                
                print("\nYour results:")
                print(f"  - Average server utilization: {avg_util:.2f}%")
                print(f"  - Load distribution variance: {variance:.2f}")
                
                if 15 <= avg_util <= 30:
                    print("  ✓ Utilization is in expected range!")
                else:
                    print("  ⚠ Utilization outside expected range")
                
                if variance < 50:
                    print("  ✓ Load distribution is good!")
                else:
                    print("  ⚠ High variance - load not evenly distributed")


def main():
    """Main entry point"""
    print("\n" + "="*70)
    print("MQTT LOAD BALANCING - PERFORMANCE ANALYZER")
    print("="*70)
    
    analyzer = PerformanceAnalyzer()
    
    # Parse logs
    print("\nStep 1: Extracting logs from Docker containers...")
    import subprocess
    
    try:
        # Extract coordinator logs
        result = subprocess.run(
            ['docker-compose', 'logs', 'coordinator'],
            capture_output=True,
            text=True
        )
        with open('coordinator_logs.txt', 'w') as f:
            f.write(result.stdout)
        
        # Extract subscriber logs
        result = subprocess.run(
            ['docker-compose', 'logs', 'subscriber'],
            capture_output=True,
            text=True
        )
        with open('subscriber_logs.txt', 'w') as f:
            f.write(result.stdout)
        
        print("✓ Logs extracted successfully")
    except Exception as e:
        print(f"⚠ Could not extract logs automatically: {e}")
        print("  Please run manually:")
        print("    docker-compose logs coordinator > coordinator_logs.txt")
        print("    docker-compose logs subscriber > subscriber_logs.txt")
    
    # Parse logs
    print("\nStep 2: Parsing logs...")
    analyzer.parse_coordinator_logs('coordinator_logs.txt')
    analyzer.parse_subscriber_logs('subscriber_logs.txt')
    
    # Calculate statistics
    print("\nStep 3: Calculating statistics...")
    analyzer.calculate_statistics()
    
    # Generate report
    print("\nStep 4: Generating report...")
    analyzer.generate_report()
    
    # Compare with baseline
    print("\nStep 5: Comparing with paper results...")
    analyzer.compare_with_baseline()
    
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE")
    print("="*70)
    print("\nNext steps:")
    print("  - Review performance_report.txt for detailed analysis")
    print("  - Adjust configuration if needed (see Configuration Guide)")
    print("  - Run 'make monitor' for real-time monitoring")
    print()


if __name__ == "__main__":
    main()
