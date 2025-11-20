#!/usr/bin/env python3
"""
Real-time Overhead Monitoring Dashboard
Visualizes coordination overhead vs performance benefits
"""

import json
import time
import os
import sys
from datetime import datetime
from collections import deque
import threading

try:
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    from matplotlib.gridspec import GridSpec

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    print("Warning: matplotlib not available. Install with: pip install matplotlib")
    MATPLOTLIB_AVAILABLE = False


class OverheadMonitor:
    """Monitor and visualize overhead metrics"""

    def __init__(self, data_file='/app/overhead_data.json', history_size=100):
        self.data_file = data_file
        self.history_size = history_size

        # Time series data
        self.timestamps = deque(maxlen=history_size)
        self.total_overhead = deque(maxlen=history_size)
        self.registration_overhead = deque(maxlen=history_size)
        self.mapping_overhead = deque(maxlen=history_size)
        self.migration_overhead = deque(maxlen=history_size)
        self.stats_overhead = deque(maxlen=history_size)
        self.efficiency_ratio = deque(maxlen=history_size)
        self.avg_utilization = deque(maxlen=history_size)
        self.load_variance = deque(maxlen=history_size)

        # Current values
        self.current_data = {}
        self.last_update = None

    def load_data(self):
        """Load overhead data from JSON file"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    self.current_data = json.load(f)
                    self.last_update = datetime.now()
                    return True
        except Exception as e:
            print(f"Error loading data: {e}")
        return False

    def update_time_series(self):
        """Update time series from current data"""
        if not self.current_data:
            return

        breakdown = self.current_data.get('breakdown', {})
        components = breakdown.get('components', {})

        # Add timestamp
        self.timestamps.append(datetime.now())

        # Overhead components (in KB)
        self.total_overhead.append(breakdown.get('total_overhead_bytes', 0) / 1024)
        self.registration_overhead.append(components.get('registration', {}).get('total_bytes', 0) / 1024)
        self.mapping_overhead.append(components.get('mapping', {}).get('total_bytes', 0) / 1024)
        self.migration_overhead.append(components.get('migration', {}).get('total_bytes', 0) / 1024)
        self.stats_overhead.append(components.get('stats', {}).get('total_bytes', 0) / 1024)

        # Performance metrics
        self.efficiency_ratio.append(self.current_data.get('efficiency_ratio', 0))

        perf = self.current_data.get('performance_metrics', {})
        utilizations = perf.get('broker_utilizations', [])
        if utilizations:
            avg_util = sum(sum(u) / len(u) for u in utilizations) / len(utilizations)
            self.avg_utilization.append(avg_util * 100)  # Convert to percentage

        variances = perf.get('load_imbalance_variance', [])
        if variances:
            self.load_variance.append(variances[-1])

    def print_console_report(self):
        """Print text-based report to console"""
        if not self.current_data:
            print("No data available yet...")
            return

        os.system('clear' if os.name == 'posix' else 'cls')

        print("=" * 80)
        print(" MQTT LOAD BALANCING - OVERHEAD MONITORING DASHBOARD".center(80))
        print("=" * 80)
        print(f"Last Update: {self.last_update.strftime('%Y-%m-%d %H:%M:%S')}" if self.last_update else "")
        print()

        breakdown = self.current_data.get('breakdown', {})
        components = breakdown.get('components', {})

        # Summary metrics
        print("SUMMARY METRICS")
        print("-" * 80)
        print(f"Total Overhead:       {breakdown.get('total_overhead_mb', 0):.2f} MB")
        print(f"κ Coefficient:        {breakdown.get('kappa_coefficient', 0)}")
        print(f"Migration Cycles:     {breakdown.get('migration_cycles', 0)}")
        print(f"Avg Migrations/Cycle: {breakdown.get('avg_migrations_per_cycle', 0):.1f}")
        print(f"Efficiency Ratio:     {self.current_data.get('efficiency_ratio', 0):.2f} msg/KB")
        print()

        # Overhead breakdown
        print("OVERHEAD BREAKDOWN")
        print("-" * 80)
        print(f"{'Component':<20} {'Messages':<12} {'Total (KB)':<15} {'Avg (bytes)':<15} {'%':<8}")
        print("-" * 80)

        for comp_name, comp_data in components.items():
            print(f"{comp_name.upper():<20} "
                  f"{comp_data.get('count', 0):<12} "
                  f"{comp_data.get('total_bytes', 0) / 1024:<15.2f} "
                  f"{comp_data.get('avg_size_bytes', 0):<15.0f} "
                  f"{comp_data.get('percentage', 0):<8.1f}")
        print()

        # Performance metrics
        perf = self.current_data.get('performance_metrics', {})
        print("PERFORMANCE METRICS")
        print("-" * 80)
        print(f"Messages Delivered:   {perf.get('total_messages_delivered', 0)}")

        utilizations = perf.get('broker_utilizations', [])
        if utilizations:
            avg_util = sum(sum(u) / len(u) for u in utilizations) / len(utilizations)
            print(f"Avg Broker Util:      {avg_util:.2%}")

        variances = perf.get('load_imbalance_variance', [])
        if variances:
            print(f"Load Variance:        {variances[-1]:.4f}")

        print()

        # Migration cycles detail
        cycles = self.current_data.get('migration_cycles', [])
        if cycles:
            print("RECENT MIGRATION CYCLES")
            print("-" * 80)
            print(f"{'Timestamp':<20} {'Duration (s)':<15} {'Migrations':<12}")
            print("-" * 80)
            for cycle in cycles[-5:]:  # Last 5 cycles
                ts = datetime.fromtimestamp(cycle['timestamp']).strftime('%H:%M:%S')
                print(f"{ts:<20} {cycle['duration']:<15.2f} {cycle['num_migrations']:<12}")

        print("=" * 80)
        print(f"Press Ctrl+C to exit")
        print("=" * 80)

    def create_visualization(self):
        """Create matplotlib visualization"""
        if not MATPLOTLIB_AVAILABLE:
            print("Matplotlib not available. Using console mode only.")
            return None

        # Create figure with subplots
        fig = plt.figure(figsize=(16, 10))
        fig.suptitle('MQTT Load Balancing - Overhead Monitoring', fontsize=16, fontweight='bold')

        gs = GridSpec(3, 2, figure=fig, hspace=0.3, wspace=0.3)

        # Subplot 1: Total overhead over time
        ax1 = fig.add_subplot(gs[0, :])
        ax1.set_title('Total Coordination Overhead Over Time')
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Overhead (KB)')
        ax1.grid(True, alpha=0.3)

        # Subplot 2: Overhead breakdown
        ax2 = fig.add_subplot(gs[1, 0])
        ax2.set_title('Overhead Component Breakdown')
        ax2.set_ylabel('Overhead (KB)')
        ax2.grid(True, alpha=0.3)

        # Subplot 3: Efficiency ratio
        ax3 = fig.add_subplot(gs[1, 1])
        ax3.set_title('Efficiency Ratio (Higher is Better)')
        ax3.set_xlabel('Time')
        ax3.set_ylabel('Messages per KB Overhead')
        ax3.grid(True, alpha=0.3)

        # Subplot 4: Broker utilization
        ax4 = fig.add_subplot(gs[2, 0])
        ax4.set_title('Average Broker Utilization')
        ax4.set_xlabel('Time')
        ax4.set_ylabel('Utilization (%)')
        ax4.grid(True, alpha=0.3)

        # Subplot 5: Load variance
        ax5 = fig.add_subplot(gs[2, 1])
        ax5.set_title('Load Imbalance Variance (Lower is Better)')
        ax5.set_xlabel('Time')
        ax5.set_ylabel('Variance')
        ax5.grid(True, alpha=0.3)

        return fig, (ax1, ax2, ax3, ax4, ax5)

    def update_plots(self, frame, axes):
        """Update all plots"""
        self.load_data()
        self.update_time_series()

        if len(self.timestamps) < 2:
            return

        ax1, ax2, ax3, ax4, ax5 = axes

        # Clear all axes
        for ax in axes:
            ax.clear()
            ax.grid(True, alpha=0.3)

        # Plot 1: Total overhead
        ax1.plot(list(self.timestamps), list(self.total_overhead), 'b-', linewidth=2, label='Total')
        ax1.set_title('Total Coordination Overhead Over Time')
        ax1.set_ylabel('Overhead (KB)')
        ax1.legend()
        ax1.tick_params(axis='x', rotation=45)

        # Plot 2: Overhead breakdown (stacked area)
        if len(self.timestamps) > 0:
            ax2.stackplot(
                list(self.timestamps),
                list(self.registration_overhead),
                list(self.mapping_overhead),
                list(self.migration_overhead),
                list(self.stats_overhead),
                labels=['Registration (Ω₁)', 'Mapping (Ω₂)', 'Migration (Ω₃)', 'Stats'],
                alpha=0.7
            )
            ax2.set_title('Overhead Component Breakdown')
            ax2.set_ylabel('Overhead (KB)')
            ax2.legend(loc='upper left')
            ax2.tick_params(axis='x', rotation=45)

        # Plot 3: Efficiency ratio
        if len(self.efficiency_ratio) > 0:
            ax3.plot(list(self.timestamps), list(self.efficiency_ratio), 'g-', linewidth=2)
            ax3.set_title('Efficiency Ratio (Higher is Better)')
            ax3.set_ylabel('Messages per KB Overhead')
            ax3.tick_params(axis='x', rotation=45)

            # Add trend line
            if len(self.efficiency_ratio) > 5:
                from numpy import polyfit, poly1d
                x = range(len(self.efficiency_ratio))
                y = list(self.efficiency_ratio)
                z = polyfit(x, y, 1)
                p = poly1d(z)
                ax3.plot(list(self.timestamps), p(x), 'r--', linewidth=1, alpha=0.5, label='Trend')
                ax3.legend()

        # Plot 4: Broker utilization
        if len(self.avg_utilization) > 0:
            ax4.plot(list(self.timestamps), list(self.avg_utilization), 'm-', linewidth=2)
            ax4.axhline(y=22, color='r', linestyle='--', label='Target (22%)')
            ax4.set_title('Average Broker Utilization')
            ax4.set_ylabel('Utilization (%)')
            ax4.legend()
            ax4.tick_params(axis='x', rotation=45)

        # Plot 5: Load variance
        if len(self.load_variance) > 0:
            ax5.plot(list(self.timestamps), list(self.load_variance), 'orange', linewidth=2)
            ax5.set_title('Load Imbalance Variance (Lower is Better)')
            ax5.set_ylabel('Variance')
            ax5.tick_params(axis='x', rotation=45)

        plt.tight_layout()

    def run_console_mode(self):
        """Run in console text mode"""
        print("Starting console monitoring mode...")
        print("Data file:", self.data_file)
        print()

        try:
            while True:
                if self.load_data():
                    self.update_time_series()
                    self.print_console_report()
                else:
                    print("Waiting for data...")
                time.sleep(5)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")

    def run_gui_mode(self):
        """Run with matplotlib GUI"""
        if not MATPLOTLIB_AVAILABLE:
            print("Matplotlib not available. Falling back to console mode.")
            return self.run_console_mode()

        print("Starting GUI monitoring mode...")
        print("Data file:", self.data_file)

        fig, axes = self.create_visualization()

        # Animate
        ani = animation.FuncAnimation(
            fig,
            self.update_plots,
            fargs=(axes,),
            interval=5000,  # Update every 5 seconds
            cache_frame_data=False
        )

        plt.show()


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='MQTT Load Balancing Overhead Monitor')
    parser.add_argument('--mode', choices=['console', 'gui'], default='console',
                        help='Display mode: console (text) or gui (matplotlib)')
    parser.add_argument('--data-file', default='/app/overhead_data.json',
                        help='Path to overhead data JSON file')
    parser.add_argument('--history', type=int, default=100,
                        help='Number of data points to keep in history')

    args = parser.parse_args()

    monitor = OverheadMonitor(data_file=args.data_file, history_size=args.history)

    if args.mode == 'gui':
        monitor.run_gui_mode()
    else:
        monitor.run_console_mode()


if __name__ == "__main__":
    main()