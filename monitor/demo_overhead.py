#!/usr/bin/env python3
"""
Automated Demo Script for Overhead Monitoring
Demonstrates the trade-off between overhead and performance
Perfect for class presentations
"""

import subprocess
import time
import sys
import os
from datetime import datetime
import json


class DemoController:
    """Controls the demonstration flow"""

    def __init__(self):
        self.demo_step = 0
        self.overhead_data_file = "coordinator/overhead_data.json"

    def print_header(self, title):
        """Print a formatted header"""
        print("\n" + "=" * 80)
        print(f" {title}".center(80))
        print("=" * 80 + "\n")

    def print_step(self, description):
        """Print a demo step"""
        self.demo_step += 1
        print(f"\n[Step {self.demo_step}] {description}")
        print("-" * 80)

    def wait_for_user(self, message="Press Enter to continue..."):
        """Pause for user interaction"""
        input(f"\n{message}")

    def run_command(self, command, description=None):
        """Run a command and display output"""
        if description:
            print(f"\nRunning: {description}")
        print(f"Command: {command}\n")

        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            if result.stdout:
                print(result.stdout)
            if result.returncode != 0 and result.stderr:
                print(f"Error: {result.stderr}")
            return result.returncode == 0
        except Exception as e:
            print(f"Error executing command: {e}")
            return False

    def check_prerequisites(self):
        """Check if system is ready"""
        self.print_header("CHECKING PREREQUISITES")

        checks = [
            ("docker --version", "Docker"),
            ("docker-compose --version", "Docker Compose"),
        ]

        all_good = True
        for cmd, name in checks:
            result = subprocess.run(cmd, shell=True, capture_output=True)
            if result.returncode == 0:
                print(f"✓ {name} is installed")
            else:
                print(f"✗ {name} is NOT installed")
                all_good = False

        if not all_good:
            print("\nPlease install missing prerequisites before continuing.")
            sys.exit(1)

        print("\n✓ All prerequisites satisfied!")

    def start_system(self):
        """Start the MQTT load balancing system"""
        self.print_header("STARTING MQTT LOAD BALANCING SYSTEM")

        print("This will start:")
        print("  • 4 MQTT brokers (different capacities)")
        print("  • Coordination service with overhead tracking")
        print("  • 5 publishers (generating traffic on 20 topics)")
        print("  • 20 subscribers (with varying subscription patterns)")
        print()

        self.wait_for_user("Press Enter to start the system...")

        self.run_command(
            "docker-compose up -d",
            "Starting Docker containers"
        )

        print("\nWaiting for services to initialize (15 seconds)...")
        for i in range(15, 0, -1):
            print(f"\r{i} seconds remaining... ", end="", flush=True)
            time.sleep(1)
        print("\r✓ Services started!                    ")

    def demo_overhead_components(self):
        """Demonstrate the three overhead components"""
        self.print_header("DEMONSTRATION 1: OVERHEAD COMPONENTS")

        print("The paper identifies three types of coordination overhead:")
        print()
        print("  Ω₁ (Registration): Initial client registration messages")
        print("  Ω₂ (Mapping):      Client-to-broker mapping messages")
        print("  Ω₃ (Migration):    Topic re-assignment and Trie updates")
        print()
        print("Let's watch these in real-time...")
        print()

        self.wait_for_user()

        # Show overhead monitoring
        self.run_command(
            "docker-compose exec -T coordinator python overhead_monitor.py --mode console 2>/dev/null | head -50",
            "Real-time overhead monitoring (5-second snapshot)"
        )

        print("\nNotice:")
        print("  • Registration overhead (Ω₁) is highest initially")
        print("  • Mapping overhead (Ω₂) occurs during load balancing")
        print("  • Migration overhead (Ω₃) includes Trie data structure updates")
        print()

        self.wait_for_user("Press Enter to continue to the next demonstration...")

    def demo_tradeoff_analysis(self):
        """Demonstrate the trade-off between overhead and performance"""
        self.print_header("DEMONSTRATION 2: OVERHEAD vs PERFORMANCE TRADE-OFF")

        print("Section IV-B-11 of the paper discusses the trade-off:")
        print("  Higher overhead → Better load balancing → Better performance")
        print()
        print("Let's analyze this trade-off in our implementation...")
        print()

        self.wait_for_user()

        # Generate comparison report
        self.run_command(
            "docker-compose exec -T coordinator python overhead_comparison.py 2>/dev/null",
            "Generating trade-off analysis"
        )

        print("\nKey Observations:")
        print("  1. Efficiency Ratio shows messages delivered per KB of overhead")
        print("  2. Target utilization from paper is ~22%")
        print("  3. Load variance indicates how well-balanced the system is")
        print()

        self.wait_for_user("Press Enter to continue...")

    def demo_overhead_growth(self):
        """Demonstrate how overhead grows with system size"""
        self.print_header("DEMONSTRATION 3: OVERHEAD SCALING")

        print("The paper shows overhead = κ(Ω₁ + Ω₂ + Ω₃) + periodic")
        print("where κ = topic_size × num_brokers")
        print()
        print("This means:")
        print("  • More brokers → Higher overhead coefficient")
        print("  • Larger topics → Higher overhead per operation")
        print()
        print("Let's examine our system's κ value and overhead breakdown...")
        print()

        self.wait_for_user()

        # Check if data file exists
        if os.path.exists(self.overhead_data_file):
            try:
                with open(self.overhead_data_file) as f:
                    data = json.load(f)
                    breakdown = data.get('breakdown', {})

                    print("\nCurrent System Configuration:")
                    print(f"  Number of Brokers:    4")
                    print(f"  Avg Topic Size:       550 KB")
                    print(f"  κ Coefficient:        {breakdown.get('kappa_coefficient', 'N/A')}")
                    print()
                    print(f"  Total Overhead:       {breakdown.get('total_overhead_mb', 0):.2f} MB")
                    print(f"  Migration Cycles:     {breakdown.get('migration_cycles', 0)}")
                    print()

                    components = breakdown.get('components', {})
                    for comp, data in components.items():
                        pct = data.get('percentage', 0)
                        print(f"  {comp.capitalize():<15} {pct:>6.1f}%")

            except Exception as e:
                print(f"Could not load overhead data: {e}")
        else:
            print("Overhead data not yet available. System still initializing...")

        print()
        self.wait_for_user("Press Enter to continue...")

    def demo_paper_comparison(self):
        """Compare results with paper"""
        self.print_header("DEMONSTRATION 4: COMPARISON WITH PAPER RESULTS")

        print("The paper reports:")
        print("  • 11% reduction in client waiting time")
        print("  • 20% more even load distribution")
        print("  • ~22% average server utilization")
        print()
        print("Let's see how our implementation compares...")
        print()

        self.wait_for_user()

        if os.path.exists(self.overhead_data_file):
            try:
                with open(self.overhead_data_file) as f:
                    data = json.load(f)
                    perf = data.get('performance_metrics', {})

                    # Calculate metrics
                    utilizations = perf.get('broker_utilizations', [])
                    if utilizations:
                        all_utils = []
                        for util_snapshot in utilizations:
                            all_utils.extend(util_snapshot)
                        avg_util = sum(all_utils) / len(all_utils) if all_utils else 0
                    else:
                        avg_util = 0

                    variances = perf.get('load_imbalance_variance', [])
                    avg_variance = sum(variances) / len(variances) if variances else 0

                    print("\nOur Implementation:")
                    print(f"  Average Utilization:  {avg_util:.2%}")
                    print(f"  Load Variance:        {avg_variance:.4f}")
                    print(f"  Messages Delivered:   {perf.get('total_messages_delivered', 0):,}")
                    print()

                    # Comparison
                    util_diff = abs(0.22 - avg_util)
                    if util_diff < 0.05:
                        print("  ✓ Utilization matches paper target (22%)")
                    else:
                        print(f"  ○ Utilization differs by {util_diff:.2%} from target")

                    if avg_variance < 0.01:
                        print("  ✓ Load distribution is well-balanced")
                    else:
                        print(f"  ○ Load variance: {avg_variance:.4f}")

            except Exception as e:
                print(f"Could not load data: {e}")
        else:
            print("Data not yet available...")

        print()
        self.wait_for_user("Press Enter to continue...")

    def demo_live_monitoring(self):
        """Show live monitoring in action"""
        self.print_header("DEMONSTRATION 5: LIVE MONITORING")

        print("Now let's watch the system in action with live monitoring.")
        print()
        print("You'll see:")
        print("  • Overhead accumulating over time")
        print("  • Hot topic detection cycles")
        print("  • Migration events")
        print("  • Performance metrics updating")
        print()
        print("The monitor will update every 5 seconds.")
        print("Press Ctrl+C to stop monitoring.")
        print()

        self.wait_for_user("Press Enter to start live monitoring...")

        try:
            subprocess.run(
                "docker-compose exec coordinator python overhead_monitor.py --mode console",
                shell=True
            )
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")

    def generate_report(self):
        """Generate final report"""
        self.print_header("GENERATING FINAL REPORT")

        print("Creating comprehensive overhead analysis report...")
        print()

        # Export comparison
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_file = f"overhead_report_{timestamp}.json"

        self.run_command(
            f"docker-compose exec -T coordinator python overhead_comparison.py --export /app/{export_file} 2>/dev/null",
            "Exporting comparison data"
        )

        print(f"\nReport saved to: coordinator/{export_file}")
        print()

        # Generate LaTeX
        print("LaTeX table for academic presentation:")
        print()
        self.run_command(
            "docker-compose exec -T coordinator python overhead_comparison.py --latex 2>/dev/null | tail -20",
            "Generating LaTeX table"
        )

    def cleanup(self):
        """Clean up demo"""
        self.print_header("DEMO COMPLETE")

        print("Would you like to:")
        print("  1. Keep system running for further exploration")
        print("  2. Stop all services")
        print()

        choice = input("Enter choice (1 or 2): ").strip()

        if choice == '2':
            print("\nStopping all services...")
            self.run_command("docker-compose down", "Shutting down")
            print("\n✓ All services stopped")
        else:
            print("\nServices are still running. You can:")
            print("  • View logs: docker-compose logs -f")
            print("  • Monitor overhead: docker-compose exec coordinator python overhead_monitor.py")
            print("  • Stop later: docker-compose down")

        print("\n" + "=" * 80)
        print("Thank you for watching the demonstration!")
        print("=" * 80 + "\n")

    def run_full_demo(self):
        """Run the complete demonstration"""
        print("\n" + "=" * 80)
        print(" MQTT LOAD BALANCING - OVERHEAD MONITORING DEMONSTRATION".center(80))
        print("=" * 80)
        print("\nThis demo will showcase:")
        print("  1. Overhead component identification (Ω₁, Ω₂, Ω₃)")
        print("  2. Trade-off analysis (overhead vs performance)")
        print("  3. Overhead scaling with system size")
        print("  4. Comparison with paper results")
        print("  5. Live monitoring")
        print()
        print("Estimated duration: 10-15 minutes")
        print()

        self.wait_for_user("Press Enter to begin the demonstration...")

        try:
            # Run demonstration steps
            self.check_prerequisites()
            self.start_system()
            self.demo_overhead_components()
            self.demo_tradeoff_analysis()
            self.demo_overhead_growth()
            self.demo_paper_comparison()

            # Ask if user wants live monitoring
            print("\nWould you like to see live monitoring? (y/n): ", end="")
            if input().lower().strip() == 'y':
                self.demo_live_monitoring()

            self.generate_report()
            self.cleanup()

        except KeyboardInterrupt:
            print("\n\nDemo interrupted by user.")
            self.cleanup()
        except Exception as e:
            print(f"\n\nError during demo: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Main entry point"""
    demo = DemoController()

    if len(sys.argv) > 1 and sys.argv[1] == '--quick':
        print("Running quick demo (monitoring only)...")
        demo.check_prerequisites()
        demo.start_system()
        demo.demo_live_monitoring()
        demo.cleanup()
    else:
        demo.run_full_demo()


if __name__ == "__main__":
    main()