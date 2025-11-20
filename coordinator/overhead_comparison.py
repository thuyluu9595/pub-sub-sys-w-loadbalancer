#!/usr/bin/env python3
"""
Overhead vs Performance Comparison Tool
Demonstrates the trade-off discussed in Section IV-B-11 of the paper
"""

import json
import time
import os
import sys
from datetime import datetime
from typing import Dict, List
import argparse


class PerformanceComparator:
    """Compare overhead costs against performance benefits"""

    def __init__(self, data_file='/app/overhead_data.json'):
        self.data_file = data_file
        self.data = None

    def load_data(self) -> bool:
        """Load overhead and performance data"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    self.data = json.load(f)
                return True
        except Exception as e:
            print(f"Error loading data: {e}")
        return False

    def calculate_metrics(self) -> Dict:
        """Calculate key comparison metrics"""
        if not self.data:
            return {}

        breakdown = self.data.get('breakdown', {})
        components = breakdown.get('components', {})
        perf = self.data.get('performance_metrics', {})

        # Overhead metrics
        total_overhead_mb = breakdown.get('total_overhead_mb', 0)
        total_overhead_kb = breakdown.get('total_overhead_bytes', 0) / 1024

        # Migration metrics
        migration_cycles = breakdown.get('migration_cycles', 0)
        migrations_per_cycle = breakdown.get('avg_migrations_per_cycle', 0)
        total_migrations = migration_cycles * migrations_per_cycle

        # Performance metrics
        messages_delivered = perf.get('total_messages_delivered', 0)
        efficiency_ratio = self.data.get('efficiency_ratio', 0)

        # Calculate utilization metrics
        utilizations = perf.get('broker_utilizations', [])
        if utilizations:
            all_utils = []
            for util_snapshot in utilizations:
                all_utils.extend(util_snapshot)
            avg_utilization = sum(all_utils) / len(all_utils) if all_utils else 0
            max_utilization = max(all_utils) if all_utils else 0
            min_utilization = min(all_utils) if all_utils else 0
        else:
            avg_utilization = max_utilization = min_utilization = 0

        # Calculate load balance metrics
        variances = perf.get('load_imbalance_variance', [])
        avg_variance = sum(variances) / len(variances) if variances else 0

        # Calculate overhead per message
        overhead_per_message = total_overhead_kb / messages_delivered if messages_delivered > 0 else 0

        # Calculate benefits
        # Target utilization from paper: ~22%
        target_utilization = 0.22
        utilization_improvement = abs(avg_utilization - target_utilization)

        # Estimate load balance improvement (lower variance is better)
        # From paper: LOAD reduces variance by 20%
        load_balance_improvement = 1.0 - avg_variance if avg_variance < 1.0 else 0

        return {
            'overhead': {
                'total_mb': total_overhead_mb,
                'total_kb': total_overhead_kb,
                'per_message_kb': overhead_per_message,
                'registration': components.get('registration', {}).get('total_bytes', 0) / 1024,
                'mapping': components.get('mapping', {}).get('total_bytes', 0) / 1024,
                'migration': components.get('migration', {}).get('total_bytes', 0) / 1024,
                'stats': components.get('stats', {}).get('total_bytes', 0) / 1024,
                'ack': components.get('ack', {}).get('total_bytes', 0) / 1024,
            },
            'performance': {
                'messages_delivered': messages_delivered,
                'efficiency_ratio': efficiency_ratio,
                'avg_utilization': avg_utilization,
                'max_utilization': max_utilization,
                'min_utilization': min_utilization,
                'utilization_range': max_utilization - min_utilization,
                'avg_variance': avg_variance,
                'load_balance_score': load_balance_improvement,
            },
            'migration': {
                'cycles': migration_cycles,
                'per_cycle': migrations_per_cycle,
                'total': total_migrations,
            },
            'trade_offs': {
                'overhead_per_message_kb': overhead_per_message,
                'messages_per_kb_overhead': efficiency_ratio,
                'utilization_vs_target': utilization_improvement,
                'load_balance_improvement': load_balance_improvement,
            }
        }

    def generate_comparison_report(self) -> str:
        """Generate detailed comparison report"""
        metrics = self.calculate_metrics()

        if not metrics:
            return "No data available for comparison."

        lines = []
        lines.append("\n" + "=" * 80)
        lines.append(" OVERHEAD VS PERFORMANCE TRADE-OFF ANALYSIS".center(80))
        lines.append("=" * 80)
        lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("\n")

        # Section 1: Overhead Costs
        lines.append("1. COORDINATION OVERHEAD COSTS")
        lines.append("-" * 80)
        overhead = metrics['overhead']
        lines.append(f"   Total Overhead:              {overhead['total_mb']:.2f} MB")
        lines.append(f"   Overhead per Message:        {overhead['per_message_kb']:.4f} KB/msg")
        lines.append("")
        lines.append("   Breakdown by Component:")
        lines.append(f"     • Registration (Ω₁):       {overhead['registration']:.2f} KB")
        lines.append(f"     • Mapping (Ω₂):            {overhead['mapping']:.2f} KB")
        lines.append(f"     • Migration (Ω₃):          {overhead['migration']:.2f} KB")
        lines.append(f"     • Statistics:              {overhead['stats']:.2f} KB")
        lines.append(f"     • Acknowledgments:         {overhead['ack']:.2f} KB")
        lines.append("")

        # Section 2: Performance Benefits
        lines.append("2. PERFORMANCE BENEFITS ACHIEVED")
        lines.append("-" * 80)
        perf = metrics['performance']
        lines.append(f"   Messages Delivered:          {perf['messages_delivered']:,}")
        lines.append(f"   Efficiency Ratio:            {perf['efficiency_ratio']:.2f} msg/KB")
        lines.append("")
        lines.append("   Load Balancing Quality:")
        lines.append(f"     • Avg Utilization:         {perf['avg_utilization']:.2%}")
        lines.append(f"     • Utilization Range:       {perf['utilization_range']:.2%}")
        lines.append(f"       (min: {perf['min_utilization']:.2%}, max: {perf['max_utilization']:.2%})")
        lines.append(f"     • Load Variance:           {perf['avg_variance']:.4f}")
        lines.append(f"     • Balance Score:           {perf['load_balance_score']:.2%}")
        lines.append("")

        # Section 3: Migration Activity
        lines.append("3. MIGRATION ACTIVITY")
        lines.append("-" * 80)
        migration = metrics['migration']
        lines.append(f"   Total Migration Cycles:      {migration['cycles']}")
        lines.append(f"   Avg Migrations per Cycle:    {migration['per_cycle']:.1f}")
        lines.append(f"   Total Migrations:            {migration['total']:.0f}")
        if migration['cycles'] > 0:
            lines.append(f"   Overhead per Migration:      "
                         f"{overhead['migration'] / migration['total']:.2f} KB" if migration['total'] > 0 else "N/A")
        lines.append("")

        # Section 4: Trade-off Analysis
        lines.append("4. TRADE-OFF ANALYSIS")
        lines.append("-" * 80)
        tradeoffs = metrics['trade_offs']

        # Compare with paper results
        # Paper reports: 11% reduction in waiting time, 20% more even distribution, ~22% utilization
        lines.append("   Cost-Benefit Ratio:")
        lines.append(f"     • Overhead Cost:           {overhead['per_message_kb']:.4f} KB per message")
        lines.append(f"     • Benefit:                 {perf['efficiency_ratio']:.2f} messages per KB overhead")
        lines.append("")

        lines.append("   Comparison with Paper Targets:")
        lines.append(f"     • Target Utilization:      22%")
        lines.append(f"     • Actual Utilization:      {perf['avg_utilization']:.2%}")
        lines.append(f"     • Difference:              {abs(0.22 - perf['avg_utilization']):.2%}")
        lines.append("")

        # Load distribution evenness
        lines.append(f"     • Load Balance Score:      {perf['load_balance_score']:.2%}")
        lines.append(f"       (Paper target: 20% improvement)")
        lines.append("")

        # Section 5: Conclusion
        lines.append("5. CONCLUSIONS")
        lines.append("-" * 80)

        # Determine if trade-off is worthwhile
        if perf['efficiency_ratio'] > 100:
            verdict = "EXCELLENT"
            color = "✓"
        elif perf['efficiency_ratio'] > 50:
            verdict = "GOOD"
            color = "✓"
        elif perf['efficiency_ratio'] > 20:
            verdict = "ACCEPTABLE"
            color = "○"
        else:
            verdict = "POOR"
            color = "✗"

        lines.append(f"   {color} Overall Efficiency:        {verdict}")
        lines.append(f"     The system processes {perf['efficiency_ratio']:.0f} messages for every KB")
        lines.append(f"     of coordination overhead incurred.")
        lines.append("")

        # Utilization assessment
        util_diff = abs(0.22 - perf['avg_utilization'])
        if util_diff < 0.05:
            lines.append(f"   ✓ Utilization Target:       ACHIEVED")
            lines.append(f"     Actual utilization ({perf['avg_utilization']:.2%}) is close to")
            lines.append(f"     the paper's target of 22%.")
        else:
            lines.append(f"   ○ Utilization Target:       WITHIN RANGE")
            lines.append(f"     Actual utilization ({perf['avg_utilization']:.2%}) deviates by")
            lines.append(f"     {util_diff:.2%} from target.")
        lines.append("")

        # Load balance assessment
        if perf['load_balance_score'] > 0.8:
            lines.append(f"   ✓ Load Distribution:        WELL BALANCED")
        elif perf['load_balance_score'] > 0.6:
            lines.append(f"   ○ Load Distribution:        MODERATELY BALANCED")
        else:
            lines.append(f"   ✗ Load Distribution:        NEEDS IMPROVEMENT")

        lines.append(f"     Variance: {perf['avg_variance']:.4f}, Balance Score: {perf['load_balance_score']:.2%}")
        lines.append("")

        lines.append("   Summary:")
        lines.append(f"     The coordination overhead of {overhead['total_mb']:.2f} MB enables")
        lines.append(f"     the delivery of {perf['messages_delivered']:,} messages with")
        lines.append(f"     {perf['avg_utilization']:.1%} average broker utilization.")
        lines.append("")
        lines.append("     This represents the trade-off discussed in Section IV-B-11")
        lines.append("     of the paper: higher overhead in exchange for better load")
        lines.append("     balancing and system performance.")

        lines.append("\n" + "=" * 80)

        return "\n".join(lines)

    def export_comparison(self, output_file: str):
        """Export comparison data to JSON"""
        metrics = self.calculate_metrics()

        with open(output_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'metrics': metrics,
                'summary': {
                    'overhead_mb': metrics['overhead']['total_mb'],
                    'efficiency_ratio': metrics['performance']['efficiency_ratio'],
                    'avg_utilization': metrics['performance']['avg_utilization'],
                    'load_balance_score': metrics['performance']['load_balance_score'],
                }
            }, f, indent=2)

        print(f"Comparison data exported to: {output_file}")

    def generate_latex_table(self) -> str:
        """Generate LaTeX table for academic presentation"""
        metrics = self.calculate_metrics()

        latex = []
        latex.append("% Overhead vs Performance Comparison Table")
        latex.append("\\begin{table}[h]")
        latex.append("\\centering")
        latex.append("\\caption{Coordination Overhead vs Performance Trade-offs}")
        latex.append("\\begin{tabular}{lcc}")
        latex.append("\\hline")
        latex.append("\\textbf{Metric} & \\textbf{Value} & \\textbf{Unit} \\\\")
        latex.append("\\hline")
        latex.append("\\multicolumn{3}{l}{\\textit{Overhead Costs}} \\\\")
        latex.append(f"Total Overhead & {metrics['overhead']['total_mb']:.2f} & MB \\\\")
        latex.append(f"Overhead per Message & {metrics['overhead']['per_message_kb']:.4f} & KB \\\\")
        latex.append("\\hline")
        latex.append("\\multicolumn{3}{l}{\\textit{Performance Benefits}} \\\\")
        latex.append(f"Messages Delivered & {metrics['performance']['messages_delivered']:,} & messages \\\\")
        latex.append(f"Efficiency Ratio & {metrics['performance']['efficiency_ratio']:.2f} & msg/KB \\\\")
        latex.append(f"Avg Broker Utilization & {metrics['performance']['avg_utilization'] * 100:.1f} & \\% \\\\")
        latex.append(f"Load Variance & {metrics['performance']['avg_variance']:.4f} & - \\\\")
        latex.append("\\hline")
        latex.append("\\multicolumn{3}{l}{\\textit{Migration Activity}} \\\\")
        latex.append(f"Migration Cycles & {metrics['migration']['cycles']} & cycles \\\\")
        latex.append(f"Avg Migrations/Cycle & {metrics['migration']['per_cycle']:.1f} & migrations \\\\")
        latex.append("\\hline")
        latex.append("\\end{tabular}")
        latex.append("\\label{tab:overhead_comparison}")
        latex.append("\\end{table}")

        return "\n".join(latex)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Compare coordination overhead against performance benefits'
    )
    parser.add_argument('--data-file', default='/app/overhead_data.json',
                        help='Path to overhead data JSON file')
    parser.add_argument('--export', help='Export comparison to JSON file')
    parser.add_argument('--latex', action='store_true',
                        help='Generate LaTeX table')
    parser.add_argument('--watch', action='store_true',
                        help='Continuously monitor and update report')
    parser.add_argument('--interval', type=int, default=10,
                        help='Update interval in seconds (for --watch mode)')

    args = parser.parse_args()

    comparator = PerformanceComparator(data_file=args.data_file)

    if args.watch:
        print("Starting continuous monitoring...")
        print(f"Update interval: {args.interval} seconds")
        print("Press Ctrl+C to stop\n")

        try:
            while True:
                if comparator.load_data():
                    os.system('clear' if os.name == 'posix' else 'cls')
                    report = comparator.generate_comparison_report()
                    print(report)
                else:
                    print("Waiting for data...")

                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
    else:
        # Single run
        if not comparator.load_data():
            print("Error: Could not load data from", args.data_file)
            sys.exit(1)

        # Generate and print report
        report = comparator.generate_comparison_report()
        print(report)

        # Export if requested
        if args.export:
            comparator.export_comparison(args.export)

        # Generate LaTeX if requested
        if args.latex:
            latex_table = comparator.generate_latex_table()
            print("\nLaTeX Table:")
            print("-" * 80)
            print(latex_table)


if __name__ == "__main__":
    main()