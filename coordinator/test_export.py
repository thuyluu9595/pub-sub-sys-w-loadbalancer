#!/usr/bin/env python3
"""
Test overhead tracker export functionality
"""

import sys

sys.path.insert(0, '/app')

print('Testing overhead tracker export...')

try:
    from overhead_tracker import OverheadTracker

    # Create tracker
    tracker = OverheadTracker(num_brokers=4, avg_topic_size_kb=550)

    # Add some test data
    tracker.track_registration('test', 5, 1024)
    tracker.track_mapping('test', 'test/topic', 512)
    tracker.update_performance_metrics(messages_delivered=100)

    # Try to export
    print('Attempting export to /app/overhead_data.json...')
    tracker.export_json('/app/overhead_data.json')

    print('SUCCESS: Export succeeded!')

    # Check if file exists
    import os

    if os.path.exists('/app/overhead_data.json'):
        size = os.path.getsize('/app/overhead_data.json')
        print(f'SUCCESS: File exists! Size: {size} bytes')
    else:
        print('ERROR: File was not created!')

except Exception as e:
    print(f'ERROR: {e}')
    import traceback

    traceback.print_exc()