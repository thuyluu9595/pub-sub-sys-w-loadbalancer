#!/usr/bin/env python3
"""
Validation Script - Verify the load balancing system is working
Checks for registration, statistics, hot topic detection, and migration
"""

import subprocess
import time
import re
import sys


# --- bcolors class for console colors ---
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


# --- Print helper functions ---

def print_header(text):
    print(f"\n{bcolors.HEADER}{bcolors.BOLD}{'=' * 70}{bcolors.ENDC}")
    print(f"{bcolors.HEADER}{bcolors.BOLD}{text.center(70)}{bcolors.ENDC}")
    print(f"{bcolors.HEADER}{bcolors.BOLD}{'=' * 70}{bcolors.ENDC}\n")


def print_ok(text):
    print(f"{bcolors.OKGREEN}✓ {text}{bcolors.ENDC}")


def print_fail(text):
    print(f"{bcolors.FAIL}✗ {text}{bcolors.ENDC}")


def print_warning(text):
    print(f"{bcolors.WARNING}⚠ {text}{bcolors.ENDC}")


def print_info(text):
    print(f"{bcolors.OKCYAN}ℹ {text}{bcolors.ENDC}")


# --- Core functions ---

def get_logs(service, tail=500):
    """Get logs from a service"""
    try:
        result = subprocess.run(
            ['docker-compose', 'logs', '--tail', str(tail), service],
            capture_output=True,
            text=True,
            timeout=10,
            check=True
        )
        return result.stdout
    except Exception as e:
        print_fail(f"Failed to get logs for {service}: {e}")
        return ""


def check_containers_running():
    """Check if all containers are running"""
    print_header("STEP 1: Container Status")

    try:
        result = subprocess.run(
            ['docker-compose', 'ps', '--services'],
            capture_output=True,
            text=True,
            check=True
        )
        services = result.stdout.strip().split('\n')

        result = subprocess.run(
            ['docker-compose', 'ps'],
            capture_output=True,
            text=True,
            check=True
        )
        ps_output = result.stdout

        required = ['broker1', 'broker2', 'broker3', 'broker4', 'coordinator', 'publisher', 'subscriber']
        running_count = 0

        for container in required:
            if container not in services:
                print_fail(f"{container} is not defined in services")
                continue

            # Check if container is present and 'Up'
            if re.search(r"\b" + re.escape(container) + r"\b.*(Up|Running)", ps_output, re.IGNORECASE):
                print_ok(f"{container} is running")
                running_count += 1
            else:
                print_fail(f"{container} is NOT running")

        if running_count == len(required):
            print_ok(f"All {len(required)} containers are running")
            return True
        else:
            print_fail(f"Only {running_count}/{len(required)} containers running")
            return False

    except Exception as e:
        print_fail(f"Failed to check containers: {e}")
        return False


def check_registration():
    """Check if clients registered with coordinator"""
    print_header("STEP 2: Client Registration")

    logs = get_logs('coordinator')

    # Check for publisher registrations
    pub_pattern = r"Registered publisher (\w+) with (\d+) topics"
    pub_matches = re.findall(pub_pattern, logs)

    if pub_matches:
        print_ok(f"Found {len(pub_matches)} publisher registrations:")
        for pub_id, num_topics in pub_matches[-3:]:  # Log last 3
            print_info(f"  - {pub_id}: {num_topics} topics")
    else:
        print_fail("No publisher registrations found")
        print_warning("Publishers may not have connected yet")

    # Check for subscriber registrations
    sub_pattern = r"Registered subscriber (\w+) with (\d+) topics"
    sub_matches = re.findall(sub_pattern, logs)

    if sub_matches:
        print_ok(f"Found {len(sub_matches)} subscriber registrations:")
        for sub_id, num_topics in sub_matches[-3:]:  # Log last 3
            print_info(f"  - {sub_id}: {num_topics} topics")
    else:
        print_fail("No subscriber registrations found")
        print_warning("Subscribers may not have connected yet")

    return len(pub_matches) > 0 and len(sub_matches) > 0


def check_statistics():
    """Check if coordinator is receiving statistics"""
    print_header("STEP 3: Statistics Collection")

    logs = get_logs('coordinator')

    # Check for LoOP scores (indicates statistics were collected)
    loop_pattern = r"LoOP Scores: (\{[^}]+\})"
    loop_matches = re.findall(loop_pattern, logs)

    if loop_matches:
        print_ok(f"Found {len(loop_matches)} LoOP score calculations")
        print_info(f"Latest: {loop_matches[-1][:100]}...")
        return True
    else:
        print_fail("No LoOP scores found")
        print_warning("System may need more time to collect statistics")
        print_info("Tip: Wait 30-60 seconds after startup")
        return False


def check_hot_topics():
    """Check if hot topics were detected"""
    print_header("STEP 4: Hot Topic Detection")

    logs = get_logs('coordinator')

    # Check for hot topic detection
    hot_pattern = r"Hot Topics detected: (\{[^}]*\}|set\(\))"
    hot_matches = re.findall(hot_pattern, logs)

    if hot_matches:
        print_ok(f"Found {len(hot_matches)} hot topic detections")

        # Find non-empty detections
        non_empty = [m for m in hot_matches if 'set()' not in m and m != '{}']

        if non_empty:
            print_ok(f"Hot topics were detected in {len(non_empty)} cycles:")
            print_info(f"Latest: {non_empty[-1]}")
            return True
        else:
            print_warning("No *active* hot topics detected.")
            print_info("This might be OK if the load is naturally balanced.")
            return True  # Not a failure, the check ran
    else:
        print_fail("No hot topic detection logs found.")
        return False


def check_migration_commands():
    """Check if coordinator sent migration commands"""
    print_header("STEP 5: Coordinator Migration Commands")
    logs = get_logs('coordinator')

    # Pattern from coordinator.py: logger.info(f"Migrating {client_id} to {target_broker.host}")
    pattern = r"Migrating (\S+) to (\S+)"
    matches = re.findall(pattern, logs)

    if matches:
        print_ok(f"Found {len(matches)} migration commands sent by coordinator.")
        # Log a few recent ones
        recent_migrations = {}
        for client, broker in matches:
            recent_migrations[client] = broker

        print_info("Recent migrations (client -> broker):")
        for client, broker in list(recent_migrations.items())[-5:]:  # Get last 5 unique
            print_info(f"  - {client} -> {broker}")
        return True
    else:
        print_fail("No migration commands found in coordinator logs.")
        print_warning("Balancing may not have been triggered, or no migration was needed.")
        return False


def check_client_migration():
    """Check if clients acknowledged migration"""
    print_header("STEP 6: Client Migration Acknowledgment")

    pub_logs = get_logs('publisher')
    sub_logs = get_logs('subscriber')

    # Pattern from publisher/subscriber.py: logger.info(f"Received migration command: {new_host}:{new_port}")
    pattern = r"Received migration command: (\S+)"

    pub_matches = re.findall(pattern, pub_logs)
    sub_matches = re.findall(pattern, sub_logs)

    total_acks = len(pub_matches) + len(sub_matches)

    if pub_matches:
        print_ok(f"Found {len(pub_matches)} migration ACKs from publishers.")
    else:
        print_warning("No publisher migration ACKs found.")

    if sub_matches:
        print_ok(f"Found {len(sub_matches)} migration ACKs from subscribers.")
    else:
        print_warning("No subscriber migration ACKs found.")

    if total_acks > 0:
        print_ok(f"Total {total_acks} migration ACKs found across clients.")
        return True
    else:
        print_fail("No clients acknowledged migration.")
        print_warning("This is expected if no migration commands were sent.")
        return False


def check_message_delivery():
    """Check if subscribers are receiving messages"""
    print_header("STEP 7: Subscriber Message Delivery")
    logs = get_logs('subscriber')

    # Pattern from subscriber.py: logger.info("AGGREGATED STATISTICS")
    # And: logger.info(f"  Avg Latency: {avg_latency:.3f}s")

    if "AGGREGATED STATISTICS" not in logs:
        print_fail("No 'AGGREGATED STATISTICS' block found in subscriber logs.")
        print_warning("Subscribers may not have run long enough to report stats (reports every 30s).")
        return False

    print_ok("Found subscriber 'AGGREGATED STATISTICS' block.")

    # Extract latency info
    pattern = r"Avg Latency: (\d+\.\d+)s"
    matches = re.findall(pattern, logs)

    if matches:
        avg_latencies = [float(l) for l in matches]
        overall_avg = sum(avg_latencies) / len(avg_latencies)

        print_ok(f"Found {len(matches)} latency reports.")
        print_info(f"  - Average reported latency: {overall_avg:.3f}s")
        print_info(f"  - Last reported latency: {avg_latencies[-1]:.3f}s")
        return True
    else:
        print_fail("Found stats block, but no 'Avg Latency' reports.")
        return False


def main():
    """Main validation function"""
    results = {}
    print_header("STARTING EXPERIMENT VALIDATION")

    results['containers'] = check_containers_running()
    if not results['containers']:
        print_fail("Initial check failed. Aborting.")
        sys.exit(1)

    print_info("Waiting 20 seconds for clients to register...")
    time.sleep(20)

    results['registration'] = check_registration()

    print_info("Waiting 15 seconds for first statistics and balancing cycle...")
    time.sleep(15)

    results['statistics'] = check_statistics()
    results['hot_topics'] = check_hot_topics()

    # Only check for migration if stats were found
    if results['statistics']:
        results['migration_sent'] = check_migration_commands()
        # Only check for ACKs if commands were sent
        if results['migration_sent']:
            results['migration_acked'] = check_client_migration()
        else:
            print_warning("Skipping client ACK check as no migration commands were sent.")
            results['migration_acked'] = 'skipped'
    else:
        print_warning("Skipping migration checks as no stats were found.")
        results['migration_sent'] = 'skipped'
        results['migration_acked'] = 'skipped'

    print_info("Waiting 15 seconds for subscriber stats report...")
    time.sleep(15)

    results['delivery'] = check_message_delivery()

    print_header("VALIDATION COMPLETE - FINAL SUMMARY")

    failures = 0
    for check, result in results.items():
        if result == True:
            print_ok(f"Check '{check}' PASSED")
        elif result == 'skipped':
            print_warning(f"Check '{check}' SKIPPED")
        else:
            print_fail(f"Check '{check}' FAILED")
            failures += 1

    if failures > 0:
        print_fail(f"\nValidation finished with {failures} failure(s).")
        sys.exit(1)
    else:
        print_ok("\nAll checks passed! The system appears to be working correctly.")


if __name__ == "__main__":
    main()