#!/usr/bin/env python3
"""
Validation Script - Verify the load balancing system is working
Checks for registration, statistics, hot topic detection, and migration
"""

import subprocess
import time
import re
import sys

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    print(f"\n{bcolors.HEADER}{bcolors.BOLD}{'='*70}{bcolors.ENDC}")
    print(f"{bcolors.HEADER}{bcolors.BOLD}{text.center(70)}{bcolors.ENDC}")
    print(f"{bcolors.HEADER}{bcolors.BOLD}{'='*70}{bcolors.ENDC}\n")

def print_ok(text):
    print(f"{bcolors.OKGREEN}✓ {text}{bcolors.ENDC}")

def print_fail(text):
    print(f"{bcolors.FAIL}✗ {text}{bcolors.ENDC}")

def print_warning(text):
    print(f"{bcolors.WARNING}⚠ {text}{bcolors.ENDC}")

def print_info(text):
    print(f"{bcolors.OKCYAN}ℹ {text}{bcolors.ENDC}")

def get_logs(service, tail=500):
    """Get logs from a service"""
    try:
        result = subprocess.run(
            ['docker-compose', 'logs', '--tail', str(tail), service],
            capture_output=True,
            text=True,
            timeout=10
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
            ['docker-compose', 'ps'],
            capture_output=True,
            text=True
        )
        
        required = ['broker1', 'broker2', 'broker3', 'broker4', 'coordinator', 'publisher', 'subscriber']
        running_count = 0
        
        for container in required:
            if container in result.stdout and 'Up' in result.stdout:
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
        for pub_id, num_topics in pub_matches:
            print_info(f"  - {pub_id}: {num_topics} topics")
    else:
        print_fail("No publisher registrations found")
        print_warning("Publishers may not have connected yet")
    
    # Check for subscriber registrations
    sub_pattern = r"Registered subscriber (\w+) with (\d+) topics"
    sub_matches = re.findall(sub_pattern, logs)
    
    if sub_matches:
        print_ok(f"Found {len(sub_matches)} subscriber registrations:")
        for sub_id, num_topics in sub_matches:
            print_info(f"  - {sub_id}: {num_topics} topics")
    else:
        print_fail("No subscriber registrations found")
        print_warning("Subscribers may not have connected yet")
    
    return len(pub_matches) > 0 or len(sub_matches) > 0

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
            print_warning