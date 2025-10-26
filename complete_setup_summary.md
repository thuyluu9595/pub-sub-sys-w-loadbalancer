# Complete Setup Summary

## Overview

This is a Docker-based implementation of the paper **"Efficient Multi-Broker Load Balancing in Event Driven Pub-Sub Networks"** (IEEE TNSM, Aug 2024).

### What This System Does

1. **Runs 4 MQTT brokers** with different capacities (350-600 Mbps)
2. **Detects hot topics** using LoOP (Local Outlier Probability) algorithm
3. **Balances load** across brokers using Algorithm 1 from the paper
4. **Uses Trie data structure** for efficient topic management
5. **Simulates IoT traffic** with publishers and subscribers

### Expected Results

- ✓ 11% reduction in average waiting time
- ✓ 20% more even load distribution  
- ✓ ~22% average server utilization
- ✓ Automatic hot topic detection and reallocation

---

## Complete File Structure

After setup, your directory should look like this:

```
mqtt-load-balancing/
│
├── docker-compose.yml              # Main orchestration file
├── setup.sh                        # Setup script
├── Makefile                        # Convenience commands
├── README.md                       # Main documentation
├── Quick-Start-Guide.md           # Getting started
├── Configuration-Guide.md          # Customization options
├── Troubleshooting-Guide.md       # Problem solving
│
├── coordinator/                    # Load balancer service
│   ├── Dockerfile
│   ├── coordinator.py             # Main algorithm implementation
│   └── requirements.txt
│
├── publisher/                      # Message publishers
│   ├── Dockerfile
│   ├── publisher.py               # Generates traffic
│   └── requirements.txt
│
├── subscriber/                     # Message subscribers
│   ├── Dockerfile
│   ├── subscriber.py              # Consumes messages
│   └── requirements.txt
│
├── mosquitto/                      # Broker configurations
│   ├── broker1/
│   │   ├── config/
│   │   │   └── mosquitto.conf
│   │   ├── data/
│   │   └── log/
│   ├── broker2/
│   │   └── ... (same structure)
│   ├── broker3/
│   │   └── ... (same structure)
│   └── broker4/
│       └── ... (same structure)
│
├── test.py                         # System tests
├── monitor.py                      # Real-time monitoring
├── analyze.py                      # Performance analysis
│
└── logs_backup/                    # Generated log backups
```

---

## Installation Checklist

### Prerequisites
- [ ] Docker 20.10+ installed
- [ ] Docker Compose 1.29+ installed
- [ ] 4GB RAM available
- [ ] 2GB disk space available
- [ ] Ports 1883-1886 available
- [ ] Python 3.7+ (for monitoring/testing)

### Setup Steps

```bash
# 1. Create project directory
mkdir mqtt-load-balancing
cd mqtt-load-balancing

# 2. Save all provided files to this directory
#    (docker-compose.yml, setup.sh, Makefile, etc.)

# 3. Make scripts executable
chmod +x setup.sh

# 4. Run setup
./setup.sh

# 5. Copy application files
cp coordinator.py coordinator/
cp publisher.py publisher/
cp subscriber.py subscriber/

# 6. Build containers
make build

# 7. Start system
make start

# 8. Verify running
make ps

# 9. Check logs
make logs
```

---

## Quick Command Reference

### Essential Commands

```bash
make help              # Show all commands
make start             # Start all services
make stop              # Stop all services
make restart           # Restart all services
make logs              # View all logs
make ps                # Show container status
make clean             # Stop and remove containers
```

### Monitoring Commands

```bash
make monitor           # Real-time dashboard
make stats             # Quick statistics
make hot-topics        # Show detected hot topics
make load-distribution # Show broker loads
```

### Testing Commands

```bash
make test              # Run automated tests
make health-check      # Check system health
```

### Analysis Commands

```bash
python3 analyze.py     # Analyze performance
make backup-logs       # Save logs with timestamp
```

### Advanced Commands

```bash
make scale-subscribers N=100  # Scale subscribers
make dev-coordinator          # Shell into coordinator
make network-test            # Test connectivity
```

---

## Key Components Explained

### 1. Coordinator Service (coordinator.py)

**Implements:**
- Hot Topic Detection using LoOP (Equations 1-3 from paper)
- Trie data structure for topic management
- Load Balancing Algorithm 1
- Optimal utilization calculation (Equation 15)
- Cost function for broker selection (Equation 17)

**Key Classes:**
- `HotTopicDetector`: Implements LoOP algorithm
- `TopicTrie`: Hierarchical topic storage
- `LoadBalancer`: Algorithm 1 implementation
- `CoordinationService`: Main orchestrator

### 2. Publisher Service (publisher.py)

**Generates:**
- 20 hierarchical topics (home/room/sensor)
- Variable message rates (0.1-2.0 msg/sec)
- 500-600 KB messages (as per paper)
- Power-law distribution creating hot topics

**Configuration:**
- 5 publishers by default
- Round-robin broker assignment
- Multithreaded message generation

### 3. Subscriber Service (subscriber.py)

**Measures:**
- Message delivery latency
- Message counts per topic
- Subscriber-level statistics
- Aggregated system metrics

**Configuration:**
- 50 subscribers by default
- 1-5 random topic subscriptions each
- 30-second stats reporting

### 4. Brokers (Mosquitto)

**Configuration:**
- 4 brokers with capacities: 350, 450, 550, 600 Mbps
- Persistent storage enabled
- QoS 0-2 supported
- Anonymous connections allowed (for testing)

---

## Verification Steps

### After Starting System

**1. Check all containers running (2-3 minutes):**
```bash
make ps
# Should show 7 containers: 4 brokers + coordinator + publisher + subscriber
```

**2. Verify connectivity (30 seconds):**
```bash
make test
# All tests should pass
```

**3. Check hot topic detection (1-2 minutes):**
```bash
make hot-topics
# Should show 3-5 topics identified
```

**4. Monitor load distribution (ongoing):**
```bash
make monitor
# Should show balanced loads across brokers
```

**5. Analyze performance (after 10+ minutes):**
```bash
python3 analyze.py
# Should show statistics close to paper results
```

---

## Expected Behavior

### First 30 Seconds
- Containers starting
- Brokers initializing
- Publishers connecting
- Subscribers connecting

### 30-60 Seconds
- Messages flowing
- Initial topic statistics collecting
- First hot topic detection
- Initial load balancing

### 1-5 Minutes
- Hot topics stabilizing
- Load balancing active
- Metrics accumulating
- System reaching steady state

### 5+ Minutes
- Optimal performance
- Clear hot topics identified
- Balanced broker loads
- Stable latencies

---

## Key Metrics to Watch

### Broker Utilization
```
Target: 15-30% average per broker
Warning: >80% indicates overload
Critical: >95% system unstable
```

### Hot Topic Detection
```
Expected: 3-5 topics marked as hot
LoOP score: >0.8 for hot topics
Frequency: Detected within 60 seconds
```

### Latency
```
Good: <100ms average
Acceptable: <200ms average
Poor: >500ms average
```

### Load Distribution
```
Good: Variance <50 across brokers
Moderate: Variance 50-100
Poor: Variance >100
```

---

## Customization Quick Reference

### Change Number of Brokers
1. Edit `docker-compose.yml` (add/remove broker services)
2. Edit `coordinator/coordinator.py` (_init_brokers method)
3. Rebuild: `make build`

### Change Message Rate
Edit `publisher/publisher.py`:
```python
rates[topic] = random.uniform(MIN, MAX)  # Adjust MIN/MAX
```

### Change Hot Topic Threshold
Edit `coordinator/coordinator.py`:
```python
hot_topics = self.hot_detector.compute_loop(request_rates, threshold=0.8)
# Lower threshold = more topics marked as hot
```

### Change Number of Subscribers
```bash
make scale-subscribers N=100
```

---

## Troubleshooting Quick Fixes

### System won't start
```bash
docker-compose down -v
docker system prune
make build
make start
```

### No hot topics detected
```python
# In publisher.py, increase rate difference:
hot_topics: 3.0-5.0 msg/sec
normal: 0.1-0.2 msg/sec
```

### High latency
```python
# In publisher.py, reduce message size:
size_bytes = random.randint(50*1024, 100*1024)  # 50-100 KB
```

### Uneven load distribution
```python
# In coordinator.py, increase balancing frequency:
time.sleep(5)  # Instead of 10 seconds
```

---

## Performance Benchmarks

### From the Paper
- Average waiting time: 11% reduction
- Load distribution: 20% more even
- Server utilization: ~22% (near optimal)
- Broker utilization variance: Low

### Your Results (check with analyze.py)
- Should be within 10-15% of paper results
- May vary based on system resources
- Longer runs (1+ hour) give better results

---

## Data Collection for Research

### Short Test (10 minutes)
```bash
make start
sleep 600
make backup-logs
python3 analyze.py
```

### Medium Test (1 hour)
```bash
make start
sleep 3600
make backup-logs
python3 analyze.py
```

### Long Test (24 hours)
```bash
make start
# Wait 24 hours
make backup-logs
python3 analyze.py
```

### Collect Metrics
```bash
# Save logs
make backup-logs

# Generate analysis
python3 analyze.py > results.txt

# Export to CSV (custom script)
# Parse logs for broker utilization over time
# Parse logs for hot topic frequencies
# Parse logs for latency percentiles
```

---

## Next Steps

### For Basic Users
1. Follow Quick Start Guide
2. Run system for 10 minutes
3. View monitoring dashboard
4. Run analysis script
5. Review results

### For Researchers
1. Complete setup and verification
2. Run long-term tests (hours/days)
3. Export detailed metrics
4. Compare with paper results
5. Experiment with parameters
6. Document findings

### For Developers
1. Study coordinator.py implementation
2. Review Algorithm 1 implementation
3. Understand LoOP detection
4. Modify load balancing strategy
5. Test alternative approaches
6. Benchmark improvements

---

## Additional Resources

### Paper Reference
- **Title:** "Efficient Multi-Broker Load Balancing in Event Driven Pub-Sub Networks"
- **Authors:** Surabhi Sharma, Sateesh Kumar Peddoju
- **Journal:** IEEE TNSM, Vol. 21, No. 4, August 2024
- **DOI:** 10.1109/TNSM.2024.3401484

### Key Algorithms Implemented
- LoOP (Local Outlier Probability) - Section III-A
- Trie Formation - Section III-B
- Topic-Aware Load Balancing - Section III-D
- Algorithm 1 - Allocation Matrix - Section III-D3

### Technologies Used
- **MQTT:** Eclipse Mosquitto 2.0
- **Python:** 3.9 with paho-mqtt, numpy, scipy
- **Docker:** Container orchestration
- **Networks:** Bridge networking for isolation

---

## Success Indicators

✓ All containers running
✓ No error messages in logs
✓ Hot topics detected
✓ Load balancing active
✓ Broker utilizations balanced
✓ Messages flowing
✓ Latency acceptable
✓ Test suite passes
✓ Monitoring dashboard shows data
✓ Analysis script generates report

---

## Support and Community

### Before Asking for Help

1. Check Troubleshooting Guide
2. Review logs for errors
3. Verify all containers running
4. Run health check
5. Try clean rebuild

### When Reporting Issues

Include:
- Output of `docker-compose ps`
- Relevant log excerpts
- System information (Docker version, OS)
- What you've tried
- Expected vs actual behavior

### Useful Debug Commands

```bash
# Full diagnostic
docker-compose config
docker-compose ps
docker stats --no-stream
docker network inspect mqtt_network
make logs > debug.log

# Package into archive
tar -czf debug-info.tar.gz debug.log coordinator_logs.txt subscriber_logs.txt
```

---

## License and Citation

This implementation is for educational and research purposes based on the IEEE paper cited above.

If you use this implementation in research, please cite the original paper.

---

## Summary

You now have everything needed to:
1. ✓ Build the system
2. ✓ Run experiments
3. ✓ Monitor performance
4. ✓ Analyze results
5. ✓ Troubleshoot issues
6. ✓ Customize behavior

**Start with:** `./setup.sh && make build && make start`

**Monitor with:** `make monitor`

**Analyze with:** `python3 analyze.py`

Good luck with your experiments! 🚀
