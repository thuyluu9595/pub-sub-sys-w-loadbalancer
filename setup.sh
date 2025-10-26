#!/bin/bash
# setup.sh - Setup script for MQTT Load Balancing Experiment

set -e  # Exit on error

echo "=========================================="
echo "MQTT Load Balancing Experiment Setup"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check requirements
echo "Checking requirements..."

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker found${NC}"
echo -e "${GREEN}✓ Docker Compose found${NC}"
echo ""

# Create directory structure
echo "Creating directory structure..."
mkdir -p coordinator publisher subscriber
mkdir -p mosquitto/broker{1,2,3,4}/{config,data,log}
mkdir -p logs_backup
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# Create Coordinator Dockerfile
cat > coordinator/Dockerfile << 'EOF'
FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt==1.6.1 \
    numpy==1.24.3 \
    scipy==1.10.1

COPY coordinator.py .

CMD ["python", "-u", "coordinator.py"]
EOF

# Create Publisher Dockerfile
cat > publisher/Dockerfile << 'EOF'
FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt==1.6.1

COPY publisher.py .

CMD ["python", "-u", "publisher.py"]
EOF

# Create Subscriber Dockerfile
cat > subscriber/Dockerfile << 'EOF'
FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt==1.6.1

COPY subscriber.py .

CMD ["python", "-u", "subscriber.py"]
EOF

# Create Mosquitto configuration for each broker
for i in {1..4}; do
cat > mosquitto/broker${i}/config/mosquitto.conf << EOF
# Mosquitto Broker ${i} Configuration

# Network
listener 1883
protocol mqtt

# Persistence
persistence true
persistence_location /mosquitto/data/

# Logging
log_dest file /mosquitto/log/mosquitto.log
log_dest stdout
log_type all
log_timestamp true

# Connection limits
max_connections -1
max_queued_messages 1000

# Security (for testing - allow anonymous)
allow_anonymous true

# Message size
message_size_limit 0

# QoS
max_qos 2
EOF
done

# Create requirements.txt files
cat > coordinator/requirements.txt << 'EOF'
paho-mqtt==1.6.1
numpy==1.24.3
scipy==1.10.1
EOF

cat > publisher/requirements.txt << 'EOF'
paho-mqtt==1.6.1
EOF

cat > subscriber/requirements.txt << 'EOF'
paho-mqtt==1.6.1
EOF

# Make directory writable for mosquitto
chmod -R 777 mosquitto/

echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Copy the coordinator.py to ./coordinator/"
echo "2. Copy the publisher.py to ./publisher/"
echo "3. Copy the subscriber.py to ./subscriber/"
echo "4. Run: docker-compose up --build"
