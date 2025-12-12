FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt==1.6.1 \
    numpy==1.24.3 \
    scipy==1.10.1

COPY coordinator.py .

CMD ["python", "-u", "coordinator.py"]
