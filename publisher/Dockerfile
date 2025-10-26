FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt==1.6.1

COPY publisher.py .

CMD ["python", "-u", "publisher.py"]
