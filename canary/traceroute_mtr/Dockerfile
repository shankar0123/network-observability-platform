FROM python:3.9-slim

# Install mtr (and traceroute for later)
# mtr-tiny is often sufficient for JSON output and smaller. Full mtr might be needed for ASN.
RUN apt-get update && apt-get install -y mtr traceroute iputils-ping && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY canary/traceroute_mtr/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY canary/traceroute_mtr/traceroute_mtr_canary.py .

# Ensure the script is executable if needed, though python script.py works
# RUN chmod +x traceroute_mtr_canary.py

# Set environment variables for InfluxDB, Kafka, targets etc. in docker-compose or Kubernetes
# Example (can be overridden):
# ENV INFLUXDB_URL="http://influxdb:8086"
# ENV TARGET_HOSTS="1.1.1.1,8.8.8.8"

CMD ["python", "traceroute_mtr_canary.py"]
