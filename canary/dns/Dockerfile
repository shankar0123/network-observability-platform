# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir can make the image smaller
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY dns_canary.py .

# Define environment variables (can be overridden at runtime)
ENV KAFKA_BROKER="kafka:9092"
ENV KAFKA_TOPIC="canary-results"
ENV CANARY_ID="dns-canary-default-01"
ENV TARGET_DOMAINS="google.com,github.com"
ENV DNS_INTERVAL_SECONDS="60"
ENV QUERY_TYPE="A"
# Empty TARGET_RESOLVER means use system default inside container
ENV TARGET_RESOLVER=""
ENV QUERY_TIMEOUT_SECONDS="5"
# Add PYTHONUNBUFFERED to ensure logs are sent straight to stdout/stderr
ENV PYTHONUNBUFFERED=1

# Run dns_canary.py when the container launches
CMD ["python", "dns_canary.py"]
