# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Install fping
RUN apt-get update && apt-get install -y fping --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY ping_canary.py .

# Make port 9092 available to the world outside this container (optional, Kafka client doesn't need inbound)
# EXPOSE 9092

# Define environment variables (can be overridden at runtime)
ENV KAFKA_BROKER="kafka:9092"
ENV KAFKA_TOPIC="canary-results"
ENV CANARY_ID="ping-canary-default-01"
ENV TARGET_HOSTS="8.8.8.8,1.1.1.1"
ENV PING_INTERVAL_SECONDS="60"
ENV PING_COUNT="5"
# Add PYTHONUNBUFFERED to ensure logs are sent straight to stdout/stderr
ENV PYTHONUNBUFFERED=1

# Run ping_canary.py when the container launches
CMD ["python", "ping_canary.py"]
