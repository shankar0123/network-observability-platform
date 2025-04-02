# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY kafka_consumer.py .

# Make port 8000 available to the world outside this container for Prometheus scraping
EXPOSE 8000

# Define environment variables (can be overridden at runtime)
ENV KAFKA_BROKER="kafka:9092"
ENV KAFKA_TOPIC="canary-results"
ENV CONSUMER_GROUP_ID="canary-consumer-group-1"
ENV PROMETHEUS_PORT="8000"
# Add PYTHONUNBUFFERED to ensure logs are sent straight to stdout/stderr
ENV PYTHONUNBUFFERED=1

# Run kafka_consumer.py when the container launches
CMD ["python", "kafka_consumer.py"]
