# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY http_canary.py .

# Define environment variables (can be overridden at runtime)
ENV KAFKA_BROKER="kafka:9092"
ENV KAFKA_TOPIC="canary-results"
ENV CANARY_ID="http-canary-default-01"
ENV TARGET_URLS="https://google.com,https://github.com"
ENV HTTP_INTERVAL_SECONDS="60"
ENV REQUEST_TIMEOUT_SECONDS="10"
# Optional: Check for this string in response body
ENV EXPECTED_STRING=""
# Optional: Set custom User-Agent
ENV USER_AGENT=""
# Add PYTHONUNBUFFERED to ensure logs are sent straight to stdout/stderr
ENV PYTHONUNBUFFERED=1

# Run http_canary.py when the container launches
CMD ["python", "http_canary.py"]
