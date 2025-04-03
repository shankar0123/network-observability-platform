# Use an official Python runtime as a parent image
FROM python:3.10-slim

# pybgpstream often requires C build tools and potentially libbz2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libbz2-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY bgp_analyzer.py .

# Make port 8001 available for Prometheus scraping
EXPOSE 8001

# Define environment variables (can be overridden at runtime)
ENV PREFIXES_TO_MONITOR="1.1.1.0/24,8.8.8.0/24"
ENV BGPSTREAM_PROJECTS="routeviews,ris"
# Default to live stream if BGPSTREAM_START_TIME is empty
ENV BGPSTREAM_START_TIME=""
ENV PROMETHEUS_PORT="8001"
ENV LOG_LEVEL="INFO"
# Add PYTHONUNBUFFERED to ensure logs are sent straight to stdout/stderr
ENV PYTHONUNBUFFERED=1

# Run bgp_analyzer.py when the container launches
CMD ["python", "bgp_analyzer.py"]
