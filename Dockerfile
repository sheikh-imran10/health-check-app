# Use a lightweight, official Python base image
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Prevent Python from writing .pyc files and buffer stdout/stderr for real-time logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy and install dependencies first (better Docker caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container
COPY monitor.py .

# Run the script when the container starts
CMD ["python", "monitor.py"]
