# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies (e.g., for building dependencies)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . .

# Set environment variables
ENV AWS_ACCESS_KEY="AKIA47R274ALI7GMXTMF"
ENV AWS_SECRET_KEY="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"
ENV AWS_BUCKET_NAME="ippo-taskmanager2000-test"
ENV AWS_REGION="us-east-1"

# Expose port 8000
EXPOSE 8000

# Command to run the FastAPI application
CMD ["python", "main.py"]