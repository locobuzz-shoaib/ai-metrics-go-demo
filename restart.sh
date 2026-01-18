#!/bin/bash

CONTAINER_NAME="creatorverse-ai-token-metrics-service"
IMAGE_NAME="creatorverse-ai-token-metrics-service:1.0.0"

echo "Stopping and removing existing container..."
docker rm -f $CONTAINER_NAME 2>/dev/null

echo "Building Docker image (no cache)..."
docker build --no-cache -t $IMAGE_NAME -t creatorverse-ai-token-metrics-service:latest .

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Starting container..."
docker run -d --name $CONTAINER_NAME -p 8080:8080 $IMAGE_NAME

echo "Container started. Checking status..."
docker ps --filter "name=$CONTAINER_NAME"

echo ""
echo "View logs with: docker logs -f $CONTAINER_NAME"
