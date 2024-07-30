#!/bin/bash

# Set variables
IMAGE_NAME="cdm_assignment"
DOCKER_USERNAME="umesh"
TAG="latest"
DOCKERFILE_PATH="."

# Build the Docker image
echo "Building Docker image..."
docker build -t $DOCKER_USERNAME/$IMAGE_NAME:$TAG $DOCKERFILE_PATH

# Check if the build was successful
if [ $? -ne 0 ]; then
  echo "Docker build failed!"
  exit 1
fi

# Push the Docker image to Docker Hub
echo "Pushing Docker image to Docker Hub..."
docker push $DOCKER_USERNAME/$IMAGE_NAME:$TAG

# Check if the push was successful
if [ $? -ne 0 ]; then
  echo "Docker push failed!"
  exit 1
fi

# Make the repository public (optional, Docker Hub repositories are public by default when created)
# You can use the Docker Hub API to set the visibility if needed
echo "Making the repository public..."
curl -u "$DOCKER_USERNAME" -X PATCH "https://hub.docker.com/v2/repositories/$DOCKER_USERNAME/$IMAGE_NAME/" -H "Content-Type: application/json" -d '{"is_private": false}'

echo "Docker image pushed successfully and repository is set to public!"
