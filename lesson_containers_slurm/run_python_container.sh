#!/bin/bash
# Helper script to run Python in container
SCRIPT_NAME=$1

echo "Running $SCRIPT_NAME in container..."
podman run --rm -v $(pwd):/work docker://jupyter/scipy-notebook:latest python /work/$SCRIPT_NAME
echo "Container execution finished."
