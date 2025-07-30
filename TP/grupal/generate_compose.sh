#!/bin/bash

# Default values
DEFAULT_CONFIG="infra/config.ini"
DEFAULT_OUTPUT="docker-compose.yaml"

# Use defaults if parameters are not provided
CONFIG_PATH=${1:-$DEFAULT_CONFIG}
OUTPUT_PATH=${2:-$DEFAULT_OUTPUT}

echo "Using configuration file: $CONFIG_PATH"
echo "Output docker compose file: $OUTPUT_PATH"

python3 infra/generate_compose.py "$CONFIG_PATH" "$OUTPUT_PATH"

echo "Generated docker compose file $OUTPUT_PATH"
