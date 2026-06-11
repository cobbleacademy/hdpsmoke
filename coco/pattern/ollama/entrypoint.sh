#!/bin/bash
set -e

# Start Ollama server in the background
ollama serve &
SERVER_PID=$!

# Wait for the server to be ready
until ollama list >/dev/null 2>&1; do
  sleep 1
done

# Pull the model if it isn't already present
if ! ollama list | awk '{print $1}' | grep -qx "${MODEL}"; then
  echo "Pulling model: ${MODEL}"
  ollama pull "${MODEL}"
fi

wait $SERVER_PID
