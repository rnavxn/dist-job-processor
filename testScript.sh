#!/bin/bash

URL="http://localhost:8080/api/jobs/enqueue"

if ! [[ "$1" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 <number>"
  exit 1
fi

JOB_COUNT=$1
BATCH_ID=$(date +%s) # Unique ID for this specific run

echo "Firing $JOB_COUNT tasks concurrently (Batch: $BATCH_ID)..."

for ((i=1; i<=JOB_COUNT; i++))
do
  curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{
      "type": "EMAIL_SEND",
      "payload": "Batch '"$BATCH_ID"' - Task Number '"$i"'",
      "callbackUrl": ""
    }' \
    "$URL" > /dev/null &
done

wait
echo "All $JOB_COUNT tasks enqueued!"