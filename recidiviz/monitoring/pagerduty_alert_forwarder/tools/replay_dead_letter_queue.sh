# This script pulls all messages from the dead letter queue and retries them on the alert forwarder queue

# To replay an individual message
# $ DECODED_DATA=$(echo "BASE64 ENCODED MESSAGE DATA " | base64 -d)
# $ echo "${DECODED_DATA}"
# $ gcloud pubsub topics publish "${FORWARDER_TOPIC_NAME}" --project "${PROJECT_ID}"
FORWARDER_TOPIC_NAME="cloud-monitoring-alerts-to-forward"
DLQ_REPLAY_SUBSCRIPTION_NAME="pagerduty-alert-forwarder-dlq-pull"
PROJECT_ID="recidiviz-123"

# Get script directory and create output file
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_FILE="${SCRIPT_DIR}/dlq_messages_${TIMESTAMP}.json"

# Initialize output file as JSON array
echo "[" > "${OUTPUT_FILE}"
FIRST_MESSAGE=true

# Pull messages and republish them
while true; do
    # Pull messages (adjust max-messages as needed)
    messages=$(gcloud pubsub subscriptions pull "${DLQ_REPLAY_SUBSCRIPTION_NAME}" \
        --auto-ack \
        --limit=100 \
        --format=json \
        --project="${PROJECT_ID}")

    # Check if we got any messages
    if [ -z "$messages" ] || [ "$messages" = "[]" ]; then
        echo "No more messages"
        break
    fi

    echo "Replaying the following messages:"
    echo "================================="
    echo "${messages}"

    # Save messages to file
    if [ "$FIRST_MESSAGE" = true ]; then
        echo "${messages}" | jq '.[]' >> "${OUTPUT_FILE}"
        FIRST_MESSAGE=false
    else
        echo "," >> "${OUTPUT_FILE}"
        echo "${messages}" | jq '.[]' >> "${OUTPUT_FILE}"
    fi

    # Publish each message to the target topic
    echo "${messages}" | jq -r '.[] | .message.data' | while read -r DATA; do
        DECODED_DATA=$(echo "${DATA}" | base64 -d)
        gcloud pubsub topics publish "${FORWARDER_TOPIC_NAME}" --message="$DECODED_DATA" --project "${PROJECT_ID}"
    done
done

# Close JSON array
echo "]" >> "${OUTPUT_FILE}"

echo ""
echo "Messages saved to file in case you need them again: ${OUTPUT_FILE}"
