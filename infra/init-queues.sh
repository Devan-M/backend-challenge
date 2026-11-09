#!/bin/sh
# Fail immediately so Compose never starts the API with a partially provisioned queue set.
set -eu

endpoint=http://localstack:4566
dlq_url="$endpoint/000000000000/wager-transactions-dlq.fifo"

# Create the FIFO DLQ first so its ARN can be installed as the source queue redrive target.
aws --endpoint-url="$endpoint" sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true

dlq_arn=$(aws --endpoint-url="$endpoint" sqs get-queue-attributes \
  --queue-url "$dlq_url" \
  --attribute-names QueueArn \
  --query Attributes.QueueArn \
  --output text)

# SQS expects RedrivePolicy as a JSON-encoded string within the queue attributes document.
redrive_policy=$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}' "$dlq_arn")
escaped_redrive_policy=$(printf '%s' "$redrive_policy" | sed 's/"/\\"/g')
queue_attributes="{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"RedrivePolicy\":\"$escaped_redrive_policy\"}"

# Create the provider input queue with automatic redrive after five receives.
aws --endpoint-url="$endpoint" sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "$queue_attributes"

# Integration events have their own FIFO destination and are published from the SQL outbox.
aws --endpoint-url="$endpoint" sqs create-queue \
  --queue-name wagering-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false