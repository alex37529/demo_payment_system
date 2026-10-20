#!/usr/bin/env bash
# 4 canonical topics + 3 DLQ (TЗ v2 §3.11; DLQ = per producer).
# Runs in the kafka container (kafka scripts live in /opt/kafka/bin, not on PATH).
set -euo pipefail
KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
BOOT="--bootstrap-server ${BOOTSTRAP}"

create_topic() {
  local name="$1"
  # `--if-not-exists` makes the creation itself idempotent: the check below
  # is racy when the init runs twice concurrently (a re-`up` without wiping
  # the volume), and the loser of that race used to fail the whole init with
  # 'TopicExistsException'.
  if "$KAFKA_BIN/kafka-topics.sh" $BOOT --list 2>/dev/null | grep -xq "$name"; then
    echo "topic exists: $name"
  else
    "$KAFKA_BIN/kafka-topics.sh" $BOOT --create --if-not-exists \
      --partitions 3 --replication-factor 1 --topic "$name"
    echo "created: $name"
  fi
}

create_topic payments.commands
create_topic payments.events
create_topic ledger.commands
create_topic ledger.events
create_topic payments.commands.dlq
create_topic ledger.commands.dlq
create_topic payments.events.dlq

echo "kafka topics ready"
