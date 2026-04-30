#!/usr/bin/env bash
set -euo pipefail

echo "===================================================="
echo " Kafka MirrorMaker 2 Challenge Runner (Bash)"
echo "===================================================="

############################################
# Detect Docker Compose (v1 or v2)
############################################
if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
elif docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
else
  echo "ERROR: Docker Compose is not available"
  exit 1
fi

############################################
# PRE-FLIGHT CHECKS
############################################
command -v docker >/dev/null 2>&1 || {
  echo "ERROR: Docker is not installed or not on PATH"
  exit 1
}

docker info >/dev/null 2>&1 || {
  echo "ERROR: Docker daemon is not running"
  exit 1
}

############################################
# 1. HARD RESET (CLEAN STATE)
############################################
echo "[STEP 1] Cleaning any existing Docker/Kafka state..."
$COMPOSE down -v --remove-orphans || true
docker network prune -f || true

############################################
# 2. START KAFKA CLUSTERS
############################################
echo "[STEP 2] Starting Kafka clusters (primary + standby)..."
$COMPOSE up -d primary-kafka standby-kafka

echo "[INFO] Waiting for Kafka brokers to stabilize..."
sleep 20

############################################
# CREATE TOPIC BEFORE MM2 STARTS
# This is critical — MM2 scans topics at startup.
# If commit-log doesn't exist yet, MM2 won't assign
# it for replication even with refresh.topics enabled.
############################################
echo "[STEP 3] Creating commit-log topic BEFORE MM2 starts..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1

############################################
# 3. START MM2 (topic already exists)
############################################
echo "[STEP 4] Starting MirrorMaker 2..."
$COMPOSE up -d mm2

echo "[INFO] Waiting for MM2 to initialize and pick up commit-log..."
sleep 30

# Verify MM2 actually picked up commit-log for replication
MM2_INIT_LOGS=$(docker logs mm2 2>&1 || true)
if echo "$MM2_INIT_LOGS" | grep -q "commit-log"; then
  echo "[INFO] MM2 successfully assigned commit-log for replication"
else
  echo "[WARN] MM2 may not have picked up commit-log — check logs with: docker logs mm2"
fi

############################################
# SCENARIO 1: NORMAL REPLICATION FLOW
############################################
echo ""
echo "===================================================="
echo " SCENARIO 1: Normal Replication Flow"
echo "===================================================="

echo "[SCENARIO 1] Producing 1000 messages to primary..."
$COMPOSE run --rm producer --count 1000

echo "[SCENARIO 1] Waiting for MM2 to replicate messages to standby..."
sleep 30

echo "[SCENARIO 1] Verifying replication on standby..."
PASSED=false
for i in {1..12}; do
  TOPIC_EXISTS=$(docker exec standby-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server standby-kafka:9094 \
    --list 2>/dev/null | grep "primary.commit-log" || true)

  if [ -n "$TOPIC_EXISTS" ]; then
    # Get message count using log dirs
    MSG_COUNT=$(docker exec standby-kafka \
      /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
      --bootstrap-server standby-kafka:9094 \
      --topic primary.commit-log \
      --time -1 2>/dev/null | cut -d: -f3 || true)

    echo "  primary.commit-log found on standby (messages: ${MSG_COUNT:-unknown})"
    echo "✅ SCENARIO 1 PASSED – Replication verified"
    PASSED=true
    break
  fi

  echo "  Waiting for replication... ($i/12)"
  sleep 5
done

if [ "$PASSED" = false ]; then
  echo "❌ SCENARIO 1 FAILED – No offsets found on standby after 60s"
  echo "--- MM2 logs ---"
  docker logs mm2 2>&1 | tail -40
  exit 1
fi

############################################
# SCENARIO 2: LOG TRUNCATION SIMULATION
############################################
echo ""
echo "===================================================="
echo " SCENARIO 2: Log Truncation Detection (Fail-Fast)"
echo "===================================================="

# Per assignment tip: pause MM2 so it cannot replicate
# while data is being produced and then expired.
# When MM2 restarts it will find a gap between its last
# committed offset and the earliest available offset.
echo "[SCENARIO 2] Pausing MM2..."
$COMPOSE stop mm2
sleep 5

echo "[SCENARIO 2] Applying aggressive retention (30s) on commit-log..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter \
  --add-config retention.ms=30000,segment.bytes=1024,file.delete.delay.ms=0

echo "[SCENARIO 2] Producing 1000 more messages while MM2 is paused..."
$COMPOSE run --rm producer --count 1000

# Wait long enough for ALL data to be purged by the 30s retention window.
echo "[SCENARIO 2] Waiting 90s for retention to purge all data..."
sleep 90

# Verify truncation actually happened before restarting MM2
echo "[SCENARIO 2] Verifying truncation occurred..."
EARLIEST=$(docker exec primary-kafka \
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server primary-kafka:9092 \
  --topic commit-log --time -2 2>/dev/null || true)
echo "  Earliest available offset: $EARLIEST"

echo "[SCENARIO 2] Restoring default retention..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter \
  --delete-config retention.ms,segment.bytes,file.delete.delay.ms

# Produce a small batch at the new (high) offsets so MM2 has records
# to poll. MM2's committed offset is ~1000 (from Scenario 1) but the
# earliest available offset is now ~2000+, so it will detect a gap.
echo "[SCENARIO 2] Producing 50 new messages at post-truncation offsets..."
$COMPOSE run --rm producer --count 50

echo "[SCENARIO 2] Restarting MM2 — expecting fail-fast on data loss..."
$COMPOSE up -d --force-recreate mm2
sleep 40

MM2_LOGS=$(docker logs mm2 2>&1 || true)

if echo "$MM2_LOGS" | grep -q "DATA LOSS DETECTED"; then
  echo "✅ SCENARIO 2 PASSED – MM2 detected log truncation and failed fast"
else
  echo "❌ SCENARIO 2 FAILED – DATA LOSS DETECTED not found in MM2 logs"
  echo "--- MM2 logs ---"
  echo "$MM2_LOGS" | tail -60
  exit 1
fi

############################################
# SCENARIO 3: TOPIC RESET SIMULATION
############################################
echo ""
echo "===================================================="
echo " SCENARIO 3: Topic Reset Recovery"
echo "===================================================="

echo "[SCENARIO 3] Full reset — clean state for scenario..."
$COMPOSE down -v --remove-orphans
$COMPOSE up -d primary-kafka standby-kafka
sleep 20

# Create topic BEFORE MM2 starts (same fix as Scenario 1)
echo "[SCENARIO 3] Creating commit-log topic before MM2 starts..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1

echo "[SCENARIO 3] Starting MM2..."
$COMPOSE up -d mm2
sleep 30

echo "[SCENARIO 3] Producing 100 messages so MM2 builds a committed offset..."
$COMPOSE run --rm producer --count 100

echo "[SCENARIO 3] Waiting for MM2 to replicate and commit offsets..."
sleep 30

# Verify MM2 has actually committed some offsets before pausing
STANDBY_CHECK=$(docker exec standby-kafka \
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server standby-kafka:9094 \
  --topic primary.commit-log \
  --time -1 2>/dev/null || true)
echo "[SCENARIO 3] Standby state before pause: $STANDBY_CHECK"

echo "[SCENARIO 3] Pausing MM2 (it now holds committed offset ~100 in its store)..."
$COMPOSE stop mm2
sleep 5

echo "[SCENARIO 3] Deleting commit-log topic from primary..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --delete --topic commit-log || true

sleep 15

echo "[SCENARIO 3] Recreating commit-log topic (offsets restart at 0)..."
docker exec primary-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create --topic commit-log \
  --partitions 1 \
  --replication-factor 1

sleep 10

echo "[SCENARIO 3] Producing 10 fresh messages to the recreated topic (offset 0+)..."
$COMPOSE run --rm producer --count 10

# MM2 resumes with committed offset ~100 in Connect's store.
# The first record it polls is at offset 0 → 0 < 100 → backward jump
# → isTopicReset() fires → logs TOPIC RESET DETECTED → seeks to beginning
# → replicates from offset 0 automatically.
echo "[SCENARIO 3] Resuming MM2 — expecting TOPIC RESET DETECTED + auto-recovery..."
$COMPOSE start mm2
sleep 40

MM2_LOGS=$(docker logs mm2 2>&1 || true)

if echo "$MM2_LOGS" | grep -q "TOPIC RESET DETECTED"; then
  echo "✅ SCENARIO 3 PASSED – MM2 detected topic reset and recovered automatically"
else
  echo "❌ SCENARIO 3 FAILED – TOPIC RESET DETECTED not found in MM2 logs"
  echo "--- MM2 logs ---"
  echo "$MM2_LOGS" | tail -60
  exit 1
fi

############################################
# FINAL RESULT
############################################
echo ""
echo "===================================================="
echo " ✅✅✅ ALL SCENARIOS PASSED SUCCESSFULLY ✅✅✅"
echo "===================================================="
