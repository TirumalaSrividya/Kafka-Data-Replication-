# Kafka MirrorMaker 2 Data Replication 

## Repository Links

| Resource | URL |
|---|---|
| Kafka Fork | https://github.com/TirumalaSrividya/kafka |
| Pull Request | https://github.com/TirumalaSrividya/kafka/pull/6

---

## Docker Hub Images

| Image | Tag | Description |
|---|---|---|
| `srividyatirumala/custom-mm2` | `latest` | MirrorMaker 2 with data-loss and topic-reset detection |
| `srividyatirumala/kafka-producer` | `latest` | Java producer that sends JSON commit-log events |

---

## Setup Instructions

### Prerequisites

- Docker ≥ 24
- Docker Compose v2 (or v1 with `docker-compose`)

### Start the stack

```bash
# Clone the repo
git clone https://github.com/TirumalaSrividya/Kafka-Data-Replication-
cd <Kafka-Data-Replication->

# Start everything (Kafka clusters + MM2 + producer)
docker compose up -d
```

MirrorMaker 2 is configured via `mm2.properties` in the project root. It replicates the `commit-log` topic from `primary-kafka:9092` → `standby-kafka:9094` and exposes its admin listener on port `8083`.

---

## Test Execution

Run the full challenge script:

```bash
chmod +x run_challenge.sh
./run_challenge.sh
```

The script executes three scenarios in sequence and prints a `PASSED` or `FAILED` result for each.

### What each scenario does

**Scenario 1 — Normal Replication**
Produces 1 000 messages to the primary cluster and verifies that the topic `primary.commit-log` appears on the standby cluster with the expected message count.

**Scenario 2 — Log Truncation / Data Loss Detection**
Pauses MM2, sets an aggressive 30-second retention on the primary, produces 1 000 messages, waits for them to be purged, then restarts MM2. Expected result: MM2 logs `DATA LOSS DETECTED` and halts instead of silently skipping offsets.

**Scenario 3 — Topic Reset Recovery**
Produces 100 messages, pauses MM2, deletes and recreates the `commit-log` topic on the primary (resetting offsets to 0), then resumes MM2. Expected result: MM2 logs `TOPIC RESET DETECTED` and automatically seeks to the beginning to replicate from offset 0.

### Passing output

```
✅ SCENARIO 1 PASSED – Replication verified
✅ SCENARIO 2 PASSED – MM2 detected log truncation and failed fast
✅ SCENARIO 3 PASSED – MM2 detected topic reset and recovered automatically

```       
---

## Log Analysis

### Useful commands

```bash
# Live MM2 logs
docker logs -f mm2

# Last 60 lines
docker logs mm2 2>&1 | tail -60

# Check replication on standby
docker exec standby-kafka \
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server standby-kafka:9094 \
  --topic primary.commit-log --time -1

# Check earliest available offset on primary
docker exec primary-kafka \
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server primary-kafka:9092 \
  --topic commit-log --time -2
```

### Key log messages to monitor

| Message | Meaning |
|---|---|
| `MM2 successfully assigned commit-log for replication` | MM2 picked up the topic at startup |
| `DATA LOSS DETECTED` | MM2's last committed offset is behind the earliest available offset; replication halted |
| `TOPIC RESET DETECTED` | Received an offset lower than the last committed one; MM2 seeks to beginning and resumes |

---

## Design Rationale

## Topic creation before MM2 startup
MM2 scans topics only at startup, so commit-log is created before MM2 starts to ensure it gets picked up for replication.

## Data-loss detection
If the incoming offset is ahead of expected, messages were purged before replication. The connector logs DATA LOSS DETECTED and stops.

## Topic-reset detection
If the incoming offset is behind expected, the topic was recreated. The connector logs TOPIC RESET DETECTED and seeks to the beginning to resume from offset 0.

### MirrorMaker 2 configuration highlights (`mm2.properties`)

| Setting | Value | Reason |
|---|---|---|
| `primary.auto.offset.reset` | `earliest` | Ensures MM2 starts from the beginning when no committed offset exists |

---

## AI Usage Documentation

- Helped troubleshoot inter-container networking issues in docker-compose.yml and mm2.properties configuration.
- Clarified retry loop patterns and sleep timings while writing run_challenge.sh.
- Explained why MM2 misses topics created after startup, which led to adding the pre-creation step.
- Helped think through the offset comparison logic for data loss and topic reset detection.
