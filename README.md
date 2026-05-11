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

## Folder Structure

Kafka-Data-Replication-/
|── producer/                        # Java producer service
│   ├── Dockerfile
│   └── src/
│       └── main/
│           └── java/
│               └── KafkaProducer.java   # sends JSON commit-log events                      
│
├── README.md
├── docker-compose.yml                 # Configuration file
├── mm2.properties                     
├── reset.json
└── run_challenge.sh                  # Automation Script


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

**Scenario 1 — Normal Replication**
Produces 1000 messages to the primary cluster and verifies that the topic `primary.commit-log` appears on the standby cluster with the expected message count.
![Scenario 1 – Normal Replication](Screenshot%202026-05-11%20114303.png)

**Scenario 2 — Log Truncation / Data Loss Detection**
Pauses MM2, sets an aggressive 30-second retention on the primary, produces 1000 messages, waits for them to be purged, then restarts MM2. Expected result: MM2 logs `DATA LOSS DETECTED` and halts instead of silently skipping offsets.
![Scenario 2 – Data Loss Detection](Screenshot%202026-05-11%20114522.png)

**Scenario 3 — Topic Reset Recovery**
Produces 100 messages, pauses MM2, deletes and recreates the `commit-log` topic on the primary (resetting offsets to 0), then resumes MM2. Expected result: MM2 logs `TOPIC RESET DETECTED` and automatically seeks to the beginning to replicate from offset 0.
![Scenario 3 – Topic Reset Recovery](Screenshot%202026-05-11%20114805.png)

---

## Unit Test Execution - Core Functionality

The core detection logic is covered by unit tests that run without Docker or a live Kafka broker.

### Prerequisites

- Java 17+
- Run from inside the Kafka fork directory

```bash
cd kafka-fork
```

### Run all three new unit tests

**Git Bash / Linux / Mac:**
```bash
./gradlew :connect:mirror:test --no-daemon \
  --tests "*MirrorSourceTaskTest.testDataLossDetectedAtStartup" \
  --tests "*MirrorSourceTaskTest.testTopicResetDoesNotDropOtherPartitions" \
  --tests "*MirrorSourceTaskTest.testOffsetOutOfRangeThrowsDataLossException"
```

**Windows CMD:**
```cmd
gradlew.bat :connect:mirror:test --no-daemon ^
  --tests "*MirrorSourceTaskTest.testDataLossDetectedAtStartup" ^
  --tests "*MirrorSourceTaskTest.testTopicResetDoesNotDropOtherPartitions" ^
  --tests "*MirrorSourceTaskTest.testOffsetOutOfRangeThrowsDataLossException"
```

| Test | What it proves |
|---|---|
| `testDataLossDetectedAtStartup` | `DataLossException` is thrown at startup when broker's earliest offset is ahead of committed offset — catches gaps that opened while MM2 was down |
| `testTopicResetDoesNotDropOtherPartitions` | Topic reset seeks to beginning without dropping other partition assignments |
| `testOffsetOutOfRangeThrowsDataLossException` | `DataLossException` is thrown via the `OffsetOutOfRangeException` path |

## Passing Output

```
MirrorSourceTaskTest > testDataLossDetectedAtStartup() PASSED
MirrorSourceTaskTest > testTopicResetDoesNotDropOtherPartitions() PASSED
MirrorSourceTaskTest > testOffsetOutOfRangeThrowsDataLossException() PASSED
BUILD SUCCESSFUL
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

## DataLossException instead of ConnectException
A typed exception lets operators and monitoring systems distinguish a data loss halt from any other connector failure. It still extends ConnectException so Connect's worker handles it correctly.

## In-memory offset tracking via expectedOffsets
OffsetOutOfRangeException fires one poll cycle too late — the broker has already rejected the fetch. The expectedOffsets map catches gaps on the very first successfully received record, before forwarding it. handleOffsetOutOfRange() acts as a secondary safety net for cases the map cannot reach.

## checkOffsetAnomaly() and handleOffsetOutOfRange() as separate methods
Kafka enforces a cyclomatic complexity limit of 16 per method. The combined detection logic pushed poll() to 18. Extracting the two helpers brings each method under the limit while keeping responsibilities clear.

## Consumer interface instead of KafkaConsumer
MockConsumer implements Consumer directly and does not extend KafkaConsumer. Changing the field to Consumer<byte[], byte[]> allows unit tests to inject MockConsumer without casting. Production code is unaffected.

## putExpectedOffset() as a test hook
A named public method is a deliberate, documented seeding operation for tests. Making the field package-private would expose the entire map to accidental mutation by any class in the package.

## Fail-fast on data loss
Silent skipping leaves the standby permanently out of sync with no alert. Throwing DataLossException forces a conscious operator decision — restore from backup, accept the gap, or trigger an incident.

## Seek to beginning on topic reset
The committed offset no longer exists on a recreated topic. Seeking to it would skip records 0 through committed-1. Seeking to beginning ensures full replication of the new topic.

## Startup gap check in initializeConsumer()
The in-memory map is lost on restart. Without a startup check the first polled record would silently baseline at the post-truncation offset. Comparing beginningOffsets() against the committed offset at startup catches data loss that occurred while MM2 was down.

## Known limitation
Partitions with no prior committed offset are not checked at startup — they baseline on first poll, which is correct since there is no prior replication state to compare against.

---

## AI Usage Documentation

- Helped troubleshoot inter-container networking issues in docker-compose.yml and mm2.properties configuration.
- Clarified retry loop patterns and sleep timings while writing run_challenge.sh.
- Explained why MM2 misses topics created after startup, which led to adding the pre-creation step.
- Helped think through the offset comparison logic for data loss and topic reset detection.
