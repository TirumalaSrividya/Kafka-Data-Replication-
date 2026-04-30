# Kafka MirrorMaker 2 Fault Tolerance Enhancements

This repository provides a production-grade fault-tolerance layer for Kafka MirrorMaker 2 (MM2), specifically designed to prevent silent data loss during log truncation and enable automated recovery during topic resets.

## 🔗 Repository Links
- **Kafka Fork**: [https://github.com/TirumalaSrividya/kafka]
- **Pull Request**: [https://github.com/TirumalaSrividya/kafka/pull/2]




##  Docker Hub Images
The project utilizes the following container images:
- `custom-mm2:latest`: Custom-built MirrorMaker 2 image based on the modified Kafka source code.
- `apache/kafka:3.7.0`: Standard Kafka image for Primary and Standby clusters.
- `gradle:8-jdk17`: Build environment for recompiling Kafka modules.





### Interpreting Results:
- **Scenario 1 (Normal)**: Replicates 1000 messages. SUCCESS if all messages are found on the Standby cluster.
- **Scenario 2 (Log Truncation)**: Triggers a "Fail-Fast" crash. SUCCESS if `DATA LOSS DETECTED` appears in logs.
- **Scenario 3 (Topic Reset)**: Deletes/recreates the primary topic. SUCCESS if `TOPIC RESET DETECTED` appears and replication resumes from 0.

##  Log Analysis
Monitor the following key messages in the MM2 container logs:
- `docker logs -f mm2`
- **Fail-Fast Error**: `ERROR DATA LOSS DETECTED on commit-log-0! Expected offset X, but got Y.`
- **Reset Detection**: `INFO TOPIC RESET DETECTED on commit-log-0. Adjusting baseline.`
- **Recovery Diagnostics**: `INFO Found progress on target cluster: X. Using as baseline.`

##  Design Rationale
The standard MirrorMaker 2 implementation relies on `auto.offset.reset`, which can lead to silent data loss if data expires on the primary cluster. Our modifications in `MirrorSourceTask.java` address this through:
1. **Expected Offset Tracking**: The task explicitly stores the next expected offset in memory. If a gap is detected during the `poll()` loop, the system halts to prevent inconsistent data replication.
2. **Cross-Cluster Validation**: Upon startup, if internal Kafka offsets are missing, the task probes the Standby cluster's end-offsets to calculate a reliable replication baseline.
3. **Explicit Reset Handling**: If the incoming offset is less than the expected offset, the system identifies a topic reset and re-synchronizes the state automatically.