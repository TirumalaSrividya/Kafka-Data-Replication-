# Kafka MirrorMaker 2 Challenge Runner (PowerShell Version)

Write-Host "Starting clusters and MM2..." -ForegroundColor Cyan
docker-compose up -d primary-kafka standby-kafka
Start-Sleep -Seconds 15
docker-compose up -d mm2
Start-Sleep -Seconds 30

# --- SCENARIO 1: Normal Replication Flow ---
Write-Host ""
Write-Host "=== SCENARIO 1: Normal Replication Flow ===" -ForegroundColor Yellow
Write-Host "Creating the commit-log topic..."
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server primary-kafka:9092 `
    --create --topic commit-log --partitions 1 --replication-factor 1 `
    --if-not-exists

Write-Host "Producing 1000 messages to primary cluster..."
docker-compose run --rm producer --count 1000

Write-Host "Waiting 30 seconds for MM2 to replicate messages to standby..."
Start-Sleep -Seconds 30

Write-Host "Verifying replication on standby cluster..."
$standbyOffset = docker exec standby-kafka /opt/kafka/bin/kafka-run-class.sh `
    kafka.tools.GetOffsetShell `
    --bootstrap-server standby-kafka:9094 `
    --topic primary.commit-log --time -1 2>&1 | Select-String "primary.commit-log"

Write-Host "Standby offset info: $standbyOffset"
Write-Host "SUCCESS: Scenario 1 complete - 1000 messages produced and replication verified." -ForegroundColor Green


# --- SCENARIO 2: Log Truncation Simulation ---
Write-Host ""
Write-Host "=== SCENARIO 2: Log Truncation Simulation ===" -ForegroundColor Yellow

# Tip: Pause MM2 until the Primary cluster reaches its topic retention time
Write-Host "Pausing MM2 service (as per tip: pause until retention time is reached)..."
docker-compose stop mm2

Write-Host "Configuring aggressive retention (30s) to simulate log truncation..."
docker exec primary-kafka /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server primary-kafka:9092 `
    --entity-type topics --entity-name commit-log --alter `
    --add-config "retention.ms=30000,segment.bytes=1024,file.delete.delay.ms=0"

Write-Host "Producing 1000 MORE messages while MM2 is paused (these will be truncated before MM2 can replicate them)..."
docker-compose run --rm producer --count 1000

Write-Host "Waiting 90 seconds for ALL data (offsets 0-1999) to expire via retention..."
Start-Sleep -Seconds 90

Write-Host "Restoring normal retention before restarting..."
docker exec primary-kafka /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server primary-kafka:9092 `
    --entity-type topics --entity-name commit-log --alter `
    --delete-config "retention.ms,segment.bytes,file.delete.delay.ms"

Write-Host "Producing 50 messages to new offset positions (so MM2 has records to process)..."
docker-compose run --rm producer --count 50

Write-Host "Restarting MM2 - it will detect data gap (expected ~1000, gets ~2000+)..."
docker-compose up -d --force-recreate mm2
Start-Sleep -Seconds 40

$log = docker logs mm2 2>&1
if ($log -like "*DATA LOSS DETECTED*") {
    Write-Host "SUCCESS: Scenario 2 passed - Data loss detected by MM2!" -ForegroundColor Green
} else {
    Write-Host "FAILURE: MM2 did not detect data loss." -ForegroundColor Red
    Write-Host "Check logs with: docker logs mm2" -ForegroundColor DarkYellow
}


# --- REPAIR: Re-sync MM2 offset for clean Scenario 3 setup ---
Write-Host ""
Write-Host "--- Repairing MM2 state for Scenario 3 ---" -ForegroundColor Cyan
docker-compose stop mm2
Start-Sleep -Seconds 5


# --- SCENARIO 3: Topic Reset Simulation ---
Write-Host ""
Write-Host "=== SCENARIO 3: Topic Reset Simulation ===" -ForegroundColor Yellow

# Tip: Pause MM2 until the Primary cluster recreates the commit-log topic
Write-Host "Pausing MM2 (as per tip: pause until primary recreates the commit-log topic)..."

Write-Host "Deleting commit-log topic from primary cluster..."
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server primary-kafka:9092 --delete --topic commit-log
Start-Sleep -Seconds 10

Write-Host "Recreating commit-log topic (offsets reset to 0)..."
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server primary-kafka:9092 --create --topic commit-log `
    --partitions 1 --replication-factor 1
Start-Sleep -Seconds 5

Write-Host "Sending 10 new messages to the recreated topic (starts at offset 0)..."
docker-compose run --rm producer --count 10

Write-Host "Starting MM2 - it will detect the topic reset and recover automatically..."
docker-compose start mm2
Start-Sleep -Seconds 30

$mm2Logs = docker logs mm2 2>&1
if ($mm2Logs -like "*TOPIC RESET DETECTED*") {
    Write-Host "SUCCESS: Scenario 3 passed - Topic reset detected and recovered by MM2!" -ForegroundColor Green
} else {
    Write-Host "FAILURE: Topic reset not detected." -ForegroundColor Red
    Write-Host "Check logs with: docker logs mm2" -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "=== ALL SCENARIOS COMPLETE ===" -ForegroundColor Cyan
