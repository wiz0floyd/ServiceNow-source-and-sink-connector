<#
.SYNOPSIS
  End-to-end smoke test for HermesSourceConnector using Confluent Platform CE in Docker.

.DESCRIPTION
  Topology (all containers):
    hermes-1  — Hermes source cluster 1 simulator (plaintext, port 14100 on host)
    hermes-2  — Hermes source cluster 2 simulator (plaintext, port 14200 on host)
    broker    — Confluent Cloud destination simulator (plaintext, port 9092 on host)
    connect   — Kafka Connect worker (port 8083)

  Test flow:
    1. Build connector JAR
    2. Copy JAR to plugins/ so Connect can load it
    3. docker-compose up
    4. Create topics on hermes-1, hermes-2, broker
    5. Deploy source connector via Connect REST API
    6. Produce test messages to hermes-1 and hermes-2
    7. Consume from cc-destination-topic and verify messages arrive from both clusters
    8. Tear down

.NOTES
  Prerequisites: Docker Desktop, Java 11, Maven (or add to PATH first)
  Run from the repo root: .\scripts\e2e-smoke.ps1
#>

$ErrorActionPreference = "Stop"
$ROOT = Split-Path $PSScriptRoot -Parent
$HERMES_TOPIC = "hermes-source-topic"
$CC_TOPIC = "cc-destination-topic"
$CONNECT_URL = "http://localhost:8083"
$CONNECTOR_NAME = "hermes-source-e2e"
$MVN = "C:\tools\maven\apache-maven-3.9.6\bin\mvn.cmd"

function Wait-ForBroker {
    param($bootstrap, $label, [int]$maxRetries = 30)
    Write-Host "Waiting for $label ($bootstrap)..."
    for ($i = 0; $i -lt $maxRetries; $i++) {
        $result = & docker run --rm --network hermes-kafka-connector_default `
            confluentinc/cp-kafka:7.6.1 `
            kafka-broker-api-versions --bootstrap-server $bootstrap 2>$null
        if ($LASTEXITCODE -eq 0) { Write-Host "$label is ready."; return }
        Start-Sleep 2
    }
    throw "$label did not become ready in time."
}

function Wait-ForConnect {
    param([int]$maxRetries = 40)
    Write-Host "Waiting for Kafka Connect REST API..."
    for ($i = 0; $i -lt $maxRetries; $i++) {
        try {
            $r = Invoke-RestMethod "$CONNECT_URL/connectors" -TimeoutSec 3
            Write-Host "Connect is ready."; return
        } catch { Start-Sleep 3 }
    }
    throw "Connect REST API did not become available in time."
}

function Create-Topic {
    param($bootstrap, $networkAlias, $topic, $partitions = 3)
    Write-Host "Creating topic '$topic' on $networkAlias..."
    docker run --rm --network hermes-kafka-connector_default `
        confluentinc/cp-kafka:7.6.1 `
        kafka-topics --create --if-not-exists `
        --bootstrap-server $networkAlias`:9092 `
        --topic $topic --partitions $partitions --replication-factor 1
}

# ── Step 1: Build ──────────────────────────────────────────────────────────────
Write-Host "`n==> Building connector JAR..."
Push-Location $ROOT
& $MVN --batch-mode package -DskipTests -q
if ($LASTEXITCODE -ne 0) { throw "Maven build failed." }
Pop-Location

# ── Step 2: Stage plugin ───────────────────────────────────────────────────────
Write-Host "`n==> Staging connector plugin..."
$pluginsDir = Join-Path $ROOT "plugins\com.servicenow.kafka.connect\hermes-kafka-connector\0.1.0"
New-Item -ItemType Directory -Force -Path $pluginsDir | Out-Null
$zip = Get-ChildItem "$ROOT\target\components\packages\*.zip" | Select-Object -First 1
Expand-Archive -Path $zip.FullName -DestinationPath $pluginsDir -Force
Write-Host "Plugin staged at: $pluginsDir"

# ── Step 3: Start stack ────────────────────────────────────────────────────────
Write-Host "`n==> Starting Docker stack..."
docker compose -p hermes-kafka-connector -f "$ROOT\docker-compose.yml" up -d

# ── Step 4: Wait for brokers ───────────────────────────────────────────────────
Wait-ForBroker "localhost:9092"    "broker (CC destination)"
Wait-ForBroker "localhost:14100"   "hermes-1 (cluster 1)"
Wait-ForBroker "localhost:14200"   "hermes-2 (cluster 2)"

# ── Step 5: Create topics ──────────────────────────────────────────────────────
Write-Host "`n==> Creating topics..."
Create-Topic "localhost:9092"   "broker"   $CC_TOPIC 3
Create-Topic "localhost:14100"  "hermes-1" $HERMES_TOPIC 3
Create-Topic "localhost:14200"  "hermes-2" $HERMES_TOPIC 3

# ── Step 6: Wait for Connect, deploy connector ────────────────────────────────
Wait-ForConnect
Write-Host "`n==> Deploying source connector..."
$connectorConfig = Get-Content "$ROOT\config\e2e-source-test.json" -Raw
Invoke-RestMethod "$CONNECT_URL/connectors" -Method Post `
    -ContentType "application/json" -Body $connectorConfig | Out-Null

# Wait for RUNNING state
Write-Host "Waiting for connector to reach RUNNING state..."
for ($i = 0; $i -lt 20; $i++) {
    $status = Invoke-RestMethod "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
    if ($status.connector.state -eq "RUNNING") { Write-Host "Connector RUNNING."; break }
    if ($status.connector.state -eq "FAILED")  { throw "Connector FAILED: $($status.connector.trace)" }
    Start-Sleep 3
}

# ── Step 7: Produce test messages ─────────────────────────────────────────────
Write-Host "`n==> Producing test messages to hermes-1 and hermes-2..."
$msgs1 = "from-cluster-1-msg-A`nfrom-cluster-1-msg-B"
$msgs2 = "from-cluster-2-msg-X`nfrom-cluster-2-msg-Y"

docker run --rm --network hermes-kafka-connector_default `
    confluentinc/cp-kafka:7.6.1 bash -c `
    "echo '$msgs1' | kafka-console-producer --broker-list hermes-1:9092 --topic $HERMES_TOPIC"

docker run --rm --network hermes-kafka-connector_default `
    confluentinc/cp-kafka:7.6.1 bash -c `
    "echo '$msgs2' | kafka-console-producer --broker-list hermes-2:9092 --topic $HERMES_TOPIC"

# ── Step 8: Consume and verify ────────────────────────────────────────────────
Write-Host "`n==> Consuming from '$CC_TOPIC' (10s timeout)..."
$received = docker run --rm --network hermes-kafka-connector_default `
    confluentinc/cp-kafka:7.6.1 `
    kafka-console-consumer --bootstrap-server broker:9092 `
    --topic $CC_TOPIC --from-beginning `
    --max-messages 4 --timeout-ms 10000 2>$null

Write-Host "Received messages:`n$received"

$ok = ($received -match "from-cluster-1-msg-A") -and
      ($received -match "from-cluster-1-msg-B") -and
      ($received -match "from-cluster-2-msg-X") -and
      ($received -match "from-cluster-2-msg-Y")

if ($ok) {
    Write-Host "`n[PASS] All 4 messages received from both Hermes clusters. E2E smoke test passed."
} else {
    Write-Warning "`n[FAIL] Not all expected messages were received. Check connector logs:"
    Write-Host "  docker logs connect"
    exit 1
}

# ── Tear down ─────────────────────────────────────────────────────────────────
Write-Host "`n==> Tearing down..."
docker compose -p hermes-kafka-connector -f "$ROOT\docker-compose.yml" down -v
Write-Host "Done."
