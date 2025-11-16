#!/bin/bash

API_DIR="backend-api"
MOCK_GEN_DIR="mock-generator"
FLINK_PROC_DIR="flink-processor"
FRONTEND_DIR="frontend"

API_PORT=8080
FRONTEND_PORT=5173
FLINK_JOBMANAGER_WEB_PORT=8081
KAFKA_PORT=9092

PIDS=()
PIDFILES=(backend-api.pid frontend.pid mock-generator.pid)

open_browser() {
  if command -v open >/dev/null; then open "$1"
  elif command -v xdg-open >/dev/null; then xdg-open "$1"
  fi
}

check_port() {
  nc -z localhost "$1" >/dev/null 2>&1
}

build_fat_jars() {
  echo "🔨 Building all Java modules (API, Mock, Flink)..."
  mvn -B clean package -DskipTests
}

start_kafka_and_create_topics() {
  echo "🚀 Starting Kafka..."
  docker-compose up -d kafka

  echo "⌛ Waiting for Kafka container health..."
  until [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ]; do
    sleep 2
    echo "$(date '+%H:%M:%S') … waiting for kafka health…"
  done
  echo "✅ Kafka is healthy!"

  echo "📚 Creating required Kafka topics (if not exist)..."
  docker exec kafka kafka-topics --create --topic stock-prices --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --if-not-exists
  docker exec kafka kafka-topics --create --topic stock-prices-aggregated --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --if-not-exists
}

start_flink_services() {
  echo "🚀 Starting Flink JobManager and TaskManager..."
  docker-compose up -d flink-jobmanager flink-taskmanager

  echo "⌛ Waiting for Flink JM ($FLINK_JOBMANAGER_WEB_PORT) to be up..."
  until check_port $FLINK_JOBMANAGER_WEB_PORT; do
    sleep 2
    echo "$(date '+%H:%M:%S') … waiting for Flink UI port…"
  done
  echo "✅ Flink JM is ready!"
}

start_api() {
  echo "🚀 Starting backend API..."
  pushd "$API_DIR" > /dev/null
    java -jar target/backend-api*.jar &
    echo $! > ../backend-api.pid
    PIDS+=($!)
  popd > /dev/null
  echo "⌛ Waiting for API on :$API_PORT..."
  until check_port $API_PORT; do sleep 1; done
}

start_frontend() {
  echo "🚀 Starting React frontend dev server..."
  pushd "$FRONTEND_DIR" > /dev/null
    if ! [ -d node_modules ]; then
      npm install
    fi
    npm run dev -- --port $FRONTEND_PORT &
    echo $! > ../frontend.pid
    PIDS+=($!)
  popd > /dev/null
  echo "⌛ Waiting for frontend on :$FRONTEND_PORT..."
  until check_port $FRONTEND_PORT; do sleep 1; done
}

submit_flink_job() {
  echo "🔍 Locating Flink fat JAR..."
  FLINK_JAR=$(ls "$FLINK_PROC_DIR"/target/*-jar-with-dependencies.jar 2>/dev/null | head -n 1)
  if [ ! -f "$FLINK_JAR" ]; then
    echo "❌ No Flink fat jar found! Have you built flink-processor with dependencies?"
    down
    exit 1
  fi
  FLINK_JAR_NAME=$(basename "$FLINK_JAR")
  echo "📁 Ensuring /opt/flink/usrlib exists in JobManager..."
  docker exec flink-jobmanager mkdir -p /opt/flink/usrlib || { echo "❌ Could not create usrlib dir!"; down; exit 1; }
  echo "🚚 Copying fat JAR into flink-jobmanager container..."
  docker cp "$FLINK_JAR" flink-jobmanager:/opt/flink/usrlib/"$FLINK_JAR_NAME" || { echo "❌ JAR copy failed!"; down; exit 1; }
  echo "🚀 Submitting Flink job to cluster: $FLINK_JAR_NAME"
  docker exec -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -i flink-jobmanager flink run -d /opt/flink/usrlib/"$FLINK_JAR_NAME"
  if [ $? -ne 0 ]; then
    echo "❌ Flink job submission failed!"
    down
    exit 1
  fi
  echo "✅ Flink job submitted."
}

start_mock_generator() {
  echo "🚀 Starting mock-generator..."
  pushd "$MOCK_GEN_DIR" > /dev/null
    java -jar target/mock-generator*.jar &
    echo $! > ../mock-generator.pid
    PIDS+=($!)
  popd > /dev/null
}

open_uis() {
  echo "🌐 Opening Flink UI and Frontend UI in browser..."
  open_browser "http://localhost:$FLINK_JOBMANAGER_WEB_PORT"
  open_browser "http://localhost:$FRONTEND_PORT"
}

stop_process() {
  pidfile="$1"
  if [[ -f "$pidfile" ]]; then
    local pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
      sleep 1
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid"
      fi
    fi
    rm -f "$pidfile"
    echo "✋ Stopped process (pid: $pid) from $pidfile"
  fi
}

down() {
  echo "🛑 Stopping backend, frontend, mock-generator..."
  for pf in "${PIDFILES[@]}"; do
    stop_process "$pf"
  done
  echo "🛑 Shutting down docker-compose infra..."
  docker-compose down
  echo "🎉 Everything stopped."
  exit 0
}

up() {
  build_fat_jars
  start_kafka_and_create_topics
  start_flink_services
  start_api
  start_frontend
  submit_flink_job
  start_mock_generator
  open_uis
  echo ""
  echo "✅ All services are up!"
  echo ""
  echo "To stop all: ./dev.sh down OR press Ctrl+C (safely cleans up)"
  while true; do sleep 10; done
}

trap 'echo ""; echo "Caught interrupt. Tearing down..."; down; exit 130' SIGINT SIGTERM

case "$1" in
  up|"")
    up
    ;;
  down)
    down
    ;;
  *)
    echo "Usage: $0 [up|down]"
    ;;
esac