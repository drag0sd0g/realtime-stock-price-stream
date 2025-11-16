#!/bin/bash

###############################################################################
# dev.sh -- Local Development Orchestrator for Real-Time Stock Price Stream
#
# Usage:
#   ./dev.sh up     # Build and start all components + infra
#   ./dev.sh down   # Stop all components and dependent infra
#
###############################################################################

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

start_infra() {
  echo "🚀 Starting infrastructure with docker-compose..."
  docker-compose up -d
  echo "⌛ Waiting for Kafka ($KAFKA_PORT), Flink ($FLINK_JOBMANAGER_WEB_PORT) to be up..."
  until check_port $KAFKA_PORT && check_port $FLINK_JOBMANAGER_WEB_PORT; do sleep 2; done
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
  FLINK_JAR=$(ls "$FLINK_PROC_DIR"/target/*-shaded.jar 2>/dev/null | head -n 1)
  if [ ! -f "$FLINK_JAR" ]; then
    echo "❌ No Flink fat jar found! Have you built flink-processor?"
    return
  fi
  echo "🚀 Submitting Flink job to cluster: $FLINK_JAR"
  docker exec -u root -i $(docker ps --filter "name=flink-jobmanager" --format "{{.Names}}") \
    flink run -d "$FLINK_JAR"
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
      # If not dead instantly, force kill
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
  start_infra
  start_api
  start_frontend
  submit_flink_job
  start_mock_generator
  open_uis
  echo ""
  echo "✅ All services are up!"
  echo ""
  echo "To stop all: ./dev.sh down OR press Ctrl+C (safely cleans up)"

  # WAIT LOOP - needs to hang until user hits Ctrl+C
  while true; do
    sleep 10
  done
}

# Trap Ctrl+C and exit signals to perform cleanup while script is running
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