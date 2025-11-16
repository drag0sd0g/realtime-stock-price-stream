[日本語で読む (Japanese)](README.ja.md)

# 📈 Real-Time Stock Price Streaming Platform

A modern, production-grade real-time data streaming platform combining **Apache Kafka**, **Apache Flink**, **Spring Boot (SSE API)**, and a live **React** dashboard. This project demonstrates an end-to-end streaming architecture: from simulated (or real) stock market data, through tumbling window aggregation, to instant UI visualization.

> **Note:**  
> The current _mock generator_ simulates live stock price feeds.  
> **It's designed for plug-and-play replacement** with any real-world market data (exchange, broker, REST, or WebSocket vendor feed).

---

## 🏗️ Architecture Overview

```
┌─────────────────────┐
│  Mock Generator     │  • Simulated prices for 50 tech stocks, 500ms intervals
│  (Spring Boot)      │  • (Replaceable with live market data feeds)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Apache Kafka      │  • KRaft mode (no Zookeeper)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Flink Processor   │  • Tumbling event-time windows (default: 10s)
│   (Java/Flink)      │  • Aggregates per symbol (avg, min, max, count)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Apache Kafka      │  • Aggregated topic: "stock-prices-aggregated"
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Backend API       │  • Consumes aggregates, SSE endpoint: /api/stocks/stream
│   (Spring Boot)     │  • Multi-client, efficient server-push to frontend
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   React Frontend    │  • Real-time dashboard (Vite/TS)
│                     │  • Live grid, connection status, auto-refresh
└─────────────────────┘
```

---

## 🚀 Features

- **End-to-End Real-Time:** Millisecond-latency pipeline from feed → dashboard
- **Pluggable Data Source:** Swap out simulated prices for true live equity/ticker feeds anytime
- **Event-Driven Microservices:** All parts communicate asynchronously over Kafka
- **Stream Aggregation with Flink:** Tumbling windows, min/max/avg/count per symbol
- **Production SSE Streaming:** Scalable, minimal-latency server-push with backpressure
- **Live Interactive UI:** React (TS) dashboard, auto updates, connection health
- **One-Command Dev Orchestration:** All-in-one startup (dev.sh), Docker Compose infra, or classic mode

---

## 🖥️ Demo

<p align="center">
  <img src="./Demo.gif" alt="Realtime Dashboard Demo" width="700"/>
</p>

---

## 📦 Tech Stack

### Infrastructure

- **Apache Kafka 7.6.0** (KRaft mode)
- **Apache Flink 1.19.1**
- **Docker & Docker Compose**

### Backend

- **Java 17** (LTS)
- **Spring Boot 3.2.0**
- **Spring Kafka**
- **Maven**

### Frontend

- **React 18** + **TypeScript** + **Vite**
- **Recharts** (for live charts)

---

## 🏁 Quick Start

### Prerequisites

- [Docker Desktop](https://docs.docker.com/desktop/) or **Docker Engine** (for Linux)
- **Java 17** ([Download](https://adoptium.net/))
- **Maven 3.9+** ([Download](https://maven.apache.org/download.cgi))
- **Node.js 18+** ([Download](https://nodejs.org/))

---

### 1️⃣ Start All Services With a Single Command

**(Recommended for devs):**

```bash
git clone https://github.com/drag0sd0g/realtime-stock-price-stream.git
cd realtime-stock-price-stream

# Build everything & spin up all microservices/infrastructure
./dev.sh up
```

- **Flink UI:** http://localhost:8081
- **React Dashboard:** http://localhost:5173

**To stop:**

```bash
./dev.sh down
```

#### Manual mode for each component

<details>
<summary>Expand for detailed manual commands</summary>

#### Start Kafka/Flink infrastructure

```bash
docker-compose up -d
```

#### Mock Price Generator (Spring Boot)

```bash
cd mock-generator
mvn clean install
mvn spring-boot:run
```

#### Flink Job (run locally or on Flink cluster)

```bash
cd flink-processor
mvn clean package

# Local Java (dev)
mvn exec:java -Dexec.mainClass="com.dragos.stockstream.processor.StockStreamProcessor"

# On Flink cluster
docker cp target/flink-processor-*.jar flink-jobmanager:/opt/flink/usrlib/
docker exec -it flink-jobmanager flink run /opt/flink/usrlib/flink-processor-*.jar
```

#### Backend API (Spring Boot, SSE)

```bash
cd backend-api
mvn clean install
mvn spring-boot:run
# API at http://localhost:8080
```

#### React Frontend

```bash
cd frontend
npm install
npm run dev
# UI at http://localhost:5173
```

</details>

---

### 2️⃣ Explore & Verify

**Raw Stock Data**

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-prices --from-beginning
```

**Aggregated Output**

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-prices-aggregated --from-beginning
```

**REST/SSE Stream**

```bash
curl http://localhost:8080/api/stocks/stream
```

**Live UI:**  
Visit [http://localhost:5173](http://localhost:5173) — connection status ("Live") means end-to-end is streaming.

---

## 📂 Project Structure

```
realtime-stock-price-stream/
├── docker-compose.yml     # Infra config
├── dev.sh                 # Orchestrator script (local dev)
├── mock-generator/        # (Replaceable) stock price source
├── flink-processor/       # Flink: streaming job
├── backend-api/           # Spring Boot: API/SSE/kafka consumer
└── frontend/              # React dashboard
```

---

## 🔧 Configuration

### Kafka Topics

- **stock-prices:** Raw tick data
- **stock-prices-aggregated:** Windowed aggregates

### Flink

- **Window:** Tumbling, event-time (default: 10s, adjustable)
- **Watermark:** 5s out-of-orderness
- **Parallelism:** 1 (dev mode, adjust for prod)

---

## ℹ️ Swapping the Mock Generator for Real Market Data

The `mock-generator` module is a fully decoupled microservice that **only depends on Kafka**.

- **To join real data:** replace/mock out its stock feed code with your streaming or polling code (REST, WebSocket, vendor-provided SDK, etc.)
- The rest of the pipeline (Kafka, Flink, backend, UI) just works without code changes.

---

## 📊 Data Example

**Mock Generator Output (`stock-prices`):**

```json
{
  "symbol": "AAPL",
  "price": 182.45,
  "timestamp": "2025-11-15T14:29:31Z",
  "change": 1.23,
  "changePercent": 0.68
}
```

**Flink Aggregate Output (`stock-prices-aggregated`):**

```json
{
  "symbol": "AAPL",
  "avgPrice": 182.34,
  "minPrice": 181.89,
  "maxPrice": 182.67,
  "count": 10,
  "windowStart": "2025-11-15T14:29:30Z",
  "windowEnd": "2025-11-15T14:29:40Z"
}
```

---

## 🛠️ Developer Docs

### Build All

```bash
mvn clean install
```

### Run Tests for a Module

```bash
# In module dir (e.g., backend-api)
mvn test
```

### Generate Coverage Report

```bash
mvn jacoco:report
```

Open `target/site/jacoco/index.html` for the report.

### Clean Docker State

```bash
docker-compose down -v
docker system prune -a
```

---

## 🧪 Troubleshooting

- **Kafka/infra won’t start:**
  ```bash
  docker-compose down -v
  docker-compose up -d
  docker logs kafka
  ```
- **Flink job failed:**  
  Check: `docker logs flink-jobmanager`, [Flink Web UI](http://localhost:8081)
- **No data on UI:**
  - Try `/api/stocks/stream` in browser/curl
  - Check Chrome Console & Network tab for errors/event delivery
  - Check backend logs for deserialization errors or Kafka disconnects

---

## 🎯 What You'll Learn

- Microservices via events: async, decoupled with Kafka
- Stream processing and windowing in Apache Flink
- Real-time dashboards and web event streaming (SSE)
- Infrastructure-as-code for local dev
- Making simulated pipelines ready for real data sources

---

## 🤝 Contributing

All PRs and suggestions welcome!  
If you find it useful, star ⭐ the repo.

---

## 📝 License

MIT License — [LICENSE](LICENSE)

---

## 👨‍💻 Author

[@drag0sd0g](https://github.com/drag0sd0g)  
Built with ☕ and a love of data streaming.

---

## 🔗 Useful Links

- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Apache Flink Docs](https://flink.apache.org/docs/stable/)
- [Spring Kafka](https://spring.io/projects/spring-kafka)
- [React](https://react.dev/)
- [SSE (MDN)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
