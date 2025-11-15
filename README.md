# 📈 Real-Time Stock Price Streaming Platform

A comprehensive real-time data streaming platform that demonstrates modern event-driven architecture using **Apache Kafka**, **Apache Flink**, and **Server-Sent Events (SSE)**. This project processes live stock price data through a complete streaming pipeline, from data generation to real-time visualization.

## 🏗️ Architecture Overview

```
┌─────────────────────┐
│  Mock Generator     │  Generates mock stock prices (50 tech stocks)
│  (Spring Boot)      │  Publishes every 500ms
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Apache Kafka      │  Topic: stock-prices
│   (KRaft Mode)      │  High-throughput message broker
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Flink Processor   │  5-second tumbling windows
│   (Stream API)      │  Aggregation: min/max/avg per symbol
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Apache Kafka      │  Topic: stock-prices-aggregated
│                     │  Aggregated metrics
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Backend API       │  Consumes aggregated data
│   (Spring Boot)     │  Exposes SSE endpoint
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   React Frontend    │  Real-time dashboard
│   (TypeScript)      │  Live charts & updates
└─────────────────────┘
```

## 🚀 Features

- **Real-Time Data Processing**: Sub-second latency from generation to visualization
- **Event-Driven Architecture**: Fully decoupled microservices communicating via Kafka
- **Stream Processing**: Windowed aggregations using Apache Flink
- **Server-Sent Events (SSE)**: Efficient server-to-client streaming
- **Scalable Infrastructure**: Containerized with Docker Compose
- **Modern Tech Stack**: Java 21, Spring Boot 3.2, Flink 1.19, React 18
- **50 Tech Stocks**: Real-time simulation of major tech companies (AAPL, MSFT, GOOGL, NVDA, etc.)

## 📦 Tech Stack

### Infrastructure

- **Apache Kafka 7.6.0** (KRaft mode - no Zookeeper!)
- **Apache Flink 1.19.1** (Stream processing)
- **Docker & Docker Compose**

### Backend

- **Java 21** (LTS)
- **Spring Boot 3.2.0**
- **Spring Kafka**
- **Maven**

### Frontend

- **React 18**
- **TypeScript**
- **Vite**
- **Recharts** (for visualization)

## 🏁 Quick Start

### Prerequisites

- **Docker Desktop** (for Mac/Windows) or **Docker Engine** (for Linux)
- **Java 21** ([Download here](https://adoptium.net/))
- **Maven 3.9+** ([Download here](https://maven.apache.org/download.cgi))
- **Node.js 18+** ([Download here](https://nodejs.org/))

### 1️⃣ Start Infrastructure (Kafka + Flink)

```bash
# Clone the repository
git clone https://github.com/drag0sd0g/realtime-stock-price-stream.git
cd realtime-stock-price-stream

# Start Kafka and Flink
docker-compose up -d

# Verify containers are running
docker ps
```

You should see 3 containers running:

- `kafka` on port 9092
- `flink-jobmanager` on port 8081
- `flink-taskmanager`

**Access Flink Web UI**: http://localhost:8081

### 2️⃣ Run Mock Stock Price Generator

```bash
# Navigate to mock generator
cd mock-generator

# Build and run
mvn clean install
mvn spring-boot:run
```

The generator will start publishing stock prices for **50 tech companies** to Kafka topic `stock-prices` every 500ms.

**Stocks included**: AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, AVGO, ORCL, ADBE, CRM, CSCO, ACN, INTC, AMD, IBM, INTU, NOW, QCOM, TXN, AMAT, PANW, MU, ADI, LRCX, KLAC, SNPS, CDNS, MCHP, NXPI, COIN, SQ, SHOP, SNOW, DDOG, ZS, CRWD, NET, OKTA, TEAM, SPLK, MDB, WDAY, ZM, DOCU, TWLO, UBER, LYFT, DASH, ABNB

**Verify it's working**:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-prices \
  --from-beginning
```

### 3️⃣ Run Flink Stream Processor

**Option A: Run Locally (for development)**

```bash
# Navigate to Flink processor
cd flink-processor

# Build the project
mvn clean package

# Run locally
mvn exec:java -Dexec.mainClass="com.dragos.stockstream.processor.StockStreamProcessor"
```

**Option B: Submit to Flink Cluster (production-like)**

```bash
# Build the JAR
cd flink-processor
mvn clean package

# Copy JAR to Flink JobManager
docker cp target/flink-processor-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/usrlib/

# Submit job to Flink
docker exec -it flink-jobmanager flink run /opt/flink/usrlib/flink-processor-1.0-SNAPSHOT.jar
```

**Monitor the job**: http://localhost:8081

The processor will:

- Consume from `stock-prices` topic
- Apply **5-second tumbling windows**
- Calculate **min, max, avg** price per symbol
- Publish aggregates to `stock-prices-aggregated` topic

**Verify aggregated data**:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-prices-aggregated \
  --from-beginning
```

### 4️⃣ Run Backend API (Coming Soon)

```bash
cd backend-api
mvn clean install
mvn spring-boot:run
```

Backend will expose SSE endpoint at: `http://localhost:8080/api/stocks/stream`

### 5️⃣ Run React Frontend (Coming Soon)

```bash
cd frontend
npm install
npm run dev
```

Frontend will be available at: `http://localhost:5173`

## 📂 Project Structure

```
realtime-stock-price-stream/
├── docker-compose.yml              # Infrastructure setup
├── pom.xml                         # Parent POM
├── mock-generator/                 # Stock price generator
│   ├── pom.xml
│   └── src/main/java/com/dragos/stockstream/generator/
│       ├── MockGeneratorApplication.java
│       ├── model/
│       │   └── StockPrice.java
│       ├── service/
│       │   └── StockPriceGeneratorService.java
│       └── config/
│           └── KafkaProducerConfig.java
├── flink-processor/                # Stream processing
│   ├── pom.xml
│   └── src/main/java/com/dragos/stockstream/processor/
│       ├── StockStreamProcessor.java
│       ├── model/
│       │   ├── StockPrice.java
│       │   └── StockPriceAggregate.java
│       ├── serialization/
│       │   ├── StockPriceDeserializationSchema.java
│       │   └── StockPriceAggregateSerializationSchema.java
│       └── functions/
│           └── StockPriceAggregator.java
├── backend-api/                    # Spring Boot API (Coming Soon)
│   └── ...
└── frontend/                       # React UI (Coming Soon)
    └── ...
```

## 🔧 Configuration

### Kafka Topics

- **`stock-prices`**: Raw stock price events (50 stocks, 500ms interval)
- **`stock-prices-aggregated`**: Windowed aggregations (5-second windows)

### Flink Configuration

- **Parallelism**: 1 (optimized for development on M1 Mac 8GB RAM)
- **Window Type**: Tumbling Event Time Windows (5 seconds)
- **Watermark Strategy**: 5-second bounded out-of-orderness
- **Checkpointing**: Disabled (enable for production)

## 📊 Data Flow Example

**Raw Stock Price (from Generator)**:

```json
{
  "symbol": "AAPL",
  "price": 182.45,
  "timestamp": "2025-11-15T14:29:31Z",
  "change": 1.23,
  "changePercent": 0.68
}
```

**Aggregated Stock Price (from Flink)**:

```json
{
  "symbol": "AAPL",
  "avgPrice": 182.34,
  "minPrice": 181.89,
  "maxPrice": 182.67,
  "count": 10,
  "windowStart": "2025-11-15T14:29:30Z",
  "windowEnd": "2025-11-15T14:29:35Z"
}
```

## 🛠️ Development

### Build All Modules

```bash
mvn clean install
```

### Run Tests

```bash
mvn test
```

## 🧪 Testing

This project includes a comprehensive test suite covering unit tests, integration tests, and end-to-end scenarios.

### Run All Tests

```bash
mvn test
```

### Run Tests for Specific Module

```bash
# Mock Generator tests
cd mock-generator
mvn test

# Flink Processor tests
cd flink-processor
mvn test

# Backend API tests
cd backend-api
mvn test
```

### Generate Coverage Report

```bash
mvn jacoco:report
# Open target/site/jacoco/index.html in browser
```

### Test Coverage Goals

- **Unit Tests**: 80%+ coverage
- **Integration Tests**: Cover all critical paths
- **End-to-End Tests**: Complete pipeline validation

### Test Structure

- **Mock Generator Tests**: Test data generation, Kafka production, and scheduling
- **Flink Processor Tests**: Test aggregation functions, serialization/deserialization
- **Backend API Tests**: Test SSE streaming, Kafka consumption, and REST endpoints

### CI/CD Pipeline

Tests are automatically run on every push and pull request via GitHub Actions. The pipeline includes:
- Building all modules
- Running all tests
- Generating coverage reports
- Publishing test results

### Clean Docker Resources

```bash
docker-compose down -v
docker system prune -a
```

### Troubleshooting

**Kafka won't start:**

```bash
docker-compose down -v
docker-compose up -d
docker logs kafka
```

**Flink job failed:**

- Check logs: `docker logs flink-jobmanager`
- Verify Kafka is running: `docker ps`
- Check Flink Web UI: http://localhost:8081

## 🎯 Learning Objectives

This project demonstrates:

1. **Event-Driven Microservices**: Decoupled services communicating via Kafka
2. **Stream Processing**: Real-time aggregation with Apache Flink
3. **Windowing Concepts**: Tumbling, sliding, and session windows
4. **Event Time Processing**: Handling out-of-order events with watermarks
5. **Server-Sent Events (SSE)**: Efficient server-to-client streaming
6. **Docker Orchestration**: Multi-container application setup
7. **Modern Java**: Java 21 features, Spring Boot 3, reactive programming
8. **Real-Time Visualization**: Building responsive dashboards with React

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Dragos** ([@drag0sd0g](https://github.com/drag0sd0g))

Built with ☕ and passion for real-time data streaming!

## 🔗 Useful Links

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Flink Documentation](https://flink.apache.org/docs/stable/)
- [Spring Kafka Reference](https://spring.io/projects/spring-kafka)
- [React Documentation](https://react.dev/)
- [Server-Sent Events (MDN)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)

---

**⭐ If you find this project useful, please consider giving it a star!**
