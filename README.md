# Real-time Stock Price Streaming System

## Project Overview
This is a real-time stock price streaming system that allows users to view stock prices as they are updated in real-time.

## Architecture Diagram
The architecture consists of the following components:
- **Mock Generator**: Generates random stock price data.
- **Kafka**: Streams the data.
- **Flink**: Processes the data in real-time.
- **SSE**: Serves the processed data.
- **React**: Provides a frontend for users to interact with the data.

## Tech Stack
- **Java**: 21
- **Flink**: 1.19.1
- **Kafka**: 3.8.0 KRaft mode
- **Spring Boot**
- **React**

## Prerequisites and Setup Instructions
1. Ensure JDK 21 is installed.
2. Install Docker and Docker Compose.
3. Clone the repository.
4. Run `docker-compose up` in the project directory.

## Module Descriptions
- **mock-generator**: Generates mock stock price data.
- **flink-processor**: Processes the streaming data using Flink.
- **backend-api**: Serves the processed data to the frontend.
- **frontend**: A React application to display stock prices.

## Development Phases
1. Setup
2. Development
3. Testing
4. Deployment

## How to Run with Docker Compose
Use the following command:
```
 docker-compose up
```

## Running the Mock Generator

1. Start Kafka and Flink:
   ```bash
   docker-compose up -d
   ```

2. Build and run the mock generator:
   ```bash
   cd mock-generator
   mvn clean install
   mvn spring-boot:run
   ```

3. The generator will start publishing stock prices to Kafka topic `stock-prices` every 500ms

The mock generator produces realistic stock price data for 50 top tech companies including AAPL, MSFT, GOOGL, AMZN, NVDA, and more. Each stock has:
- Different volatility levels based on market cap (2% for large caps, 3% for mid caps, 5% for smaller caps)
- Random walk price movement with mean reversion
- Realistic base prices and price changes
