package com.dragos.stockstream.api.service;

import com.dragos.stockstream.api.model.StockPriceAggregate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Service for consuming aggregated stock price data from Kafka
 * and broadcasting to connected clients via SSE.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final StockStreamService stockStreamService;

    /**
     * Kafka listener that consumes aggregated stock price data and broadcasts to clients.
     * 
     * @param aggregate Stock price aggregate message from Kafka
     */
    @KafkaListener(topics = "${spring.kafka.consumer.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeStockAggregate(StockPriceAggregate aggregate) {
        log.debug("Received stock aggregate from Kafka: symbol={}, avgPrice={}, count={}", 
                aggregate.getSymbol(), aggregate.getAvgPrice(), aggregate.getCount());
        
        // Broadcast to all connected SSE clients
        stockStreamService.broadcastStockUpdate(aggregate);
    }
}
