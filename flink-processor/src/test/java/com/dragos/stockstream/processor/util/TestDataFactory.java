package com.dragos.stockstream.processor.util;

import com.dragos.stockstream.processor.model.StockPrice;
import com.dragos.stockstream.processor.model.StockPriceAggregate;

import java.time.Instant;

/**
 * Factory class for creating test data objects for Flink processor.
 */
public class TestDataFactory {
    
    public static StockPrice createStockPrice(String symbol, Double price) {
        return StockPrice.builder()
                .symbol(symbol)
                .price(price)
                .timestamp(Instant.now())
                .change(0.5)
                .changePercent(0.27)
                .build();
    }
    
    public static StockPrice createStockPriceWithTimestamp(String symbol, Double price, Instant timestamp) {
        return StockPrice.builder()
                .symbol(symbol)
                .price(price)
                .timestamp(timestamp)
                .change(0.5)
                .changePercent(0.27)
                .build();
    }
    
    public static StockPriceAggregate createAggregate(String symbol) {
        return StockPriceAggregate.builder()
                .symbol(symbol)
                .avgPrice(100.0)
                .minPrice(99.0)
                .maxPrice(101.0)
                .count(10L)
                .windowStart(Instant.now().minusSeconds(5))
                .windowEnd(Instant.now())
                .build();
    }
    
    public static StockPriceAggregate createAggregateWithPrices(String symbol, double min, double max, double avg, long count) {
        return StockPriceAggregate.builder()
                .symbol(symbol)
                .avgPrice(avg)
                .minPrice(min)
                .maxPrice(max)
                .count(count)
                .windowStart(Instant.now().minusSeconds(5))
                .windowEnd(Instant.now())
                .build();
    }
}
