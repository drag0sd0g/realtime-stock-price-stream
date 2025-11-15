package com.dragos.stockstream.api.util;

import com.dragos.stockstream.api.model.StockPriceAggregate;

import java.time.Instant;

/**
 * Factory class for creating test data objects for backend API.
 */
public class TestDataFactory {
    
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
    
    public static StockPriceAggregate createAggregateWithWindow(String symbol, Instant start, Instant end) {
        return StockPriceAggregate.builder()
                .symbol(symbol)
                .avgPrice(100.0)
                .minPrice(99.0)
                .maxPrice(101.0)
                .count(10L)
                .windowStart(start)
                .windowEnd(end)
                .build();
    }
}
