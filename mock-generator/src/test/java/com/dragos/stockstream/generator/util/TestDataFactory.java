package com.dragos.stockstream.generator.util;

import com.dragos.stockstream.generator.model.StockPrice;

import java.time.Instant;

/**
 * Factory class for creating test data objects.
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
    
    public static StockPrice createStockPriceWithChange(String symbol, Double price, Double change, Double changePercent) {
        return StockPrice.builder()
                .symbol(symbol)
                .price(price)
                .timestamp(Instant.now())
                .change(change)
                .changePercent(changePercent)
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
}
