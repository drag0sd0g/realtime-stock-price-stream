package com.dragos.stockstream.processor.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * Represents a stock price data point.
 * Contains symbol, price, timestamp, and change information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockPrice implements Serializable {
    
    /**
     * Stock symbol (e.g., AAPL, MSFT)
     */
    private String symbol;
    
    /**
     * Current stock price
     */
    private Double price;
    
    /**
     * Timestamp when the price was generated
     */
    @JsonDeserialize(using = InstantDeserializer.class)
    private Instant timestamp;
    
    /**
     * Absolute change in price from previous value
     */
    private Double change;
    
    /**
     * Percentage change in price from previous value
     */
    private Double changePercent;
}
