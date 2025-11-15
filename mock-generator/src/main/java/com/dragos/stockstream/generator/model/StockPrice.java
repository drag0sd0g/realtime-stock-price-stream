package com.dragos.stockstream.generator.model;

import com.fasterxml.jackson.annotation.JsonFormat;
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
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
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
