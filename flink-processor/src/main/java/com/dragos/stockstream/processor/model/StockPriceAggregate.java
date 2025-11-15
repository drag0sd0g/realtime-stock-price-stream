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
 * Represents aggregated stock price data over a time window.
 * Contains symbol, statistics (avg, min, max), count, and window boundaries.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockPriceAggregate implements Serializable {
    
    /**
     * Stock symbol (e.g., AAPL, MSFT)
     */
    private String symbol;
    
    /**
     * Average price in the time window
     */
    private Double avgPrice;
    
    /**
     * Minimum price in the time window
     */
    private Double minPrice;
    
    /**
     * Maximum price in the time window
     */
    private Double maxPrice;
    
    /**
     * Number of price updates in the time window
     */
    private Long count;
    
    /**
     * Start of the time window
     */
    @JsonDeserialize(using = InstantDeserializer.class)
    private Instant windowStart;
    
    /**
     * End of the time window
     */
    @JsonDeserialize(using = InstantDeserializer.class)
    private Instant windowEnd;
}
