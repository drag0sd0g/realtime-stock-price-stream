package com.dragos.stockstream.api.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents aggregated stock price data over a time window.
 * Contains symbol, statistics (avg, min, max), count, and window boundaries.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockPriceAggregate {
    private String symbol;
    private Double avgPrice;
    private Double minPrice;
    private Double maxPrice;
    private Long count;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
    private Instant windowStart;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
    private Instant windowEnd;
}
