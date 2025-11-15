package com.dragos.stockstream.processor.functions;

import com.dragos.stockstream.processor.model.StockPrice;
import com.dragos.stockstream.processor.model.StockPriceAggregate;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Aggregate function that computes statistics (avg, min, max, count) 
 * for stock prices within a time window.
 */
public class StockPriceAggregator implements AggregateFunction<StockPrice, StockPriceAggregator.Accumulator, StockPriceAggregate> {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Accumulator to track intermediate aggregation state
     */
    public static class Accumulator {
        public String symbol;
        public double sum = 0.0;
        public double min = Double.MAX_VALUE;
        public double max = Double.MIN_VALUE;
        public long count = 0;
    }
    
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }
    
    @Override
    public Accumulator add(StockPrice value, Accumulator accumulator) {
        if (accumulator.symbol == null) {
            accumulator.symbol = value.getSymbol();
        }
        
        accumulator.sum += value.getPrice();
        accumulator.min = Math.min(accumulator.min, value.getPrice());
        accumulator.max = Math.max(accumulator.max, value.getPrice());
        accumulator.count++;
        
        return accumulator;
    }
    
    @Override
    public StockPriceAggregate getResult(Accumulator accumulator) {
        double avgPrice = accumulator.count > 0 ? accumulator.sum / accumulator.count : 0.0;
        
        return StockPriceAggregate.builder()
                .symbol(accumulator.symbol)
                .avgPrice(avgPrice)
                .minPrice(accumulator.min != Double.MAX_VALUE ? accumulator.min : 0.0)
                .maxPrice(accumulator.max != Double.MIN_VALUE ? accumulator.max : 0.0)
                .count(accumulator.count)
                .build();
    }
    
    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator merged = new Accumulator();
        merged.symbol = a.symbol != null ? a.symbol : b.symbol;
        merged.sum = a.sum + b.sum;
        merged.min = Math.min(a.min, b.min);
        merged.max = Math.max(a.max, b.max);
        merged.count = a.count + b.count;
        return merged;
    }
}
