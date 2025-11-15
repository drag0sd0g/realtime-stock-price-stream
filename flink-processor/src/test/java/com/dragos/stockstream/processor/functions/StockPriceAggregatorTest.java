package com.dragos.stockstream.processor.functions;

import com.dragos.stockstream.processor.model.StockPrice;
import com.dragos.stockstream.processor.model.StockPriceAggregate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for StockPriceAggregator.
 */
class StockPriceAggregatorTest {

    private StockPriceAggregator aggregator;

    @BeforeEach
    void setUp() {
        aggregator = new StockPriceAggregator();
    }

    @Test
    void createAccumulator_initializesCorrectly() {
        // When
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();

        // Then
        assertThat(accumulator).isNotNull();
        assertThat(accumulator.symbol).isNull();
        assertThat(accumulator.sum).isZero();
        assertThat(accumulator.count).isZero();
        assertThat(accumulator.min).isEqualTo(Double.MAX_VALUE);
        assertThat(accumulator.max).isEqualTo(Double.MIN_VALUE);
    }

    @Test
    void add_updatesMinMaxSumCountCorrectly() {
        // Given
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();
        StockPrice price1 = createStockPrice("AAPL", 150.0);
        StockPrice price2 = createStockPrice("AAPL", 155.0);
        StockPrice price3 = createStockPrice("AAPL", 148.0);

        // When
        accumulator = aggregator.add(price1, accumulator);
        accumulator = aggregator.add(price2, accumulator);
        accumulator = aggregator.add(price3, accumulator);

        // Then
        assertThat(accumulator.symbol).isEqualTo("AAPL");
        assertThat(accumulator.count).isEqualTo(3);
        assertThat(accumulator.sum).isEqualTo(453.0);
        assertThat(accumulator.min).isEqualTo(148.0);
        assertThat(accumulator.max).isEqualTo(155.0);
    }

    @Test
    void add_setsSymbolOnFirstAdd() {
        // Given
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();
        StockPrice price = createStockPrice("MSFT", 300.0);

        // When
        accumulator = aggregator.add(price, accumulator);

        // Then
        assertThat(accumulator.symbol).isEqualTo("MSFT");
    }

    @Test
    void getResult_calculatesAverageCorrectly() {
        // Given
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();
        accumulator = aggregator.add(createStockPrice("GOOGL", 100.0), accumulator);
        accumulator = aggregator.add(createStockPrice("GOOGL", 110.0), accumulator);
        accumulator = aggregator.add(createStockPrice("GOOGL", 105.0), accumulator);

        // When
        StockPriceAggregate result = aggregator.getResult(accumulator);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getSymbol()).isEqualTo("GOOGL");
        assertThat(result.getAvgPrice()).isEqualTo(105.0);
        assertThat(result.getMinPrice()).isEqualTo(100.0);
        assertThat(result.getMaxPrice()).isEqualTo(110.0);
        assertThat(result.getCount()).isEqualTo(3);
    }

    @Test
    void getResult_handlesSingleValue() {
        // Given
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();
        accumulator = aggregator.add(createStockPrice("NVDA", 480.0), accumulator);

        // When
        StockPriceAggregate result = aggregator.getResult(accumulator);

        // Then
        assertThat(result.getSymbol()).isEqualTo("NVDA");
        assertThat(result.getAvgPrice()).isEqualTo(480.0);
        assertThat(result.getMinPrice()).isEqualTo(480.0);
        assertThat(result.getMaxPrice()).isEqualTo(480.0);
        assertThat(result.getCount()).isEqualTo(1);
    }

    @Test
    void getResult_handlesEmptyAccumulator() {
        // Given
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();

        // When
        StockPriceAggregate result = aggregator.getResult(accumulator);

        // Then
        assertThat(result.getAvgPrice()).isZero();
        assertThat(result.getMinPrice()).isZero();
        assertThat(result.getMaxPrice()).isZero();
        assertThat(result.getCount()).isZero();
    }

    @Test
    void getResult_handlesSameValues() {
        // Given
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();
        accumulator = aggregator.add(createStockPrice("TSLA", 240.0), accumulator);
        accumulator = aggregator.add(createStockPrice("TSLA", 240.0), accumulator);
        accumulator = aggregator.add(createStockPrice("TSLA", 240.0), accumulator);

        // When
        StockPriceAggregate result = aggregator.getResult(accumulator);

        // Then
        assertThat(result.getAvgPrice()).isEqualTo(240.0);
        assertThat(result.getMinPrice()).isEqualTo(240.0);
        assertThat(result.getMaxPrice()).isEqualTo(240.0);
        assertThat(result.getCount()).isEqualTo(3);
    }

    @Test
    void merge_combinesTwoAccumulatorsCorrectly() {
        // Given
        StockPriceAggregator.Accumulator acc1 = aggregator.createAccumulator();
        acc1 = aggregator.add(createStockPrice("META", 330.0), acc1);
        acc1 = aggregator.add(createStockPrice("META", 335.0), acc1);

        StockPriceAggregator.Accumulator acc2 = aggregator.createAccumulator();
        acc2 = aggregator.add(createStockPrice("META", 325.0), acc2);
        acc2 = aggregator.add(createStockPrice("META", 340.0), acc2);

        // When
        StockPriceAggregator.Accumulator merged = aggregator.merge(acc1, acc2);

        // Then
        assertThat(merged.symbol).isEqualTo("META");
        assertThat(merged.count).isEqualTo(4);
        assertThat(merged.sum).isEqualTo(1330.0); // 330 + 335 + 325 + 340
        assertThat(merged.min).isEqualTo(325.0);
        assertThat(merged.max).isEqualTo(340.0);
    }

    @Test
    void merge_handlesOneEmptyAccumulator() {
        // Given
        StockPriceAggregator.Accumulator acc1 = aggregator.createAccumulator();
        acc1 = aggregator.add(createStockPrice("AMZN", 145.0), acc1);

        StockPriceAggregator.Accumulator acc2 = aggregator.createAccumulator();

        // When
        StockPriceAggregator.Accumulator merged = aggregator.merge(acc1, acc2);

        // Then
        assertThat(merged.symbol).isEqualTo("AMZN");
        assertThat(merged.count).isEqualTo(1);
        assertThat(merged.sum).isEqualTo(145.0);
    }

    @Test
    void merge_handlesExtremeValues() {
        // Given
        StockPriceAggregator.Accumulator acc1 = aggregator.createAccumulator();
        acc1 = aggregator.add(createStockPrice("INTC", 1.0), acc1);

        StockPriceAggregator.Accumulator acc2 = aggregator.createAccumulator();
        acc2 = aggregator.add(createStockPrice("INTC", 1000.0), acc2);

        // When
        StockPriceAggregator.Accumulator merged = aggregator.merge(acc1, acc2);

        // Then
        assertThat(merged.min).isEqualTo(1.0);
        assertThat(merged.max).isEqualTo(1000.0);
        assertThat(merged.sum).isEqualTo(1001.0);
    }

    @Test
    void endToEnd_multipleAdditionsAndMerge() {
        // Given
        StockPriceAggregator.Accumulator accumulator = aggregator.createAccumulator();
        
        // Add several prices
        accumulator = aggregator.add(createStockPrice("AMD", 120.0), accumulator);
        accumulator = aggregator.add(createStockPrice("AMD", 122.0), accumulator);
        accumulator = aggregator.add(createStockPrice("AMD", 118.0), accumulator);

        // When
        StockPriceAggregate result = aggregator.getResult(accumulator);

        // Then
        assertThat(result.getSymbol()).isEqualTo("AMD");
        assertThat(result.getAvgPrice()).isEqualTo(120.0);
        assertThat(result.getMinPrice()).isEqualTo(118.0);
        assertThat(result.getMaxPrice()).isEqualTo(122.0);
        assertThat(result.getCount()).isEqualTo(3);
    }

    private StockPrice createStockPrice(String symbol, Double price) {
        return StockPrice.builder()
                .symbol(symbol)
                .price(price)
                .timestamp(Instant.now())
                .change(0.5)
                .changePercent(0.27)
                .build();
    }
}
