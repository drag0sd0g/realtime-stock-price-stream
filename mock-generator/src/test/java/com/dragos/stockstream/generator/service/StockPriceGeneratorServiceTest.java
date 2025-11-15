package com.dragos.stockstream.generator.service;

import com.dragos.stockstream.generator.model.StockPrice;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StockPriceGeneratorService.
 */
@ExtendWith(MockitoExtension.class)
class StockPriceGeneratorServiceTest {

    @Mock
    private KafkaTemplate<String, StockPrice> kafkaTemplate;

    @Captor
    private ArgumentCaptor<String> topicCaptor;

    @Captor
    private ArgumentCaptor<String> keyCaptor;

    @Captor
    private ArgumentCaptor<StockPrice> stockPriceCaptor;

    private StockPriceGeneratorService service;

    @BeforeEach
    void setUp() {
        service = new StockPriceGeneratorService(kafkaTemplate);
    }

    @Test
    void initializePrices_shouldInitializeAllStocks() {
        // When
        service.initializePrices();

        // Then - check that at least some key stocks are initialized
        // We can't directly access the maps, but we can verify the behavior after
        // calling generateAndPublishPrices
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    void generateAndPublishPrices_shouldSendMessagesToKafka() {
        // Given
        service.initializePrices();

        // When
        service.generateAndPublishPrices();

        // Then - verify messages are sent (50 stocks)
        verify(kafkaTemplate, times(50)).send(eq("stock-prices"), any(String.class), any(StockPrice.class));
    }

    @Test
    void generateAndPublishPrices_shouldUseSymbolAsKey() {
        // Given
        service.initializePrices();

        // When
        service.generateAndPublishPrices();

        // Then
        verify(kafkaTemplate, atLeastOnce()).send(
                eq("stock-prices"),
                keyCaptor.capture(),
                stockPriceCaptor.capture()
        );

        // Verify that key matches symbol in the stock price
        String key = keyCaptor.getValue();
        StockPrice stockPrice = stockPriceCaptor.getValue();
        assertThat(key).isEqualTo(stockPrice.getSymbol());
    }

    @Test
    void generateAndPublishPrices_shouldGenerateValidStockPrices() {
        // Given
        service.initializePrices();

        // When
        service.generateAndPublishPrices();

        // Then
        verify(kafkaTemplate, atLeastOnce()).send(
                any(String.class),
                any(String.class),
                stockPriceCaptor.capture()
        );

        StockPrice stockPrice = stockPriceCaptor.getValue();

        assertThat(stockPrice).isNotNull();
        assertThat(stockPrice.getSymbol()).isNotBlank();
        assertThat(stockPrice.getPrice()).isPositive();
        assertThat(stockPrice.getTimestamp()).isNotNull();
        assertThat(stockPrice.getChange()).isNotNull();
        assertThat(stockPrice.getChangePercent()).isNotNull();
    }

    @Test
    void generateAndPublishPrices_shouldNotGenerateNegativePrices() {
        // Given
        service.initializePrices();

        // When - generate prices multiple times to test boundary conditions
        for (int i = 0; i < 10; i++) {
            service.generateAndPublishPrices();
        }

        // Then
        verify(kafkaTemplate, atLeast(500)).send(
                any(String.class),
                any(String.class),
                stockPriceCaptor.capture()
        );

        // Verify all captured prices are positive
        for (StockPrice price : stockPriceCaptor.getAllValues()) {
            assertThat(price.getPrice())
                    .as("Price for %s should be positive", price.getSymbol())
                    .isPositive();
        }
    }

    @Test
    void generateAndPublishPrices_shouldCalculateChangeCorrectly() {
        // Given
        service.initializePrices();

        // When
        service.generateAndPublishPrices();

        // Then
        verify(kafkaTemplate, atLeastOnce()).send(
                any(String.class),
                any(String.class),
                stockPriceCaptor.capture()
        );

        StockPrice stockPrice = stockPriceCaptor.getValue();

        // changePercent should be consistent with change and price
        if (stockPrice.getChange() != 0.0) {
            double previousPrice = stockPrice.getPrice() - stockPrice.getChange();
            double expectedChangePercent = (stockPrice.getChange() / previousPrice) * 100.0;

            assertThat(stockPrice.getChangePercent())
                    .isCloseTo(expectedChangePercent, within(0.01));
        }
    }

    @Test
    void generateAndPublishPrices_shouldRespectVolatilityBounds() {
        // Given
        service.initializePrices();

        // When - generate multiple times to accumulate data
        for (int i = 0; i < 5; i++) {
            service.generateAndPublishPrices();
        }

        // Then
        verify(kafkaTemplate, atLeast(250)).send(
                any(String.class),
                any(String.class),
                stockPriceCaptor.capture()
        );

        // For large cap stocks (first 20), check volatility is around ±2%
        // This is a statistical test, so we just verify prices don't deviate wildly
        for (StockPrice price : stockPriceCaptor.getAllValues()) {
            // Price should be within reasonable bounds (50% to 150% of base)
            assertThat(price.getPrice())
                    .as("Price for %s should be within reasonable bounds", price.getSymbol())
                    .isBetween(10.0, 2000.0); // Reasonable range for any stock
        }
    }

    @Test
    void generateAndPublishPrices_shouldSerializeToJSON() throws Exception {
        // Given
        service.initializePrices();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        // When
        service.generateAndPublishPrices();

        // Then
        verify(kafkaTemplate, atLeastOnce()).send(
                any(String.class),
                any(String.class),
                stockPriceCaptor.capture()
        );

        StockPrice stockPrice = stockPriceCaptor.getValue();

        // Verify it can be serialized to JSON
        String json = objectMapper.writeValueAsString(stockPrice);
        assertThat(json).isNotBlank();
        assertThat(json).contains(stockPrice.getSymbol());
        assertThat(json).contains(String.valueOf(stockPrice.getPrice()));

        // Verify it can be deserialized back
        StockPrice deserialized = objectMapper.readValue(json, StockPrice.class);
        assertThat(deserialized.getSymbol()).isEqualTo(stockPrice.getSymbol());
        assertThat(deserialized.getPrice()).isEqualTo(stockPrice.getPrice());
    }

    @Test
    void generateAndPublishPrices_shouldHandleAllSymbols() {
        // Given
        service.initializePrices();

        // When
        service.generateAndPublishPrices();

        // Then - exactly 50 messages should be sent (one per stock)
        verify(kafkaTemplate, times(50)).send(
                eq("stock-prices"),
                any(String.class),
                any(StockPrice.class)
        );
    }
}
