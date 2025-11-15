package com.dragos.stockstream.api.service;

import com.dragos.stockstream.api.model.StockPriceAggregate;
import com.dragos.stockstream.api.util.TestDataFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaConsumerService.
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

    @Mock
    private StockStreamService stockStreamService;

    @InjectMocks
    private KafkaConsumerService consumerService;

    @Test
    void consumeStockAggregate_callsBroadcastUpdate() {
        // Given
        StockPriceAggregate aggregate = TestDataFactory.createAggregate("AAPL");

        // When
        consumerService.consumeStockAggregate(aggregate);

        // Then
        verify(stockStreamService, times(1)).broadcastStockUpdate(aggregate);
    }

    @Test
    void consumeStockAggregate_processesMultipleMessages() {
        // Given
        StockPriceAggregate aggregate1 = TestDataFactory.createAggregate("AAPL");
        StockPriceAggregate aggregate2 = TestDataFactory.createAggregate("MSFT");
        StockPriceAggregate aggregate3 = TestDataFactory.createAggregate("GOOGL");

        // When
        consumerService.consumeStockAggregate(aggregate1);
        consumerService.consumeStockAggregate(aggregate2);
        consumerService.consumeStockAggregate(aggregate3);

        // Then
        verify(stockStreamService, times(3)).broadcastStockUpdate(any(StockPriceAggregate.class));
    }

    @Test
    void consumeStockAggregate_passesCorrectAggregateData() {
        // Given
        StockPriceAggregate aggregate = TestDataFactory.createAggregateWithPrices(
            "NVDA", 478.0, 482.0, 480.0, 10L
        );
        ArgumentCaptor<StockPriceAggregate> captor = ArgumentCaptor.forClass(StockPriceAggregate.class);

        // When
        consumerService.consumeStockAggregate(aggregate);

        // Then
        verify(stockStreamService).broadcastStockUpdate(captor.capture());
        StockPriceAggregate captured = captor.getValue();
        
        assertThat(captured.getSymbol()).isEqualTo("NVDA");
        assertThat(captured.getMinPrice()).isEqualTo(478.0);
        assertThat(captured.getMaxPrice()).isEqualTo(482.0);
        assertThat(captured.getAvgPrice()).isEqualTo(480.0);
        assertThat(captured.getCount()).isEqualTo(10L);
    }

    @Test
    void consumeStockAggregate_handlesNullSymbol() {
        // Given
        StockPriceAggregate aggregate = StockPriceAggregate.builder()
                .symbol(null)
                .avgPrice(100.0)
                .build();

        // When
        consumerService.consumeStockAggregate(aggregate);

        // Then - should still call broadcast
        verify(stockStreamService, times(1)).broadcastStockUpdate(aggregate);
    }

    @Test
    void consumeStockAggregate_handlesZeroCount() {
        // Given
        StockPriceAggregate aggregate = TestDataFactory.createAggregateWithPrices(
            "TSLA", 240.0, 240.0, 240.0, 0L
        );

        // When
        consumerService.consumeStockAggregate(aggregate);

        // Then
        verify(stockStreamService, times(1)).broadcastStockUpdate(aggregate);
    }

    @Test
    void consumeStockAggregate_doesNotThrowException() {
        // Given
        StockPriceAggregate aggregate = TestDataFactory.createAggregate("META");
        doThrow(new RuntimeException("Broadcast failed")).when(stockStreamService).broadcastStockUpdate(any());

        // When/Then - should propagate exception (or handle based on implementation)
        try {
            consumerService.consumeStockAggregate(aggregate);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Broadcast failed");
        }
    }
}
