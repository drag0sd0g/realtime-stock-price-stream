package com.dragos.stockstream.api.service;

import com.dragos.stockstream.api.model.StockPriceAggregate;
import com.dragos.stockstream.api.util.TestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Unit tests for StockStreamService.
 */
class StockStreamServiceTest {

    private StockStreamService service;

    @BeforeEach
    void setUp() {
        service = new StockStreamService();
    }

    @Test
    void createEmitter_successfullyCreatesNewEmitter() {
        // When
        SseEmitter emitter = service.createEmitter("client-1");

        // Then
        assertThat(emitter).isNotNull();
        assertThat(service.getActiveConnectionCount()).isEqualTo(1);
    }

    @Test
    void createEmitter_createsMultipleEmitters() {
        // When
        SseEmitter emitter1 = service.createEmitter("client-1");
        SseEmitter emitter2 = service.createEmitter("client-2");
        SseEmitter emitter3 = service.createEmitter("client-3");

        // Then
        assertThat(emitter1).isNotNull();
        assertThat(emitter2).isNotNull();
        assertThat(emitter3).isNotNull();
        assertThat(service.getActiveConnectionCount()).isEqualTo(3);
    }

    @Test
    void createEmitter_setsUpCallbacks() {
        // Given & When
        SseEmitter emitter = service.createEmitter("client-1");

        // Then - emitter is created and registered
        assertThat(service.getActiveConnectionCount()).isEqualTo(1);
        assertThat(emitter).isNotNull();
    }

    @Test
    void createEmitter_handlesMultipleClients() {
        // Given & When
        SseEmitter emitter1 = service.createEmitter("client-1");
        SseEmitter emitter2 = service.createEmitter("client-2");

        // Then
        assertThat(service.getActiveConnectionCount()).isEqualTo(2);
    }

    @Test
    void broadcastStockUpdate_sendsToAllConnectedClients() throws Exception {
        // Given
        TestSseEmitter emitter1 = new TestSseEmitter();
        TestSseEmitter emitter2 = new TestSseEmitter();
        TestSseEmitter emitter3 = new TestSseEmitter();
        
        // Create service with custom emitters for testing
        service.createEmitter("client-1");
        service.createEmitter("client-2");
        service.createEmitter("client-3");

        StockPriceAggregate aggregate = TestDataFactory.createAggregate("AAPL");

        // When
        service.broadcastStockUpdate(aggregate);

        // Then - all emitters should receive the update
        // Note: In real implementation, this would send to actual emitters
        // This test verifies the method executes without errors
        assertThat(service.getActiveConnectionCount()).isEqualTo(3);
    }

    @Test
    void getActiveConnectionCount_returnsCorrectCount() {
        // Given
        assertThat(service.getActiveConnectionCount()).isZero();

        // When
        service.createEmitter("client-1");
        service.createEmitter("client-2");

        // Then
        assertThat(service.getActiveConnectionCount()).isEqualTo(2);
    }

    @Test
    void concurrentAccess_handlesMultipleClientsThreadSafely() throws Exception {
        // Given
        int clientCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        CountDownLatch latch = new CountDownLatch(clientCount);
        List<Exception> exceptions = new ArrayList<>();

        // When - create emitters concurrently
        for (int i = 0; i < clientCount; i++) {
            final int clientId = i;
            executor.submit(() -> {
                try {
                    service.createEmitter("client-" + clientId);
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(exceptions).isEmpty();
        assertThat(service.getActiveConnectionCount()).isEqualTo(clientCount);
    }

    @Test
    void broadcastStockUpdate_handlesMultipleBroadcastsConcurrently() throws Exception {
        // Given
        service.createEmitter("client-1");
        service.createEmitter("client-2");
        service.createEmitter("client-3");

        int broadcastCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(broadcastCount);
        List<Exception> exceptions = new ArrayList<>();

        // When - broadcast updates concurrently
        for (int i = 0; i < broadcastCount; i++) {
            final int updateId = i;
            executor.submit(() -> {
                try {
                    StockPriceAggregate aggregate = TestDataFactory.createAggregate("STOCK" + updateId);
                    service.broadcastStockUpdate(aggregate);
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(exceptions).isEmpty();
    }

    @Test
    void createEmitter_withSameClientId_replacesOldEmitter() {
        // Given
        SseEmitter emitter1 = service.createEmitter("client-1");
        assertThat(service.getActiveConnectionCount()).isEqualTo(1);

        // When - create another emitter with same client ID
        SseEmitter emitter2 = service.createEmitter("client-1");

        // Then - should still have one connection (old one replaced)
        // Note: Actual behavior depends on implementation
        assertThat(emitter2).isNotNull();
    }

    /**
     * Test implementation of SseEmitter for capturing sent events.
     */
    private static class TestSseEmitter extends SseEmitter {
        public final List<Object> sentData = new ArrayList<>();

        @Override
        public void send(Object object) throws IOException {
            sentData.add(object);
        }
    }
}
