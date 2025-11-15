package com.dragos.stockstream.api.service;

import com.dragos.stockstream.api.model.StockPriceAggregate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service for managing Server-Sent Events (SSE) connections and streaming stock data to clients.
 */
@Service
@Slf4j
public class StockStreamService {

    /**
     * Thread-safe map to store SSE emitters for each client.
     * Key: unique client identifier, Value: SseEmitter instance
     */
    private final ConcurrentHashMap<String, SseEmitter> emitters = new ConcurrentHashMap<>();

    /**
     * List to keep track of all active emitters.
     */
    private final CopyOnWriteArrayList<SseEmitter> activeEmitters = new CopyOnWriteArrayList<>();

    /**
     * Creates and registers a new SSE emitter for a client.
     * 
     * @param clientId Unique identifier for the client
     * @return SseEmitter instance for the client
     */
    public SseEmitter createEmitter(String clientId) {
        SseEmitter emitter = new SseEmitter(0L); // 0 = no timeout

        emitter.onCompletion(() -> {
            log.info("SSE connection completed for client: {}", clientId);
            removeEmitter(clientId);
        });

        emitter.onTimeout(() -> {
            log.warn("SSE connection timed out for client: {}", clientId);
            removeEmitter(clientId);
        });

        emitter.onError((ex) -> {
            log.error("SSE connection error for client: {}", clientId, ex);
            removeEmitter(clientId);
        });

        emitters.put(clientId, emitter);
        activeEmitters.add(emitter);
        log.info("New SSE connection established for client: {}. Total active connections: {}", 
                clientId, activeEmitters.size());

        return emitter;
    }

    /**
     * Removes an emitter from the active connections.
     * 
     * @param clientId Unique identifier for the client
     */
    private void removeEmitter(String clientId) {
        SseEmitter emitter = emitters.remove(clientId);
        if (emitter != null) {
            activeEmitters.remove(emitter);
            log.info("SSE connection removed for client: {}. Total active connections: {}", 
                    clientId, activeEmitters.size());
        }
    }

    /**
     * Broadcasts a stock price aggregate to all connected clients.
     * 
     * @param aggregate Stock price aggregate to broadcast
     */
    public void broadcastStockUpdate(StockPriceAggregate aggregate) {
        log.debug("Broadcasting stock update for symbol: {} to {} clients", 
                aggregate.getSymbol(), activeEmitters.size());

        activeEmitters.forEach(emitter -> {
            try {
                emitter.send(SseEmitter.event()
                        .name("stock-update")
                        .data(aggregate));
            } catch (IOException e) {
                log.error("Error sending stock update to client", e);
                activeEmitters.remove(emitter);
            }
        });
    }

    /**
     * Gets the count of active SSE connections.
     * 
     * @return Number of active connections
     */
    public int getActiveConnectionCount() {
        return activeEmitters.size();
    }
}
