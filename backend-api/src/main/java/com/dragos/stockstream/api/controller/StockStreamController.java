package com.dragos.stockstream.api.controller;

import com.dragos.stockstream.api.service.StockStreamService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;

/**
 * REST controller for stock streaming endpoints.
 * Provides Server-Sent Events (SSE) endpoint for real-time stock data.
 */
@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
@Slf4j
public class StockStreamController {

    private final StockStreamService stockStreamService;

    /**
     * SSE endpoint for streaming real-time stock price aggregates.
     * Clients can connect to this endpoint to receive continuous updates.
     * 
     * @return SseEmitter for streaming stock updates
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamStockUpdates() {
        String clientId = UUID.randomUUID().toString();
        log.info("New SSE stream request from client: {}", clientId);
        
        return stockStreamService.createEmitter(clientId);
    }

    /**
     * Health check endpoint to verify the API is running.
     * 
     * @return Simple health status message
     */
    @GetMapping("/health")
    public String health() {
        int activeConnections = stockStreamService.getActiveConnectionCount();
        return String.format("Backend API is running. Active SSE connections: %d", activeConnections);
    }
}
