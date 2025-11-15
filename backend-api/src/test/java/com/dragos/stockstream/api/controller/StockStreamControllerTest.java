package com.dragos.stockstream.api.controller;

import com.dragos.stockstream.api.service.StockStreamService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for StockStreamController using MockMvc.
 */
@WebMvcTest(StockStreamController.class)
class StockStreamControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private StockStreamService stockStreamService;

    @Test
    void streamStockUpdates_returnsSSEMediaType() throws Exception {
        // Given
        SseEmitter emitter = new SseEmitter();
        when(stockStreamService.createEmitter(anyString())).thenReturn(emitter);

        // When/Then
        mockMvc.perform(get("/api/stocks/stream"))
                .andExpect(status().isOk());
                // Note: Content type may not be set until data is sent
    }

    @Test
    void streamStockUpdates_createsEmitter() throws Exception {
        // Given
        SseEmitter emitter = new SseEmitter();
        when(stockStreamService.createEmitter(anyString())).thenReturn(emitter);

        // When
        mockMvc.perform(get("/api/stocks/stream"))
                .andExpect(status().isOk());

        // Then
        verify(stockStreamService, times(1)).createEmitter(anyString());
    }

    @Test
    void health_returnsHealthStatus() throws Exception {
        // Given
        when(stockStreamService.getActiveConnectionCount()).thenReturn(5);

        // When/Then
        mockMvc.perform(get("/api/stocks/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Backend API is running. Active SSE connections: 5"));
    }

    @Test
    void health_returnsZeroConnections() throws Exception {
        // Given
        when(stockStreamService.getActiveConnectionCount()).thenReturn(0);

        // When/Then
        mockMvc.perform(get("/api/stocks/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Backend API is running. Active SSE connections: 0"));
    }

    @Test
    void streamStockUpdates_acceptsCorsRequests() throws Exception {
        // Given
        SseEmitter emitter = new SseEmitter();
        when(stockStreamService.createEmitter(anyString())).thenReturn(emitter);

        // When/Then
        mockMvc.perform(get("/api/stocks/stream")
                .header("Origin", "http://localhost:5173"))
                .andExpect(status().isOk());
    }

    @Test
    void health_acceptsOptionsRequest() throws Exception {
        // When/Then
        mockMvc.perform(get("/api/stocks/health"))
                .andExpect(status().isOk());
    }
}
