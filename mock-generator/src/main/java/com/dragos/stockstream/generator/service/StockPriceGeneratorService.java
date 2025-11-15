package com.dragos.stockstream.generator.service;

import com.dragos.stockstream.generator.model.StockPrice;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service that generates mock stock prices for top 50 tech companies.
 * Publishes generated prices to Kafka topic "stock-prices" every 500ms.
 */
@Service
@Slf4j
public class StockPriceGeneratorService {

    private static final String TOPIC = "stock-prices";
    
    // Top 50 tech company symbols
    private static final String[] SYMBOLS = {
        // Large caps (2% volatility)
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "ADBE",
        "CRM", "CSCO", "ACN", "INTC", "AMD", "IBM", "INTU", "NOW", "QCOM", "TXN",
        // Mid caps (3% volatility)
        "AMAT", "PANW", "MU", "ADI", "LRCX", "KLAC", "SNPS", "CDNS", "MCHP", "NXPI",
        "COIN", "SQ", "SHOP", "SNOW", "DDOG", "ZS", "CRWD", "NET", "OKTA", "TEAM",
        // Smaller caps (5% volatility)
        "SPLK", "MDB", "WDAY", "ZM", "DOCU", "TWLO", "UBER", "LYFT", "DASH", "ABNB"
    };
    
    // Base prices for each symbol
    private final Map<String, Double> basePrices = new HashMap<>();
    private final Map<String, Double> currentPrices = new HashMap<>();
    private final Random random = new Random();
    private final AtomicLong messageCount = new AtomicLong(0);
    
    private final KafkaTemplate<String, StockPrice> kafkaTemplate;

    public StockPriceGeneratorService(KafkaTemplate<String, StockPrice> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Initialize base prices for all stock symbols.
     */
    @PostConstruct
    public void initializePrices() {
        // Large caps - realistic base prices
        basePrices.put("AAPL", 180.0);
        basePrices.put("MSFT", 370.0);
        basePrices.put("GOOGL", 140.0);
        basePrices.put("AMZN", 145.0);
        basePrices.put("NVDA", 480.0);
        basePrices.put("META", 330.0);
        basePrices.put("TSLA", 240.0);
        basePrices.put("AVGO", 890.0);
        basePrices.put("ORCL", 110.0);
        basePrices.put("ADBE", 550.0);
        basePrices.put("CRM", 220.0);
        basePrices.put("CSCO", 50.0);
        basePrices.put("ACN", 320.0);
        basePrices.put("INTC", 45.0);
        basePrices.put("AMD", 120.0);
        basePrices.put("IBM", 160.0);
        basePrices.put("INTU", 580.0);
        basePrices.put("NOW", 650.0);
        basePrices.put("QCOM", 140.0);
        basePrices.put("TXN", 170.0);
        
        // Mid caps
        basePrices.put("AMAT", 160.0);
        basePrices.put("PANW", 280.0);
        basePrices.put("MU", 85.0);
        basePrices.put("ADI", 190.0);
        basePrices.put("LRCX", 780.0);
        basePrices.put("KLAC", 560.0);
        basePrices.put("SNPS", 490.0);
        basePrices.put("CDNS", 250.0);
        basePrices.put("MCHP", 85.0);
        basePrices.put("NXPI", 210.0);
        basePrices.put("COIN", 120.0);
        basePrices.put("SQ", 70.0);
        basePrices.put("SHOP", 65.0);
        basePrices.put("SNOW", 170.0);
        basePrices.put("DDOG", 110.0);
        basePrices.put("ZS", 180.0);
        basePrices.put("CRWD", 210.0);
        basePrices.put("NET", 75.0);
        basePrices.put("OKTA", 80.0);
        basePrices.put("TEAM", 200.0);
        
        // Smaller caps
        basePrices.put("SPLK", 150.0);
        basePrices.put("MDB", 380.0);
        basePrices.put("WDAY", 240.0);
        basePrices.put("ZM", 70.0);
        basePrices.put("DOCU", 60.0);
        basePrices.put("TWLO", 65.0);
        basePrices.put("UBER", 60.0);
        basePrices.put("LYFT", 15.0);
        basePrices.put("DASH", 90.0);
        basePrices.put("ABNB", 140.0);
        
        // Initialize current prices with base prices
        currentPrices.putAll(basePrices);
        
        log.info("Initialized {} stock symbols with base prices", basePrices.size());
    }

    /**
     * Generate and publish stock prices every 500ms.
     * Uses different volatility levels based on market cap.
     */
    @Scheduled(fixedRate = 500)
    public void generateAndPublishPrices() {
        for (int i = 0; i < SYMBOLS.length; i++) {
            String symbol = SYMBOLS[i];
            double volatility = getVolatilityForSymbol(i);
            
            double previousPrice = currentPrices.get(symbol);
            double basePrice = basePrices.get(symbol);
            
            // Random walk with mean reversion
            double randomChange = (random.nextGaussian() * volatility * previousPrice) / 100.0;
            double meanReversion = (basePrice - previousPrice) * 0.01; // 1% mean reversion
            double newPrice = previousPrice + randomChange + meanReversion;
            
            // Ensure price doesn't go negative or too far from base
            newPrice = Math.max(basePrice * 0.5, Math.min(basePrice * 1.5, newPrice));
            
            double change = newPrice - previousPrice;
            double changePercent = (change / previousPrice) * 100.0;
            
            currentPrices.put(symbol, newPrice);
            
            StockPrice stockPrice = StockPrice.builder()
                    .symbol(symbol)
                    .price(Math.round(newPrice * 100.0) / 100.0) // Round to 2 decimal places
                    .timestamp(Instant.now())
                    .change(Math.round(change * 100.0) / 100.0)
                    .changePercent(Math.round(changePercent * 100.0) / 100.0)
                    .build();
            
            kafkaTemplate.send(TOPIC, symbol, stockPrice);
            
            // Log every 20th message to avoid spam
            long count = messageCount.incrementAndGet();
            if (count % 20 == 0) {
                log.info("Published price for {}: ${} ({}{}%)", 
                        symbol, 
                        stockPrice.getPrice(), 
                        change >= 0 ? "+" : "", 
                        stockPrice.getChangePercent());
            }
        }
    }

    /**
     * Determine volatility based on symbol index (market cap tier).
     * 
     * @param index Symbol index in the SYMBOLS array
     * @return Volatility percentage
     */
    private double getVolatilityForSymbol(int index) {
        if (index < 20) {
            return 2.0; // Large caps: 2% volatility
        } else if (index < 40) {
            return 3.0; // Mid caps: 3% volatility
        } else {
            return 5.0; // Smaller caps: 5% volatility
        }
    }
}
