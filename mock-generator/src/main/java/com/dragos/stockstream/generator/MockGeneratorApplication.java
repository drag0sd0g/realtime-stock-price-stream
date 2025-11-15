package com.dragos.stockstream.generator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for the Mock Stock Price Generator.
 * Generates and publishes mock stock price data to Kafka.
 */
@SpringBootApplication
@EnableScheduling
public class MockGeneratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MockGeneratorApplication.class, args);
    }
}
