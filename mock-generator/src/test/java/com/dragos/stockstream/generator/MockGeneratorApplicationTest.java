package com.dragos.stockstream.generator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MockGeneratorApplication.
 * Verifies that the application context loads successfully and scheduling is enabled.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"stock-prices"}, 
    brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MockGeneratorApplicationTest {

    @Test
    void contextLoads_successfully() {
        // When the test runs, Spring Boot context should load
        // This test passes if context loads without errors
    }

    @Test
    void schedulingIsEnabled_whenApplicationStarts() {
        // Verify @EnableScheduling is present on the main application class
        EnableScheduling annotation = MockGeneratorApplication.class.getAnnotation(EnableScheduling.class);
        assertThat(annotation).isNotNull();
    }
}
