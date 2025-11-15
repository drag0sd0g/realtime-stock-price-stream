package com.dragos.stockstream.api;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for BackendApiApplication.
 * Verifies that the application context loads successfully.
 */
@SpringBootTest
@ActiveProfiles("test")
class BackendApiApplicationTest {

    @Test
    void contextLoads_successfully() {
        // When the test runs, Spring Boot context should load
        // This test passes if context loads without errors
    }
}
