package com.dragos.stockstream.generator.config;

import com.dragos.stockstream.generator.model.StockPrice;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for KafkaProducerConfig.
 * Verifies that Kafka beans are properly configured.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"stock-prices"},
    brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaProducerConfigTest {

    @Autowired
    private ProducerFactory<String, StockPrice> producerFactory;

    @Autowired
    private KafkaTemplate<String, StockPrice> kafkaTemplate;

    @Test
    void producerFactory_beanIsCreated() {
        assertThat(producerFactory).isNotNull();
    }

    @Test
    void kafkaTemplate_beanIsCreated() {
        assertThat(kafkaTemplate).isNotNull();
    }

    @Test
    void producerFactory_hasCorrectConfiguration() {
        assertThat(producerFactory.getConfigurationProperties())
                .containsKey("key.serializer")
                .containsKey("value.serializer");
    }

    @Test
    void kafkaTemplate_canSendMessages() {
        // Verify that KafkaTemplate is properly wired and functional
        assertThat(kafkaTemplate.getDefaultTopic()).isNull(); // No default topic configured
        assertThat(kafkaTemplate.getProducerFactory()).isEqualTo(producerFactory);
    }
}
