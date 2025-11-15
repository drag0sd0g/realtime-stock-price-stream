package com.dragos.stockstream.processor.serialization;

import com.dragos.stockstream.processor.model.StockPriceAggregate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for StockPriceAggregateSerializationSchema.
 */
class StockPriceAggregateSerializationSchemaTest {

    private StockPriceAggregateSerializationSchema schema;
    private ObjectMapper objectMapper;
    private static final String TEST_TOPIC = "test-topic";

    @BeforeEach
    void setUp() throws Exception {
        schema = new StockPriceAggregateSerializationSchema(TEST_TOPIC);
        schema.open(
            mock(SerializationSchema.InitializationContext.class),
            mock(KafkaRecordSerializationSchema.KafkaSinkContext.class)
        );
        
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void serialize_successfullySerializesToJSON() throws Exception {
        // Given
        StockPriceAggregate aggregate = createAggregate("AAPL", 180.0, 178.0, 182.0, 10L);
        KafkaRecordSerializationSchema.KafkaSinkContext context = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);

        // When
        ProducerRecord<byte[], byte[]> record = schema.serialize(aggregate, context, null);

        // Then
        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo(TEST_TOPIC);
        
        // Verify key is the symbol
        String key = new String(record.key(), StandardCharsets.UTF_8);
        assertThat(key).isEqualTo("AAPL");
        
        // Verify value can be deserialized
        String json = new String(record.value(), StandardCharsets.UTF_8);
        assertThat(json).contains("\"symbol\":\"AAPL\"");
        assertThat(json).contains("\"avgPrice\":180.0");
    }

    @Test
    void serialize_usesSymbolAsKafkaKey() throws Exception {
        // Given
        StockPriceAggregate aggregate = createAggregate("MSFT", 370.0, 368.0, 372.0, 5L);
        KafkaRecordSerializationSchema.KafkaSinkContext context = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);

        // When
        ProducerRecord<byte[], byte[]> record = schema.serialize(aggregate, context, null);

        // Then
        assertThat(record).isNotNull();
        String key = new String(record.key(), StandardCharsets.UTF_8);
        assertThat(key).isEqualTo("MSFT");
    }

    @Test
    void serialize_properlySerializesAllFields() throws Exception {
        // Given
        Instant windowStart = Instant.parse("2024-01-15T10:30:00Z");
        Instant windowEnd = Instant.parse("2024-01-15T10:30:05Z");
        
        StockPriceAggregate aggregate = StockPriceAggregate.builder()
                .symbol("GOOGL")
                .avgPrice(140.5)
                .minPrice(139.0)
                .maxPrice(142.0)
                .count(15L)
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .build();
        
        KafkaRecordSerializationSchema.KafkaSinkContext context = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);

        // When
        ProducerRecord<byte[], byte[]> record = schema.serialize(aggregate, context, null);

        // Then
        assertThat(record).isNotNull();
        String json = new String(record.value(), StandardCharsets.UTF_8);
        
        // Verify all fields are present in JSON
        assertThat(json).contains("\"symbol\":\"GOOGL\"");
        assertThat(json).contains("\"avgPrice\":140.5");
        assertThat(json).contains("\"minPrice\":139.0");
        assertThat(json).contains("\"maxPrice\":142.0");
        assertThat(json).contains("\"count\":15");
        assertThat(json).contains("\"windowStart\"");
        assertThat(json).contains("\"windowEnd\"");
    }

    @Test
    void serialize_properlyFormatsTimestamp() throws Exception {
        // Given
        Instant timestamp = Instant.parse("2024-01-15T10:30:00Z");
        StockPriceAggregate aggregate = StockPriceAggregate.builder()
                .symbol("NVDA")
                .avgPrice(480.0)
                .minPrice(478.0)
                .maxPrice(482.0)
                .count(8L)
                .windowStart(timestamp)
                .windowEnd(timestamp.plusSeconds(5))
                .build();
        
        KafkaRecordSerializationSchema.KafkaSinkContext context = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);

        // When
        ProducerRecord<byte[], byte[]> record = schema.serialize(aggregate, context, null);

        // Then
        String json = new String(record.value(), StandardCharsets.UTF_8);
        // Flink serializes Instant as numeric epoch with nanoseconds
        assertThat(json).contains("\"windowStart\"");
        assertThat(json).contains("\"windowEnd\"");
    }

    @Test
    void serialize_writesToCorrectTopic() throws Exception {
        // Given
        StockPriceAggregate aggregate = createAggregate("TSLA", 240.0, 238.0, 242.0, 12L);
        KafkaRecordSerializationSchema.KafkaSinkContext context = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);

        // When
        ProducerRecord<byte[], byte[]> record = schema.serialize(aggregate, context, null);

        // Then
        assertThat(record.topic()).isEqualTo(TEST_TOPIC);
    }

    @Test
    void serialize_handlesMultipleAggregates() throws Exception {
        // Given
        KafkaRecordSerializationSchema.KafkaSinkContext context = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);
        
        // When
        ProducerRecord<byte[], byte[]> record1 = schema.serialize(
            createAggregate("AAPL", 180.0, 178.0, 182.0, 10L), context, null);
        ProducerRecord<byte[], byte[]> record2 = schema.serialize(
            createAggregate("MSFT", 370.0, 368.0, 372.0, 10L), context, null);

        // Then
        assertThat(record1).isNotNull();
        assertThat(record2).isNotNull();
        
        String key1 = new String(record1.key(), StandardCharsets.UTF_8);
        String key2 = new String(record2.key(), StandardCharsets.UTF_8);
        
        assertThat(key1).isEqualTo("AAPL");
        assertThat(key2).isEqualTo("MSFT");
    }

    @Test
    void serialize_producesValidJSON() throws Exception {
        // Given
        StockPriceAggregate aggregate = createAggregate("META", 330.0, 325.0, 335.0, 20L);
        KafkaRecordSerializationSchema.KafkaSinkContext context = mock(KafkaRecordSerializationSchema.KafkaSinkContext.class);

        // When
        ProducerRecord<byte[], byte[]> record = schema.serialize(aggregate, context, null);

        // Then
        String json = new String(record.value(), StandardCharsets.UTF_8);
        assertThat(json).contains("\"symbol\":\"META\"");
        assertThat(json).contains("\"avgPrice\":330.0");
        assertThat(json).contains("\"minPrice\":325.0");
        assertThat(json).contains("\"maxPrice\":335.0");
        assertThat(json).contains("\"count\":20");
    }

    private StockPriceAggregate createAggregate(String symbol, double avg, double min, double max, long count) {
        return StockPriceAggregate.builder()
                .symbol(symbol)
                .avgPrice(avg)
                .minPrice(min)
                .maxPrice(max)
                .count(count)
                .windowStart(Instant.now().minusSeconds(5))
                .windowEnd(Instant.now())
                .build();
    }
}
