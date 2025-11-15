package com.dragos.stockstream.processor.serialization;

import com.dragos.stockstream.processor.model.StockPrice;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for StockPriceDeserializationSchema.
 * Note: Full deserialization tests require proper Flink runtime context.
 * These tests focus on schema configuration and error handling.
 */
class StockPriceDeserializationSchemaTest {

    private StockPriceDeserializationSchema schema;

    @BeforeEach
    void setUp() throws Exception {
        schema = new StockPriceDeserializationSchema();
        schema.open(mock(DeserializationSchema.InitializationContext.class));
    }

    @Test
    void getProducedType_returnsCorrectTypeInformation() {
        // When
        TypeInformation<StockPrice> typeInfo = schema.getProducedType();

        // Then
        assertThat(typeInfo).isNotNull();
        assertThat(typeInfo.getTypeClass()).isEqualTo(StockPrice.class);
    }

    @Test
    void deserialize_handlesNullValue() throws Exception {
        // Given
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic", 0, 0L, null, null);
        TestCollector collector = new TestCollector();

        // When
        schema.deserialize(record, collector);

        // Then - no exception should be thrown, and nothing should be collected
        assertThat(collector.collected).isEmpty();
    }

    @Test
    void deserialize_handlesMalformedJSON() throws Exception {
        // Given
        byte[] malformedJson = "{\"symbol\":\"AAPL\", invalid}".getBytes();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic", 0, 0L, null, malformedJson);
        TestCollector collector = new TestCollector();

        // When
        schema.deserialize(record, collector);

        // Then - should handle gracefully without throwing exception
        assertThat(collector.collected).isEmpty();
    }

    @Test
    void deserialize_handlesEmptyInput() throws Exception {
        // Given
        byte[] emptyBytes = new byte[0];
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic", 0, 0L, null, emptyBytes);
        TestCollector collector = new TestCollector();

        // When
        schema.deserialize(record, collector);

        // Then
        assertThat(collector.collected).isEmpty();
    }

    @Test
    void schema_canBeInitializedMultipleTimes() throws Exception {
        // Given
        StockPriceDeserializationSchema newSchema = new StockPriceDeserializationSchema();

        // When
        newSchema.open(mock(DeserializationSchema.InitializationContext.class));
        newSchema.open(mock(DeserializationSchema.InitializationContext.class));

        // Then - no exception should be thrown
        assertThat(newSchema.getProducedType()).isNotNull();
    }

    /**
     * Test collector implementation for capturing deserialized values.
     */
    private static class TestCollector implements Collector<StockPrice> {
        public final List<StockPrice> collected = new ArrayList<>();

        @Override
        public void collect(StockPrice record) {
            collected.add(record);
        }

        @Override
        public void close() {
        }
    }
}
