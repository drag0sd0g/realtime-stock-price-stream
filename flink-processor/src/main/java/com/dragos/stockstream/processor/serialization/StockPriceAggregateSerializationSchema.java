package com.dragos.stockstream.processor.serialization;

import com.dragos.stockstream.processor.model.StockPriceAggregate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Serializes StockPriceAggregate objects into Kafka records.
 * Uses Jackson for JSON serialization and sets the symbol as the Kafka key.
 */
public class StockPriceAggregateSerializationSchema implements KafkaRecordSerializationSchema<StockPriceAggregate> {
    
    private static final Logger LOG = LoggerFactory.getLogger(StockPriceAggregateSerializationSchema.class);
    private static final long serialVersionUID = 1L;
    
    private final String topic;
    private transient ObjectMapper objectMapper;
    
    public StockPriceAggregateSerializationSchema(String topic) {
        this.topic = topic;
    }
    
    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(StockPriceAggregate aggregate, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] keyBytes = aggregate.getSymbol().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = objectMapper.writeValueAsBytes(aggregate);
            
            return new ProducerRecord<>(topic, keyBytes, valueBytes);
        } catch (Exception e) {
            LOG.error("Error serializing StockPriceAggregate for symbol {}: {}", 
                     aggregate.getSymbol(), e.getMessage());
            return null;
        }
    }
}
