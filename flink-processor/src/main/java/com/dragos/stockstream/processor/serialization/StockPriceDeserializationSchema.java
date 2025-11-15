package com.dragos.stockstream.processor.serialization;

import com.dragos.stockstream.processor.model.StockPrice;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes Kafka records into StockPrice objects.
 * Handles JSON deserialization with proper error handling and timestamp extraction.
 */
public class StockPriceDeserializationSchema implements KafkaRecordDeserializationSchema<StockPrice> {
    
    private static final Logger LOG = LoggerFactory.getLogger(StockPriceDeserializationSchema.class);
    private static final long serialVersionUID = 1L;
    
    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<StockPrice> out) throws IOException {
        try {
            if (record.value() == null) {
                LOG.warn("Received null value for record at offset {}", record.offset());
                return;
            }
            
            StockPrice stockPrice = objectMapper.readValue(record.value(), StockPrice.class);
            
            if (stockPrice != null) {
                out.collect(stockPrice);
            } else {
                LOG.warn("Deserialized null StockPrice at offset {}", record.offset());
            }
        } catch (Exception e) {
            LOG.error("Error deserializing record at offset {}: {}", record.offset(), e.getMessage());
            // Skip the record - don't propagate the exception to avoid job failure
        }
    }
    
    @Override
    public TypeInformation<StockPrice> getProducedType() {
        return TypeInformation.of(StockPrice.class);
    }
}
