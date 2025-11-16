package com.dragos.stockstream.processor;

import com.dragos.stockstream.processor.functions.StockPriceAggregator;
import com.dragos.stockstream.processor.model.StockPrice;
import com.dragos.stockstream.processor.model.StockPriceAggregate;
import com.dragos.stockstream.processor.serialization.StockPriceAggregateSerializationSchema;
import com.dragos.stockstream.processor.serialization.StockPriceDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * Main Flink Stream Processor application.
 * Consumes stock prices from Kafka, aggregates them in time windows,
 * and publishes aggregated results back to Kafka.
 */
public class StockStreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StockStreamProcessor.class);

    private static final String KAFKA_BOOTSTRAP_SERVERS = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String INPUT_TOPIC = getEnvOrDefault("INPUT_TOPIC", "stock-prices");
    private static final String OUTPUT_TOPIC = getEnvOrDefault("OUTPUT_TOPIC", "stock-prices-aggregated");
    private static final String CONSUMER_GROUP = getEnvOrDefault("CONSUMER_GROUP", "flink-stock-processor");
    private static final int WINDOW_SIZE_SECONDS = Integer.parseInt(getEnvOrDefault("WINDOW_SIZE_SECONDS", "10"));

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Stock Stream Processor");
        LOG.info("Kafka Bootstrap Servers: {}", KAFKA_BOOTSTRAP_SERVERS);
        LOG.info("Input Topic: {}", INPUT_TOPIC);
        LOG.info("Output Topic: {}", OUTPUT_TOPIC);
        LOG.info("Window Size: {} seconds", WINDOW_SIZE_SECONDS);

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka source
        KafkaSource<StockPrice> kafkaSource = KafkaSource.<StockPrice>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new StockPriceDeserializationSchema())
                .build();

        // Create watermark strategy for event time processing
        WatermarkStrategy<StockPrice> watermarkStrategy = WatermarkStrategy
                .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() != null
                        ? element.getTimestamp().toEpochMilli()
                        : System.currentTimeMillis());

        // Read from Kafka
        DataStream<StockPrice> stockPriceStream = env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka Source")
                .name("Stock Price Source");

        // Aggregate by symbol in tumbling windows
        DataStream<StockPriceAggregate> aggregatedStream = stockPriceStream
                .keyBy(StockPrice::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .aggregate(
                        new StockPriceAggregator(),
                        new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<StockPriceAggregate, StockPriceAggregate, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<StockPriceAggregate> elements,
                                    org.apache.flink.util.Collector<StockPriceAggregate> out) {
                                for (StockPriceAggregate aggregate : elements) {
                                    aggregate.setWindowStart(Instant.ofEpochMilli(context.window().getStart()));
                                    aggregate.setWindowEnd(Instant.ofEpochMilli(context.window().getEnd()));
                                    out.collect(aggregate);
                                }
                            }
                        })
                .name("Stock Price Aggregation");

        // Configure Kafka sink
        KafkaSink<StockPriceAggregate> kafkaSink = KafkaSink.<StockPriceAggregate>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        (KafkaRecordSerializationSchema<StockPriceAggregate>) new StockPriceAggregateSerializationSchema(
                                OUTPUT_TOPIC))
                .build();

        // Write to Kafka
        aggregatedStream.sinkTo(kafkaSink).name("Kafka Sink");

        // Execute the job
        env.execute("Stock Stream Processor");
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
