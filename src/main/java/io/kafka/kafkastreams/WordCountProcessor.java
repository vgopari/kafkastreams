package io.kafka.kafkastreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class WordCountProcessor {

    private static final Logger logger = LoggerFactory.getLogger(WordCountProcessor.class);

    private static final Serde<String> STRING_SERDES = Serdes.String();

    private final KafkaStreamsConfiguration kStreamsConfig;

    @Autowired
    public WordCountProcessor(@Qualifier("defaultKafkaStreamsConfig") KafkaStreamsConfiguration kStreamsConfig) {
        this.kStreamsConfig = kStreamsConfig;
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder.stream("streams-app-wordcount-input", Consumed.with(STRING_SERDES, STRING_SERDES));

        KTable<String, Long> wordCount = messageStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(STRING_SERDES, STRING_SERDES))
                .count(Named.as("Counts"));
        wordCount.toStream().foreach((key, value) -> logger.info(String.format("Key: %s, Value: %s", key, value))); // Print stream
        wordCount.toStream().to("streams-app-wordcount-output");
        try (KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kStreamsConfig.asProperties())) {
            // Add shutdown hook to handle shutdown gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        }
    }
}
