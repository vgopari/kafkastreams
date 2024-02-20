package io.kafka.kafkastreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
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
public class FavoriteColorProcessor {

    private static final Logger logger = LoggerFactory.getLogger(FavoriteColorProcessor.class);

    private static final Serde<String> STRING_SERDES = Serdes.String();

    private final KafkaStreamsConfiguration kStreamsConfig;

    @Autowired
    public FavoriteColorProcessor(@Qualifier("defaultKafkaStreamsConfig") KafkaStreamsConfiguration kStreamsConfig) {
        this.kStreamsConfig = kStreamsConfig;
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder.stream("fav-color-input", Consumed.with(STRING_SERDES, STRING_SERDES));

        KStream<String, String> usersAndColors = messageStream.
                filter((key, value)  -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(values -> values.split(",")[1].toLowerCase())
                .filter((user, color) -> Arrays.asList("red", "green", "blue").contains(color));

        usersAndColors.to("user-keys-and-colors");

        KTable<String, String> usersAndColorValueTable = streamsBuilder.table("user-keys-and-colors");

        KTable<String, Long> favColors = usersAndColorValueTable
                .groupBy((user, color) -> new KeyValue<>(color, color)).count(Named.as("colorCount"));

        favColors.toStream().foreach((key, value) -> logger.info(String.format("Key: %s, Value: %s", key, value))); // Print stream

        favColors.toStream().to("fav-color-output");

        try (KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kStreamsConfig.asProperties())) {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            // Add shutdown hook to handle shutdown gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        }
    }

}
