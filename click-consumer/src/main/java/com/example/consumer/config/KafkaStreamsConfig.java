package com.example.consumer.config;

import com.example.avro.ClickEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaStreamsConfig {
    
    @Bean
    public KafkaStreams kafkaStreams() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("schema.registry.url", "http://localhost:8081");
        
        StreamsBuilder builder = new StreamsBuilder();
        
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", "http://localhost:8081");
        
        SpecificAvroSerde<ClickEvent> clickEventSerde = new SpecificAvroSerde<>();
        clickEventSerde.configure(serdeConfig, false);
        
        KStream<String, ClickEvent> clickStream = builder.stream(
            "user-clicks",
            Consumed.with(Serdes.String(), clickEventSerde)
        );
        
        // Agregar por usuário (userId já é a chave, então groupByKey funciona)
        KTable<String, Long> userClickCounts = clickStream
            .groupByKey()
            .count(Materialized.as("user-click-counts"));
        
        // Agregar por página - CONVERTER CharSequence PARA String
        KTable<String, Long> pageClickCounts = clickStream
            .groupBy((key, event) -> event.getPage().toString(), 
                     Grouped.with(Serdes.String(), clickEventSerde))
            .count(Materialized.as("page-click-counts"));
        
        // Agregar por elemento - CONVERTER CharSequence PARA String
        KTable<String, Long> elementClickCounts = clickStream
            .filter((key, event) -> event.getElementId() != null)
            .groupBy((key, event) -> event.getElementId().toString(), 
                     Grouped.with(Serdes.String(), clickEventSerde))
            .count(Materialized.as("element-click-counts"));
        
        userClickCounts.toStream().to("user-click-stats", 
            Produced.with(Serdes.String(), Serdes.Long()));
        pageClickCounts.toStream().to("page-click-stats", 
            Produced.with(Serdes.String(), Serdes.Long()));
        elementClickCounts.toStream().to("element-click-stats", 
            Produced.with(Serdes.String(), Serdes.Long()));
        
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        return streams;
    }
}