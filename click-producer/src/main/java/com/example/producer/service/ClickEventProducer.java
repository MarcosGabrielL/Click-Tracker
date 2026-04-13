package com.example.producer.service;

import com.example.avro.ClickEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class ClickEventProducer {
    
    private static final String TOPIC = "user-clicks";
    
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    public CompletableFuture<SendResult<String, Object>> sendClickEvent(ClickEvent event) {
        // CORREÇÃO: Converter CharSequence para String
        String userId = event.getUserId().toString();
        
        return kafkaTemplate.send(TOPIC, userId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        System.out.printf("Evento enviado: userId=%s, page=%s, partition=%d, offset=%d%n",
                                userId,
                                event.getPage(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        System.err.println("Erro ao enviar evento: " + ex.getMessage());
                    }
                });
    }
}