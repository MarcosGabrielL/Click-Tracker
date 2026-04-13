package com.example.consumer.service;

import com.example.avro.ClickEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ClickAggregatorService {
    
    private final Map<String, AtomicLong> userClickCount = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> pageClickCount = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> elementClickCount = new ConcurrentHashMap<>();
    private final AtomicLong totalClicks = new AtomicLong(0);
    
    @KafkaListener(topics = "user-clicks", groupId = "click-aggregator")
    public void consume(ClickEvent event) {
        processEvent(event);
    }
    
    private void processEvent(ClickEvent event) {
        totalClicks.incrementAndGet();
        
        // Converter CharSequence para String ANTES de usar no Map
        String userId = event.getUserId().toString();
        String page = event.getPage().toString();
        
        userClickCount.computeIfAbsent(userId, k -> new AtomicLong(0)).incrementAndGet();
        pageClickCount.computeIfAbsent(page, k -> new AtomicLong(0)).incrementAndGet();
        
        if (event.getElementId() != null) {
            String elementId = event.getElementId().toString();
            elementClickCount.computeIfAbsent(elementId, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        System.out.printf("Processed click - User: %s, Page: %s, Total: %d%n",
                userId, page, totalClicks.get());
    }
    
    public ClickStats getCurrentStats() {
        return new ClickStats(
            totalClicks.get(),
            new ConcurrentHashMap<>(userClickCount),
            new ConcurrentHashMap<>(pageClickCount),
            new ConcurrentHashMap<>(elementClickCount)
        );
    }
    
    public record ClickStats(
        long totalClicks,
        Map<String, AtomicLong> userClickCount,
        Map<String, AtomicLong> pageClickCount,
        Map<String, AtomicLong> elementClickCount
    ) {}
}