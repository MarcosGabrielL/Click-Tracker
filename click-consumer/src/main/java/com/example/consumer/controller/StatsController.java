package com.example.consumer.controller;

import com.example.consumer.service.ClickAggregatorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/stats")
@CrossOrigin(origins = "*")
public class StatsController {
    
    @Autowired
    private ClickAggregatorService aggregatorService;
    
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    
    @GetMapping
    public ClickAggregatorService.ClickStats getStats() {
        return aggregatorService.getCurrentStats();
    }
    
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamStats() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        
        executor.scheduleAtFixedRate(() -> {
            try {
                ClickAggregatorService.ClickStats stats = aggregatorService.getCurrentStats();
                
                // Converter AtomicLong para Long para serialização JSON
                Map<String, Long> userCounts = new HashMap<>();
                stats.userClickCount().forEach((k, v) -> userCounts.put(k, v.get()));
                
                Map<String, Long> pageCounts = new HashMap<>();
                stats.pageClickCount().forEach((k, v) -> pageCounts.put(k, v.get()));
                
                Map<String, Long> elementCounts = new HashMap<>();
                stats.elementClickCount().forEach((k, v) -> elementCounts.put(k, v.get()));
                
                Map<String, Object> response = Map.of(
                    "totalClicks", stats.totalClicks(),
                    "userClickCount", userCounts,
                    "pageClickCount", pageCounts,
                    "elementClickCount", elementCounts,
                    "timestamp", System.currentTimeMillis()
                );
                
                emitter.send(SseEmitter.event()
                    .data(response)
                    .id(String.valueOf(System.currentTimeMillis())));
                    
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
        }, 0, 2, TimeUnit.SECONDS);
        
        emitter.onCompletion(() -> executor.shutdown());
        emitter.onTimeout(() -> {
            emitter.complete();
            executor.shutdown();
        });
        
        return emitter;
    }
}