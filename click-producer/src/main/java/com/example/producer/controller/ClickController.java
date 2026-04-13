package com.example.producer.controller;

import com.example.avro.ClickEvent;
import com.example.producer.service.ClickEventProducer;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/clicks")
@CrossOrigin(origins = "*")
public class ClickController {
    
    @Autowired
    private ClickEventProducer producer;
    
    @PostMapping
    public ResponseEntity<String> trackClick(@RequestBody ClickRequest request,
                                            HttpServletRequest httpRequest) {
        ClickEvent event = ClickEvent.newBuilder()
                .setUserId(request.getUserId())
                .setPage(request.getPage())
                .setElementId(request.getElementId())
                .setTimestamp(Instant.now().toEpochMilli())
                .setSessionId(request.getSessionId() != null ? 
                        request.getSessionId() : UUID.randomUUID().toString())
                .setUserAgent(httpRequest.getHeader("User-Agent"))
                .setIpAddress(httpRequest.getRemoteAddr())
                .build();
        
        producer.sendClickEvent(event);
        
        return ResponseEntity.ok("Click tracked successfully");
    }
    
    // Classe interna para request
    static class ClickRequest {
        private String userId;
        private String page;
        private String elementId;
        private String sessionId;
        
        // Getters e setters
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getPage() { return page; }
        public void setPage(String page) { this.page = page; }
        public String getElementId() { return elementId; }
        public void setElementId(String elementId) { this.elementId = elementId; }
        public String getSessionId() { return sessionId; }
        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    }
}