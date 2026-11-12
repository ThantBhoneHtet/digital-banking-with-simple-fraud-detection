package com.banking.fraud_detection_service.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor 
public class FraudDetectionEventConsumer {

    private final FraudDetectionService fraudDetectionService;
    
    @KafkaListener(topics = "transaction.initiated", groupId = "fraud-detection-group")
    public void consumeTransactionInitiatedEvent(
        @Payload Map<String, Object> payload
    ) {
        log.info("Received transaction initiated event for fraud check: {}", payload.get("transactionId"));
        try {
            fraudDetectionService.checkTransactionForFraud(payload);
        } catch (Exception e) {
            log.error("Error processing transaction initiated event for fraud check: {}", payload.get("transactionId"), e);
        }
    }
}
