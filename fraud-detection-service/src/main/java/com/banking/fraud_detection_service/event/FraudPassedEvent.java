package com.banking.fraud_detection_service.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@AllArgsConstructor 
@NoArgsConstructor
public class FraudPassedEvent {
    private String transactionId;
    private boolean isFraud;
    private String reason;
}
