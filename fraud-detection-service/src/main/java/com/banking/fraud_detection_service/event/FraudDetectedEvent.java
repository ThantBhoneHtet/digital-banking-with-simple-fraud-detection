package com.banking.fraud_detection_service.event;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@AllArgsConstructor 
@NoArgsConstructor
public class FraudDetectedEvent {
    private String transactionId;
    private String accountNumber;
    private BigDecimal amount;
    private String reason;
}
