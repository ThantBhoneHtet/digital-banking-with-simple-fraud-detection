package com.banking.fraud_detection_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@AllArgsConstructor 
@NoArgsConstructor 
public class FraudCheckResult {
    private boolean isFraud;
    private String reason;
}