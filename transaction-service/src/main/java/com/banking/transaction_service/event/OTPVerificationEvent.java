package com.banking.transaction_service.event;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@NoArgsConstructor 
@AllArgsConstructor
public class OTPVerificationEvent {
    private String transactionId;
    private String accountNumber;
    private String reason;
    private String otp;
    private BigDecimal amount;
}
