package com.banking.payment_service.event;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@NoArgsConstructor 
@AllArgsConstructor 
public class PaymentFailureEvent {
    private String paymentId;
    private String accountNumber;
    private BigDecimal amount;
    private String reason;
}
