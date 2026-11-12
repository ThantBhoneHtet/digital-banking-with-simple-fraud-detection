package com.banking.payment_service.dto;

import java.math.BigDecimal;

import com.banking.payment_service.entity.PaymentStatus;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@AllArgsConstructor 
@NoArgsConstructor 
public class PaymentResponse {
    private String paymentId;
    private String razorpayPaymentId;
    private BigDecimal amount;
    private String currency;
    private PaymentStatus status;
    private String razorpayKeyId;
}
