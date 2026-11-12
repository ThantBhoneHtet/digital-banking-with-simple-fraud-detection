package com.banking.payment_service.controller;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.banking.payment_service.dto.PaymentRequest;
import com.banking.payment_service.dto.PaymentResponse;
import com.banking.payment_service.service.PaymentService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController 
@RequestMapping("/api/v1/payments")
@Slf4j 
@RequiredArgsConstructor 
public class PaymentController {
    private final PaymentService paymentService;

    @PostMapping("/create-order")
    public ResponseEntity<PaymentResponse> createPaymentOrder(
        @Valid @RequestBody PaymentRequest request
    ) {
        return ResponseEntity.status(HttpStatus.CREATED).body(paymentService.createPaymentOrder(request));
    }

    
    @PostMapping("/webhook")
    public ResponseEntity<String> handleWebhook(
        @RequestBody Map<String, Object> payload
    ) {
        paymentService.handleWebhook(payload);

        return ResponseEntity.ok("Webhook processed");
    }
}
