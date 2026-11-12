package com.banking.payment_service.service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.payment_service.dto.PaymentRequest;
import com.banking.payment_service.dto.PaymentResponse;
import com.banking.payment_service.entity.Payment;
import com.banking.payment_service.entity.PaymentStatus;
import com.banking.payment_service.event.PaymentFailureEvent;
import com.banking.payment_service.event.PaymentSuccessEvent;
import com.banking.payment_service.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;

@Service 
@RequiredArgsConstructor 
@Slf4j 
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    private final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private final String PAYMENT_FAILED_TOPIC = "payment.failed";
    

    /**
     * Flow:
     * 1) Create order in razorpay
     * 2) Save record in db
     * 3) Return order details to frontend
     * 4) Frontend shows razorpay checkout page
     * 5) User pays
     * 6) razorpay calls webhook
     * @param request
     * @return
     */
    public PaymentResponse createPaymentOrder(PaymentRequest request) {

        try {
            log.info("Creating payment order for account: {} amount: {}",
                    request.getAccountNumber(), request.getAmount());

            RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

            // Convert amount to smallest unit
            int convertedAmount = request.getAmount()
                    .multiply(BigDecimal.valueOf(100))
                    .intValue();

            JSONObject orderRequest = new JSONObject();
            orderRequest.put("amount", convertedAmount);
            orderRequest.put("currency", "USD");
            orderRequest.put("receipt",
                    "rcpt_" +
                    System.currentTimeMillis() +
                    UUID.randomUUID().toString().replace("-", "").substring(0, 10));

            Order razorpayOrder = razorpayClient.orders.create(orderRequest);

            log.info("Razorpay order created: {}",  String.valueOf(razorpayOrder.get("id")));

            Payment payment = Payment.builder()
                    .razorpayOrderId(razorpayOrder.get("id").toString())
                    .accountNumber(request.getAccountNumber())
                    .amount(request.getAmount())
                    .currency("USD")
                    .status(PaymentStatus.CREATED)
                    .description(request.getDescription())
                    .build();

            Payment savedPayment = paymentRepository.save(payment);

            return new PaymentResponse(
                    savedPayment.getId(),
                    savedPayment.getRazorpayOrderId(),
                    savedPayment.getAmount(),
                    savedPayment.getCurrency(),
                    savedPayment.getStatus(),
                    keyId
            );

        } catch (RazorpayException e) {
            log.error("Razorpay error while creating payment order", e);
            throw new RuntimeException("Unable to create Razorpay payment order", e);
        }
    }

    public void handleWebhook(Map<String,Object> payload) {
        log.info("Received razorpay webhook: {}", 
         String.valueOf(payload.get("event")));

        String event = (String) payload.get("event");

        if ("payment.captured".equals(event)) {
            handlePaymentSuccess(payload);
        }
        else if ("payment.failed".equals(event)) {
            handlePaymentFailure(payload);
        }
    }

    private void handlePaymentSuccess(Map<String,Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId).
                    orElseThrow(() -> new RuntimeException("Payment not found for order: " + orderId));
            
            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            PaymentSuccessEvent paymentEvent = new PaymentSuccessEvent(
                payment.getId(),
                payment.getAccountNumber(),
                payment.getAmount(),
                paymentId
            );

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), paymentEvent);

            log.info("Payment completed: {}", payment.getId());
        } catch (Exception e) {
            log.error("Error handling payment success: {}", e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String,Object> payload) {
        try {
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId).
                    orElseThrow(() -> new RuntimeException("Payment not found for order: " + orderId));
            
            payment.setStatus(PaymentStatus.FAILDED);
            payment.setFailureReson("Payment failed for Razorpay");
            paymentRepository.save(payment);

            PaymentFailureEvent paymentFailureEvent = new PaymentFailureEvent(
                payment.getId(),
                payment.getAccountNumber(),
                payment.getAmount(),
                payment.getFailureReson()
            );

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), paymentFailureEvent);

            log.warn("Payment failed: {}", payment.getId());
        } catch (Exception e) {
            log.error("Error handling payment failure: {}" + e.getMessage());
        }
    }

    private Map<String, Object> extractPaymentData(Map<String,Object> payload) {
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }
    
}
