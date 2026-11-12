package com.banking.transaction_service.service;

import java.time.Duration;
import java.util.Map;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.event.OTPVerificationEvent;
import com.banking.transaction_service.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j 
@RequiredArgsConstructor 
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final TransactionService transactionService;
    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final long OTP_EXPIRATION_MINUTES = 5; // OTP expiration time in minutes
    private final String OTP_GENERATED_TOPIC = "transaction.otp.generated";

    /**
     * Consume verification.required event from kafka
     * Generate OTP and send to user for verification
     * @param payload
     */
    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequiredEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required - transaction: {} reason: {}", transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));
            
            if (transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.warn("Transaction {} is not in PROCESSING state, current state: {}", transactionId, transaction.getStatus());
                return;
            }

            // Generate 6-digit OTP
            String otp = String.format("%06d", (int) (Math.random() * 900000 + 100000));

            // Store OTP in Redis - expires in 5 minutes
            String otpKey = "verification:otp:" + transactionId;

            redisTemplate.opsForValue().set(
                otpKey,
                otp,
                Duration.ofMinutes(OTP_EXPIRATION_MINUTES)
            );

            // Update transaction status to PENDING_VERIFICATION
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction: {} expires in {} min", 
                    transactionId, OTP_EXPIRATION_MINUTES);

            // Notify user
            OTPVerificationEvent otpEvent = new OTPVerificationEvent(
                transactionId,
                accountNumber,
                reason,
                otp,
                transaction.getAmount()
            );

            kafkaTemplate.send(OTP_GENERATED_TOPIC, transactionId, otpEvent);
        } catch (Exception e) {
            log.error("Error processing verification.required event: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(
        @Payload Map<String, Object> payload
    ) {
        try {
            String transactionId = (String) payload.get("transactionId");
            transactionService.processFraudCleanResult(transactionId);

            log.info(transactionId);
        } catch (Exception e) {
            log.error("Error processing fraud clean result: {}", e.getMessage());
        }
    }
}
