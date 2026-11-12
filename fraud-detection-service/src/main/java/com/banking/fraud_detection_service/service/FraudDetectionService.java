package com.banking.fraud_detection_service.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.fraud_detection_service.client.AccountServiceClient;
import com.banking.fraud_detection_service.dto.FraudCheckResult;
import com.banking.fraud_detection_service.event.FraudDetectedEvent;
import com.banking.fraud_detection_service.event.FraudPassedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class FraudDetectionService {

    @Value("${fraud.max-transactions-per-minute}")
    private int MAX_TRANSACTIONS_PER_MINUTE;

    @Value("${fraud.suspicious-amount-multiplier}")
    private double SUSPICIOUS_AMOUNT_MULTIPLIER;

    @Value("${fraud.max-balance-percentage}")
    private double MAX_BALANCE_PERCENTAGE;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final AccountServiceClient accountServiceClient;

    private final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";
    

    public void checkTransactionForFraud(Map<String,Object> payload) {
        String transactionId = (String) payload.get("transactionId");
        String senderAccountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal transactionAmount = new BigDecimal(payload.get("amount").toString());

        BigDecimal senderBalance = accountServiceClient.getBalance(senderAccountNumber);

        log.info("Checking transaction {} for fraud. Sender account: {}, Transaction amount: {}, Sender balance: {}", 
                 transactionId, senderAccountNumber, transactionAmount, senderBalance);

        FraudCheckResult result = performFraudCheck(senderAccountNumber, transactionAmount, senderBalance);

        if (result.isFraud()) {
            log.warn("Suspicious transaction detected. Account: {} - Reason: {} - requesting OTP verification", senderAccountNumber, result.getReason());
            FraudDetectedEvent fraudDetectedEvent = new FraudDetectedEvent(transactionId, senderAccountNumber, transactionAmount, result.getReason());
            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, fraudDetectedEvent);
        }
        else{  
            log.info("Transaction {} passed fraud check.", transactionId);
            FraudPassedEvent fraudPassedEvent = new FraudPassedEvent(transactionId, false, null);
            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, fraudPassedEvent);
        }
    }


    /**
     * 1) Velocity check: If there are multiple transactions in a short period of time
     * 2) Abnormal transaction amount: If the transaction amount is significantly higher than the average transaction amount for that account
     * 3) Huge transaction amount: If the transaction amount is above a certain threshold (e.g., $10,000)
     * @param senderAccountNumber
     * @param transactionAmount
     * @param senderBalance
     * @return FraudCheckResult
     */
    private FraudCheckResult performFraudCheck(
        String senderAccountNumber, 
        BigDecimal transactionAmount,
        BigDecimal senderBalance) 
    {
        if (velocityExceeded(senderAccountNumber)) {
            return new FraudCheckResult(true, "Velocity check failed: Too many transactions in 60 seconds.");
        }

        if (isAmountAbnormal(senderAccountNumber, transactionAmount)) {
            return new FraudCheckResult(true, "Abnormal transaction amount: Significantly higher than average.");
        }

        if (isAmountHuge(senderBalance, transactionAmount)) {
            return new FraudCheckResult(true, "Huge transaction amount: 90% of account balance exceeded.");
        }

        return new FraudCheckResult(false, null);
    }

    private boolean velocityExceeded(String senderAccountNumber) {
        String key = "fraud_detection:velocity:" + senderAccountNumber;
        Long transactionCount = redisTemplate.opsForValue().increment(key);

        // Set expiration for the key if it's the first transaction
        if (transactionCount != null && transactionCount == 1) {
            redisTemplate.expire(key, Duration.ofSeconds(60));
        }
        
        log.info("Velocity check - account: {} transactionCount: {}/{}", senderAccountNumber, transactionCount, MAX_TRANSACTIONS_PER_MINUTE);

        return transactionCount != null && transactionCount > MAX_TRANSACTIONS_PER_MINUTE;
    }

    private boolean isAmountAbnormal(String senderAccountNumber, BigDecimal transactionAmount) {
        String key = "fraud_detection:avg_amount:" + senderAccountNumber;
        String avgStr = redisTemplate.opsForValue().get(key);

        if (avgStr == null) {
            redisTemplate.opsForValue().set(key, transactionAmount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal newAvg = avgAmount.add(transactionAmount).divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP);

        redisTemplate.opsForValue().set(key, newAvg.toString());

        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(SUSPICIOUS_AMOUNT_MULTIPLIER));

        boolean isSuspicious = transactionAmount.compareTo(threshold) > 0;

        log.info("Amount check - amount: {} threshold: {} suspicious: {}", transactionAmount, threshold, isSuspicious);
        return isSuspicious;
    }

    private boolean isAmountHuge(BigDecimal senderBalance, BigDecimal transactionAmount) {
        BigDecimal maxAllowed = senderBalance.multiply(BigDecimal.valueOf(MAX_BALANCE_PERCENTAGE));

        boolean isHugeAmount = transactionAmount.compareTo(maxAllowed) > 0;

        log.info("Huge balance check - amount: {} maxAllowed: {} suspicious: {}", transactionAmount, maxAllowed, isHugeAmount);

        return isHugeAmount;
    }
    
}
