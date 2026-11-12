package com.banking.transaction_service.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
import com.banking.transaction_service.event.FraudDetectedEvent;
import com.banking.transaction_service.event.RefundEvent;
import com.banking.transaction_service.event.TransactionCompletedEvent;
import com.banking.transaction_service.event.TransactionInitiatedEvent;
import com.banking.transaction_service.repository.TransactionRepository;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    /**
     * SAGA step 1: Initiate the transaction and send a message to the transaction.initiated topic
     * 1) Deducts from sender via feign
     * 2) Saves transaction as PROCESSING
     * 3) Publishs event to kafka for fraud check
     * 4) Returns 
     * @param request
     * @return
     */
    public TransactionResponse transfer(TransferRequest request) {
        log.info("SAGA start - Transfer: {} -> {} amount: {}",
            request.getSenderAccountNumber(), 
            request.getReceiverAccountNumber(), 
            request.getAmount()
        );

        // SAGA step 1: Deduct from sender account
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

        // Save transaction as PROCESSING
        Transaction transaction = Transaction.builder()
            .senderAccountNumber(request.getSenderAccountNumber())
            .receiverAccountNumber(request.getReceiverAccountNumber())
            .amount(request.getAmount())
            .type(TransactionType.TRANSFER)
            .status(TransactionStatus.PROCESSING)
            .description(request.getDescription())
            .referenceNumber(UUID.randomUUID().toString())
            .build();

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved: {}", savedTransaction.getId());

        // SAGA step 2: Publishs event to kafka for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
            savedTransaction.getId(),
            savedTransaction.getSenderAccountNumber(),
            savedTransaction.getReceiverAccountNumber(),
            savedTransaction.getAmount(),
            savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA step 2 - TransactionInitiatedEvent sent to topic {}: {}", TRANSACTION_INITIATED_TOPIC, event);

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId) {
        Transaction transaction = transactionRepository
                                    .findById(transactionId)
                                    .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));
        return mapToResponse(transaction);
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        List<Transaction> transactions = transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber);
        return transactions.stream()
            .map(this::mapToResponse)
            .toList();
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        log.info("OTP verification for the transaction: {}", transactionId);

        Transaction transaction = transactionRepository.findById(transactionId)
            .orElseThrow(() -> new RuntimeException("Transaction not found" + transactionId));

        String otpKey = "verification:otp:" + transactionId;

        String generatedOTP = redisTemplate.opsForValue().get(otpKey);

        if (generatedOTP == null) {
            // OTP expired
            log.warn("OTP expired for transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        boolean isOTPCorrect = otp.equals(generatedOTP);

        if (!isOTPCorrect) {
            // block account and refund
            log.warn("Wrong OTP - blocking account and refunding amount for transaction id: {}", transactionId);

            redisTemplate.delete(otpKey);

            blockAccountAndCompensate(
                transaction, 
                "Wrong OTP entered - transaction cancelled, " +
                "account blocked for security."
            );
            
            return mapToResponse(transaction);
        }

        log.info("OTP verified - completing transaction: {}", transactionId);

        redisTemplate.delete(otpKey);

        completeTransaction(transaction);

        return mapToResponse(transaction);
    }

    public void processFraudCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
            .orElseThrow(() -> new RuntimeException("Transaction not found" + transactionId));

        if (transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.warn("Transaction {} is not in PROCESSING state, current state: {}", transactionId, transaction.getStatus());
            return;
        }

        completeTransaction(transaction);
    }
    
    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent event = new TransactionCompletedEvent(
            transaction.getId(),
            transaction.getSenderAccountNumber(),
            transaction.getReceiverAccountNumber(),
            transaction.getAmount(),
            transaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), event);

        log.info("SAGA complete - Transaction {} completed", transaction.getId());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {
        FraudDetectedEvent fraudEvent = new FraudDetectedEvent(
            transaction.getId(),
            transaction.getSenderAccountNumber(),
            transaction.getFailureReason()
        );

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("Fraud detection event pubished - account: {} will be blocked, kindly contact  to your bank",
            transaction.getSenderAccountNumber()
        );

        // refund sender
        compensateTransaction(transaction, reason);
    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA Compensation - refunding to: {} amount: {}",
            transaction.getSenderAccountNumber(),
            transaction.getAmount()
        );
        
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(
            reason +
            " - SAGA compensation executed, amount refunded at " +
            LocalDateTime.now()
        );

        transactionRepository.save(transaction);

        // publish refund event to sender
        RefundEvent refundEvent = new RefundEvent(
            transaction.getId(),
            transaction.getSenderAccountNumber(),
            transaction.getAmount(),
            transaction.getFailureReason()
        );
        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);

        log.info("SAGA compensation complete - amount of {} refunded to {}",
            transaction.getAmount(),
            transaction.getSenderAccountNumber()
        );
    }

    private TransactionResponse mapToResponse(Transaction savedTransaction) {
        return new TransactionResponse(
            savedTransaction.getId(),
            savedTransaction.getSenderAccountNumber(),
            savedTransaction.getReceiverAccountNumber(),
            savedTransaction.getAmount(),
            savedTransaction.getType(),
            savedTransaction.getStatus(),
            savedTransaction.getDescription(),
            savedTransaction.getFailureReason(),
            savedTransaction.getReferenceNumber(),
            savedTransaction.getCreatedAt(),
            savedTransaction.getCompletedAt()
        );
    }
}
