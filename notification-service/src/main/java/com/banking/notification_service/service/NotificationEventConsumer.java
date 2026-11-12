package com.banking.notification_service.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class NotificationEventConsumer {

    private final NotificationService notificationService;

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGeneratedEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String transactionId = (String) payload.get("transactionId");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            notificationService.sendAlert(
                accountNumber,
                "TRANSACTION VERIFICATION REQUIRED",
                String.format(
                    "Suspicious activity detected on your account. " +
                    "Reason: %s " +
                    "A transaction of %s is pending verification. " +
                    "Your OTP is %s. Valid for 5 minutes. " +
                    "If this wasn't you - ignore this message.",
                    reason, amount, otp
                )
            );
        } catch (Exception e) {
            log.error("Error sending OTP notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompletedEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            // debit alert
            notificationService.sendAlert(
                senderAccount,
                "DEBIT ALERT", 
                String.format(
                    "%s debited from account %s",
                    amount, senderAccount
                )
            );

            // credit alert
            notificationService.sendAlert(
                receiverAccount,
                "CREDIT ALERT",
                String.format(
                    "%s credited to account %s",
                    amount, receiverAccount
                )
            );
        } catch (Exception e) {
            log.error("Error sending transaction notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public  void consumeFraudDetectedEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");
            
            notificationService.sendAlert(
                accountNumber,
                "SUSPICIOUS ACTIVITY DETECTED ALERT", 
                String.format(
                    "Your account %s has been blocked. " +
                    "Reason: %s. " +
                    "Please contact your bank immediately.",
                    accountNumber, reason
                )
            );
        } catch (Exception e) {
            log.error("Error sending fraud alert: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefundedEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String senderAccount = (String) payload.get("accountNumber");       
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");    
            
            notificationService.sendAlert(
                senderAccount,
                "REFUND PROCESSED", 
                String.format(
                    "Your transaction of %s was cancelled. " +
                    "Reason: %s. " +
                    "%s has been refunded to account %s.",
                    amount, reason, amount, senderAccount
                )
            );

        } catch (Exception e) {
            log.error("Error sending refund notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompletedEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String razorpayPaymentId = (String) payload.get("razorpayPaymentId");

            notificationService.sendAlert(
                accountNumber,
                "PAYMENT SUCCESSFUL", 
                String.format(
                    "Your payment of %s is completed. " +
                    "Razorpay ID: %s",
                    amount, razorpayPaymentId, amount
                )
            );
            
        } catch (Exception e) {
            log.error("Error sending payment success notification: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailedEvent(
        @Payload Map<String, Object> payload
    ) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            notificationService.sendAlert(
                accountNumber,
                "PAYMENT FAILURE", 
                String.format(
                    "Your payment of %s could not be processed. " +
                    "Reason: %s. " +
                    "Please try again or contact support.",
                    amount, reason
                )
            );
            
        } catch (Exception e) {
            log.error("Error sending payment failure notification: {}", e.getMessage());
        }
    }
}
