package com.banking.notification_service.service;

import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j 
public class NotificationService {
    public void sendAlert(String accountNumber, String subject, String message) {
        log.info("____________________________________________________");
        log.info("Account: {}", accountNumber);
        log.info("Subject: {}", subject);
        log.info("Messge: {}", message);
        log.info("____________________________________________________");
    }   
}
