package com.banking.account_service.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.banking.account_service.dto.AccountResponse;
import com.banking.account_service.dto.CreateAccountRequest;
import com.banking.account_service.service.AccountService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController 
@RequestMapping("/api/v1/accounts")
@Slf4j 
@RequiredArgsConstructor 
public class AccountController {
    private final AccountService accountService;

    @PostMapping("/create")
    public ResponseEntity<AccountResponse> postMethodName(
        @Valid @RequestBody CreateAccountRequest request) {
            return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createAccount(request));
    }
    
    @GetMapping("/{accountNumber}") 
    public ResponseEntity<AccountResponse> getAccount(
        @PathVariable String accountNumber) {
            return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }
    
    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getBalance(
        @PathVariable String accountNumber) {
            return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> blockAccount(
        @PathVariable String accountNumber) {
            accountService.blockAccount(accountNumber);
            return ResponseEntity.ok("Account blocked successfully");
    }

    /**
     * SAGA Step 1 - deduct balance
     * called by Transaction Service when transfer is initiated
    */
    
    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductBalance(
        @PathVariable String accountNumber, 
        @RequestParam BigDecimal amount) {
            accountService.deductBalance(accountNumber, amount);
            return ResponseEntity.ok("Balance deducted successfully");
    }

    /**
     * SAGA Step 4 - compensating transaction to rollback the balance deduction
     * called by Transaction Service in 2 scenarios: 
     * 1. Fraud detected -> refund sender (undo step 1)
     * 2. Transaction completed -> credit receiver
    */

    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(
        @PathVariable String accountNumber,
        @RequestParam BigDecimal amount
    ) {
        accountService.creditBalance(accountNumber, amount);
        return ResponseEntity.ok("Balance credited successfully");
    }
}
