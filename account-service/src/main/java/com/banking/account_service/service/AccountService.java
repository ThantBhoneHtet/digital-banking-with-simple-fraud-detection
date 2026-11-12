package com.banking.account_service.service;

import java.math.BigDecimal;
import java.security.SecureRandom;

import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Service;
import com.banking.account_service.dto.AccountResponse;
import com.banking.account_service.dto.CreateAccountRequest;
import com.banking.account_service.entity.Account;
import com.banking.account_service.entity.AccountStatus;
import com.banking.account_service.entity.AccountType;
import com.banking.account_service.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class AccountService {

    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating account for: {}", request.getEmail());

        if(accountRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("Account with email " + request.getEmail() + " already exists");
        }

        Account account = Account.builder()
                .accountHolderName(request.getAccountHolderName())
                .email(request.getEmail())
                .phone(request.getPhone())
                .accountType(request.getAccountType())
                .accountStatus(AccountStatus.ACTIVE)
                .balance(request.getInitialDeposit())
                .accountNumber(generateAccountNumber())
                .dailyTransactionLimit(
                    request.getAccountType() == AccountType.SAVINGS
                    ? new BigDecimal("100000")
                    : new BigDecimal("500000")
                )
                .build();
        
        Account savedAccount = accountRepository.save(account);
        log.info("Account created successfully for: {}", savedAccount.getEmail());

        return mapToResponse(savedAccount);
    }

    public AccountResponse getAccount(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() ->  new RuntimeException("Account not found."));
        return mapToResponse(account);
    }

	public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() ->  new RuntimeException("Account not found."));

        return account.getBalance();
	}

    /**
     * Block account - called by Fraud Detection Service 
     * via Kafka when suspicious activity is detected
     * @param accountNumber
     */
    public void blockAccount(String accountNumber) {
        log.info("Blocking account: {}", accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() ->  new RuntimeException("Account not found."));

        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

        log.info("Account {} blocked successfully", accountNumber);
    }

    /**
     * Deduct balance from sender account - called by Transaction Service when transfer is initiated
     * @param accountNumber
     * @param amount
     */
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting {} from account: {}", amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() ->  new RuntimeException("Account not found."));

        if (account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account is not active. Cannot deduct balance from " + accountNumber);
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient balance in account " + accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance amount of {} is deducted successfully from account: {}", amount, accountNumber);
    }

    /**
     * Credit balance to receiver account - called by Transaction Service when transfer is initiated
     * @param accountNumber
     * @param amount
     */
	public void creditBalance(String accountNumber, BigDecimal amount) {
		log.info("Crediting {} to account: {}", amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() ->  new RuntimeException("Account not found."));

        if (account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account is not active. Cannot credit balance to " + accountNumber);
        }

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balance amount of {} is credited successfully to account: {}", amount, accountNumber);
    }

    /**
     * 
     * @return a unique 12-digit account number
     */
    private String generateAccountNumber() {
        String accountNumber;

        do{
            long number = secureRandom.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        } while (accountRepository.existsById(accountNumber));
        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account) {
        return new AccountResponse(
            account.getId(),
            account.getAccountNumber(),
            account.getAccountHolderName(),
            account.getEmail(),
            account.getPhone(),
            account.getAccountType(),
            account.getAccountStatus(),
            account.getBalance(),
            account.getDailyTransactionLimit(),
            account.getCreatedAt()
        );
    }
}
