package com.banking.account_service.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import com.banking.account_service.entity.Account;


public interface AccountRepository extends JpaRepository<Account, String> {

    boolean existsByEmail(String email);

    Optional<Account> findByAccountNumber(String accountNumber);
    
}
