package com.banking.transaction_service.entity;

/**
 * 
 * Transaction Lifecycle
 * 
 * 1) PENDING -> PROCESSING -> COMPLETED (clean transaction)
 * 
 * 2) PENDING -> PENDING_VERIFICATION (suspicious detection)
 *         -> COMPLETED (verified)
 *         -> FLAGGED (SAGA refund)
 * 
 * 3) PENDING -> FAILED ()
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}
