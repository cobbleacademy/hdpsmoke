package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/**
 * Demo-only. Ported from app/demo/consumer_store.py's ConsumerAccount. Models the
 * *other half* of the architecture: this HSM service never stores ciphertext itself;
 * a calling app (simulated here as payments-svc) owns its own schema and stores the
 * opaque ciphertext_token next to its own non-sensitive columns. account_number
 * itself is never stored -- only the 4 fields needed to build a DecryptRequest
 * later travel in ciphertext_token.
 */
@Entity
@Table(name = "consumer_customer_accounts")
public class ConsumerAccount {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "customer_name", nullable = false, length = 128)
    private String customerName;

    @Column(name = "email", nullable = false, length = 256)
    private String email;

    @Column(name = "ciphertext_token", nullable = false, length = 512)
    private String ciphertextToken;

    // Every account created via DemoController.createConsumerAccount() shares the same
    // dekName ("customers.account_number"), so this is null only for accounts created
    // before that DEK-naming wiring landed (see V8 migration).
    @Column(name = "dek_name", length = 256)
    private String dekName;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    protected ConsumerAccount() {
        // JPA
    }

    public ConsumerAccount(String customerName, String email, String ciphertextToken, String dekName) {
        this.customerName = customerName;
        this.email = email;
        this.ciphertextToken = ciphertextToken;
        this.dekName = dekName;
        this.createdAt = OffsetDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getEmail() {
        return email;
    }

    public String getCiphertextToken() {
        return ciphertextToken;
    }

    public String getDekName() {
        return dekName;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
