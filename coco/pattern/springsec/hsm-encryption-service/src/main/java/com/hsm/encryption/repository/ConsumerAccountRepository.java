package com.hsm.encryption.repository;

import com.hsm.encryption.model.ConsumerAccount;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ConsumerAccountRepository extends JpaRepository<ConsumerAccount, Long> {

    List<ConsumerAccount> findAllByOrderByCreatedAtDesc();
}
