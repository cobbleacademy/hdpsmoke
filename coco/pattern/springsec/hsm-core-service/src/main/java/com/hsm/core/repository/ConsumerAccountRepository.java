package com.hsm.core.repository;

import com.hsm.core.model.ConsumerAccount;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ConsumerAccountRepository extends JpaRepository<ConsumerAccount, Long> {

    List<ConsumerAccount> findAllByOrderByCreatedAtDesc();
}
