package com.hsm.encryption.repository;

import com.hsm.encryption.model.AppDecryptGrant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppDecryptGrantRepository extends JpaRepository<AppDecryptGrant, AppDecryptGrant.Key> {
}
