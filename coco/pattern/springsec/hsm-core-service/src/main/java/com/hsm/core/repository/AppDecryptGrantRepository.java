package com.hsm.core.repository;

import com.hsm.core.model.AppDecryptGrant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppDecryptGrantRepository extends JpaRepository<AppDecryptGrant, AppDecryptGrant.Key> {
}
