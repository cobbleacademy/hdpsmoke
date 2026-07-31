package com.hsm.bulk.repository;

import com.hsm.bulk.model.AppDecryptGrant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppDecryptGrantRepository extends JpaRepository<AppDecryptGrant, AppDecryptGrant.Key> {
}
