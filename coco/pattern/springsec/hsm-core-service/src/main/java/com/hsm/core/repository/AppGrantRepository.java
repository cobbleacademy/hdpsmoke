package com.hsm.core.repository;

import com.hsm.core.model.AppGrant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppGrantRepository extends JpaRepository<AppGrant, AppGrant.Key> {
}
