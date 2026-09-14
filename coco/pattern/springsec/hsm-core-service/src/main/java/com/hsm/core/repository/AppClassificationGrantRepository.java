package com.hsm.core.repository;

import com.hsm.core.model.AppClassificationGrant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppClassificationGrantRepository extends JpaRepository<AppClassificationGrant, AppClassificationGrant.Key> {

    /** Phase 1 shadow-mode check / Phase 2 enforcement -- see ClassificationGovernanceService. */
    boolean existsByAppIdAndDataClassification(String appId, String dataClassification);
}
