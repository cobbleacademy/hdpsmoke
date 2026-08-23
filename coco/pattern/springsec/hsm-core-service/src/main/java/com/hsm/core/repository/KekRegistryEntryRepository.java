package com.hsm.core.repository;

import com.hsm.core.model.KekRegistryEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface KekRegistryEntryRepository extends JpaRepository<KekRegistryEntry, KekRegistryEntry.Key> {

    /** One exact lookup per resolution tier -- see KekRegistryService. */
    Optional<KekRegistryEntry> findByAppIdAndDekNameAndDataClassification(
            String appId, String dekName, String dataClassification);
}
