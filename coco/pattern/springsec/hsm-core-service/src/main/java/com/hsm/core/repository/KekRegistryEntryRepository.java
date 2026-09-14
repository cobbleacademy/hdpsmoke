package com.hsm.core.repository;

import com.hsm.core.model.KekRegistryEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface KekRegistryEntryRepository extends JpaRepository<KekRegistryEntry, KekRegistryEntry.Key> {

    /** One exact lookup per resolution tier -- see KekRegistryService. */
    Optional<KekRegistryEntry> findByAppIdAndDekNameAndDataClassification(
            String appId, String dekName, String dataClassification);

    /**
     * Does a DIFFERENT app already hold an exact-dek_name (tier 1) row for
     * this dek_name? dataClassification is always passed as KekRegistryEntry.UNSET
     * here -- only tier-1 rows (dek_name set, classification unset) carry
     * dek_name-reservation intent; classification/app-default tier rows
     * (dek_name unset) don't name a specific dek_name at all. See
     * DekNameReservationService.
     */
    Optional<KekRegistryEntry> findFirstByDekNameAndDataClassificationAndAppIdNot(
            String dekName, String dataClassification, String appId);
}
