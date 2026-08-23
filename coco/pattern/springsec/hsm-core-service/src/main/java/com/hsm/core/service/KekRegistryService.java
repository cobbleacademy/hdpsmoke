package com.hsm.core.service;

import com.hsm.core.config.HsmProperties;
import com.hsm.core.model.KekRegistryEntry;
import com.hsm.core.repository.KekRegistryEntryRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Resolves which KEK a brand-new EDEK should be wrapped under, from
 * (app_id, dek_name, data_classification). Consulted exactly once, at encrypt
 * time when minting a new EDEK -- see EncryptionService.resolveDek and
 * EdekRecord's own javadoc for why nothing downstream of that (decrypt,
 * rotation) ever calls back into this class.
 *
 * <p>Three-tier resolution, most specific first:
 * <ol>
 *   <li>exact (app_id, dek_name) match</li>
 *   <li>(app_id, data_classification) match, when dek_name wasn't set or tier 1 missed</li>
 *   <li>(app_id) per-app default -- (app_id, "", "")</li>
 * </ol>
 * If nothing at all is registered for this app_id, falls back to the legacy
 * single-KEK config value (hsm.service.azure.kek-name) -- identical to how
 * every app behaved before multi-KEK support existed, so an app that hasn't
 * opted into per-purpose KEKs sees no behavior change. This is the one
 * exception to "unprovisioned combinations fail closed": failing closed here
 * would break every existing app the moment this table exists, since none of
 * them would have any rows in it yet.
 */
@Service
public class KekRegistryService {

    private final KekRegistryEntryRepository repository;
    private final String legacyDefaultKekName;

    public KekRegistryService(KekRegistryEntryRepository repository, HsmProperties properties) {
        this.repository = repository;
        this.legacyDefaultKekName = properties.azure().kekName();
    }

    public String resolve(String appId, String dekName, String dataClassification) {
        boolean hasDekName = dekName != null && !dekName.isBlank();
        boolean hasClassification = dataClassification != null && !dataClassification.isBlank();

        if (hasDekName) {
            Optional<KekRegistryEntry> tier1 = repository.findByAppIdAndDekNameAndDataClassification(
                    appId, dekName, KekRegistryEntry.UNSET);
            if (tier1.isPresent()) {
                return tier1.get().getKekName();
            }
        }

        if (hasClassification) {
            Optional<KekRegistryEntry> tier2 = repository.findByAppIdAndDekNameAndDataClassification(
                    appId, KekRegistryEntry.UNSET, dataClassification);
            if (tier2.isPresent()) {
                return tier2.get().getKekName();
            }
        }

        Optional<KekRegistryEntry> tier3 = repository.findByAppIdAndDekNameAndDataClassification(
                appId, KekRegistryEntry.UNSET, KekRegistryEntry.UNSET);
        if (tier3.isPresent()) {
            return tier3.get().getKekName();
        }

        return legacyDefaultKekName;
    }

    /** For decrypt of pre-multi-KEK rows where EdekRecord.kekName is NULL -- see EdekRecord's javadoc. */
    public String getLegacyDefaultKekName() {
        return legacyDefaultKekName;
    }
}
