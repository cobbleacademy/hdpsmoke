package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.model.AppClassificationGrant;
import com.hsm.core.repository.AppClassificationGrantRepository;
import com.hsm.core.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Data-classification governance -- see V15's migration comment for the full
 * rationale. data_classification is free text otherwise (EncryptRequest.dataClassification's
 * own comment: "never enforced here"), and the only other existing check
 * (EncryptionService.checkClassificationMatch / DekIssueService's equivalent)
 * only prevents relabeling an ALREADY-minted dek_name -- it never governs
 * which classification an app may declare in the first place.
 *
 * <p>Two checkpoints, both gated by the same {@code hsm.classification-governance.enforce}
 * flag (default false -- Phase 1/shadow mode, logs only; true -- Phase 2,
 * rejects):
 * <ul>
 *   <li>{@link #checkFirstMintClassification} -- on a fresh mint (no existing
 *   dek_name row), the same point kek_registry resolution already happens,
 *   for the same reason: once a dek_name exists, its classification is
 *   governed by checkClassificationMatch instead, which this does not
 *   replace or duplicate.</li>
 *   <li>{@link #checkReuseClassification} -- on a CROSS-APP reuse of an
 *   existing dek_name (a grantee, not the owner), verifying the grantee
 *   itself is approved for that classification, not just that it holds an
 *   encrypt grant on the dek_name. Never applied to same-app reuse -- an
 *   owner's own classification approval is checked once, at mint time, not
 *   re-validated on every subsequent call, consistent with this codebase's
 *   "resolved once, never re-consulted" precedent for kek_registry.</li>
 * </ul>
 *
 * <p>Deliberately NOT re-validated on decrypt or rotation -- both already
 * only need the dek_name-level grant (app_grants/app_dek_grants), which is a
 * question about WHICH key an app may touch, orthogonal to WHAT label an app
 * may declare when minting one (see V15's migration comment for the full
 * "orthogonal axes" reasoning).
 */
@Service
public class ClassificationGovernanceService {

    private static final Logger log = LoggerFactory.getLogger(ClassificationGovernanceService.class);

    private final AppClassificationGrantRepository repository;
    private final AuditLogger auditLogger;
    private final boolean enforce;

    public ClassificationGovernanceService(AppClassificationGrantRepository repository, AuditLogger auditLogger,
                                            HsmProperties properties) {
        this.repository = repository;
        this.auditLogger = auditLogger;
        this.enforce = properties.classificationGovernance().enforce();
    }

    /** Fresh mint (no existing dek_name row) -- see class javadoc. */
    public void checkFirstMintClassification(String appId, String dekName, String dataClassification) {
        check(appId, dekName, dataClassification, "mint");
    }

    /** Cross-app reuse of an existing dek_name -- call only for a grantee, never the owner (see class javadoc). */
    public void checkReuseClassification(String granteeAppId, String dekName, String dataClassification) {
        check(granteeAppId, dekName, dataClassification, "reuse");
    }

    private void check(String appId, String dekName, String dataClassification, String context) {
        if (dataClassification == null || dataClassification.isBlank()) {
            return;
        }
        boolean approved = repository.existsByAppIdAndDataClassification(appId, dataClassification);
        if (approved) {
            return;
        }

        if (enforce) {
            auditLogger.log("classification_check",
                    "app_id", appId, "dek_name", dekName, "data_classification", dataClassification,
                    "context", context, "status", "denied");
            throw new ApiException(HttpStatus.FORBIDDEN,
                    "app '" + appId + "' is not approved to use data_classification '" + dataClassification + "'");
        }

        log.warn("classification_shadow_mode_unapproved app_id={} dek_name={} data_classification={} context={}",
                appId, dekName, dataClassification, context);
        auditLogger.log("classification_check",
                "app_id", appId, "dek_name", dekName, "data_classification", dataClassification,
                "context", context, "status", "unapproved_shadow_mode");
    }

    /** Phase 3 admin surface -- POST /admin/apps/classifications. Idempotent: re-granting an existing (app_id, classification) pair is a no-op, not an error. */
    @Transactional
    public AppClassificationGrant addGrant(String appId, String dataClassification, String grantedBy) {
        AppClassificationGrant.Key key = new AppClassificationGrant.Key(appId, dataClassification);
        AppClassificationGrant existing = repository.findById(key).orElse(null);
        return existing != null ? existing : repository.save(new AppClassificationGrant(appId, dataClassification, grantedBy));
    }

    /** Phase 3 admin surface -- DELETE /admin/apps/classifications. */
    @Transactional
    public void removeGrant(String appId, String dataClassification) {
        repository.deleteById(new AppClassificationGrant.Key(appId, dataClassification));
    }

    /** Phase 3 admin surface -- GET /admin/apps/classifications. */
    public List<AppClassificationGrant> listGrants() {
        return repository.findAll();
    }
}
