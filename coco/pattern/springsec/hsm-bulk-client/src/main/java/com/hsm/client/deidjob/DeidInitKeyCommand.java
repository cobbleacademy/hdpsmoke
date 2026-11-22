package com.hsm.client.deidjob;

import com.hsm.client.svc.SvcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * {@code --deid.init-key --name <dek-name>} (DEIDENTIFICATION.md section 7): issues a
 * named DEK through {@code /dek/issue} and prints its {@code edek_id}, to pin in the
 * profile ({@code token-key-edek-id}) or the job config
 * ({@code client.deid.surrogate.key-edek-id}).
 *
 * <p>Needs the {@code dek_issue} scope. The wrapped DEK in the response is discarded
 * unread: only the id is printed. If the name already has a current DEK owned by this
 * app, hsm-core returns that one and this says so.
 */
public final class DeidInitKeyCommand {

    private static final Logger log = LoggerFactory.getLogger(DeidInitKeyCommand.class);

    /** Names the bulk client's own keys follow: deid-<app>-<vendor>-tok, deid-<app>-surrogate. */
    private static final String NAME_PATTERN = "deid-[a-z0-9][a-z0-9-]*[a-z0-9]";

    private final SvcClient svc;

    public DeidInitKeyCommand(SvcClient svc) {
        this.svc = svc;
    }

    /** What was issued. */
    public record Issued(String name, UUID edekId, String ownerAppId, boolean reused) {
    }

    public Issued run(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("--deid.init-key needs --name <dek-name>, e.g. deid-giftcard-zen-tok");
        }
        if (!name.matches(NAME_PATTERN)) {
            throw new IllegalArgumentException("DEK name '" + name + "' should look like deid-<app>-<vendor>-tok or "
                    + "deid-<app>-surrogate (lower case, digits and hyphens)");
        }
        List<SvcClient.IssueResult> results = svc.issue(List.of(new SvcClient.IssueItem("deid-init-key", null, name)));
        SvcClient.IssueResult r = results.get(0);
        if (!"success".equals(r.status())) {
            throw new IllegalStateException("dek/issue failed for '" + name + "': " + r.detail());
        }
        Issued issued = new Issued(name, r.edekId(), r.ownerAppId(), r.reused());
        log.info("deid_init_key name={} edek_id={} owner_app_id={} reused={}", name, r.edekId(), r.ownerAppId(), r.reused());
        System.out.println("name:      " + name);
        System.out.println("edek_id:   " + r.edekId());
        System.out.println("owner:     " + r.ownerAppId());
        System.out.println(r.reused()
                ? "status:    existing key (the name already had a current DEK); pin this edek_id"
                : "status:    new key; pin this edek_id in the profile or job config");
        return issued;
    }
}
