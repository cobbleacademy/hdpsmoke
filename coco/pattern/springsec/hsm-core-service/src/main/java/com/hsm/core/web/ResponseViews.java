package com.hsm.core.web;

/**
 * Jackson {@code @JsonView} markers controlling how much detail /encrypt and
 * /decrypt responses expose. Minimal is the default for every real caller --
 * just the field needed to store/echo plus the response envelope. Full adds
 * the informational/audit fields (edek_id, owner_app_id, algorithm, encoding,
 * kek_version) for callers who want them, selected via the X-Response-Detail
 * request header ("full" | anything else/absent = minimal).
 *
 * <p>Full extends Minimal so a field tagged with just {@code @JsonView(Minimal.class)}
 * (or left untagged, which Jackson always serializes regardless of view) still
 * appears under the Full view too -- Full is a superset, not a parallel set.
 */
public final class ResponseViews {

    private ResponseViews() {
    }

    public interface Minimal {
    }

    public interface Full extends Minimal {
    }
}
