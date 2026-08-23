package com.hsm.core.crypto;

/**
 * Wrap/unwrap operations against an HSM-bound KEK, identified by name. The
 * KEK never leaves the HSM boundary -- only the wrapped DEK (EDEK) crosses
 * it. Implemented by {@link AzureKeyVaultKekClient} in production and
 * {@link MockKekClient} in demo mode.
 *
 * <p>Deliberately knows nothing about app_id, dek_name, or data_classification
 * -- resolving which kekName to use for a given (app_id, dek_name) pair is a
 * business/DB concern (see KekRegistryService), not a crypto one. By the time
 * anything calls wrapDek/unwrapDek here, that resolution is already done;
 * this interface only ever deals in a concrete, already-known key identity,
 * matching Key Vault's own actual surface (a key IS identified by name +
 * version, nothing else -- Key Vault itself has no concept of app_id or
 * dek_name).
 */
public interface KekClient {

    /** Wrap a DEK using the named KEK. Returns the EDEK bytes and the KEK version used. */
    WrapResult wrapDek(byte[] dek, String kekName);

    /** Unwrap an EDEK using the specific KEK (name + version) it was wrapped with. */
    byte[] unwrapDek(byte[] edek, String kekName, String kekVersion);

    /** The named KEK's current (latest) version identifier. */
    String getCurrentKekVersion(String kekName);

    /** Retrieve a secret value from Key Vault Secrets (regular vault, not Managed HSM). */
    String fetchSecret(String secretName);

    /** Retrieve a secret's value together with its Key Vault version id. */
    SecretWithVersion fetchSecretWithVersion(String secretName);

    void close();

    record WrapResult(byte[] edekBytes, String kekVersion) {
    }

    record SecretWithVersion(String value, String kvVersion) {
    }
}
