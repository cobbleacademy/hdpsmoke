package com.hsm.encryption.crypto;

/**
 * Wrap/unwrap operations against the HSM-bound KEK. The KEK never leaves the
 * HSM boundary -- only the wrapped DEK (EDEK) crosses it. Implemented by
 * {@link AzureKeyVaultKekClient} in production and {@link MockKekClient} in demo mode.
 */
public interface KekClient {

    /** Wrap a DEK using the HSM-bound KEK. Returns the EDEK bytes and the KEK version used. */
    WrapResult wrapDek(byte[] dek);

    /** Unwrap an EDEK using the specific KEK version it was wrapped with. */
    byte[] unwrapDek(byte[] edek, String kekVersion);

    /** The KEK's current (latest) version identifier. */
    String getCurrentKekVersion();

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
