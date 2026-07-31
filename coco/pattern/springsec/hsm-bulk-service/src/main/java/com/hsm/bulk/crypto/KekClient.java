package com.hsm.bulk.crypto;

/**
 * Duplicated from com.hsm.core.crypto.KekClient -- wrap/unwrap operations against the
 * HSM-bound KEK. The KEK never leaves the HSM boundary -- only the wrapped DEK (EDEK)
 * crosses it. Implemented by {@link AzureKeyVaultKekClient} in production and
 * {@link MockKekClient} for local/PoC runs.
 */
public interface KekClient {

    WrapResult wrapDek(byte[] dek);

    byte[] unwrapDek(byte[] edek, String kekVersion);

    String getCurrentKekVersion();

    String fetchSecret(String secretName);

    SecretWithVersion fetchSecretWithVersion(String secretName);

    void close();

    record WrapResult(byte[] edekBytes, String kekVersion) {
    }

    record SecretWithVersion(String value, String kvVersion) {
    }
}
