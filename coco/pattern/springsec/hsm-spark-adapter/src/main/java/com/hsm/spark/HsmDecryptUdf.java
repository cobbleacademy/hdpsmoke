package com.hsm.spark;

import org.apache.spark.sql.api.java.UDF1;

/**
 * {@code hsm_decrypt(ciphertext)} -- single form, no {@code dek_name}
 * argument needed at all: the packed token already carries {@code edek_id},
 * so which DEK to use is self-describing, the same reason
 * {@code HsmCryptoClient.decrypt(String)} itself takes only one argument.
 * One registration transparently serves every column, encrypted under any
 * {@code dek_name} (or none).
 */
public class HsmDecryptUdf implements UDF1<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String call(String ciphertextToken) {
        return HsmCryptoClientHolder.get().decryptToString(ciphertextToken);
    }
}
