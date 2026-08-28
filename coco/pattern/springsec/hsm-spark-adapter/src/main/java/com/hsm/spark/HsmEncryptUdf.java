package com.hsm.spark;

import org.apache.spark.sql.api.java.UDF3;

/**
 * {@code hsm_encrypt(plaintext, dek_name, data_classification)} -- one
 * function, many DEKs: {@code dek_name} is a per-call SQL argument, not
 * baked into this class, so a single registration serves every column
 * (each column just passes its own {@code dek_name} literal in the query --
 * see java/docs/SPARK_ADAPTER.md's usage examples). {@code data_classification}
 * may be {@code null}. Returns the same {@code "v1...."} packed-token format
 * hsm-core-service's own /encrypt produces.
 *
 * <p>{@code dek_name} must be a literal constant per column, not a per-row
 * expression -- a value that varies per row defeats the DEK-reuse cache
 * entirely, degrading back to a fresh /dek/issue call per row.
 */
public class HsmEncryptUdf implements UDF3<String, String, String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String call(String plaintext, String dekName, String dataClassification) {
        return HsmCryptoClientHolder.get().encrypt(plaintext, dekName, dataClassification);
    }
}
