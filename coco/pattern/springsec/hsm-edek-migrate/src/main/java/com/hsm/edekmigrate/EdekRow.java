package com.hsm.edekmigrate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.UUID;

/**
 * One row of hsm-core's {@code edek_records}, every column. The EDEK blob is the DEK
 * wrapped by a KEK: ciphertext, safe to hold in memory and in the staging table.
 */
public record EdekRow(
        UUID edekId,
        String appId,
        String edekBlob,
        String kekVersion,
        String kekName,
        String previousKekName,
        String previousKekVersion,
        String previousEdekBlob,
        String algorithm,
        String encoding,
        String dataClassification,
        String rotationStatus,
        OffsetDateTime createdAt,
        OffsetDateTime rotatedAt,
        String fingerprint,
        String dekName,
        String currentDekName) {

    /** The column list, in this record's order, for SELECTs from {@code edek_records}. */
    public static final String COLUMNS = "edek_id, app_id, edek_blob, kek_version, kek_name, previous_kek_name, "
            + "previous_kek_version, previous_edek_blob, algorithm, encoding, data_classification, rotation_status, "
            + "created_at, rotated_at, fingerprint, dek_name, current_dek_name";

    public static EdekRow read(ResultSet rs) throws SQLException {
        return new EdekRow(
                rs.getObject("edek_id", UUID.class),
                rs.getString("app_id"),
                rs.getString("edek_blob"),
                rs.getString("kek_version"),
                rs.getString("kek_name"),
                rs.getString("previous_kek_name"),
                rs.getString("previous_kek_version"),
                rs.getString("previous_edek_blob"),
                rs.getString("algorithm"),
                rs.getString("encoding"),
                rs.getString("data_classification"),
                rs.getString("rotation_status"),
                rs.getObject("created_at", OffsetDateTime.class),
                rs.getObject("rotated_at", OffsetDateTime.class),
                rs.getString("fingerprint"),
                rs.getString("dek_name"),
                rs.getString("current_dek_name"));
    }

    /** The KEK this row was wrapped with: its kek_name, or the source's legacy default when that is NULL. */
    public String resolvedKekName(String legacyDefault) {
        return kekName == null ? legacyDefault : kekName;
    }

    /**
     * SHA-256 over every column, so a later {@code stage} sees a row that changed in the
     * source. Each value is length-prefixed and NULL is distinct from empty, so two
     * different rows can't produce the same input. Holds no key material: the blobs are
     * wrapped.
     */
    public String sourceHash() {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        for (Object v : new Object[]{edekId, appId, edekBlob, kekVersion, kekName, previousKekName,
                previousKekVersion, previousEdekBlob, algorithm, encoding, dataClassification, rotationStatus,
                createdAt == null ? null : createdAt.toInstant(), rotatedAt == null ? null : rotatedAt.toInstant(),
                fingerprint, dekName, currentDekName}) {
            if (v == null) {
                md.update((byte) 0);
            } else {
                byte[] b = v.toString().getBytes(StandardCharsets.UTF_8);
                md.update((byte) 1);
                md.update(intBytes(b.length));
                md.update(b);
            }
        }
        return HexFormat.of().formatHex(md.digest());
    }

    private static byte[] intBytes(int n) {
        return new byte[]{(byte) (n >>> 24), (byte) (n >>> 16), (byte) (n >>> 8), (byte) n};
    }
}
