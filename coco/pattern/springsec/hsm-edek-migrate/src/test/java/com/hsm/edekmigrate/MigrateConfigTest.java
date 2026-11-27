package com.hsm.edekmigrate;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MigrateConfigTest {

    static Properties good() {
        Properties p = new Properties();
        p.setProperty("source.jdbc-url", "jdbc:postgresql://a/hsm");
        p.setProperty("source.user", "reader");
        p.setProperty("source.password-env", "SRC_PW");
        p.setProperty("source.crypto-schema", "hsm_crypto");
        p.setProperty("source.vault-url", "https://hsm-a.managedhsm.azure.net");
        p.setProperty("source.legacy-kek-name", "hsm-master-kek");
        p.setProperty("source.credential", "client-secret");
        p.setProperty("source.tenant-id", "tenant-a");
        p.setProperty("source.client-id", "client-a");
        p.setProperty("source.client-secret-env", "SRC_SECRET");
        p.setProperty("target.jdbc-url", "jdbc:postgresql://b/hsm");
        p.setProperty("target.user", "writer");
        p.setProperty("target.password-env", "TGT_PW");
        p.setProperty("target.crypto-schema", "hsm_crypto");
        p.setProperty("target.access-schema", "hsm_access");
        p.setProperty("target.vault-url", "https://hsm-b.managedhsm.azure.net");
        p.setProperty("target.credential", "managed-identity");
        p.setProperty("kek-map.hsm-master-kek", "hsm-b-kek");
        return p;
    }

    static final Map<String, String> ENV = Map.of("SRC_PW", "p1", "TGT_PW", "p2", "SRC_SECRET", "s3cret");

    @Test
    void aCompleteFileLoads() {
        MigrateConfig c = MigrateConfig.from(good(), ENV);
        assertEquals("p1", c.source().password());
        assertEquals("s3cret", c.source().credential().clientSecret());
        assertEquals("managed-identity", c.target().credential().mode());
        assertNull(c.target().credential().clientId());
        assertEquals("edek_migrate", c.stagingSchema(), "the default staging schema");
        assertEquals(500, c.pageSize());
        assertEquals(10, c.maxConsecutiveFailures());
        assertEquals("hsm-b-kek", c.targetKekFor("hsm-master-kek"));
        assertNull(c.targetKekFor("other"));
    }

    @Test
    void everyProblemIsReportedAtOnce() {
        Properties p = good();
        p.remove("source.user");
        p.setProperty("target.crypto-schema", "hsm_crypto; DROP TABLE x");
        p.setProperty("target.staging-schema", "hsm_access");
        p.setProperty("source.credential", "password");
        p.remove("kek-map.hsm-master-kek");
        p.setProperty("page-size", "0");
        MigrateConfig.ConfigException e = assertThrows(MigrateConfig.ConfigException.class,
                () -> MigrateConfig.from(p, Map.of("SRC_PW", "p1")));
        String all = String.join("\n", e.problems());
        assertTrue(all.contains("source.user is required"), all);
        assertTrue(all.contains("target.crypto-schema must be a lower-case identifier"), all);
        assertTrue(all.contains("target.password-env names TGT_PW, which isn't set"), all);
        assertTrue(all.contains("source.credential 'password' is not one of"), all);
        assertTrue(all.contains("no kek-map"), all);
        assertTrue(all.contains("page-size must be a positive whole number"), all);
    }

    @Test
    void stagingCantBeTheLiveSchema() {
        Properties p = good();
        p.setProperty("target.staging-schema", "hsm_crypto");
        MigrateConfig.ConfigException e = assertThrows(MigrateConfig.ConfigException.class,
                () -> MigrateConfig.from(p, ENV));
        assertTrue(e.problems().contains("target.staging-schema must differ from target.crypto-schema"));
    }

    @Test
    void credentialModesNeedTheirFields() {
        Properties p = good();
        p.remove("source.tenant-id");
        p.setProperty("target.credential", "client-certificate");
        MigrateConfig.ConfigException e = assertThrows(MigrateConfig.ConfigException.class,
                () -> MigrateConfig.from(p, ENV));
        String all = String.join("\n", e.problems());
        assertTrue(all.contains("source.credential=client-secret needs source.tenant-id and source.client-id"), all);
        assertTrue(all.contains("target.credential=client-certificate needs target.tenant-id"), all);
        assertTrue(all.contains("target.client-certificate-path is required"), all);
    }

    @Test
    void workloadIdentityTakesTheTokenFileFromTheEnvironment() {
        Properties p = good();
        p.setProperty("target.credential", "workload-identity");
        p.setProperty("target.tenant-id", "tenant-b");
        p.setProperty("target.client-id", "client-b");
        Map<String, String> env = new java.util.HashMap<>(ENV);
        env.put("AZURE_FEDERATED_TOKEN_FILE", "/tmp/token");
        assertEquals("/tmp/token", MigrateConfig.from(p, env).target().credential().tokenFile());
    }

    @Test
    void secretsNeverAppearInToString() {
        MigrateConfig c = MigrateConfig.from(good(), ENV);
        String s = c.toString();
        assertFalse(s.contains("p1") || s.contains("p2") || s.contains("s3cret"), s);
        assertTrue(s.contains("<redacted>"), s);
    }

    @Test
    void kekNamesAreChecked() {
        Properties p = good();
        p.setProperty("kek-map.bad name", "x");
        MigrateConfig.ConfigException e = assertThrows(MigrateConfig.ConfigException.class,
                () -> MigrateConfig.from(p, ENV));
        assertTrue(e.problems().get(0).contains("kek-map entry 'kek-map.bad name'"), e.problems().toString());
    }
}
