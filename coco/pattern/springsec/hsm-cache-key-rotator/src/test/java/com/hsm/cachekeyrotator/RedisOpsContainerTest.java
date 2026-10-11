package com.hsm.cachekeyrotator;

import com.hsm.crypto.DekCacheCodec;
import com.hsm.crypto.DekManager;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RedisOps against a real Redis: entries are seeded exactly as hsm-core-service's
 * RedisDekCache writes them (DekCacheCodec, contract C7), so a pass means a rotation
 * leaves the cache readable by the service under the new CEK. Skipped without Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisOpsContainerTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    private static final String OLD_VERSION = DekCacheCodec.version("alpha", "kv-old");
    private static final String NEW_VERSION = DekCacheCodec.version("beta", "kv-new");

    private RedisClient client;
    private StatefulRedisConnection<byte[], byte[]> connection;
    private RedisCommands<byte[], byte[]> redis;
    private RedisOps ops;

    @BeforeEach
    void connect() {
        client = RedisClient.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        connection = client.connect(ByteArrayCodec.INSTANCE);
        redis = connection.sync();
        redis.flushall();
        ops = new RedisOps(redis);
    }

    @AfterEach
    void disconnect() {
        connection.close();
        client.shutdown();
    }

    private Map<String, byte[]> seed(byte[] cek, int count) {
        Map<String, byte[]> deks = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String edekId = UUID.randomUUID().toString();
            byte[] dek = DekManager.generateDek();
            redis.set(DekCacheCodec.key(OLD_VERSION, edekId), DekCacheCodec.encode(dek, cek), SetArgs.Builder.ex(300));
            deks.put(edekId, dek);
        }
        return deks;
    }

    @Test
    void rekeyMovesEveryEntryToTheNewVersionReadableUnderTheNewCek() {
        byte[] oldCek = DekManager.generateDek();
        byte[] newCek = DekManager.generateDek();
        Map<String, byte[]> deks = seed(oldCek, 3);
        byte[] unrelated = "not-a-cache-entry".getBytes(StandardCharsets.UTF_8);
        redis.set(unrelated, unrelated);

        RedisOps.RekeyResult result = ops.rekeyDekCache(oldCek, newCek, OLD_VERSION, NEW_VERSION, 60);

        assertEquals(new RedisOps.RekeyResult(3, 0, 0), result);
        deks.forEach((edekId, dek) -> {
            assertNull(redis.get(DekCacheCodec.key(OLD_VERSION, edekId)), "old entry removed");
            byte[] value = redis.get(DekCacheCodec.key(NEW_VERSION, edekId));
            assertNotNull(value, "new entry written");
            assertArrayEquals(dek, DekCacheCodec.decode(value, newCek));
            long ttl = redis.ttl(DekCacheCodec.key(NEW_VERSION, edekId));
            assertTrue(ttl > 0 && ttl <= 300, "remaining TTL carried over, got " + ttl);
        });
        assertArrayEquals(unrelated, redis.get(unrelated), "non-cache keys untouched");
        assertEquals(Map.of(NEW_VERSION, 3), ops.countByVersion());
    }

    @Test
    void entryThatDoesNotDecryptIsCountedAsFailedAndLeftInPlace() {
        byte[] oldCek = DekManager.generateDek();
        seed(oldCek, 2);
        byte[] corruptKey = DekCacheCodec.key(OLD_VERSION, UUID.randomUUID().toString());
        redis.set(corruptKey, DekCacheCodec.encode(DekManager.generateDek(), DekManager.generateDek()));

        RedisOps.RekeyResult result = ops.rekeyDekCache(oldCek, DekManager.generateDek(), OLD_VERSION, NEW_VERSION, 60);

        assertEquals(new RedisOps.RekeyResult(2, 0, 1), result);
        assertNotNull(redis.get(corruptKey));
    }

    @Test
    void flushRemovesOnlyCacheEntries() {
        seed(DekManager.generateDek(), 4);
        byte[] unrelated = "keep-me".getBytes(StandardCharsets.UTF_8);
        redis.set(unrelated, unrelated);

        assertEquals(4, ops.flushDekCache());
        assertEquals(Map.of(), ops.countByVersion());
        assertArrayEquals(unrelated, redis.get(unrelated));
    }
}
