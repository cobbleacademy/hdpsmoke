package com.hsm.client.svc;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * apiVersion picks both the prefix and the JSON field naming: V1 (the default when
 * unset) is exactly the pre-v2 request, V2 goes to the v2 prefix in camelCase.
 */
class SvcClientApiVersionTest {

    private static final UUID EDEK = UUID.fromString("00000000-0000-0000-0000-000000000042");

    private HttpServer server;
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startFakeCore() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        respond("/api/sensec/hsm/v1/dek/unwrap", "{\"items\":[{\"key\":\"k\",\"status\":\"success\",\"edek_id\":\"" + EDEK
                + "\",\"wrapped_dek_b64\":\"d3JhcHBlZA==\",\"owner_app_id\":\"owner\",\"detail\":null}]}");
        respond("/api/sensec/hsm/v2/dek/unwrap", "{\"items\":[{\"key\":\"k\",\"status\":\"success\",\"edekId\":\"" + EDEK
                + "\",\"wrappedDekB64\":\"d3JhcHBlZA==\",\"ownerAppId\":\"owner\",\"detail\":null}]}");
        server.start();
    }

    private void respond(String path, String json) {
        server.createContext(path, exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private SvcConfig config(SvcConfig.ApiVersion version) {
        return new SvcConfig("http://127.0.0.1:" + server.getAddress().getPort(), null, "test-app",
                SvcConfig.AuthMode.STATIC, "t", null, 1, null, null, null, null, null, version, null);
    }

    private SvcClient.UnwrapResult unwrap(SvcConfig.ApiVersion version) {
        return new SvcClient(config(version)).unwrap(List.of(new SvcClient.UnwrapItem("k", EDEK))).get(0);
    }

    @Test
    void unsetMeansV1SnakeCase() {
        assertEquals(SvcConfig.ApiVersion.V1, config(null).apiVersion());
        SvcClient.UnwrapResult result = unwrap(null);
        assertEquals(List.of("/api/sensec/hsm/v1/dek/unwrap"), paths);
        assertTrue(bodies.get(0).contains("\"edek_id\""), bodies.get(0));
        assertEquals("d3JhcHBlZA==", result.wrappedDekB64());
        assertEquals("owner", result.ownerAppId());
    }

    @Test
    void v2IsCamelCaseOnTheV2Prefix() {
        SvcClient.UnwrapResult result = unwrap(SvcConfig.ApiVersion.V2);
        assertEquals(List.of("/api/sensec/hsm/v2/dek/unwrap"), paths);
        assertTrue(bodies.get(0).contains("\"edekId\""), bodies.get(0));
        assertTrue(!bodies.get(0).contains("_"), "no snake_case field on v2: " + bodies.get(0));
        assertEquals(EDEK, result.edekId());
        assertEquals("d3JhcHBlZA==", result.wrappedDekB64());
        assertEquals("owner", result.ownerAppId());
    }

    @Test
    void prefixesDefaultWhenBlank() {
        SvcConfig config = config(SvcConfig.ApiVersion.V2);
        assertEquals("/api/sensec/hsm/v1", config.apiV1Prefix());
        assertEquals("/api/sensec/hsm/v2", config.apiPrefix());
    }
}
