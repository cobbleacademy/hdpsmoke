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

/**
 * The optional correlation-id hook: sent as X-Correlation-Id when the supplier has a
 * value, and absent otherwise -- so callers that don't set one (hsm-bulk-client, the
 * Spark adapter) send exactly the requests they always did.
 */
class SvcClientCorrelationIdTest {

    private HttpServer server;
    private final List<String> seen = new CopyOnWriteArrayList<>();

    @BeforeEach
    void startFakeCore() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/sensec/hsm/v1/dek/unwrap", exchange -> {
            String id = exchange.getRequestHeaders().getFirst(SvcClient.CORRELATION_ID_HEADER);
            seen.add(id == null ? "<absent>" : id);
            exchange.getRequestBody().readAllBytes();
            byte[] body = "{\"items\":[]}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private SvcConfig config() {
        return new SvcConfig("http://127.0.0.1:" + server.getAddress().getPort(), "/api/sensec/hsm/v1", "test-app",
                SvcConfig.AuthMode.STATIC, "t", null, 1, null, null, null, null, null);
    }

    private void unwrapOnce(SvcClient client) {
        client.unwrap(List.of(new SvcClient.UnwrapItem("k", UUID.randomUUID())));
    }

    @Test
    void headerSentWhenTheSupplierHasAValue() {
        unwrapOnce(new SvcClient(config(), () -> "bff-trace-77"));
        assertEquals(List.of("bff-trace-77"), seen);
    }

    @Test
    void noHeaderWithoutASupplierOrWithABlankValue() {
        unwrapOnce(new SvcClient(config()));
        unwrapOnce(new SvcClient(config(), () -> null));
        unwrapOnce(new SvcClient(config(), () -> "  "));
        assertEquals(List.of("<absent>", "<absent>", "<absent>"), seen);
    }

    @Test
    void supplierIsReadAtEachCall() {
        String[] current = {"first"};
        SvcClient client = new SvcClient(config(), () -> current[0]);
        unwrapOnce(client);
        current[0] = "second";
        unwrapOnce(client);
        assertEquals(List.of("first", "second"), seen);
    }
}
