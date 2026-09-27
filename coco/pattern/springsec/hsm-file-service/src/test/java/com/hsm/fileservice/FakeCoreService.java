package com.hsm.fileservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hsm.client.crypto.TransportWrapper;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal stand-in for hsm-core-service's {@code POST /dek/unwrap}: returns each known
 * DEK RSA-OAEP-wrapped to the file service's public key, with its owner app_id. Unknown
 * ids answer status=error (what core does for a missing grant or a shredded key);
 * ids in {@link #failHttp} answer HTTP 500 (core unavailable).
 */
final class FakeCoreService implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    record Dek(byte[] raw, String owner) {
    }

    final Map<UUID, Dek> deks = new ConcurrentHashMap<>();
    final Set<UUID> failHttp = ConcurrentHashMap.newKeySet();
    final AtomicInteger unwrapCalls = new AtomicInteger();
    private final HttpServer server;
    private final PublicKey serviceTransportKey;

    FakeCoreService(String apiPrefix, PublicKey serviceTransportKey) throws IOException {
        this.serviceTransportKey = serviceTransportKey;
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(apiPrefix + "/dek/unwrap", exchange -> {
            unwrapCalls.incrementAndGet();
            JsonNode req = MAPPER.readTree(exchange.getRequestBody());
            ArrayNode items = MAPPER.createArrayNode();
            int status = 200;
            for (JsonNode item : req.get("items")) {
                UUID id = UUID.fromString(item.get("edek_id").asText());
                if (failHttp.contains(id)) {
                    status = 500;
                }
                Dek dek = deks.get(id);
                ObjectNode out = items.addObject()
                        .put("key", item.get("key").asText())
                        .put("edek_id", id.toString());
                if (dek == null) {
                    out.put("status", "error").putNull("wrapped_dek_b64").putNull("owner_app_id")
                            .put("detail", "EDEK not found or not authorized");
                } else {
                    out.put("status", "success")
                            .put("wrapped_dek_b64", Base64.getEncoder().encodeToString(
                                    TransportWrapper.wrap(dek.raw(), serviceTransportKey)))
                            .put("owner_app_id", dek.owner())
                            .putNull("detail");
                }
            }
            byte[] body = status == 200
                    ? MAPPER.writeValueAsBytes(MAPPER.createObjectNode().set("items", items))
                    : "{\"detail\":\"boom\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
