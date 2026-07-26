package com.hsm.core.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

/**
 * Loads the PlainID PBAC integration config from a JSON file and merges it with
 * hardcoded defaults (deep merge, override wins). If the path is empty or the file
 * is absent, returns the defaults -- the service starts fine with no file mounted.
 * Ported from app/auth/pbac_client.py's load_integration_config / _deep_merge.
 */
public final class PbacIntegrationConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(PbacIntegrationConfigLoader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String DEFAULTS_JSON = """
            {
              "endpoint_path": "/v2/isPermitted",
              "auth": {
                "header_name": "Authorization",
                "header_value_template": "Bearer {api_key}"
              },
              "request": {
                "principal_field": "principal",
                "action_field": "action",
                "resource_field": "resource",
                "context_field": "context"
              },
              "response": {
                "permitted_path": "permitted"
              },
              "resource_templates": {
                "encrypt": "hsm:encrypt:{data_classification}",
                "decrypt": "hsm:decrypt:{data_classification}"
              }
            }
            """;

    private PbacIntegrationConfigLoader() {
    }

    public static ObjectNode load(String configPath) {
        ObjectNode config;
        try {
            config = (ObjectNode) MAPPER.readTree(DEFAULTS_JSON);
        } catch (IOException e) {
            throw new IllegalStateException("Invalid embedded PBAC defaults", e);
        }

        if (configPath == null || configPath.isBlank()) {
            return config;
        }
        Path path = Path.of(configPath);
        if (!Files.exists(path)) {
            log.warn("pbac_integration_config_not_found path={}", path);
            return config;
        }
        try {
            JsonNode overrides = MAPPER.readTree(Files.readString(path));
            deepMerge(config, overrides);
            log.info("pbac_integration_config_loaded path={}", path);
        } catch (IOException e) {
            log.error("pbac_integration_config_load_failed path={} error={}", path, e.getMessage());
        }
        return config;
    }

    private static void deepMerge(ObjectNode base, JsonNode override) {
        Iterator<Map.Entry<String, JsonNode>> fields = override.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String key = entry.getKey();
            if (key.startsWith("_")) {
                continue; // comment keys
            }
            JsonNode value = entry.getValue();
            JsonNode existing = base.get(key);
            if (value.isObject() && existing != null && existing.isObject()) {
                deepMerge((ObjectNode) existing, value);
            } else {
                base.set(key, value);
            }
        }
    }

    /**
     * Navigate a nested JsonNode via a dot-notation path, e.g. "result.allowed".
     * Returns null if any segment is missing or not an object.
     */
    public static JsonNode getNested(JsonNode body, String dotPath) {
        JsonNode current = body;
        for (String segment : dotPath.split("\\.")) {
            if (current == null || !current.isObject()) {
                return null;
            }
            current = current.get(segment);
        }
        return current;
    }
}
