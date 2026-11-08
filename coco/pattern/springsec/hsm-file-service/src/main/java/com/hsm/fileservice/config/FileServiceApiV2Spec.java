package com.hsm.fileservice.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The API v2 OpenAPI document, derived from the generated v1 document. The v2 endpoint
 * (hsm.file-service.server.api-v2-prefix) is the same handler -- same path, parameters,
 * headers and status codes -- and differs only in the error body's field names
 * (ErrorResponseV2: errorCode, requestId). Deriving it, rather than asking springdoc for a
 * second document, keeps /actuator/openapi exactly as published: springdoc serves grouped
 * documents on the management port only under /actuator/openapi/{group}.
 *
 * <p>Works on the plain JSON tree (maps and lists), so the result is exactly what a client
 * reads. The committed copy is helm/hsm-file-service/openapi-v2.yaml (OpenApiContractTest).
 */
public final class FileServiceApiV2Spec {

    public static final String VERSION = "2.0.0";
    static final String NOTE = "API v2: the same endpoint and file bytes as v1; the error body is camelCase "
            + "(errorCode, message, requestId).";

    private FileServiceApiV2Spec() {
    }

    /** A new document; {@code v1} is not modified. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> fromV1(Map<String, Object> v1, String v2Prefix) {
        Map<String, Object> v2 = (Map<String, Object>) copy(v1);

        Map<String, Object> info = (Map<String, Object>) v2.get("info");
        info.put("version", VERSION);
        info.put("description", String.valueOf(info.get("description")).strip() + "\n\n" + NOTE);

        for (Map<String, Object> server : (List<Map<String, Object>>) v2.getOrDefault("servers", List.of())) {
            Map<String, Map<String, Object>> variables = (Map<String, Map<String, Object>>) server.get("variables");
            if (variables != null && variables.containsKey("apiPrefix")) {
                variables.get("apiPrefix").put("default", v2Prefix);
                variables.get("apiPrefix").put("description", "This deployment's API v2 prefix (chart "
                        + "config.server.apiV2Prefix, env FILE_SERVICE_API_V2_PREFIX). Default "
                        + FileServiceProperties.Server.DEFAULT_API_V2_PREFIX + ".");
            }
        }

        Map<String, Object> schemas = (Map<String, Object>) ((Map<String, Object>) v2.get("components")).get("schemas");
        Map<String, Object> error = (Map<String, Object>) schemas.get(FileServiceOpenApiConfig.ERROR_SCHEMA);
        Map<String, Object> renamed = new LinkedHashMap<>();
        ((Map<String, Object>) error.get("properties")).forEach((name, property) -> renamed.put(camelCase(name), property));
        error.put("properties", renamed);
        if (error.get("required") instanceof List<?> required) {
            error.put("required", required.stream().map(name -> camelCase((String) name)).toList());
        }
        return v2;
    }

    static String camelCase(String snake) {
        StringBuilder out = new StringBuilder(snake.length());
        boolean upper = false;
        for (char c : snake.toCharArray()) {
            if (c == '_') {
                upper = true;
            } else {
                out.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return out.toString();
    }

    private static Object copy(Object node) {
        if (node instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((k, v) -> out.put((String) k, copy(v)));
            return out;
        }
        if (node instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            list.forEach(v -> out.add(copy(v)));
            return out;
        }
        return node;
    }
}
