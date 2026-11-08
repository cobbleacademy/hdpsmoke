package com.hsm.core.web;

import com.hsm.core.config.HsmProperties;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverters;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.deser.DeserializationProblemHandler;
import tools.jackson.databind.json.JsonMapper;

/**
 * One JSON wire format per API version, from the same DTOs:
 * <ul>
 *   <li><b>v1</b> ({@code hsm.service.api-v1-prefix}): snake_case, exactly as before.
 *       Unknown request fields are still ignored, but each one is now logged
 *       ({@code v1_unknown_field}) so callers sending misspelled or camelCase fields can
 *       be found and fixed before v1 starts rejecting them.</li>
 *   <li><b>v2</b> ({@code hsm.service.api-v2-prefix}): camelCase -- the Java field names
 *       themselves -- and unknown request fields are rejected (400, GlobalExceptionHandler)
 *       instead of silently dropped: a camelCase/snake_case mix-up must fail loudly, not
 *       turn into a null edekId or a missing audit field.</li>
 * </ul>
 *
 * <p>Both mappers are copies of Spring Boot's configured {@link JsonMapper} (spring.jackson.*)
 * with just those changes, so every other setting stays identical between versions. Each
 * converter only claims requests under its own prefix and is registered ahead of Boot's
 * converter, which keeps handling everything else (demo endpoints, Boot's error page) and
 * is itself unchanged. Per-request {@code @JsonView} selection (ResponseDetailBodyAdvice)
 * works unchanged: it hands its view to whichever Jackson converter writes the body.
 */
@Configuration
public class ApiVersionJsonConfig implements WebMvcConfigurer {

    private static final Logger log = LoggerFactory.getLogger(ApiVersionJsonConfig.class);

    private final JsonMapper bootMapper;
    private final HsmProperties properties;

    public ApiVersionJsonConfig(JsonMapper bootMapper, HsmProperties properties) {
        this.bootMapper = bootMapper;
        this.properties = properties;
    }

    @Override
    public void configureMessageConverters(HttpMessageConverters.ServerBuilder builder) {
        builder.addCustomConverter(new PrefixScopedJsonConverter(v2Mapper(bootMapper), properties.service().apiV2Prefix()));
        builder.addCustomConverter(new PrefixScopedJsonConverter(v1Mapper(bootMapper), properties.service().apiV1Prefix()));
    }

    static JsonMapper v1Mapper(JsonMapper bootMapper) {
        return bootMapper.rebuild()
                .addHandler(new UnknownFieldLogger())
                .build();
    }

    static JsonMapper v2Mapper(JsonMapper bootMapper) {
        return bootMapper.rebuild()
                .propertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)
                .clearProblemHandlers()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

    /** A Jackson converter that only reads/writes bodies of requests under one API prefix. */
    static final class PrefixScopedJsonConverter extends JacksonJsonHttpMessageConverter {

        private final String prefix;

        PrefixScopedJsonConverter(JsonMapper mapper, String prefix) {
            super(mapper);
            this.prefix = prefix + "/";
        }

        /**
         * Registered ahead of Spring's String/byte[] converters, so it must leave those
         * types alone -- otherwise an already-serialized body (springdoc's spec, served
         * under the v1 prefix) would be written again as one JSON string.
         */
        private boolean inScope(Class<?> clazz) {
            if (clazz == null || CharSequence.class.isAssignableFrom(clazz) || clazz == byte[].class) {
                return false;
            }
            HttpServletRequest request = currentRequest();
            return request != null && request.getRequestURI().startsWith(prefix);
        }

        @Override
        public boolean canRead(ResolvableType type, MediaType mediaType) {
            return inScope(type.toClass()) && super.canRead(type, mediaType);
        }

        @Override
        public boolean canRead(Class<?> clazz, MediaType mediaType) {
            return inScope(clazz) && super.canRead(clazz, mediaType);
        }

        @Override
        public boolean canWrite(ResolvableType type, Class<?> clazz, MediaType mediaType) {
            return inScope(clazz) && super.canWrite(type, clazz, mediaType);
        }

        @Override
        public boolean canWrite(Class<?> clazz, MediaType mediaType) {
            return inScope(clazz) && super.canWrite(clazz, mediaType);
        }
    }

    /** Skips an unknown field exactly as before (v1 behavior unchanged) after logging it. */
    static final class UnknownFieldLogger extends DeserializationProblemHandler {
        @Override
        public boolean handleUnknownProperty(DeserializationContext ctxt, JsonParser p, ValueDeserializer<?> deserializer,
                                             Object beanOrClass, String propertyName) {
            HttpServletRequest request = currentRequest();
            Object target = beanOrClass instanceof Class<?> c ? c.getSimpleName() : beanOrClass.getClass().getSimpleName();
            log.warn("v1_unknown_field app_id={} path={} field={} type={}",
                    callerAppId(), request != null ? request.getRequestURI() : null, propertyName, target);
            p.skipChildren();
            return true;
        }
    }

    private static HttpServletRequest currentRequest() {
        return RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes
                ? attributes.getRequest() : null;
    }

    private static String callerAppId() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication != null && authentication.getPrincipal() instanceof AuthenticatedCaller caller
                ? caller.appId() : null;
    }
}
