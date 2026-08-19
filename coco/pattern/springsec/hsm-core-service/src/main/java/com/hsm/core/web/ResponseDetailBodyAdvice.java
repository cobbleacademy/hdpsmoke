package com.hsm.core.web;

import com.fasterxml.jackson.annotation.JsonView;
import com.hsm.core.dto.BatchDecryptResponse;
import com.hsm.core.dto.BatchEncryptResponse;
import com.hsm.core.dto.DecryptResponse;
import com.hsm.core.dto.EncryptResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Selects ResponseViews.Minimal vs Full per-request via the X-Response-Detail
 * header for /encrypt and /decrypt responses (single and batch) -- see
 * ResponseViews and ResponseDetailResolver.
 *
 * <p>Spring's own {@code JsonViewResponseBodyAdvice} does the same job, but
 * only for a static {@code @JsonView} annotation on the controller method --
 * it can't vary per request. It (and the natural-looking approach of
 * extending {@code AbstractMappingJacksonResponseBodyAdvice} and overriding
 * {@code beforeBodyWriteInternal}) works by wrapping the body in a
 * {@code MappingJacksonValue} and calling {@code setSerializationView} on it
 * -- but {@code AbstractMappingJacksonResponseBodyAdvice.beforeBodyWrite}
 * SKIPS that wrapping entirely (confirmed by decompiling it) whenever the
 * selected converter is the modern Jackson-3-based
 * {@code AbstractJacksonHttpMessageConverter} (this app's converter, since
 * Spring Boot 4 / Jackson 3) -- that wrapping path only still exists for
 * backward compat with the old Jackson 2 converter. So
 * {@code beforeBodyWriteInternal} never runs here, and any state it would
 * try to stash for {@link #determineWriteHints} to read is never set.
 *
 * <p>The fix: implement {@link ResponseBodyAdvice} directly (skip the
 * MappingJacksonValue detour) and resolve the view straight from the current
 * request inside {@code determineWriteHints} itself, via
 * {@link RequestContextHolder} -- that's bound for the whole synchronous
 * request-handling thread, including the message-converter write step, so
 * it's available even though {@code determineWriteHints}'s own parameters
 * don't include the request.
 *
 * <p>{@code supports()} is deliberately scoped to exactly the four response
 * types below, not left unconditional -- Jackson 3's
 * {@code MapperFeature.DEFAULT_VIEW_INCLUSION} defaults to {@code false}
 * (unlike Jackson 2's {@code true}), so having ANY view active at all hides
 * every unannotated field on whatever gets written, not just the annotated
 * ones. An unconditional {@code supports()} silently broke every other
 * response type in the app with no {@code @JsonView} annotations of its own
 * (HealthResponse, grant responses, BatchEncryptResponse's own {@code items}
 * field, ...) -- found live via the actual test suite, not by inspection.
 */
@RestControllerAdvice
public class ResponseDetailBodyAdvice implements ResponseBodyAdvice<Object> {

    private static final Set<Class<?>> VIEWED_TYPES =
            Set.of(EncryptResponse.class, DecryptResponse.class, BatchEncryptResponse.class, BatchDecryptResponse.class);

    @Override
    public boolean supports(MethodParameter returnType, Class<? extends HttpMessageConverter<?>> converterType) {
        Class<?> type = returnType.getParameterType();
        // /encrypt returns ResponseEntity<EncryptResponse> -- unwrap the generic body
        // type rather than matching on the ResponseEntity wrapper itself.
        if (ResponseEntity.class.isAssignableFrom(type)) {
            Type generic = returnType.getGenericParameterType();
            if (generic instanceof ParameterizedType parameterized
                    && parameterized.getActualTypeArguments().length == 1
                    && parameterized.getActualTypeArguments()[0] instanceof Class<?> bodyType) {
                type = bodyType;
            }
        }
        return VIEWED_TYPES.contains(type);
    }

    @Override
    public Object beforeBodyWrite(Object body, MethodParameter returnType, MediaType selectedContentType,
                                   Class<? extends HttpMessageConverter<?>> selectedConverterType,
                                   ServerHttpRequest request, ServerHttpResponse response) {
        return body;
    }

    @Override
    public Map<String, Object> determineWriteHints(Object body, MethodParameter returnType, MediaType selectedContentType,
                                                     Class<? extends HttpMessageConverter<?>> selectedConverterType) {
        HttpServletRequest servletRequest =
                ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getRequest();
        Class<?> view = ResponseDetailResolver.resolve(servletRequest);
        return Collections.singletonMap(JsonView.class.getName(), view);
    }
}
