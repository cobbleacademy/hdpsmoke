package com.hsm.encryption.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import java.io.IOException;
import java.util.Map;

/**
 * Safety net only -- JwtAppIdAuthenticationFilter already handles the normal
 * "no/invalid credentials" case directly (writing its own 401 response with a
 * specific audit reason) for every path it's configured to protect. This only
 * fires if Spring Security's authorization layer somehow reaches a request with
 * no Authentication set despite that.
 */
public class HsmAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException)
            throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(MAPPER.writeValueAsString(Map.of("detail", "Bearer token required")));
    }
}
