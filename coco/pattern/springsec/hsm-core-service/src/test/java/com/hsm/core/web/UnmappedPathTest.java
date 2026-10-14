package com.hsm.core.web;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CORE-2: a request to a path nothing handles is a 404 {"detail":"Not found"} and logs no ERROR,
 * whether Spring reports it as NoResourceFoundException (under the API prefix, where the demo UI's
 * resource handler is tried) or NoHandlerFoundException (everywhere else, and every path when
 * demo mode is off). Before the fix the second kind reached the catch-all: 500 plus an ERROR line
 * with a full stack trace for every scanner hit or mistyped URL.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class UnmappedPathTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:unmapped-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    private final Logger handlerLog = (Logger) LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private final ListAppender<ILoggingEvent> lines = new ListAppender<>();

    @BeforeEach
    void captureHandlerLog() {
        lines.start();
        handlerLog.addAppender(lines);
    }

    @AfterEach
    void detach() {
        handlerLog.detachAppender(lines);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/no-such-endpoint", "/actuator/health", "/api/sensec/hsm/v1/no-such-endpoint"})
    void unknownPathIs404WithoutAnErrorLine(String path) {
        ResponseEntity<String> response = rest.getForEntity(path, String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(404);
        assertThat(response.getBody()).isEqualTo("{\"detail\":\"Not found\"}");
        assertThat(lines.list).noneMatch(e -> e.getLevel() == Level.ERROR);
    }
}
