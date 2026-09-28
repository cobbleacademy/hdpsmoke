package com.hsm.cachekeyrotator;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.util.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression guard for the HTTP/Netty stack. The Azure SDK must use the JDK HTTP client
 * (azure-core-http-jdk-httpclient), with reactor-netty and azure-core-http-netty kept off
 * the classpath: that pairing was built for a different Netty line than the rest of the
 * build and failed at runtime with NoClassDefFoundError:
 * io/netty/channel/MultiThreadIoEventLoopGroup -- on every real Azure call, while every
 * demo-mode test still passed. Each probe below makes a real call to a closed local port:
 * a connect failure proves the stack links; a LinkageError anywhere in the cause chain
 * is the bug coming back.
 */
class HttpStackTest {

    static void assertFailsOnlyToConnect(Runnable call) {
        Throwable t = assertThrows(Throwable.class, call::run);
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof LinkageError) {
                throw new AssertionError("client failed to link, not to connect: " + c, t);
            }
        }
    }

    static void assertAbsent(String className) {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(className),
                className + " must not be on the classpath");
    }

    @Test
    void azureSdkUsesTheJdkHttpClient_andItLinks() {
        HttpClient client = HttpClient.createDefault();
        assertEquals("com.azure.core.http.jdk.httpclient.JdkHttpClient", client.getClass().getName());
        assertFailsOnlyToConnect(() -> client.sendSync(
                new HttpRequest(HttpMethod.GET, "http://127.0.0.1:9/"), Context.NONE).close());
    }

    @Test
    void nettyBasedAzureTransportIsAbsent() {
        assertAbsent("com.azure.core.http.netty.NettyAsyncHttpClient");
        assertAbsent("reactor.netty.http.client.HttpClient");
    }

    @Test
    void lettuceLinks() {
        // Redis client (core's DEK cache, hsm-cache-key-rotator's RedisOps): Netty-based, so it
        // must link against the single Netty version the parent pom pins.
        io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create("redis://127.0.0.1:9");
        try {
            assertFailsOnlyToConnect(() -> client.connect().close());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void exactlyOneNettyVersionOnTheClasspath() {
        java.util.Set<String> versions = new java.util.TreeSet<>();
        io.netty.util.Version.identify().values().forEach(v -> versions.add(v.artifactVersion()));
        assertEquals(1, versions.size(), "mixed Netty versions: " + io.netty.util.Version.identify());
    }
}
