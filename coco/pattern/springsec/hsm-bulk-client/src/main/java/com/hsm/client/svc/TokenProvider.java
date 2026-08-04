package com.hsm.client.svc;

/**
 * Supplies the bearer token SvcClient sends on every /dek/issue or /dek/unwrap
 * call. Called fresh before each request, not cached by SvcClient itself --
 * implementations decide their own caching (see AzureAdTokenProvider).
 */
public interface TokenProvider {
    String getBearerToken();
}
