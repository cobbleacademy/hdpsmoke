package com.hsm.client.svc;

/** Today's default behavior, unchanged -- a fixed config value, valid for demo/mock-mode tokens which never expire (see MockJwtValidator on SVC). */
public class StaticTokenProvider implements TokenProvider {

    private final String token;

    public StaticTokenProvider(String token) {
        this.token = token;
    }

    @Override
    public String getBearerToken() {
        return token;
    }
}
