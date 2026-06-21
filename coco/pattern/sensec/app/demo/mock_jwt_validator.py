"""
Stand-in JWT validator for DEMO MODE ONLY.

Real deployments validate RS256-signed tokens against an identity provider
(see app.auth.jwt_validator.JWTValidator). For the demo, callers present a
plain bearer string from DEMO_TOKENS below — there is no signature, no
expiry, no issuer check. This exists purely so the UI can illustrate the
auth flow without standing up Azure AD.
"""

from __future__ import annotations

from app.auth.jwt_validator import TokenValidationError

DEMO_TOKENS: dict[str, dict[str, str]] = {
    "demo-token-payments-svc": {"sub": "demo-user-1", "app_id": "payments-svc"},
    "demo-token-reporting-app": {"sub": "demo-user-2", "app_id": "reporting-app"},
    "demo-token-ops-admin": {"sub": "demo-user-3", "app_id": "ops-admin"},
}

DEMO_SCOPES: dict[str, list[str]] = {
    "payments-svc": ["encrypt", "decrypt"],
    "reporting-app": ["decrypt"],
    "ops-admin": ["encrypt", "decrypt", "rotate", "grant", "manage_apps"],
}

# Seeded at startup: reporting-app may decrypt anything payments-svc encrypted.
DEMO_GRANTS: list[tuple[str, str]] = [
    ("reporting-app", "payments-svc"),
]


class MockJWTValidator:
    def validate(self, token: str) -> dict[str, str]:
        claims = DEMO_TOKENS.get(token)
        if claims is None:
            raise TokenValidationError("Unknown demo token")
        return claims
