-- Second, independent RSA public key per app: signing_public_key_pem is used by
-- SelfSignedAppKeyJwtValidator to verify a self-issued bearer JWT (RFC 7523-style
-- client authentication -- the caller signs a short-lived assertion locally with
-- its own private key instead of renewing a token from an external IdP). NULL is
-- the deliberate legacy switch, not an unset-marker: an app with only
-- public_key_pem (the existing DEK-transport-wrap key) registered gets that same
-- key used for signature verification too -- one keypair, two purposes, for
-- callers that would rather not manage two. Modern callers register both,
-- keeping the two purposes (encryption vs. signing) on separate keys.
ALTER TABLE ${access_schema}.app_registrations ADD COLUMN signing_public_key_pem TEXT;
