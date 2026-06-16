# Custom CA certificates

Drop additional trusted CA/root certificates here as `.pem` or `.crt` files
(concatenated PEM format is fine — one file with multiple certs, or several
files).

This directory is copied into the backend image at `/app/certs/` and bind-mounted
read-only in `docker-compose.yml` so certs can be added without a rebuild.

To use them, set `NODE_EXTRA_CA_CERTS` to the path of a bundle inside this
directory, e.g.:

```
NODE_EXTRA_CA_CERTS=/app/certs/ca-bundle.pem
```

This is needed when a provider endpoint (e.g. an APIGEE gateway) presents a
certificate chain signed by an internal/corporate CA that isn't in Node's
built-in trust store — without it, calls fail with
`unable to get local issuer certificate`.

Leave `NODE_EXTRA_CA_CERTS` unset if no custom CAs are required.
