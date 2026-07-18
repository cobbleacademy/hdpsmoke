FROM python:3.14-slim

# Create a non-root user identical in name/uid to the pattern used by the
# main HSM service Dockerfile so ops tooling can apply the same UID-based
# policies across both images.
RUN groupadd --gid 1001 cekrotator \
    && useradd --uid 1001 --gid 1001 --no-create-home --shell /sbin/nologin cekrotator

WORKDIR /app

# Copy only what this service needs — the full app/ tree is not required.
COPY pyproject.toml ./
COPY cek_rotation/ ./cek_rotation/

# Install runtime deps from pyproject.toml without dev extras.
# --no-cache-dir keeps the image lean; --no-build-isolation is fine here
# because we are not building C extensions.
RUN pip install --no-cache-dir \
        "azure-keyvault-secrets>=4.8.0" \
        "azure-identity>=1.17.0" \
        "cryptography>=42.0.0" \
        "pydantic-settings>=2.0.0" \
        "structlog>=24.0.0" \
        "redis[hiredis]>=5.0.0" \
    && pip install --no-cache-dir --no-deps .

USER cekrotator

# No EXPOSE — this service has no HTTP listener.
CMD ["python", "-m", "cek_rotation.main"]
