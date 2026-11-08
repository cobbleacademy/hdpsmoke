# API versions (v1 and v2)

hsm-core-service and hsm-file-service each serve two versions of their HTTP API
side by side. The two versions run the **same endpoints, code and data**. Only the
JSON field naming (and a few strictness rules) differ. A ciphertext or DEK created
through one version works through the other.

| | v1 | v2 |
|---|---|---|
| hsm-core-service prefix | `/api/sensec/hsm/v1` (`API_V1_PREFIX`) | `/api/sensec/hsm/v2` (`API_V2_PREFIX`) |
| hsm-file-service prefix | `/api/sensec/file/v1` (`FILE_SERVICE_API_PREFIX`) | `/api/sensec/file/v2` (`FILE_SERVICE_API_V2_PREFIX`) |
| JSON field names | snake_case (`data_classification`, `edek_id`) | camelCase (`dataClassification`, `edekId`) |
| Unknown request field | ignored, and logged as `v1_unknown_field` | **400** `{"detail": "Unknown field 'x'"}` |
| Malformed JSON | 400 | 400 |
| Headers (`X-App-ID`, `X-Correlation-Id`, `X-Request-Id`, `X-Response-Detail`, …) | unchanged | unchanged (same names) |
| Authorization rules (`hsm.security.access-rules`) | relative to the prefix | the same rules, applied to the v2 prefix too |
| Batch correlation (`key`, per-item `status`) | unchanged | unchanged |
| core error body | `{"detail"}` | `{"detail"}` |
| file-service error body | `{"error_code","message","request_id"}` | `{"errorCode","message","requestId"}` |
| File bytes and `X-HSM-*` headers (file-service) | unchanged | identical |

v2 exists because the target API gateway (Apigee) standard requires camelCase. v1 stays
until every caller has moved (see [Retiring v1](#retiring-v1)).

## Specs

| Service | v1 | v2 |
|---|---|---|
| hsm-core-service, generated at runtime (when `SPRINGDOC_ENABLED=true` or the demo profile is on) | `<v1 prefix>/openapi` (`.yaml`) | `<v2 prefix>/openapi` (`.yaml`) |
| hsm-core-service, committed | — | [`docs/oas/hsm-core-service-v2.yaml`](oas/hsm-core-service-v2.yaml) (`ApiV2SpecTest` fails if it drifts) |
| hsm-file-service, runtime | `/actuator/openapi` (management port); docs profile: `<v1 prefix>/openapi` | docs profile only: `<v2 prefix>/openapi` |
| hsm-file-service, committed | `helm/hsm-file-service/openapi.yaml` | `helm/hsm-file-service/openapi-v2.yaml` (`OpenApiContractTest`) |

The file-service v2 document is derived from v1 (`FileServiceApiV2Spec`), because the
only difference is the error body. `/actuator/openapi` stays v1, exactly as published.

To regenerate a committed spec after an intentional API change, run the test with
`-Dopenapi.update=true`, review the diff, and commit it.

## Swagger UI

There is one Swagger UI page per service, still served under the v1 prefix, with a
**version dropdown** (v1, v2). Open it with:

- `<v1 prefix>/docs`, which shows v1, or
- `<v2 prefix>/docs`, which opens the same page with v2 selected.

"Try it out" sends each version's calls to that version's prefix. All URLs are relative
(hsm-chassis-web `ApiV2Docs`, enabled by `hsm.chassis.web.api-v2-prefix`), so they work
behind a gateway that rewrites prefixes. This holds as long as the external v1 and v2
paths keep the same layout as the internal ones (by default they are siblings, `…/v1`
and `…/v2`).

## Switching a client to v2

Every client defaults to **v1**, so nothing changes until a client is switched. To switch
one, set its version to V2. Change its v2 prefix only if the service's prefix is not the
default.

| Client | Setting |
|---|---|
| hsm-crypto-client (Java) | `HsmCryptoClient.builder().apiVersion(SvcConfig.ApiVersion.V2)` (`.apiV2Prefix(...)`), or `SvcConfig.apiVersion` |
| hsm-bulk-client | `SVC_API_VERSION=V2` (`client.svc.api-version`), `SVC_API_V2_PREFIX` |
| hsm-file-service → core (outbound) | `HSM_CORE_API_VERSION=V2` (`hsm.file-service.core.api-version`), `HSM_CORE_API_V2_PREFIX` |
| Spark adapter | `spark.hsm.apiVersion V2`, `spark.hsm.apiV2Prefix` |
| hsm-databricks-udf | `HSM_API_VERSION=V2`, **and** `HSM_SERVICE_BASE_URL` ending in the v2 prefix |
| Python examples (`hsm_core_batch_file.py`, `hsm_bulk_file_reader.py`) | `HSM_CORE_API_VERSION=V2`, `HSM_CORE_API_V2_PREFIX` |
| `run_bulk_file_job.py` | `SVC_API_VERSION=V2`, `SVC_API_V2_PREFIX` |
| .NET example | `HSM_CORE_API_VERSION=V2`, `HSM_CORE_API_V2_PREFIX` (`HsmCoreClient(..., apiVersion: "V2")`) |
| Your own caller | Call the v2 prefix with the camelCase field names from the v2 spec |

Each client builds its requests the same way for both versions and renames fields only
on the wire. A client and server that disagree on the version fail loudly with a 400
naming the field. They never silently drop data.

Verified against a real hsm-core-service:

- `CoreBulkFileInteropTest` (crypto-client: a DEK issued on v2 unwraps to the same bytes on v1).
- `test_live_interop.py::test_api_v2_issue_unwraps_to_the_same_dek_via_v1` (UDF).
- The Python example's `__main__` round trip with `HSM_CORE_API_VERSION=V2`.

## Retiring v1

v1 is turned off in stages. None of them needs a code change until the last one.

1. **Announce.** Set `API_V1_DEPRECATED_AT` and `API_V1_SUNSET_AT` (ISO dates) on
   hsm-core-service. Every v1 response then carries `Deprecation` (RFC 9745), `Sunset`
   (RFC 8594) and `Link: <v2 prefix>; rel="successor-version"`. v2 responses never do.
   Both are unset by default.
2. **Find the callers still on v1.** Use the Istio access logs or Apigee analytics
   (request path per app or source), plus `v1_unknown_field` warnings, which point to
   callers sending field names that v1 silently ignores.
3. **Stop routing v1.** Remove the `/v1` route from Apigee and the Istio VirtualService
   (core), and the v1 path from the AuthorizationPolicy (file-service). Callers still
   on v1 then get a 404 at the gateway. Restoring the route undoes this.
4. **Remove v1 from the code.** In a later release, delete the v1 mappings and the
   v1 JSON converter, and make v2 the only document. This is the only irreversible step.

## Deployment (Helm and Istio)

| Chart | Settings (`values.yaml`) | Default |
|---|---|---|
| `hsm-core-service`, `hsm-bulk-service` | `config.apiV2Prefix` (→ `API_V2_PREFIX`) | `/api/sensec/hsm/v2` |
| | `config.apiV1DeprecatedAt`, `config.apiV1SunsetAt` (ISO dates; the chart refuses anything else) | empty (headers off) |
| `hsm-file-service` | `config.server.apiV2Prefix` (→ `FILE_SERVICE_API_V2_PREFIX`; must differ from `apiPrefix`) | `/api/sensec/file/v2` |
| | `config.core.apiVersion` (`V1`/`V2`), `config.core.apiV2Prefix` | `V1`, `/api/sensec/hsm/v2` |

Every pod serves both versions as soon as it runs this build, with no settings needed.

**hsm-file-service** is ready in the cluster as soon as the chart is deployed. Its
AuthorizationPolicy allows the BFF to call `<apiPrefix>/files/*` **and**
`<apiV2Prefix>/files/*`. With `docs.enabled`, it also allows the gateway to reach
`<apiV2Prefix>/docs` and `/openapi`, plus `/files/*` when `docs.tryItOut` is on.

**hsm-core-service / bulk: the platform's VirtualService needs a v2 route.** That
VirtualService is not part of these charts. It maps external prefixes onto the
internal one ([`ADMIN_OPERATIONS.md`](ADMIN_OPERATIONS.md) "Behind a path-rewriting
gateway"). v2 needs one more match per release, next to the existing v1 match:

```yaml
# Next to the existing /api/dsec/core/v1 -> /api/sensec/hsm/v1 match (core release);
# the same for bulk (/api/dsec/bulk/v2).
- match:
    - uri:
        prefix: /api/dsec/core/v2/
  rewrite:
    uri: /api/sensec/hsm/v2/
  route:
    - destination:
        host: hsm-core-service          # the same Service the v1 match uses
```

Keep the external v1 and v2 paths side by side (`…/v1`, `…/v2`). Swagger UI's v1/v2
links are relative and assume that layout. Apigee then needs the same `/v2` path
proxied to that route.

The bulk-client Job chart (`hsm-bulk-client-job`) has no core-API settings of its own.
A job's config file sets `client.svc.api-version` (or the env `SVC_API_VERSION`).

## Not done yet

- **The platform VirtualService and Apigee routes** for core/bulk `/v2` (above). Until
  they exist, core v2 can be reached in the cluster only from inside the mesh.
- **Apigee validation.** The v2 spec and the header names still need to be checked
  against the Apigee proxy. Header names are unchanged on the assumption that Apigee
  passes them through.
- **Per-version request counts** come from Istio or Apigee, not from the services.
