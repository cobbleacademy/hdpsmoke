# Performance testing (hsm-core-service)

Closes the open "no throughput/latency numbers exist yet" gap (see
`dev-status-seed.json`'s Backlog item). Two separate, complementary tools --
don't conflate them:

| | Ongoing observability | One-off load measurement |
|---|---|---|
| Tool | Actuator + Micrometer (not yet wired up -- see "Not built yet" below) | Gatling |
| Lives | Inside the running app, always on | External, run on demand |
| Answers | "What is this instance doing right now/over time" (request percentiles, JVM/GC health) | "If N concurrent callers hit `/encrypt`/`/decrypt` right now, what's the throughput/latency, and where's the bottleneck" |

This doc covers the Gatling side, which is built. Actuator/Micrometer is
deliberately not wired up yet -- see "Not built yet" at the bottom.

## Why Gatling, not k6

The team's own tooling here is JVM/Maven-only, and installing a new Go binary
(k6) was a real concern raised during design. Gatling is JVM-native (pulled in
as a plain Maven dependency, same as any other library), produces a
self-contained HTML report with response-time percentile charts and a
requests/sec timeline with zero extra infra, and matches this repo's existing
precedent for a JVM measurement harness
(`hsm-bulk-service`'s `BulkVsBatchBenchmark`).

## Running it locally (demo mode)

```bash
# terminal 1 -- hsm-core-service, demo mode (MockKekClient, no real Azure call)
DEMO_MODE=true java -jar hsm-core-service/target/hsm-core-service.jar

# terminal 2
hsm-core-service/scripts/run-load-test.sh
```

Or directly via Maven, from `java/`:

```bash
mvn -pl hsm-core-service gatling:test \
  -Dgatling.simulationClass=com.hsm.core.loadtest.EncryptDecryptLoadSimulation
```

The report lands at `hsm-core-service/target/gatling/<run-id>/index.html`.
`gatling-maven-plugin` is deliberately bound to no lifecycle phase (see its
`pom.xml` entry's comment) -- it never runs during `mvn test`/`mvn package`,
only when invoked explicitly like above, since it drives real HTTP traffic
against a service that has to already be running.

### Tuning

All via `-D` system properties, see `EncryptDecryptLoadSimulation`'s own
javadoc for the full list and defaults: `hsm.baseUrl`, `hsm.appId`/`hsm.token`,
`hsm.singleUsers`/`hsm.batchUsers` (concurrent virtual users per scenario),
`hsm.batchSize` (items per batch call, capped by the server's own
`hsm.service.batch-max-items`), `hsm.rampSeconds`/`hsm.holdSeconds`.

## What it measures, and what it doesn't

Demo mode runs against `MockKekClient` (no real Azure Managed HSM call) and
H2, not Postgres -- so this isolates hsm-core-service's own overhead (JSON,
Bean Validation, PBAC check, DB write/read, AOP timing) under concurrency, not
real-infra absolute throughput. A real-infra pilot against actual Managed HSM
and Postgres would still need running separately, same caveat
`BulkVsBatchBenchmark` already carries for its own numbers.

The batch scenario is also capped by `hsm.service.batch-executor-pool-size`
(default 1, i.e. fully sequential item processing inside one batch call) --
see `BULK_OPERATIONS.md`'s bounded-concurrency section before reading a low
batch-items/sec number as a Gatling-side bottleneck rather than a deliberate
server-side throttle.

## A real measurement pass (default profile, 2026-08-19)

20 single-item virtual users + 5 batch (20 items/call) virtual users, 10s
ramp, 30s hold, against demo mode on this dev machine:

| Endpoint | Requests | Mean | p50 | p95 | p99 | Max | req/s |
|---|---|---|---|---|---|---|---|
| All requests | 1,550 (0 failed) | 7 ms | 4 ms | 25 ms | 48 ms | 188 ms | 38.75 |
| `POST /encrypt` | 620 | 5 ms | 4 ms | 9 ms | 36 ms | 143 ms | 15.5 |
| `POST /decrypt` | 620 | 2 ms | 2 ms | 4 ms | 6 ms | 22 ms | 15.5 |
| `POST /encrypt/batch` (20 items) | 155 | 28 ms | 22 ms | 59 ms | 104 ms | 188 ms | 3.88 |
| `POST /decrypt/batch` (20 items) | 155 | 12 ms | 10 ms | 26 ms | 55 ms | 61 ms | 3.88 |

`/decrypt` is consistently the fastest path (no DB write, just a read + KEK
unwrap + PBAC check). Batch-encrypt's tail latency (p99 104ms for 20 items)
reflects the sequential-by-default `batch-executor-pool-size=1` throttle
below, not client-side contention. These are demo-mode-on-a-laptop numbers
(see caveat above), useful as a baseline to compare future changes against,
not a production capacity plan.

## Deployed environment

Both options below point the *same* simulation (`EncryptDecryptLoadSimulation`)
at a real deployed instance instead of `localhost:3005` -- no separate
simulation class, only different plumbing to reach it. Two things change from
the local/demo-mode run above, regardless of which option you use:

- **Real auth required.** Deployed `hsm-core-service` runs with
  `DEMO_MODE=false` and `RsaJwtValidator` (see `helm/hsm-core-service`'s
  ConfigMap: `JWT_ISSUER`/`JWT_JWKS_URL`) -- `MockJwtValidator`'s fixed
  `demo-token-*` strings don't validate here. You need a real, currently-valid
  bearer JWT for an app registered with both `encrypt` and `decrypt` scopes.
  A real Azure AD JWT is short-lived (~1h) -- keep `hsm.holdSeconds` well
  under that, or mint a fresh token right before running.
- **No Ingress exists in this repo's Helm chart** (`helm/hsm-core-service`
  has a `Service` template only, `ClusterIP` by default) -- there's no stable
  external URL to hit from outside the cluster.

### Option A: kubectl port-forward (no new infra)

`hsm-core-service/scripts/run-load-test-k8s.sh` port-forwards the deployed
Service, health-checks it, then delegates to `run-load-test.sh` with
`-Dhsm.baseUrl`/`-Dhsm.appId`/`-Dhsm.token` pointed at the tunnel:

```bash
HSM_APP_ID=<real-app-id> HSM_TOKEN=<real-jwt> \
  hsm-core-service/scripts/run-load-test-k8s.sh -n <namespace> -r <release-name> \
  -Dhsm.singleUsers=50   # any extra -D overrides pass straight through
```

`<release-name>` doubles as the Service name -- `helm.fullname` is just
`.Release.Name` (no chart-name suffix), so whatever you passed to
`helm install <release-name> helm/hsm-core-service ...` is both. Produces the
same interactive HTML report as the local run. Tradeoff: `port-forward` is a
proxying hop through the API server, so absolute latency numbers run slightly
inflated versus a real client, and your local machine's own network stack
caps how much concurrency you can actually generate -- fine for a sanity
check, not the tool for finding the service's real ceiling.

### Option B: in-cluster Kubernetes Job

`helm/hsm-core-service-loadtest-job` packages the same simulation as an image
(`java/docker/Dockerfile.hsm-core-service-loadtest`) and runs it as a one-shot
Job from inside the cluster -- no port-forward hop, more representative
numbers, higher achievable concurrency. Mirrors `helm/hsm-bulk-client-job`'s
existing one-shot-Job pattern in this repo.

```bash
# 1. Build + push the image (see the Dockerfile's own header for the command)

# 2. Create the credentials Secret (app-id + token, never a plain values.yaml string)
kubectl create secret generic loadtest-creds \
  --from-literal=app-id=<real-app-id> \
  --from-literal=token=<real-jwt> \
  --dry-run=client -o yaml -n <namespace> | kubectl apply -f -

# 3. Run it
helm install loadtest-1 helm/hsm-core-service-loadtest-job \
  --set targetSecretName=loadtest-creds \
  --set baseUrl=http://<release-name>:3005 \
  -n <namespace>

# 4. Watch it
kubectl logs -f job/loadtest-1 -n <namespace>
```

Tradeoff: this is genuinely new infra to build/push/maintain, and results are
console-only -- the container is distroless (no shell), so there's no `tar`
for `kubectl cp` to pull the HTML report out with; `kubectl logs` captures
Gatling's own live + final summary in full, but not the interactive charts.
Use Option A instead if the HTML report itself is what you need. See the
chart's own `NOTES.txt` (rendered on `helm install`) for the full command
sequence and cleanup.

### Validated (no live cluster available in this environment)

- `mvn -pl hsm-core-service -am test-compile dependency:copy-dependencies
  -DincludeScope=test -DoutputDirectory=target/lib` (the Dockerfile's build
  step) -- confirmed it resolves and produces a working flat classpath.
- Direct `java -cp .../classes:.../lib/* io.gatling.app.Gatling -s ...`
  invocation (what the container's `ENTRYPOINT` runs) -- confirmed live
  against local demo mode, including finding and fixing a real gap:
  `gatling-maven-plugin` silently adds `--add-opens
  java.base/java.lang=ALL-UNNAMED`/`--enable-native-access=ALL-UNNAMED` when
  it forks its own JVM (visible in `mvn`'s own launcher script), which a bare
  `java` invocation does not get for free -- reproduced the resulting
  `IllegalAccessException` first, then fixed it, before writing it into the
  Dockerfile's `ENTRYPOINT`.
- `helm lint`/`helm template` on `helm/hsm-core-service-loadtest-job`, both
  with and without required values set, confirming the `required(...)` guards
  on `targetSecretName`/`baseUrl` fail closed with a clear message rather than
  silently rendering an incomplete manifest.
- `hsm-core-service/scripts/run-load-test-k8s.sh`'s argument/env validation
  and its no-cluster-reachable failure path (clean error + port-forward
  process actually killed on exit, confirmed via `ps` afterward) -- the
  successful end-to-end path (`kubectl port-forward` against a real Service)
  and the Job's real cluster run are both **not** validated live, since no
  Kubernetes cluster is reachable in this environment (same limitation noted
  for every other Helm chart in this repo).

## Not built yet

Actuator + Micrometer (`spring-boot-starter-actuator`,
`micrometer-registry-prometheus`) for continuous in-app metrics -- would let
the existing `ComponentTimingAspect`/manual timers (PBAC check, KEK
wrap/unwrap, EDEK save, DEK resolution) expose real percentiles via
`/actuator/metrics` instead of only appearing as `duration_ms` log lines, and
enables a Grafana dashboard if this becomes a recurring need. Deliberately
deferred: a real dependency/attack-surface addition to a service that's
otherwise been kept minimal, worth doing once there's an actual ongoing
monitoring need, not preemptively for a single measurement pass.
