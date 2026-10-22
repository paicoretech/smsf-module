# SMSF — documentation

The SMSF is split into three independently deployable Spring Boot applications plus one internal
library, all built from this repository as a Maven multi-module project.

| Module | Deployable | Role | Doc |
|---|---|---|---|
| `smsf-core` | yes | SBI/API towards the AMF, ScyllaDB persistence, NRF integration, publishes the token to Redis and the registration event to Kafka | [smsf-core.md](smsf-core.md) |
| `smsf-n21-registration` | yes | Consumes the registration event and forwards it to the UDM over N21 | [smsf-n21-registration.md](smsf-n21-registration.md) |
| `smsf-service-mt` | yes | Consumes the priority MT topics and delivers to the AMF | [smsf-service-mt.md](smsf-service-mt.md) |
| `smsf-common` | no | Shared code: TPDU processing, NRF connection, token cache | [smsf-common.md](smsf-common.md) |

The rationale for the split, and the decisions taken while doing it, are in
[../SMSF_Module_Separation_Guidelines.md](../SMSF_Module_Separation_Guidelines.md).

---

## Architecture

```text
                            ┌─────────┐
                            │   NRF   │  OAuth2 token · JWKS · NF discovery
                            └────┬────┘
                                 │ register · heartbeat · token
                    ┌────────────▼────────────┐
        AMF ───────▶│        smsf-core        │──────▶ ScyllaDB
     (MO, register) │  SBI · TLS · NRF · MO   │        (UE SMS context,
                    └───┬─────────────────┬───┘         MO multipart)
                        │                 │
             token per  │                 │  registration event
            (gw, audience)                │  <networkId>.smsf.udm.register
                        ▼                 ▼
                   ┌─────────┐       ┌─────────┐
                   │  Redis  │       │  Kafka  │
                   └────┬────┘       └────┬────┘
                        │                 │
          ┌─────────────┴──────┬──────────┘
          │                    │
          │ AMF token          │ UDM token + event
          ▼                    ▼
 ┌──────────────────┐  ┌────────────────────────┐
 │ smsf-service-mt  │  │ smsf-n21-registration  │
 │ priority · TPDU  │  │ NF discovery · N21     │
 └────────┬─────────┘  └───────────┬────────────┘
          ▼                        ▼
         AMF                      UDM
```

Nothing calls anything else in process. The three applications only meet through Redis and Kafka,
and each can be started, stopped and restarted on its own without affecting the others.

---

## Multi-tenancy

Every module is multi-tenant over the same gateway registry. On startup each one reads the
`http2_gateways` hash from Redis and keeps **one stack per `networkId`**, and each subscribes to the
management panel's STOMP endpoints to start and stop gateways at runtime:

| Endpoint | Effect |
|---|---|
| `/app/http2/connectGateway` | (re)read the gateway from Redis and start its stack |
| `/app/http2/stopGateway` | stop the gateway's stack |
| `/app/updateErrorCodeMapping` | reload the error-code mapping (`smsf-core` and `smsf-service-mt` only) |

They register under distinct client names so the panel can address them individually:
`SMSF-CORE`, `SMSF-N21-REGISTRATION`, `SMSF-SERVICE-MT`.

The per-gateway configuration — hosts, certificates, OAuth2 credentials, MPS per priority — lives in
the `Http2Gateway` entry in Redis, not in `application.properties`.

---

## Shared contracts

### Redis — the NRF token

`smsf-core` is the only writer. The other two only read.

```text
key    smsf:token:<networkId>:<AMF|UDM>
TTL    the token's remaining lifetime
value  {"networkId":10,"targetNfType":"UDM","accessToken":"…","tokenType":"Bearer",
        "scope":"nudm-uecm","issuedAtEpochSecond":…,"expiresAtEpochSecond":…}
```

A token is bound to the audience it was minted for: the one issued for the AMF is not valid towards
the UDM, so each gateway has two independent entries.

The consumer modules do not read Redis per message — see
[the token cache](smsf-common.md#token-cache) for how the entry is cached and refreshed.

### Redis — the gateway registry

Read by all three, written by the management back end:

| Hash | Content |
|---|---|
| `http2_gateways` | `networkId` → `Http2Gateway` JSON |
| `error_code_mapping` | `mnoId` → error-code mapping list |

### Kafka

| Topic | Producer | Consumer |
|---|---|---|
| `<networkId>.smsf.udm.register` | `smsf-core` | `smsf-n21-registration` |
| `<networkId>.smsf.sms.{high,medium,low}` | routing | `smsf-service-mt` |
| `pre.message.medium` | `smsf-core` (MO) | routing |
| `pre.deliver` | `smsf-core` (delivery reports) | routing |

The registration event is keyed by `supi`, so a REGISTER and a later DEREGISTER for the same
subscriber share a partition and cannot be reordered.

---

## External dependencies

| Service | Used by | For |
|---|---|---|
| Redis | all three | gateway registry, error-code mapping, NRF token exchange |
| Kafka | all three | MO/MT/DLR traffic and the registration event |
| ScyllaDB | `smsf-core`, `smsf-service-mt` | UE SMS context and MO multipart (core); DLR correlation and the consumer's pending buffer (mt) |
| NRF | `smsf-core`, `smsf-n21-registration` | token, JWKS and NF registration (core); UDM discovery (n21) |
| AMF | `smsf-core`, `smsf-service-mt` | inbound MO and registration (core); outbound MT (mt) |
| UDM | `smsf-n21-registration` | N21 registration forwarding |
| Management back end | all three | gateway lifecycle over STOMP |

None of them is embedded: all are external services the containers connect to.

---

## Build and deployment

```bash
mvn -B clean package                      # the four modules
docker build -f docker/core/Dockerfile --build-arg VERSION=<version> -t smsf-core:<version> .
docker build -f docker/n21/Dockerfile  --build-arg VERSION=<version> -t smsf-n21-registration:<version> .
docker build -f docker/mt/Dockerfile   --build-arg VERSION=<version> -t smsf-service-mt:<version> .
```

The build context is the repository root: each Dockerfile copies its module's fat jar and its
`application.properties`. `smsf-common` needs nothing special — it is packaged inside each fat jar
under `BOOT-INF/lib/`, like any other dependency.

Reference deployments, one per module, are in `docker/{core,n21,mt}/docker-compose.yml`. To bring
all three up on one host, merge them:

```bash
docker compose \
  -f docker/core/docker-compose.yml \
  -f docker/n21/docker-compose.yml \
  -f docker/mt/docker-compose.yml up -d
```

`release/build.xml` produces the non-containerised distributions, one zip per module
(`SMSF_CORE-<version>.zip`, `SMSF_N21_REGISTRATION-<version>.zip`, `SMSF_SERVICE_MT-<version>.zip`),
each with `bin/`, `conf/` and `logs/`.

### Configuration at runtime

The images do not take a mounted properties file. `docker/common/run.sh` overlays environment
variables on the packaged `application.properties` using the module's `docker/<module>/mapping.conf`
table, then hands over to `bin/start.sh`. Each image therefore accepts exactly the variables its
module reads; anything else is ignored.

Logging goes to stdout. `SMSF_TRACE_LEVEL` sets the root level (`INFO` when unset). Mount over
`/opt/paic/<APP>/conf/logback.xml` to change the format or add file appenders.

---

## Settings that must agree across modules

| Setting | Why |
|---|---|
| `redis.*` | they exchange the token and read the gateway registry through the same Redis |
| `redis.standalone.enabled` | pointing one at a cluster and another at a standalone instance splits them silently |
| `spring.kafka.bootstrap-servers` | same reason for the events |
| `smsf.security.outbound-token.refresh-margin-seconds` | a reader with a larger margin than the core's discards tokens the core still considers valid; a smaller one lets a token die in flight |
