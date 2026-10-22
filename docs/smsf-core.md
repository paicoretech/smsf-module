# smsf-core

The SBI/API layer. It is the only module the AMF talks to, the only one that talks to the NRF, and
the only writer of the access tokens in Redis.

It runs on its own: `smsf-n21-registration` and `smsf-service-mt` are not needed for the SBI to
answer the AMF — they pick up the work it publishes.

---

## What it does

- Exposes the Nsmsf_SMService HTTP/2 SBI over TLS, with per-gateway mTLS and OAuth2 validation.
- **UE registration** (`PUT`/`DELETE /{networkId}/ue-contexts/{supi}`): persists the UE SMS context
  in ScyllaDB, answers the AMF, and publishes a registration event to Kafka. It does **not** call
  the UDM.
- **MO SMS** (`POST /{networkId}/ue-contexts/{supi}/sendsms`): decodes the TPDU, reassembles
  multipart messages, and publishes to the routing topics.
- **NRF integration**: registers the NF profile and heartbeats it, fetches the JWKS used to validate
  inbound tokens, and mints the OAuth2 tokens.
- **Token publication**: republishes every token it obtains to Redis, for both audiences.

### What it deliberately does not do

Forward registrations to the UDM, wait for the UDM, or retry against it. Those moved to
`smsf-n21-registration`. It also does not deliver MT — that is `smsf-service-mt`.

---

## Infrastructure

| Dependency | Purpose |
|---|---|
| Redis | gateway registry, error-code mapping, **writes** the token entries |
| Kafka | producer only: MO messages, delivery reports, registration events |
| ScyllaDB | UE SMS context (`smsf_ue_sms_context`) and MO multipart reassembly |
| NRF | NF registration and heartbeat, JWKS, OAuth2 token endpoint |
| AMF | inbound only — it is the client, this module is the server |
| Management back end | gateway lifecycle over STOMP, as `SMSF-CORE` |

It is the only module that **exposes a listener** (`server.port`, default 9070) and therefore the
only one that builds a server-side `SslContext`.

---

## How it works

### Startup

`ConnectionManager` reads the `http2_gateways` hash and starts one `CoreStackManager` per enabled
gateway. Each stack:

1. Connects to the NRF.
2. Starts the **AMF-audience** token refresher. A failure here aborts the gateway: the core needs
   that token as the bearer for its own NRF registration.
3. Starts the **UDM-audience** token refresher, if the gateway forwards registrations with OAuth2.
   A failure here is **not** fatal — the SBI keeps serving the AMF and only N21 is left without a
   credential.
4. Registers the NF profile against the NRF and starts the heartbeat.

`GatewaySecurityLookup` keeps the per-gateway security material, and `SmsfSslContextProvider`
rebuilds the server `SslContext` whenever a gateway is added, changed or evicted.

### Registration flow

```text
AMF ──PUT /{networkId}/ue-contexts/{supi}──▶ UeContextController
                                                │
                                                ├─▶ ScyllaDB  upsert + compute ETag
                                                ├─▶ Kafka     SmsfRegistrationEvent, key = supi
                                                └──▶ 201 Created + ETag  (or 204 No Content)
```

The HTTP contract is unchanged from before the split: `201` when the SUPI was new, `204` when it
already existed, an `ETag` computed over the persisted body, and `If-Match` honoured on `DELETE`.
That is why persistence stayed here — a core that only published to Kafka could not produce any of
it.

Publishing is best-effort in the same sense the previous in-process forwarding was: a Kafka failure
is logged and does not fail the API call, because the UE context is already stored.

`DELETE` follows the same path with `action = DEREGISTER`; the event then carries only the SUPI.

Deleting a SUPI that is not stored answers `204` all the same — an AMF retrying a `DELETE` whose
response it never saw must not start getting errors — but it publishes nothing. There is no
registration at the UDM to remove, and republishing on every retry would send the UDM a second
`DELETE`. Telling the two apart costs one read, and only when there is no `If-Match`: the
conditional path already reads the row to compare the ETag. It also avoids writing a ScyllaDB
tombstone for a row that was never there. A lightweight transaction (`DELETE ... IF EXISTS`) would
answer the same question in one statement, but it runs Paxos — far more expensive than the
key-only `SELECT supi ... WHERE supi = ?` this uses.

### Throughput metrics

| Watcher | Scope | What it counts |
|---|---|---|
| `Process-MO-Message-NetworkId-<id>` | per gateway | MO requests **published to Kafka** |
| `smsf-auth-success-per-second` | module | inbound requests whose token validated |
| `smsf-auth-failure-per-second` | module | inbound requests rejected by the security filter |

The MO watcher is owned by `CoreStackManager`, the per-gateway manager, so each tenant reports its
own rate and the watcher is released when its gateway stops — the same shape the SS7 client uses.
The counter is incremented after the publish, never on entry: under load the two diverge, and
counting on entry would report a healthy rate while nothing was leaving the module.

The auth counters are module-wide, not per gateway. The library's `SecurityAuditMetrics` callback
carries no `networkId`, so splitting them would mean changing that interface in `5g-sba-stack`.

### MO flow

```text
AMF ──POST …/sendsms──▶ MoSmsController ──▶ MoSmsService
                                              ├─ decode TPDU  (MessageTpduProcessor)
                                              ├─ status report? ─▶ validate DLR correlation
                                              │                    in ScyllaDB, publish to
                                              │                    pre.deliver
                                              ├─ multipart?    ─▶ MultiPartsHandler reassembles,
                                              │                    publishes when complete
                                              └─ otherwise     ─▶ publish to pre.message.medium
```

### Token publication

Both refreshers notify a listener on bootstrap and on every refresh; the listener writes the entry
to Redis with a TTL equal to the token's remaining lifetime. Stopping a gateway evicts both
entries, so a stopped gateway leaves no live credential behind.

The core mints the UDM token even though it never uses it: it is the only module holding the NRF
credentials. When a gateway does not forward registrations, that refresher is not started at all,
so the NRF is not asked for a token nobody reads.

---

### NRF instance identity

`nf_instance_id` is assigned by `smsc-management-be` and read from Redis with the rest of the
gateway. **The SMSF never generates one** — a locally minted UUID would register the SMSF under an
identity the platform does not know, and a new one on every restart would leave the previous profile
orphaned in the NRF until its heartbeat expired. A gateway without it does not start: the exception
names the gateway and nothing is brought up.

`nrf_register_path` configures the **service path only**:

```properties
nrf_register_path = /nnrf-nfm/v1/nf-instances
```

The instance resource of TS 29.510 is built by the code as
`<nrf_register_path>/<nf_instance_id>`, trailing slashes normalised. That is what keeps the id in
the URL and the id in the profile body the same value — the URL names the resource, the body
describes it, and the NRF requires them to agree.

It also removes a multi-gateway hazard: since the path is derived from each gateway's own
`nf_instance_id`, copying a gateway's configuration can no longer make the second one overwrite the
first one's registration.

## Properties

Environment variable names are the ones the image accepts (`docker/core/mapping.conf`).

### Application and SBI server

| Property | Env var | Default | Description |
|---|---|---|---|
| `spring.application.name` | `SPRING_APPLICATION_NAME` | `smsf-core` | Application name in logs |
| `spring.main.web-application-type` | `SPRING_MAIN_WEB_APPLICATION_TYPE` | `reactive` | Must stay `reactive`: this module serves HTTP/2 |
| `server.port` | `SERVER_PORT` | `9070` | SBI listener port |
| `server.http2.enabled` | `SERVER_HTTP2_ENABLED` | `true` | HTTP/2 on the listener |
| `smsf.sbi.server.base-path` | `SMSF_SBI_SERVER_BASE_PATH` | `/nsmsf-sms/v2` | Base path of the SBI API; also sets `spring.webflux.base-path` |
| `smsf.sbi.server.apiroot` | `SMSF_SBI_SERVER_APIROOT` | `https://api.operator.com` | API root advertised to the NRF in the NF profile |
| `smsf.sbi.public-address` | `SMSF_SBI_PUBLIC_ADDRESS` | `127.0.0.1` | IPv4 address advertised in the NF profile. Must be reachable by the AMF |

### Redis

| Property | Env var | Default | Description |
|---|---|---|---|
| `redis.cluster.nodes` | `REDIS_CLUSTER_NODES` | 6 local nodes | Comma-separated `host:port`. In standalone mode only the **first** entry is used |
| `redis.standalone.enabled` | `REDIS_STANDALONE_ENABLED` | `false` | `true` connects to a single instance instead of a cluster |
| `redis.threadPool.maxTotal` | `REDIS_THREADPOOL_MAXTOTAL` | `20` | Connection pool ceiling |
| `redis.threadPool.maxIdle` | `REDIS_THREADPOOL_MAXIDLE` | `20` | Idle connections kept |
| `redis.threadPool.minIdle` | `REDIS_THREADPOOL_MINIDLE` | `1` | Idle connections pre-created |
| `redis.threadPool.blockWhenExhausted` | `REDIS_THREADPOOL_BLOCKWHENEXHAUSTED` | `true` | Block instead of failing when the pool is exhausted |
| `redis.connection.timeout` | `REDIS_CONNECTION_TIMEOUT` | `2000` | Connect timeout (ms) |
| `redis.so.timeout` | `REDIS_SO_TIMEOUT` | `2000` | Socket read timeout (ms) |
| `redis.maxAttempts` | `REDIS_MAX_ATTEMPTS` | `20` | Cluster redirection retries |
| `redis.connection.user` | `REDIS_CONNECTION_USER` | *(empty)* | ACL user; empty for password-only or no auth |
| `redis.connection.password` | `REDIS_CONNECTION_PASSWORD` | *(empty)* | Password; empty for no auth |

### Kafka

| Property | Env var | Default | Description |
|---|---|---|---|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers. Producer only in this module |

### ScyllaDB

| Property | Env var | Default | Description |
|---|---|---|---|
| `scylla.contact.points` | `SCYLLA_CONTACT_POINTS` | `127.0.0.1:9042` | Contact points |
| `scylla.datacenter` | `SCYLLA_DATACENTER` | `datacenter1` | Local datacenter for the driver's load balancing |
| `scylla.user` | `SCYLLA_USER` | *(empty)* | User; empty for no auth |
| `scylla.password` | `SCYLLA_PASSWORD` | *(empty)* | Password |

The UE SMS context tables are created at startup by this module. The other modules do not create
them.

### Management websocket

| Property | Env var | Default | Description |
|---|---|---|---|
| `websocket.server.enabled` | `WEBSOCKET_SERVER_ENABLED` | `true` | Connect to the management back end |
| `websocket.server.host` | `WEBSOCKET_SERVER_HOST` | `localhost` | Host |
| `websocket.server.port` | `WEBSOCKET_SERVER_PORT` | `8081` | Port |
| `websocket.server.path` | `WEBSOCKET_SERVER_PATH` | `/ws` | STOMP endpoint path |
| `websocket.retry.intervalSeconds` | `WEBSOCKET_RETRY_INTERVAL_SECONDS` | `10` | Reconnection interval |
| `websocket.header.name` | `WEBSOCKET_HEADER_NAME` | `Authorization` | Auth header name |
| `websocket.header.value` | `WEBSOCKET_HEADER_VALUE` | *(token)* | Auth header value |

### Inbound security (SBI)

Validates the tokens the AMF presents. Per-gateway material (issuer, JWKS path, certificates) comes
from the `Http2Gateway` entry in Redis; these are the global settings.

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.security.enabled` | `SMSF_SECURITY_ENABLED` | `true` | Master switch for the SBI security filter |
| `smsf.security.bypass-paths` | `SMSF_SECURITY_BYPASS_PATHS` | `/actuator/**,/health,/info` | Paths served without authentication |
| `smsf.security.expected-scope` | `SMSF_SECURITY_EXPECTED_SCOPE` | `nsmsf-sms` | Scope required in the inbound token |
| `smsf.security.allowed-jws-algorithms` | `SMSF_SECURITY_ALLOWED_JWS_ALGORITHMS` | `RS256,ES256` | Accepted signature algorithms |
| `smsf.security.clock-skew-seconds` | `SMSF_SECURITY_CLOCK_SKEW_SECONDS` | `30` | Tolerance when validating `exp`/`nbf` |
| `smsf.security.log-success` | `SMSF_SECURITY_LOG_SUCCESS` | `false` | Log successful authentications too |
| `smsf.security.jwks.cache-ttl-seconds` | `SMSF_SECURITY_JWKS_CACHE_TTL_SECONDS` | `600` | JWKS cache lifetime |
| `smsf.security.jwks.cache-max-size` | `SMSF_SECURITY_JWKS_CACHE_MAX_SIZE` | `100` | Max cached keys |
| `smsf.security.jwks.fetch-timeout-ms` | `SMSF_SECURITY_JWKS_FETCH_TIMEOUT_MS` | `5000` | JWKS fetch timeout |
| `smsf.security.jwks.fetch-retry-max` | `SMSF_SECURITY_JWKS_FETCH_RETRY_MAX` | `2` | JWKS fetch retries |
| `smsf.security.bearer.header-name` | `SMSF_SECURITY_BEARER_HEADER_NAME` | `Authorization` | Header the bearer is read from |
| `smsf.security.bearer.scheme` | `SMSF_SECURITY_BEARER_SCHEME` | `Bearer` | Expected scheme |

### TLS

Only this module exposes a listener. The server certificate and the trust material come from the
`incoming_security` block of the `Http2Gateway` entries; these are the global TLS settings.

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.tls.enabled` | `SMSF_TLS_ENABLED` | `true` | `true` HTTP/2 over TLS per TS 33.501 §13.1; `false` cleartext H2C, dev only |
| `smsf.tls.server.key-password` | `SMSF_TLS_SERVER_KEY_PASSWORD` | *(empty)* | Password of the server private key |
| `smsf.tls.protocols` | `SMSF_TLS_PROTOCOLS` | `TLSv1.3,TLSv1.2` | Enabled protocols |
| `smsf.tls.session-cache-size` | `SMSF_TLS_SESSION_CACHE_SIZE` | `1024` | TLS session cache |
| `smsf.tls.handshake-timeout-ms` | `SMSF_TLS_HANDSHAKE_TIMEOUT_MS` | `10000` | Handshake timeout |

### Outbound OAuth2

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.security.outbound-token.refresh-margin-seconds` | `SMSF_SECURITY_OUTBOUND_TOKEN_REFRESH_MARGIN_SECONDS` | `30` | Refresh this long before expiry. **Must match the value in the other two modules** |
| `smsf.security.outbound-token.initial-fetch-timeout-seconds` | `SMSF_SECURITY_OUTBOUND_TOKEN_INITIAL_FETCH_TIMEOUT_SECONDS` | `10` | Timeout of the bootstrap fetch |
| `smsf.security.outbound-token.retry-backoff-seconds` | `SMSF_SECURITY_OUTBOUND_TOKEN_RETRY_BACKOFF_SECONDS` | `5` | Wait before retrying a failed refresh |

### NRF registration

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.nrf.api-full-version` | `SMSF_NRF_API_FULL_VERSION` | `2.0.0` | Service version in the NF profile |
| `smsf.nrf.heartbeat-default-seconds` | `SMSF_NRF_HEARTBEAT_DEFAULT_SECONDS` | `60` | Heartbeat used when the NRF returns none |
| `smsf.nrf.initial-register-timeout-seconds` | `SMSF_NRF_INITIAL_REGISTER_TIMEOUT_SECONDS` | `10` | Timeout of the initial registration |
| `smsf.nrf.retry-backoff-seconds` | `SMSF_NRF_RETRY_BACKOFF_SECONDS` | `5` | Wait before retrying registration |

### N21 audience

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.n21.oauth2.scope` | `SMSF_N21_OAUTH2_SCOPE` | `nudm-uecm` | Scope requested for the **UDM-audience** token. The forwarding lives in `smsf-n21-registration`; the core only needs the scope to mint the token and publish it |

### MO multipart reassembly

| Property | Env var | Default | Description |
|---|---|---|---|
| `message.parts.ttl` | `MESSAGE_PARTS_TTL` | `30` | Seconds an incomplete multipart message is held |
| `message.parts.cache.ttl-grace-seconds` | `MESSAGE_PARTS_CACHE_TTL_GRACE_SECONDS` | `5` | Grace added before evicting from the cache |
| `message.parts.cache.max-size` | `MESSAGE_PARTS_CACHE_MAX_SIZE` | `50000` | Max concurrent partial messages |

### JVM and JMX (image only)

| Env var | Default | Description |
|---|---|---|
| `JVM_XMS` / `JVM_XMX` | `-Xms128m` / `-Xmx256m` | Heap. The reference compose raises these to 512m/1024m |
| `ENABLE_JMX` | `false` | Enable remote JMX |
| `IP_JMX` | — | Required when JMX is on |
| `JMX_PORT` | `9010` | JMX port; `9071` in the reference compose |
| `SMSF_TRACE_LEVEL` | `INFO` | Root log level. Logs go to stdout |
