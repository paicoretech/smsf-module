# smsf-n21-registration

Consumes the registration events `smsf-core` publishes and forwards them to the UDM over N21
(Nudm_UECM, TS 29.503).

No listener, no database. It reads from Kafka and Redis, discovers the UDM through the NRF, and
calls the UDM.

---

## What it does

- Consumes `<networkId>.smsf.udm.register`, one consumer per gateway.
- Forwards `REGISTER` and `DEREGISTER` to the UDM, honouring the gateway's
  `forward_registration_enabled` flag.
- Discovers the UDM through the NRF, optionally subscriber-aware so the NRF returns the instance
  serving that UE.
- Takes the UDM-audience access token from Redis. It never opens its own OAuth2 flow.

### What it deliberately does not do

Persist anything. The UE SMS context is written by `smsf-core` before the event is published, so
this module is a pure forwarder — if it is down, the AMF is still answered and the events wait in
Kafka.

---

## Infrastructure

| Dependency | Purpose |
|---|---|
| Kafka | consumer of the registration topic |
| Redis | gateway registry, **reads** the UDM token |
| NRF | UDM discovery only. The token comes from Redis, but discovery cannot |
| UDM | the N21 target |
| Management back end | gateway lifecycle over STOMP, as `SMSF-N21-REGISTRATION` |

No ScyllaDB and no server port.

---

## How it works

### Startup

`ConnectionManager` reads the `http2_gateways` hash and starts one `N21StackManager` per enabled
gateway. Each stack connects to the NRF, builds its `N21Client`, and then hands the credential's
lifecycle to the shared token cache.

### The consumer is gated on the token

A gateway that requires OAuth2 towards the UDM **does not start its consumer until a usable token is
cached**. Consuming events it cannot forward would take them off the topic and drop them; leaving
them in Kafka means nothing is lost.

```text
no token in Redis  ─▶ consumer down, error logged
token published    ─▶ consumer started
token lost         ─▶ consumer stopped again, error logged
```

The error names the gateway and the Redis key, because a token that never arrives is an `smsf-core`
or NRF problem, not a problem in this module:

```text
Gateway 10 requires authentication towards the UDM but no usable token is available
(Redis key smsf:token:10:UDM) — the registration consumer is not started
```

Recovery is automatic: the cache keeps checking and starts the consumer as soon as the token lands,
so no restart is needed. A gateway with OAuth2 disabled is never gated — its consumer starts with
the stack.

### Forwarding

```text
Kafka ──SmsfRegistrationEvent──▶ onRegistrationEvent
                                    │
                                    ├─ malformed / no action / no SUPI ─▶ discarded, logged
                                    ├─ REGISTER without ueContext      ─▶ discarded, logged
                                    ├─ REGISTER ─▶ N21Client.registerSubscriber
                                    └─ DEREGISTER ─▶ N21Client.deregisterSubscriber
```

The UDM response is logged with its status; a rejection reports the `ProblemDetails` detail when
present and the raw body otherwise. A failing call is swallowed — one bad registration must not stop
the consumer.

### Client restart

If the UDM was down when the gateway started, the client is retried at most once every
`smsf.n21.restart-min-interval-seconds` on the next forward attempt, rather than being built only at
startup and never again.

### Consumer implementation

This module uses a plain Spring `ConcurrentMessageListenerContainer` per gateway rather than the
platform's `KafkaConsumerCustomImpl`. The custom consumer requires a `ScyllaManager` to buffer
pending messages, which would drag ScyllaDB into a module that otherwise persists nothing.
Registration events carry no MPS throttling, so standard offset-commit redelivery is enough.

---

### It stays up with no gateway configured

The module is a pure Kafka consumer: it opens no HTTP port, so it owns no non-daemon thread of its
own, and with no gateway in Redis there is no listener container either. Left alone it would exit a
few milliseconds after starting. `spring.main.keep-alive=true` keeps the process running, idle,
until a gateway arrives over the management websocket. The other SMSC modules get this for free from
their embedded HTTP server; these two must not open a port merely to stay alive.

## Properties

Environment variable names are the ones the image accepts (`docker/n21/mapping.conf`).

### Application

| Property | Env var | Default | Description |
|---|---|---|---|
| `spring.application.name` | `SPRING_APPLICATION_NAME` | `smsf-n21-registration` | Application name in logs |
| `spring.main.web-application-type` | — | `none` | No listener. Not exposed as a variable on purpose |

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
| `redis.connection.user` | `REDIS_CONNECTION_USER` | *(empty)* | ACL user |
| `redis.connection.password` | `REDIS_CONNECTION_PASSWORD` | *(empty)* | Password |

### Kafka

| Property | Env var | Default | Description |
|---|---|---|---|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers |
| `spring.kafka.listener.concurrency` | `KAFKA_LISTENER_CONCURRENCY` | `1` | Threads per gateway container. Raising it beyond the topic's partition count has no effect |
| `kafka.consumer.register.max-poll-records` | `KAFKA_CONSUMER_REGISTER_MAX_POLL_RECORDS` | `100` | Records per poll |

### Topics consumed

| Topic | Consumer group | Key |
|---|---|---|
| `<networkId>.smsf.udm.register` | `smsc.smsf.udm.register.<networkId>` | `supi` |

One consumer per gateway. This module produces to no topic.

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

This module subscribes only to the gateway endpoints; it has no error-code mapping to reload.

### Token

The token is minted by `smsf-core` and read from Redis. See
[the token cache](smsf-common.md#token-cache) for the refresh mechanism.

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.security.outbound-token.refresh-margin-seconds` | `SMSF_SECURITY_OUTBOUND_TOKEN_REFRESH_MARGIN_SECONDS` | `30` | Treat the token as unusable this long before expiry. **Must match the core's value**: larger discards tokens the core still considers valid, smaller lets a token die in flight |
| `smsf.security.outbound-token.redis-check-interval-seconds` | `SMSF_SECURITY_OUTBOUND_TOKEN_REDIS_CHECK_INTERVAL_SECONDS` | `10` | Recovery polling: how often Redis is checked while waiting for a first or a replacement token. Also the recovery latency of the gated consumer |
| `smsf.security.outbound-token.stale-poll-interval-ms` | `SMSF_SECURITY_OUTBOUND_TOKEN_STALE_POLL_INTERVAL_MS` | `100` | How often Redis is re-read once the rotation is due but has not landed. The window is bounded by the margin |

### N21 towards the UDM

Paths, API root, certificates and the `forward_registration_enabled` and `oauth2_enabled` flags come
from the `udm_security` block of the `Http2Gateway` entry in Redis, not from here.

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.n21.discovery.cache-ttl-seconds` | `SMSF_N21_DISCOVERY_CACHE_TTL_SECONDS` | `300` | Lifetime of a discovery result before asking the NRF again |
| `smsf.n21.discovery.subscriber-aware` | `SMSF_N21_DISCOVERY_SUBSCRIBER_AWARE` | `true` | Add supi / routing-indicator / group-id selectors so the NRF returns the UDM serving the UE |
| `smsf.n21.discovery.max-endpoints` | `SMSF_N21_DISCOVERY_MAX_ENDPOINTS` | `4` | Upper bound on UDM endpoints kept connected when different UEs resolve to different instances |
| `smsf.n21.restart-min-interval-seconds` | `SMSF_N21_RESTART_MIN_INTERVAL_SECONDS` | `30` | Minimum wait before retrying an N21 client whose startup failed |
| `smsf.n21.home-plmn.mcc` | `SMSF_N21_HOME_PLMN_MCC` | *(empty)* | Home PLMN MCC reported when the AMF sends no GUAMI. **Empty drops the registration** rather than sending an invented PLMN |
| `smsf.n21.home-plmn.mnc` | `SMSF_N21_HOME_PLMN_MNC` | *(empty)* | Home PLMN MNC, same rule |

### JVM and JMX (image only)

| Env var | Default | Description |
|---|---|---|
| `JVM_XMS` / `JVM_XMX` | `-Xms128m` / `-Xmx256m` | Heap. The reference compose uses 256m/512m |
| `ENABLE_JMX` | `false` | Enable remote JMX |
| `IP_JMX` | — | Required when JMX is on |
| `JMX_PORT` | `9010` | JMX port; `9072` in the reference compose, to avoid clashing with the other two under `network_mode: host` |
| `SMSF_TRACE_LEVEL` | `INFO` | Root log level. Logs go to stdout |
