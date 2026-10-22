# smsf-service-mt

Consumes MT messages from Kafka with the existing priority handling, builds the SBI payload, and
delivers it to the AMF.

No listener. It reads from Kafka and Redis and calls the AMF.

---

## What it does

- Consumes `<networkId>.smsf.sms.{high,medium,low}`, three consumers per gateway.
- Applies the priority weighting from the gateway's `messages_per_second_*` values.
- Builds the MT payload: TPDU encoding and the multipart/related SBI body.
- Delivers to the AMF over HTTP/2 with mTLS, taking the AMF-audience token from Redis.
- Processes the AMF response, writes the submit result to ScyllaDB and produces delivery reports.

### What it deliberately does not do

Talk to the NRF. The token comes from Redis; this module has no NRF connection at all.

---

## Infrastructure

| Dependency | Purpose |
|---|---|
| Kafka | consumer of the three priority topics; producer of delivery reports |
| Redis | gateway registry, error-code mapping, **reads** the AMF token |
| ScyllaDB | DLR/submit-result correlation and the pending-message buffer the custom consumer keeps on shutdown |
| AMF | the MT target |
| Management back end | gateway lifecycle over STOMP, as `SMSF-SERVICE-MT` |

No NRF and no server port.

---

## How it works

### Startup

`ConnectionManager` reads the `http2_gateways` hash and starts one `MtStackManager` per enabled
gateway. Each stack connects its AMF client, creates the priority consumers, starts the dispatch
executor and the poll loop, and hands the credential's lifecycle to the shared token cache.

### Priority processing

Unchanged from before the split. Three consumers are created only for the priorities the gateway
enables — a priority with `messages_per_second_* = 0` gets no consumer at all:

```text
<networkId>.smsf.sms.high    ─┐
<networkId>.smsf.sms.medium  ─┼─▶ fetchMessages(mps, high, medium, low)   once per second
<networkId>.smsf.sms.low     ─┘         │
                                        ▼
                              dispatch executor (virtual threads)
                                        │
                                        ▼
                              MessageProcessor.processOutgoingRequest
```

`fetchMessages` weights the draw across the three consumers by the per-priority MPS, bounded by the
gateway's overall `messagesPerSecond`.

### Topics consumed

One set per gateway, created only for the priorities the gateway enables:

| Topic | Consumer group | Created when |
|---|---|---|
| `<networkId>.smsf.sms.high` | `smsc.smsf.high.message.<networkId>` | `messages_per_second_high > 0` |
| `<networkId>.smsf.sms.medium` | `smsc.smsf.medium.message.<networkId>` | `messages_per_second_medium > 0` |
| `<networkId>.smsf.sms.low` | `smsc.smsf.low.message.<networkId>` | `messages_per_second_low > 0` |

It also **produces** to `pre.deliver` for the delivery reports it generates from the AMF response.

### Throughput metrics

Two watchers per gateway, one second apart:

| Watcher | What it counts |
|---|---|
| `Process-MT-Message-NetworkId-<id>` | MT messages dispatched to the AMF |
| `Process-MT-Dlr-NetworkId-<id>` | delivery receipts dispatched to the AMF |

Both leave over the same MT path, but a receipt is not a message delivery. Counted together the MT
rate reads higher than the traffic actually carried, and a DLR storm hides inside what looks like
healthy throughput.

### Delivery

```text
MessageEvent ─▶ MessageFactory / MessageTpduProcessor  encode TPDU, build multipart/related
             ─▶ Http2ClientConnection  POST to the AMF, mTLS + bearer from the cache
             ─▶ response ─▶ submit result in ScyllaDB, CDR, delivery report to Kafka
```

Multipart messages are dispatched per part. Error mapping uses the per-MNO `error_code_mapping`
loaded from Redis and refreshed over STOMP.

### When a delivery fails

A delivery that does not succeed is never simply dropped. It goes through the same decision the
SMPP and HTTP clients make, driven by the per-gateway lists that already exist on `Http2Gateway`:

```text
                       delivery failed (transport error or non-2xx from the AMF)
                                            │
                     ┌──────────────────────┴──────────────────────┐
                     ▼                                             ▼
        validity period spent, or                          still alive
        the last retry already used                             │
                     │                          ┌───────────────┼───────────────┐
                     ▼                          ▼               ▼               ▼
              CDR FAILED, closed        no_retry_error   retry_alternate   auto_retry
                                             _code        _destination      _error_code
                                               │          _error_code            │
                                               ▼               ▼                 ▼
                                        CDR FAILED,     CDR FAILED +       CDR FAILED +
                                          closed        back to routing    back to retries
```

An error listed nowhere is not retried blindly — it is closed. The CDR always records the attempt
that failed; what goes back on Kafka is a copy with its error cleared and its retry counter
advanced, so the retries module sees a fresh attempt.

Closing a transaction is three things, not one — the same as the SIP module:

1. a **failed CDR**;
2. a **delivery receipt** to `pre.deliver`, if the originator asked for one and this is the final
   segment of a split message. The error is translated through the MNO's `ErrorCodeMapping`, so the
   originator sees the operator's status and not a raw HTTP one. An MNO with no mapping for that
   code still gets a receipt, marked `UNDELIV`;
3. a **refund** to `charging.sms.<priority>`, if the message was billed.

A message that is retried gets none of the three except the CDR: it has not failed for good yet.

This is what keeps the cycle before the watchdog notices an outage from losing messages: those
deliveries fail with `503`, and if the gateway lists `503` as auto-retry they return to
`retries.sms.<priority>` instead of disappearing.

### The consumers are gated on the token

While no usable AMF token is cached, the priority consumers are **paused** rather than sending
unauthenticated MT to the AMF, and the poll loop skips its cycle. They resume as soon as a fresh
token lands. A gateway with OAuth2 disabled is never gated — its consumers start with the stack.

Sending a message reads the bearer from memory; the Redis round trip happens in the cache's
background refresh. See [the token cache](smsf-common.md#token-cache).

### The consumers are also gated on the connection

`connect()` runs once at startup, so a link that dies later would otherwise go unnoticed: the
consumers would keep draining Kafka and every delivery would fail against a dead client. A watchdog
checks the AMF connection every `smsf.mt.amf-connection.check-interval-seconds`. On a drop it
**pauses the consumers**, so messages stay on their topics instead of being consumed and lost, logs
the outage **once** — not once per retry — and retries `connect()` at that same interval until it
takes. Recovery needs no restart.

```text
The AMF connection of gateway 10 is down — MT consumers are paused and the connection will be
retried every 5s. Messages stay on their topics until it is restored.
```

The two gates are independent and neither resumes on its own: consumption restarts only once the
credential **and** the connection are both back.

TLS 1.3 makes this worth having even at startup. The client finishes its handshake before the AMF
has judged its certificate, so a rejected client certificate looks like a successful `connect()` and
the channel dies moments later. The watchdog is what turns that into a logged, recovering outage.

### mTLS towards the AMF

The client certificate, private key and truststore come from the `outbound_security` block of the
`Http2Gateway` entry. That block still carries the certificates; what this module no longer reads
from it is the OAuth2 credentials, which only `smsf-core` uses.

---

### It stays up with no gateway configured

The module is a pure Kafka consumer: it opens no HTTP port, so it owns no non-daemon thread of its
own, and with no gateway in Redis there is no listener container either. Left alone it would exit a
few milliseconds after starting. `spring.main.keep-alive=true` keeps the process running, idle,
until a gateway arrives over the management websocket. The other SMSC modules get this for free from
their embedded HTTP server; these two must not open a port merely to stay alive.

## Properties

Environment variable names are the ones the image accepts (`docker/mt/mapping.conf`).

### Application

| Property | Env var | Default | Description |
|---|---|---|---|
| `spring.application.name` | `SPRING_APPLICATION_NAME` | `smsf-service-mt` | Application name in logs |
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
| `kafka.consumer.max-poll-records` | `KAFKA_MAX_POLL_RECORDS` | `100` | Records per poll |

The delivery rate is **not** set here: it comes from the gateway's `messages_per_second` and
`messages_per_second_{high,medium,low}` in Redis.

### ScyllaDB

| Property | Env var | Default | Description |
|---|---|---|---|
| `scylla.contact.points` | `SCYLLA_CONTACT_POINTS` | `127.0.0.1:9042` | Contact points |
| `scylla.datacenter` | `SCYLLA_DATACENTER` | `datacenter1` | Local datacenter for the driver's load balancing |
| `scylla.user` | `SCYLLA_USER` | *(empty)* | User; empty for no auth |
| `scylla.password` | `SCYLLA_PASSWORD` | *(empty)* | Password |

Used for the submit-result/DLR correlation and the consumer's pending buffer. The UE SMS context
tables belong to `smsf-core` and are not created here.

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

### Token

The token is minted by `smsf-core` and read from Redis. See
[the token cache](smsf-common.md#token-cache) for the refresh mechanism.

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.security.outbound-token.refresh-margin-seconds` | `SMSF_SECURITY_OUTBOUND_TOKEN_REFRESH_MARGIN_SECONDS` | `30` | Treat the token as unusable this long before expiry. **Must match the core's value** |
| `smsf.security.outbound-token.redis-check-interval-seconds` | `SMSF_SECURITY_OUTBOUND_TOKEN_REDIS_CHECK_INTERVAL_SECONDS` | `10` | Recovery polling while waiting for a first or a replacement token. Also how long paused consumers take to resume |
| `smsf.security.outbound-token.stale-poll-interval-ms` | `SMSF_SECURITY_OUTBOUND_TOKEN_STALE_POLL_INTERVAL_MS` | `100` | How often Redis is re-read once the rotation is due but has not landed |

### AMF connection

| Property | Env var | Default | Description |
|---|---|---|---|
| `smsf.mt.amf-connection.check-interval-seconds` | `SMSF_MT_AMF_CONNECTION_CHECK_INTERVAL_SECONDS` | `5` | How often the AMF link is checked once the stack is running. On a drop it is also the retry cadence, so it sets how fast delivery recovers |

### JVM and JMX (image only)

| Env var | Default | Description |
|---|---|---|
| `JVM_XMS` / `JVM_XMX` | `-Xms128m` / `-Xmx256m` | Heap. The reference compose raises these to 512m/1024m |
| `ENABLE_JMX` | `false` | Enable remote JMX |
| `IP_JMX` | — | Required when JMX is on |
| `JMX_PORT` | `9010` | JMX port; `9073` in the reference compose |
| `SMSF_TRACE_LEVEL` | `INFO` | Root log level. Logs go to stdout |
