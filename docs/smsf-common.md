# smsf-common

Internal library. It has no `main`, produces no Docker image and is not deployed. It exists because
some code is used by more than one module and duplicating it would let the copies diverge.

It is packaged inside each application's fat jar under `BOOT-INF/lib/smsf-common-<version>.jar`, so
the Dockerfiles do not mention it — the module jar already carries it.

## Contents

| Package | Class | Used by | Depends on |
|---|---|---|---|
| `constants` | `Constants` | all three | — |
| `http2` | `Http2HostUtils` | all three | — |
| `http2` | `Http2ClientNrfConnection` | core, n21 | sba-stack |
| `processors` | `MessageTpduProcessor`, `MessageFactory` | core (MO), mt (MT) | sba-stack, restcomm SS7 |
| `processors.dto` | `MessageRequestDecodeDataDTO` | core, mt | restcomm SS7 |
| `redis` | `SmsfSecurityToken`, `TokenKey`, `SmsfTokenRedisStore`, `SmsfTokenCache` | all three | smsc-utils |
| `redis` | `CachedTokenAuthorizationProvider` | n21, mt | sba-stack |
| `event` | `SmsfRegistrationEvent`, `RegistrationAction` | core, n21 | sba-stack (only for `UeSmsContextData`) |

The shared test fixtures (`Http2GatewayCreator`) ship as a `test-jar` the other modules depend on.

### Why the registration event lives here and not in the library

`5g-sba-stack` is developed as a standalone product that may be licensed on its own, so it must not
carry the internal contracts of any one SMSF. `SmsfRegistrationEvent` is not a 3GPP structure: it is
the Kafka contract of *this* implementation, and it exists only because this product splits the SBI
layer from the N21 forwarding. Its `networkId` field is an SMSC gateway identifier, not a 5G core
concept. It embeds `UeSmsContextData`, which is why it cannot live in `smsc-utils` either — that
library does not depend on `sba-stack`. `smsf-common` is the only place that fits.

### Why TPDU processing is shared

`MessageTpduProcessor` and `MessageFactory` handle both directions: `smsf-core` decodes incoming MO
TPDUs and `smsf-service-mt` encodes outgoing MT ones. Keeping one copy is what stops the two
directions of the same codec from drifting apart.

---

## Token cache

`SmsfTokenCache` is the read side of the Redis token exchange. Without it, every outbound request
would hit Redis — twice in the N21 case, since the stack asks the authorization provider once for the
request and again for each NF discovery lookup.

### Two maps, one key

Both keyed by `TokenKey(networkId, targetNfType)`:

| Map | Value | Read by |
|---|---|---|
| `headerByKey` | the ready-to-use `Authorization` string | the client issuing the request — hot path |
| `tokenByKey` | the whole `SmsfSecurityToken` | the background task, for `expiresAt`, `issuedAt` and rotation checks |

Reading the header is a map lookup. No Redis, no JSON parsing.

### Refresh cycle

Readers never trigger I/O. After each successful read the next task is scheduled for
`expiresAt - margin`, which is when `smsf-core` is expected to have republished. That task compares
Redis against the cache rather than overwriting blindly:

```text
                     ┌─────────────────────────────┐
                     │  read Redis at expiresAt-M  │
                     └──────────────┬──────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
        token changed        token unchanged       still unchanged
              │              (rotation pending)      at expiresAt
              │                     │                     │
     replace both maps      re-read every 100 ms    drop both maps
     reschedule from        until expiresAt         log the error
     the new expiry                                 stop Kafka consumption
                                                    poll at the recovery
                                                    interval until it returns
```

### Cold start with nothing published

On startup the consumers are **not** started: the token gate begins closed whenever OAuth2 is
required, and only the cache opens it. If `smsf-core` has published nothing yet — the NRF was
unreachable when it came up, or OAuth2 was enabled without a working token source — the module
consumes nothing and says so on every polling cycle:

```text
OAuth2 is required for gateway 10:UDM but there is no token in Redis (key smsf:token:10:UDM) —
consumption is not started; Redis will be checked again in 10s
```

It is an error, not a warning: the module is doing no work at all. No `onUnavailable` callback fires
here, because nothing was ever started to stop. Redis is re-read every
`redis-check-interval-seconds`, and the moment the token appears the consumers start on their own.

When the credential is lost after having worked the error names the gateway too, because a token
that stops rotating is an `smsf-core` or NRF problem, not a problem in the module reporting it:

```text
The token of gateway 10:UDM is not being refreshed: it expired and smsf-core has not published
a new one in Redis (key smsf:token:10:UDM) — Kafka consumption is stopped for this gateway
```

Consumption resumes on its own when a fresh token appears, so neither consumer module needs a
restart.

### Callbacks

Each module registers its own reaction to the credential appearing and disappearing:

| Module | `onAvailable` | `onUnavailable` |
|---|---|---|
| `smsf-service-mt` | resume the priority consumers | pause them |
| `smsf-n21-registration` | start the registration consumer | stop it |

### Why not Redis keyspace notifications

Considered and rejected as the primary mechanism:

- They are **off by default** — `notify-keyspace-events` is empty on a stock Redis, and on a managed
  instance enabling it may not be in your hands.
- In **cluster** mode the events are node-local and are not propagated, so a subscriber would need a
  connection per node and would have to resubscribe on every resharding or failover.
- They are **fire-and-forget**. A momentary disconnect loses the event and the module keeps serving a
  stale credential with no way to notice.

Any design based on them still needs the expiry-driven fallback, so it would add a server-side
dependency without removing the task. It remains possible as an optimisation on top of the cache, if
`notify-keyspace-events` can be guaranteed in production.

---

## Settings

The cache takes no properties of its own. It is constructed from the consuming module's settings:

| Constructor argument | Property in the consuming module |
|---|---|
| `refreshMarginSeconds` | `smsf.security.outbound-token.refresh-margin-seconds` |
| `recoveryIntervalSeconds` | `smsf.security.outbound-token.redis-check-interval-seconds` |
| `stalePollIntervalMs` | `smsf.security.outbound-token.stale-poll-interval-ms` |

`smsf-core` does not use the cache: it is the writer, and holds its tokens in the
`OutboundTokenRefresher` instances that mint them.
