package com.paicbd.smsc.smsf.common.redis;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory view of the NRF tokens {@code smsf-core} publishes to Redis, so the modules that send
 * requests do not hit Redis on every message.
 *
 * <p>Two maps under the same {@link TokenKey}:</p>
 * <ul>
 *   <li>{@code headerByKey} — the ready-to-use {@code Authorization} value, for the hot path of the
 *       client issuing the request.</li>
 *   <li>{@code tokenByKey} — the whole {@link SmsfSecurityToken}, so the background task can reason
 *       about {@code expiresAt}, {@code issuedAt} and whether the credential actually rotated.</li>
 * </ul>
 *
 * <h2>Refresh</h2>
 *
 * <p>Readers never trigger I/O. After each successful read the next task is scheduled for
 * {@code expiresAt - margin}, which is when {@code smsf-core} is expected to have republished.
 * That task does not blindly overwrite the cache: it compares what Redis holds with what is cached.</p>
 *
 * <ul>
 *   <li><b>The token changed</b> — the caches are replaced and the next refresh is scheduled from
 *       the new expiry.</li>
 *   <li><b>The token is unchanged</b> — the core has not rotated it yet, so the entry is re-read
 *       every {@value #STALE_POLL_INTERVAL_MS} ms until {@code expiresAt}, to pick up the new value
 *       the moment it lands.</li>
 *   <li><b>Still unchanged at {@code expiresAt}</b> — the credential is dead. The cache is cleared,
 *       an error is logged naming the gateway, and {@code onUnavailable} runs so the module stops
 *       consuming from Kafka instead of producing a stream of 401s. Polling continues at the
 *       recovery interval, and {@code onAvailable} runs as soon as a fresh token appears, so the
 *       module comes back without a restart.</li>
 * </ul>
 *
 * <p>Never log {@link SmsfSecurityToken#accessToken()}: entries are reported by key only.</p>
 */
@Slf4j
public class SmsfTokenCache implements AutoCloseable {

    /**
     * How often Redis is re-read while waiting for a rotation that is due. Short on purpose: this
     * window only opens between {@code expiresAt - margin} and {@code expiresAt}, and picking the
     * new token up promptly is what keeps traffic flowing across the rotation.
     */
    public static final long STALE_POLL_INTERVAL_MS = 100L;

    private final SmsfTokenRedisStore tokenStore;
    private final long refreshMarginSeconds;
    private final long recoveryIntervalSeconds;
    private final long stalePollIntervalMs;

    /** Hot path: the header value the sending client reads. */
    private final ConcurrentMap<TokenKey, String> headerByKey = new ConcurrentHashMap<>();
    /** Background path: the full entry, for expiry and rotation checks. */
    private final ConcurrentMap<TokenKey, SmsfSecurityToken> tokenByKey = new ConcurrentHashMap<>();

    private final ConcurrentMap<TokenKey, Registration> registrations = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SmsfTokenCache(SmsfTokenRedisStore tokenStore, long refreshMarginSeconds, long recoveryIntervalSeconds) {
        this(tokenStore, refreshMarginSeconds, recoveryIntervalSeconds, STALE_POLL_INTERVAL_MS);
    }

    public SmsfTokenCache(SmsfTokenRedisStore tokenStore, long refreshMarginSeconds,
                          long recoveryIntervalSeconds, long stalePollIntervalMs) {
        this.tokenStore = Objects.requireNonNull(tokenStore, "tokenStore");
        this.refreshMarginSeconds = Math.max(0, refreshMarginSeconds);
        this.recoveryIntervalSeconds = Math.max(1, recoveryIntervalSeconds);
        this.stalePollIntervalMs = Math.max(1, stalePollIntervalMs);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "Smsf-Token-Cache-Refresher");
            thread.setDaemon(true);
            return thread;
        });
    }

    // --- hot path -------------------------------------------------------------

    /** The {@code Authorization} header, read from memory. Empty when no usable token is cached. */
    public Optional<String> authorizationHeader(int networkId, String targetNfType) {
        return Optional.ofNullable(headerByKey.get(new TokenKey(networkId, targetNfType)));
    }

    /** The cached entry, for callers that need the expiry or the issue time. */
    public Optional<SmsfSecurityToken> token(int networkId, String targetNfType) {
        return Optional.ofNullable(tokenByKey.get(new TokenKey(networkId, targetNfType)));
    }

    /** Whether a usable credential is currently cached. */
    public boolean isAvailable(int networkId, String targetNfType) {
        return headerByKey.containsKey(new TokenKey(networkId, targetNfType));
    }

    // --- registration ---------------------------------------------------------

    /**
     * Starts tracking a gateway's credential. The first read happens immediately, so a caller that
     * gates its consumers on {@link #isAvailable} gets an answer without waiting for a poll cycle.
     *
     * @param onAvailable   run when a usable token becomes cached, after having been absent
     * @param onUnavailable run when the credential is lost — expired without replacement, or gone
     *                      from Redis. This is where the module stops consuming from Kafka.
     */
    public void register(int networkId, String targetNfType, Runnable onAvailable, Runnable onUnavailable) {
        TokenKey key = new TokenKey(networkId, targetNfType);
        Registration registration = new Registration(key,
                Objects.requireNonNullElse(onAvailable, () -> { }),
                Objects.requireNonNullElse(onUnavailable, () -> { }));
        Registration previous = registrations.put(key, registration);
        if (Objects.nonNull(previous)) {
            previous.cancel();
        }
        schedule(registration, 0, TimeUnit.MILLISECONDS);
    }

    /** Stops tracking a gateway's credential and drops it from both caches. */
    public void unregister(int networkId, String targetNfType) {
        TokenKey key = new TokenKey(networkId, targetNfType);
        Registration registration = registrations.remove(key);
        if (Objects.nonNull(registration)) {
            registration.cancel();
        }
        headerByKey.remove(key);
        tokenByKey.remove(key);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        registrations.values().forEach(Registration::cancel);
        registrations.clear();
        headerByKey.clear();
        tokenByKey.clear();
        scheduler.shutdownNow();
    }

    // --- refresh cycle --------------------------------------------------------

    private void schedule(Registration registration, long delay, TimeUnit unit) {
        if (closed.get() || !registrations.containsKey(registration.key)) {
            return;
        }
        try {
            registration.task.set(scheduler.schedule(() -> tick(registration), delay, unit));
        } catch (Exception ex) {
            // The scheduler is shutting down; nothing left to do.
            log.debug("Could not schedule the token refresh for {}: {}", registration.key, ex.getMessage());
        }
    }

    /**
     * One pass of the refresh cycle. Decides purely from what Redis holds versus what is cached, so
     * a missed or duplicated run cannot leave the cache inconsistent.
     */
    private void tick(Registration registration) {
        if (closed.get() || !registrations.containsKey(registration.key)) {
            return;
        }
        TokenKey key = registration.key;
        try {
            SmsfSecurityToken cached = tokenByKey.get(key);
            SmsfSecurityToken fromRedis = tokenStore
                    .read(key.networkId(), key.targetNfType(), refreshMarginSeconds)
                    .orElse(null);

            if (Objects.nonNull(fromRedis) && fromRedis.isDifferentFrom(cached)) {
                store(registration, fromRedis);
                return;
            }

            // Nothing usable in Redis, or the same entry the core published last time.
            if (Objects.isNull(cached) || cached.isExpired(0)) {
                markUnavailable(registration, Objects.isNull(cached));
                schedule(registration, recoveryIntervalSeconds, TimeUnit.SECONDS);
                return;
            }

            // The rotation is due but has not landed yet: watch closely until the token actually dies.
            schedule(registration, stalePollIntervalMs, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            log.error("Error refreshing the token for gateway {}: {}", key, ex.getMessage(), ex);
            schedule(registration, recoveryIntervalSeconds, TimeUnit.SECONDS);
        }
    }

    private void store(Registration registration, SmsfSecurityToken token) {
        TokenKey key = registration.key;
        headerByKey.put(key, token.authorizationHeader());
        tokenByKey.put(key, token);

        long secondsToRefresh = token.expiresAtEpochSecond() - refreshMarginSeconds
                - Instant.now().getEpochSecond();
        if (registration.available.compareAndSet(false, true)) {
            log.info("Token cached for gateway {} (issued at {}, expires at {}, refresh in {}s)",
                    key, Instant.ofEpochSecond(token.issuedAtEpochSecond()),
                    Instant.ofEpochSecond(token.expiresAtEpochSecond()), Math.max(0, secondsToRefresh));
            safely(registration.onAvailable, "onAvailable", key);
        } else {
            log.debug("Token rotated for gateway {} (expires at {}, refresh in {}s)",
                    key, Instant.ofEpochSecond(token.expiresAtEpochSecond()), Math.max(0, secondsToRefresh));
        }

        if (secondsToRefresh > 0) {
            schedule(registration, secondsToRefresh, TimeUnit.SECONDS);
        } else {
            // Already inside the refresh window: watch for the next rotation straight away.
            schedule(registration, stalePollIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private void markUnavailable(Registration registration, boolean neverSeen) {
        TokenKey key = registration.key;
        headerByKey.remove(key);
        tokenByKey.remove(key);
        if (!registration.available.compareAndSet(true, false)) {
            if (neverSeen) {
                // Cold start with nothing published yet. The consumers were never started, so there
                // is no callback to fire — but this is a blocking condition, not a transient one:
                // the module is doing no work at all until smsf-core publishes.
                log.error("OAuth2 is required for gateway {} but there is no token in Redis (key {}) — "
                                + "consumption is not started; Redis will be checked again in {}s",
                        key, SmsfTokenRedisStore.keyFor(key.networkId(), key.targetNfType()),
                        recoveryIntervalSeconds);
            }
            return;
        }
        log.error("The token of gateway {} is not being refreshed: it expired and smsf-core has not "
                        + "published a new one in Redis (key {}) — Kafka consumption is stopped for this gateway",
                key, SmsfTokenRedisStore.keyFor(key.networkId(), key.targetNfType()));
        safely(registration.onUnavailable, "onUnavailable", key);
    }

    private static void safely(Runnable callback, String name, TokenKey key) {
        try {
            callback.run();
        } catch (Exception ex) {
            log.error("The {} callback failed for gateway {}: {}", name, key, ex.getMessage(), ex);
        }
    }

    /** Bookkeeping for one tracked credential. */
    private static final class Registration {
        private final TokenKey key;
        private final Runnable onAvailable;
        private final Runnable onUnavailable;
        private final AtomicBoolean available = new AtomicBoolean(false);
        private final AtomicReference<ScheduledFuture<?>> task = new AtomicReference<>();

        private Registration(TokenKey key, Runnable onAvailable, Runnable onUnavailable) {
            this.key = key;
            this.onAvailable = onAvailable;
            this.onUnavailable = onUnavailable;
        }

        private void cancel() {
            ScheduledFuture<?> current = task.get();
            if (Objects.nonNull(current)) {
                current.cancel(false);
            }
        }
    }
}
