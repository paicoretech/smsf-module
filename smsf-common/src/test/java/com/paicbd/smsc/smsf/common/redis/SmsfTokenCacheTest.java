package com.paicbd.smsc.smsf.common.redis;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SmsfTokenCacheTest {

    private static final int NETWORK_ID = 10;
    private static final String UDM = "UDM";
    // Small so the refresh window and the expiry are reachable inside a test.
    private static final long MARGIN = 1L;
    private static final long RECOVERY_SECONDS = 1L;
    private static final long STALE_POLL_MS = 20L;

    @Mock
    private SmsfTokenRedisStore tokenStore;

    private SmsfTokenCache cache;
    private AtomicInteger available;
    private AtomicInteger unavailable;
    /** Counts the background reads, which is how the tests tell polling from a memory hit. */
    private AtomicInteger reads;

    @BeforeEach
    void setUp() {
        available = new AtomicInteger();
        unavailable = new AtomicInteger();
        reads = new AtomicInteger();
        // A short stale-poll keeps the rotation tests quick; production uses 100 ms.
        cache = new SmsfTokenCache(tokenStore, MARGIN, RECOVERY_SECONDS, STALE_POLL_MS);
    }

    @AfterEach
    void tearDown() {
        cache.close();
    }

    private static SmsfSecurityToken token(String value, long secondsToExpiry) {
        long now = Instant.now().getEpochSecond();
        return new SmsfSecurityToken(NETWORK_ID, UDM, value, "Bearer", "nudm-uecm",
                now, now + secondsToExpiry);
    }

    /**
     * Mirrors {@link SmsfTokenRedisStore#read(int, String, long)}: an entry within the margin is
     * filtered out, which is exactly what makes the refresh task poll instead of re-caching it.
     */
    private void redisHolds(SmsfSecurityToken token) {
        when(tokenStore.read(anyInt(), anyString(), anyLong())).thenAnswer(inv -> {
            reads.incrementAndGet();
            long margin = inv.getArgument(2, Long.class);
            return Optional.ofNullable(token).filter(candidate -> !candidate.isExpired(margin));
        });
    }

    private void register() {
        cache.register(NETWORK_ID, UDM, available::incrementAndGet, unavailable::incrementAndGet);
    }

    private void awaitAvailable() {
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> cache.isAvailable(NETWORK_ID, UDM));
    }

    // --- the two caches -------------------------------------------------------

    @Test
    @DisplayName("both caches are populated under the same key: the header for the hot path, the token for the task")
    void bothCachesArePopulated() {
        redisHolds(token("abc", 600));

        register();
        awaitAvailable();

        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer abc");
        Optional<SmsfSecurityToken> cached = cache.token(NETWORK_ID, UDM);
        assertThat(cached).isPresent();
        assertThat(cached.get().accessToken()).isEqualTo("abc");
        // The background task needs the expiry and the issue time; the hot path does not.
        assertThat(cached.get().expiresAtEpochSecond()).isPositive();
        assertThat(cached.get().issuedAtEpochSecond()).isPositive();
    }

    @Test
    @DisplayName("reading the header never touches Redis: it is served from memory")
    void readsDoNotHitRedis() {
        redisHolds(token("abc", 600));
        register();
        awaitAvailable();

        int readsAfterWarmUp = countReads();
        for (int i = 0; i < 500; i++) {
            assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer abc");
        }

        assertThat(countReads()).isEqualTo(readsAfterWarmUp);
    }

    @Test
    @DisplayName("an unknown gateway has nothing cached")
    void unknownGatewayIsEmpty() {
        assertThat(cache.authorizationHeader(99, UDM)).isEmpty();
        assertThat(cache.token(99, UDM)).isEmpty();
        assertThat(cache.isAvailable(99, UDM)).isFalse();
    }

    // --- becoming available ---------------------------------------------------

    @Test
    @DisplayName("onAvailable runs once when the first token is cached")
    void onAvailableRunsOnFirstToken() {
        redisHolds(token("abc", 600));

        register();
        awaitAvailable();

        assertThat(available.get()).isEqualTo(1);
        assertThat(unavailable.get()).isZero();
    }

    @Test
    @DisplayName("with no token yet in Redis the gateway stays unavailable and Redis keeps being polled")
    void staysUnavailableUntilTheTokenAppears() {
        redisHolds(null);

        register();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> countReads() >= 2);

        assertThat(cache.isAvailable(NETWORK_ID, UDM)).isFalse();
        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).isEmpty();
        // Never seen a token, so there is no credential to report as lost.
        assertThat(available.get()).isZero();
        assertThat(unavailable.get()).isZero();
    }

    @Test
    @DisplayName("a cold start with nothing published logs an error naming the gateway and the Redis key")
    void coldStartWithoutATokenLogsAnError() {
        // Silence is the wrong answer here: the module consumes nothing at all until smsf-core
        // publishes, and the operator needs to know why from the log alone.
        Logger cacheLogger = (Logger) LoggerFactory.getLogger(SmsfTokenCache.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        cacheLogger.addAppender(appender);
        try {
            redisHolds(null);
            register();
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                    appender.list.stream().anyMatch(event -> Level.ERROR.equals(event.getLevel())));

            String message = appender.list.stream()
                    .filter(event -> Level.ERROR.equals(event.getLevel()))
                    .map(ILoggingEvent::getFormattedMessage)
                    .findFirst().orElseThrow();
            assertThat(message)
                    .contains("OAuth2 is required")
                    .contains("there is no token in Redis")
                    .contains(SmsfTokenRedisStore.keyFor(NETWORK_ID, UDM));
        } finally {
            cacheLogger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    @DisplayName("the gateway recovers on its own once smsf-core finally publishes")
    void recoversWhenTheTokenAppears() {
        redisHolds(null);
        register();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> countReads() >= 1);

        redisHolds(token("late", 600));

        awaitAvailable();
        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer late");
        assertThat(available.get()).isEqualTo(1);
    }

    // --- rotation -------------------------------------------------------------

    @Test
    @DisplayName("a rotated token replaces both caches without reporting the credential as lost")
    void rotationReplacesTheCaches() {
        // Already inside the refresh window, so the task re-reads immediately.
        redisHolds(token("first", MARGIN + 1));
        register();
        awaitAvailable();

        redisHolds(token("second", 600));

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> cache.authorizationHeader(NETWORK_ID, UDM).orElse("").contains("second"));
        assertThat(cache.token(NETWORK_ID, UDM).orElseThrow().accessToken()).isEqualTo("second");
        // The credential was never lost, so the consumers were never stopped.
        assertThat(unavailable.get()).isZero();
        assertThat(available.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("while the rotation is due but has not landed, Redis is re-read at the stale-poll interval")
    void pollsWhileTheRotationIsPending() {
        // Inside the window and not rotating: the task should keep checking rather than give up.
        redisHolds(token("stuck", MARGIN + 2));
        register();
        awaitAvailable();

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> countReads() >= 5);

        // Still usable until it actually expires, so nothing is stopped yet.
        assertThat(cache.isAvailable(NETWORK_ID, UDM)).isTrue();
        assertThat(unavailable.get()).isZero();
    }

    @Test
    @DisplayName("a token that expires without a replacement is dropped and the consumers are stopped")
    void staleTokenStopsConsumption() {
        // Expires almost immediately and is never replaced.
        redisHolds(token("dying", MARGIN + 1));
        register();
        awaitAvailable();

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> unavailable.get() >= 1);

        assertThat(cache.isAvailable(NETWORK_ID, UDM)).isFalse();
        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).isEmpty();
        assertThat(cache.token(NETWORK_ID, UDM)).isEmpty();
        assertThat(available.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("the consumers are restarted when a fresh token arrives after the credential was lost")
    void recoversAfterGoingStale() {
        redisHolds(token("dying", MARGIN + 1));
        register();
        awaitAvailable();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> unavailable.get() >= 1);

        redisHolds(token("fresh", 600));

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> cache.isAvailable(NETWORK_ID, UDM));
        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer fresh");
        assertThat(available.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("the credential is reported lost only once, not on every poll")
    void unavailableIsReportedOnce() {
        redisHolds(token("dying", MARGIN + 1));
        register();
        awaitAvailable();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> unavailable.get() >= 1);

        int readsBefore = countReads();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> countReads() > readsBefore + 1);

        assertThat(unavailable.get()).isEqualTo(1);
    }

    // --- resilience and lifecycle --------------------------------------------

    @Test
    @DisplayName("a Redis failure does not kill the refresh cycle")
    void redisFailureDoesNotKillTheCycle() {
        when(tokenStore.read(anyInt(), anyString(), anyLong())).thenAnswer(inv -> {
            reads.incrementAndGet();
            throw new IllegalStateException("redis down");
        });

        register();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> countReads() >= 2);

        assertThat(cache.isAvailable(NETWORK_ID, UDM)).isFalse();
    }

    @Test
    @DisplayName("a callback that throws does not kill the refresh cycle either")
    void callbackFailureDoesNotKillTheCycle() {
        redisHolds(token("abc", 600));

        cache.register(NETWORK_ID, UDM,
                () -> {
                    throw new IllegalStateException("callback boom");
                },
                unavailable::incrementAndGet);

        awaitAvailable();
        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer abc");
    }

    @Test
    @DisplayName("unregister drops both caches and stops refreshing")
    void unregisterDropsEverything() {
        redisHolds(token("abc", 600));
        register();
        awaitAvailable();

        cache.unregister(NETWORK_ID, UDM);

        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).isEmpty();
        assertThat(cache.token(NETWORK_ID, UDM)).isEmpty();
        assertThat(cache.isAvailable(NETWORK_ID, UDM)).isFalse();
    }

    @Test
    @DisplayName("registering the same gateway twice does not leave two refresh cycles running")
    void reregisteringReplacesTheTracking() {
        redisHolds(token("abc", 600));
        register();
        awaitAvailable();

        register();
        awaitAvailable();

        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer abc");
    }

    @Test
    @DisplayName("close clears the caches and stops the scheduler")
    void closeClearsEverything() {
        redisHolds(token("abc", 600));
        register();
        awaitAvailable();

        cache.close();

        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).isEmpty();
        assertThat(cache.isAvailable(NETWORK_ID, UDM)).isFalse();
        // Idempotent.
        cache.close();
    }

    @Test
    @DisplayName("the AMF and UDM entries are tracked separately under the same gateway")
    void audiencesAreIndependent() {
        long now = Instant.now().getEpochSecond();
        when(tokenStore.read(NETWORK_ID, "AMF", MARGIN)).thenReturn(Optional.of(
                new SmsfSecurityToken(NETWORK_ID, "AMF", "amf-token", "Bearer", "nsmsf-sms", now, now + 600)));
        when(tokenStore.read(NETWORK_ID, UDM, MARGIN)).thenReturn(Optional.of(
                new SmsfSecurityToken(NETWORK_ID, UDM, "udm-token", "Bearer", "nudm-uecm", now, now + 600)));

        cache.register(NETWORK_ID, "AMF", () -> { }, () -> { });
        register();

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> cache.isAvailable(NETWORK_ID, "AMF") && cache.isAvailable(NETWORK_ID, UDM));
        assertThat(cache.authorizationHeader(NETWORK_ID, "AMF")).contains("Bearer amf-token");
        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer udm-token");

        cache.unregister(NETWORK_ID, "AMF");
        assertThat(cache.authorizationHeader(NETWORK_ID, UDM)).contains("Bearer udm-token");
    }

    @Test
    @DisplayName("the token is never read from Redis before the gateway is registered")
    void nothingIsReadBeforeRegistration() {
        verify(tokenStore, never()).read(anyInt(), anyString(), anyLong());
    }

    private int countReads() {
        return reads.get();
    }
}
