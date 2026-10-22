package com.paicbd.smsc.smsf.core.security.outbound;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboundTokenRefresherTest {

    private OutboundTokenRefresher refresher;

    @AfterEach
    void tearDown() {
        if (refresher != null) {
            refresher.stop();
        }
    }

    @Test
    void bootstrapStoresTokenAndCurrentBearerHeaderReturnsFormatted() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("xyz", 300, Instant.now()));

        refresher = new OutboundTokenRefresher(1, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});
        refresher.bootstrap();

        assertEquals(Optional.of("Bearer xyz"), refresher.currentBearerHeader());
    }

    @Test
    void bootstrapPropagatesFetcherException() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong())).thenThrow(new IllegalStateException("boom"));

        refresher = new OutboundTokenRefresher(2, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});

        assertThrows(IllegalStateException.class, refresher::bootstrap);
        assertTrue(refresher.currentBearerHeader().isEmpty());
    }

    @Test
    void currentBearerHeaderEmptyWhenTokenIsExpired() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("xyz", 10, Instant.now().minusSeconds(60)));

        refresher = new OutboundTokenRefresher(3, fetcher, new OutboundTokenTimings(0, 10, 5), () -> {}, () -> {});
        refresher.bootstrap();

        assertTrue(refresher.currentBearerHeader().isEmpty());
    }

    @Test
    void schedulerReplacesTokenBeforeExpiration() throws Exception {
        CountDownLatch secondFetched = new CountDownLatch(1);
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("first", 2, Instant.now()))
                .thenAnswer(inv -> {
                    secondFetched.countDown();
                    return new OAuth2Token("second", 60, Instant.now());
                });

        refresher = new OutboundTokenRefresher(4, fetcher, new OutboundTokenTimings(0, 5, 1), () -> {}, () -> {});
        refresher.bootstrap();
        refresher.start();

        assertTrue(secondFetched.await(5, TimeUnit.SECONDS), "second fetch should have run");
        awaitUntil(() -> refresher.currentBearerHeader().equals(Optional.of("Bearer second")),
                Duration.ofSeconds(1));
        assertEquals(Optional.of("Bearer second"), refresher.currentBearerHeader());
    }

    @Test
    void pauseCalledWhenRefreshFailsAndCurrentTokenExpired() throws Exception {
        CountDownLatch paused = new CountDownLatch(1);
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("ephemeral", 1, Instant.now()))
                .thenThrow(new IllegalStateException("nrf down"));

        refresher = new OutboundTokenRefresher(5, fetcher, new OutboundTokenTimings(0, 5, 1), paused::countDown, () -> {});
        refresher.bootstrap();
        refresher.start();

        assertTrue(paused.await(6, TimeUnit.SECONDS), "pause callback should be invoked");
        verify(fetcher, atLeastOnce()).fetch(anyLong());
    }

    @Test
    void resumeCalledWhenRefreshSucceedsAfterFailure() throws Exception {
        AtomicInteger pausedCount = new AtomicInteger();
        CountDownLatch resumed = new CountDownLatch(1);
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("first", 1, Instant.now()))
                .thenThrow(new IllegalStateException("nrf down"))
                .thenReturn(new OAuth2Token("recovered", 60, Instant.now()));

        refresher = new OutboundTokenRefresher(6, fetcher, new OutboundTokenTimings(0, 5, 1),
                pausedCount::incrementAndGet, resumed::countDown);
        refresher.bootstrap();
        refresher.start();

        assertTrue(resumed.await(8, TimeUnit.SECONDS), "resume callback should be invoked");
        assertTrue(pausedCount.get() >= 1, "pause should have been invoked first");
        assertEquals(Optional.of("Bearer recovered"), refresher.currentBearerHeader());
    }

    @Test
    void stopClearsTokenAndIsIdempotent() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("xyz", 300, Instant.now()));

        refresher = new OutboundTokenRefresher(7, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});
        refresher.bootstrap();
        refresher.start();
        refresher.stop();
        refresher.stop();

        assertTrue(refresher.currentBearerHeader().isEmpty());
    }

    @Test
    void startWhenAlreadyStartedIsNoOp() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("first", 300, Instant.now()));

        refresher = new OutboundTokenRefresher(8, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});
        refresher.bootstrap();
        refresher.start();
        refresher.start();
        assertEquals(Optional.of("Bearer first"), refresher.currentBearerHeader());
    }

    @Test
    void stopWhenNeverStartedIsNoOp() {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        refresher = new OutboundTokenRefresher(9, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});
        refresher.stop();
        assertTrue(refresher.currentBearerHeader().isEmpty());
    }

    @Test
    void constructorAcceptsNullCallbacksWithoutThrowing() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("xyz", 300, Instant.now()))
                .thenThrow(new IllegalStateException("nrf down"));

        refresher = new OutboundTokenRefresher(10, fetcher, new OutboundTokenTimings(0, 5, 1), null, null);
        refresher.bootstrap();
        refresher.start();
        awaitUntil(() -> refresher.currentBearerHeader().isPresent(), Duration.ofSeconds(2));
        assertTrue(refresher.currentBearerHeader().isPresent());
    }

    @Test
    void pauseCallbackExceptionDoesNotBreakRefresher() throws Exception {
        CountDownLatch fetchCalls = new CountDownLatch(2);
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong())).thenAnswer(inv -> {
            fetchCalls.countDown();
            if (fetchCalls.getCount() == 1) {
                return new OAuth2Token("ephemeral", 1, Instant.now());
            }
            throw new IllegalStateException("nrf down");
        });

        refresher = new OutboundTokenRefresher(11, fetcher, new OutboundTokenTimings(0, 5, 1),
                () -> { throw new RuntimeException("pause boom"); },
                () -> {});
        refresher.bootstrap();
        refresher.start();

        assertTrue(fetchCalls.await(6, TimeUnit.SECONDS));
    }

    @Test
    void resumeCallbackExceptionDoesNotBreakRefresher() throws Exception {
        CountDownLatch resumedReached = new CountDownLatch(1);
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong()))
                .thenReturn(new OAuth2Token("a", 1, Instant.now()))
                .thenThrow(new IllegalStateException("nrf down"))
                .thenAnswer(inv -> {
                    resumedReached.countDown();
                    return new OAuth2Token("b", 60, Instant.now());
                });

        refresher = new OutboundTokenRefresher(12, fetcher, new OutboundTokenTimings(0, 5, 1),
                () -> {},
                () -> { throw new RuntimeException("resume boom"); });
        refresher.bootstrap();
        refresher.start();

        assertTrue(resumedReached.await(8, TimeUnit.SECONDS));
        awaitUntil(() -> refresher.currentBearerHeader().equals(Optional.of("Bearer b")),
                Duration.ofSeconds(1));
        assertEquals(Optional.of("Bearer b"), refresher.currentBearerHeader());
    }

    @Test
    void currentBearerHeaderEmptyWhenNoTokenPresent() {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        refresher = new OutboundTokenRefresher(13, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});

        assertTrue(refresher.currentBearerHeader().isEmpty());
    }

    @Test
    void computeNextDelaySecondsReturnsRetryBackoffWhenTokenIsNull() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        refresher = new OutboundTokenRefresher(14, fetcher, new OutboundTokenTimings(30, 10, 7), () -> {}, () -> {});

        Method method = OutboundTokenRefresher.class.getDeclaredMethod("computeNextDelaySeconds", OAuth2Token.class);
        method.setAccessible(true);

        long next = (long) method.invoke(refresher, new Object[]{null});

        assertEquals(7L, next);
    }

    @Test
    void computeNextDelaySecondsReturnsRetryBackoffWhenMarginConsumesTtl() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        refresher = new OutboundTokenRefresher(15, fetcher, new OutboundTokenTimings(30, 10, 7), () -> {}, () -> {});

        Method method = OutboundTokenRefresher.class.getDeclaredMethod("computeNextDelaySeconds", OAuth2Token.class);
        method.setAccessible(true);

        long next = (long) method.invoke(refresher, new OAuth2Token("x", 10, Instant.now()));

        assertEquals(7L, next);
    }

    @Test
    void scheduleNextReturnsImmediatelyWhenSchedulerIsNull() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        refresher = new OutboundTokenRefresher(16, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});

        Method method = OutboundTokenRefresher.class.getDeclaredMethod("scheduleNext", long.class);
        method.setAccessible(true);

        method.invoke(refresher, 0L);

        assertTrue(refresher.currentBearerHeader().isEmpty());
    }

    @Test
    void scheduleNextReturnsImmediatelyWhenSchedulerIsShutdown() throws Exception {
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        refresher = new OutboundTokenRefresher(17, fetcher, new OutboundTokenTimings(30, 10, 5), () -> {}, () -> {});

        var schedulerField = OutboundTokenRefresher.class.getDeclaredField("scheduler");
        schedulerField.setAccessible(true);
        var scheduledTaskField = OutboundTokenRefresher.class.getDeclaredField("scheduledTask");
        scheduledTaskField.setAccessible(true);
        ScheduledExecutorService scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        scheduler.shutdownNow();
        schedulerField.set(refresher, scheduler);

        Method method = OutboundTokenRefresher.class.getDeclaredMethod("scheduleNext", long.class);
        method.setAccessible(true);

        method.invoke(refresher, 0L);

        assertNull(scheduledTaskField.get(refresher));
        schedulerField.set(refresher, null);
    }

    @Test
    void refreshOnceWhenInterruptedDoesNotPauseConsumersAndPreservesInterruptFlag() throws Exception {
        CountDownLatch interruptedFetch = new CountDownLatch(1);
        AtomicInteger pauseCalls = new AtomicInteger();
        OAuth2ClientCredentialsFetcher fetcher = mock(OAuth2ClientCredentialsFetcher.class);
        when(fetcher.fetch(anyLong())).thenThrow(new InterruptedException("stop"));

        refresher = new OutboundTokenRefresher(18, fetcher, new OutboundTokenTimings(30, 10, 5), pauseCalls::incrementAndGet, () -> {});

        Method method = OutboundTokenRefresher.class.getDeclaredMethod("refreshOnce");
        method.setAccessible(true);

        Thread thread = new Thread(() -> {
            try {
                method.invoke(refresher);
                if (Thread.currentThread().isInterrupted()) {
                    interruptedFetch.countDown();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        thread.start();
        assertTrue(interruptedFetch.await(2, TimeUnit.SECONDS));
        thread.join(2000);
        assertEquals(0, pauseCalls.get());
    }

    private static void awaitUntil(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
        }
        throw new AssertionError("condition not met within " + timeout);
    }
}
