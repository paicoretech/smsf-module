package com.paicbd.smsc.smsf.core.security;

import com.paicbd.smsc.utils.Watcher;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class SecurityMetrics {

    private static final int WATCH_INTERVAL_SECONDS = 1;

    private final AtomicInteger authSuccessCounter = new AtomicInteger();
    private final AtomicInteger authFailureCounter = new AtomicInteger();

    private Watcher authSuccessWatcher;
    private Watcher authFailureWatcher;

    @PostConstruct
    public void start() {
        // No startWatching() here: the Watcher constructor already schedules the task. Calling it
        // again schedules a second one on the same counter, and since each tick does getAndSet(0)
        // the two race and split the count — the reported rate comes out wrong, not just doubled.
        authSuccessWatcher = new Watcher("smsf-auth-success-per-second", authSuccessCounter, WATCH_INTERVAL_SECONDS);
        authFailureWatcher = new Watcher("smsf-auth-failure-per-second", authFailureCounter, WATCH_INTERVAL_SECONDS);
    }

    @PreDestroy
    public void stop() {
        if (Objects.nonNull(authSuccessWatcher)) {
            authSuccessWatcher.stopWatching();
        }
        if (Objects.nonNull(authFailureWatcher)) {
            authFailureWatcher.stopWatching();
        }
    }

    public void incrementAuthSuccess() {
        authSuccessCounter.incrementAndGet();
    }

    public void incrementAuthFailure() {
        authFailureCounter.incrementAndGet();
    }
}
