package com.paicbd.smsc.smsf.core.security.outbound;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
public class OutboundTokenRefresher {

    private final int networkId;
    private final OAuth2ClientCredentialsFetcher fetcher;
    private final long refreshMarginSeconds;
    private final long initialFetchTimeoutSeconds;
    private final long retryBackoffSeconds;
    private final Runnable pauseConsumers;
    private final Runnable resumeConsumers;
    private final Consumer<OAuth2Token> tokenListener;

    private final AtomicReference<OAuth2Token> currentToken = new AtomicReference<>();
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledTask;

    public OutboundTokenRefresher(int networkId,
                                  OAuth2ClientCredentialsFetcher fetcher,
                                  OutboundTokenTimings timings,
                                  Runnable pauseConsumers,
                                  Runnable resumeConsumers) {
        this(networkId, fetcher, timings, pauseConsumers, resumeConsumers, null);
    }

    /**
     * @param tokenListener notified with every token this refresher obtains, on bootstrap and on
     *                      each refresh. Used to republish the token to Redis for the modules that
     *                      no longer talk to the NRF themselves. A listener that throws is logged
     *                      and ignored: a Redis problem must not break the token lifecycle.
     */
    public OutboundTokenRefresher(int networkId,
                                  OAuth2ClientCredentialsFetcher fetcher,
                                  OutboundTokenTimings timings,
                                  Runnable pauseConsumers,
                                  Runnable resumeConsumers,
                                  Consumer<OAuth2Token> tokenListener) {
        this.networkId = networkId;
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.refreshMarginSeconds = Math.max(0, timings.refreshMarginSeconds());
        this.initialFetchTimeoutSeconds = Math.max(1, timings.initialFetchTimeoutSeconds());
        this.retryBackoffSeconds = Math.max(1, timings.retryBackoffSeconds());
        this.pauseConsumers = Objects.requireNonNullElse(pauseConsumers, () -> {});
        this.resumeConsumers = Objects.requireNonNullElse(resumeConsumers, () -> {});
        this.tokenListener = Objects.requireNonNullElse(tokenListener, token -> {});
    }

    public synchronized void bootstrap() throws InterruptedException, ExecutionException, TimeoutException {
        OAuth2Token token = fetcher.fetch(initialFetchTimeoutSeconds);
        currentToken.set(token);
        notifyTokenListener(token);
        log.info("Outbound token bootstrap success for networkId={} (TTL={}s, next refresh in {}s)",
                networkId, token.expiresInSeconds(), computeNextDelaySeconds(token));
    }

    public synchronized void start() {
        if (Objects.nonNull(scheduler)) {
            log.warn("OutboundTokenRefresher for networkId={} is already started", networkId);
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("Refresh-Token-NetworkId-" + networkId);
            t.setDaemon(true);
            return t;
        });
        scheduleNext(computeNextDelaySeconds(currentToken.get()));
    }

    public synchronized void stop() {
        if (Objects.nonNull(scheduledTask)) {
            scheduledTask.cancel(true);
            scheduledTask = null;
        }
        if (Objects.nonNull(scheduler)) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        currentToken.set(null);
        if (paused.compareAndSet(true, false)) {
            safelyInvoke(resumeConsumers, "Resume consumers callback failed on stop");
        }
    }

    public Optional<String> currentBearerHeader() {
        OAuth2Token token = currentToken.get();
        if (Objects.isNull(token) || token.isExpired(0)) {
            return Optional.empty();
        }
        return Optional.of("Bearer " + token.accessToken());
    }

    private void scheduleNext(long delaySeconds) {
        if (Objects.isNull(scheduler) || scheduler.isShutdown()) {
            return;
        }
        long delay = Math.max(1, delaySeconds);
        scheduledTask = scheduler.schedule(this::refreshOnce, delay, TimeUnit.SECONDS);
    }

    private void refreshOnce() {
        try {
            OAuth2Token token = fetcher.fetch(initialFetchTimeoutSeconds);
            currentToken.set(token);
            notifyTokenListener(token);
            log.info("Outbound token refreshed for networkId={} (TTL={}s, next refresh in {}s)",
                    networkId, token.expiresInSeconds(), computeNextDelaySeconds(token));
            if (paused.compareAndSet(true, false)
                    && safelyInvoke(resumeConsumers, "Resume consumers callback failed")) {
                log.info("Consumers resumed for networkId={} after token recovery", networkId);
            }
            scheduleNext(computeNextDelaySeconds(token));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Outbound token refresh interrupted for networkId={}", networkId);
        } catch (Exception ex) {
            log.warn("Outbound token refresh failed for networkId={}: {}", networkId, ex.getMessage());
            OAuth2Token current = currentToken.get();
            if ((Objects.isNull(current) || current.isExpired(0))
                    && paused.compareAndSet(false, true)
                    && safelyInvoke(pauseConsumers, "Pause consumers callback failed")) {
                log.warn("Consumers paused for networkId={} (token expired without replacement)", networkId);
            }
            scheduleNext(retryBackoffSeconds);
        }
    }

    private void notifyTokenListener(OAuth2Token token) {
        try {
            tokenListener.accept(token);
        } catch (Exception ex) {
            log.warn("Token listener failed for networkId={}: {}", networkId, ex.getMessage());
        }
    }

    private boolean safelyInvoke(Runnable callback, String failureMessage) {
        try {
            callback.run();
            return true;
        } catch (Exception ex) {
            log.warn("{} for networkId={}: {}", failureMessage, networkId, ex.getMessage());
            return false;
        }
    }

    private long computeNextDelaySeconds(OAuth2Token token) {
        if (Objects.isNull(token)) {
            return retryBackoffSeconds;
        }
        long next = token.expiresInSeconds() - refreshMarginSeconds;
        return next > 0 ? next : retryBackoffSeconds;
    }
}
