package com.paicbd.smsc.smsf.core.security.nrf;

import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.openapi.model.NFProfile;
import com.paicore.sba.stack.openapi.model.PatchItem;
import com.paicore.sba.stack.openapi.model.PatchOperation;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Slf4j
public class NrfRegistrationManager {

    private static final int NF_INSTANCE_NOT_FOUND = 404;

    private final int networkId;
    private final NettyHttp2Client httpClient;
    private final String registerPath;
    private final NFProfile profile;
    private final Supplier<String> bearerHeaderSupplier;
    private final long initialRegisterTimeoutSeconds;
    private final long retryBackoffSeconds;

    private final AtomicReference<Integer> heartbeatSecondsRef = new AtomicReference<>();
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledTask;
    private boolean deregistered;

    public NrfRegistrationManager(int networkId,
                                  NettyHttp2Client httpClient,
                                  String registerPath,
                                  NFProfile profile,
                                  Supplier<String> bearerHeaderSupplier,
                                  NrfRegistrationTimings timings) {
        this.networkId = networkId;
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.registerPath = Objects.requireNonNull(registerPath, "registerPath");
        this.profile = Objects.requireNonNull(profile, "profile");
        this.bearerHeaderSupplier = Objects.requireNonNullElse(bearerHeaderSupplier, () -> null);
        this.initialRegisterTimeoutSeconds = Math.max(1, timings.initialRegisterTimeoutSeconds());
        this.retryBackoffSeconds = Math.max(1, timings.retryBackoffSeconds());
        this.heartbeatSecondsRef.set(Math.max(1, timings.defaultHeartbeatSeconds()));
    }

    public synchronized void register() throws Exception {
        ensureConnected();
        Http2Response response = httpClient.send(buildPutRequest()).get(initialRegisterTimeoutSeconds, TimeUnit.SECONDS);
        int status = response.getStatus();
        if (status != 200 && status != 201) {
            throw new IllegalStateException("NRF register returned HTTP " + status + " for networkId " + networkId);
        }
        Integer negotiated = extractHeartbeatTimer(response);
        if (Objects.nonNull(negotiated)) {
            heartbeatSecondsRef.set(negotiated);
        }
        log.info("NRF registration success for networkId={} (heartbeat={}s)", networkId, heartbeatSecondsRef.get());
    }

    public synchronized void start() {
        if (Objects.nonNull(scheduler)) {
            log.warn("NrfRegistrationManager for networkId={} is already started", networkId);
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("Nrf-Heartbeat-NetworkId-" + networkId);
            t.setDaemon(true);
            return t;
        });
        scheduleNext(heartbeatSecondsRef.get());
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
        deregister();
    }

    private void deregister() {
        if (deregistered || !httpClient.isConnected()) {
            return;
        }
        deregistered = true;
        try {
            httpClient.send(buildDeleteRequest()).get(initialRegisterTimeoutSeconds, TimeUnit.SECONDS);
            log.info("NRF deregister success for networkId={}", networkId);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("NRF deregister interrupted for networkId={}", networkId);
        } catch (Exception ex) {
            log.warn("NRF deregister failed for networkId={}: {}", networkId, ex.getMessage());
        }
    }

    private void scheduleNext(long delaySeconds) {
        if (Objects.isNull(scheduler) || scheduler.isShutdown()) {
            return;
        }
        scheduledTask = scheduler.schedule(this::heartbeatOnce, Math.max(1, delaySeconds), TimeUnit.SECONDS);
    }

    private void heartbeatOnce() {
        try {
            ensureConnected();
            Http2Response response = httpClient.send(buildPatchRequest()).get(initialRegisterTimeoutSeconds, TimeUnit.SECONDS);
            int status = response.getStatus();
            if (status == NF_INSTANCE_NOT_FOUND) {
                reRegister();
                return;
            }
            if (status != 200 && status != 204) {
                throw new IllegalStateException("NRF heartbeat returned HTTP " + status);
            }
            log.info("NRF heartbeat success for networkId={}", networkId);
            scheduleNext(heartbeatSecondsRef.get());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("NRF heartbeat interrupted for networkId={}", networkId);
        } catch (Exception ex) {
            log.warn("NRF heartbeat failed for networkId={}: {}", networkId, ex.getMessage());
            scheduleNext(retryBackoffSeconds);
        }
    }

    private void reRegister() {
        if (deregistered) {
            return;
        }
        log.warn("NRF no longer knows networkId={} (heartbeat returned HTTP {}), registering again",
                networkId, NF_INSTANCE_NOT_FOUND);
        try {
            register();
            scheduleNext(heartbeatSecondsRef.get());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("NRF re-registration interrupted for networkId={}", networkId);
        } catch (Exception ex) {
            log.warn("NRF re-registration failed for networkId={}: {}", networkId, ex.getMessage());
            scheduleNext(retryBackoffSeconds);
        }
    }

    private void ensureConnected() {
        if (!httpClient.isConnected()) {
            httpClient.connect();
        }
    }

    private Http2Request buildPutRequest() {
        String body = Converter.valueAsString(profile);
        Map<String, String> headers = baseHeaders(Constants.JSON_CONTENT_TYPE);
        return Http2Request.builder()
                .method("PUT")
                .path(registerPath)
                .headers(headers)
                .body(body.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private Http2Request buildPatchRequest() {
        PatchItem item = new PatchItem();
        item.setOp(PatchOperation.REPLACE.getValue());
        item.setPath("/nfStatus");
        item.setValue("REGISTERED");
        String body = Converter.valueAsString(List.of(item));
        Map<String, String> headers = baseHeaders(Constants.JSON_PATCH_CONTENT_TYPE);
        return Http2Request.builder()
                .method("PATCH")
                .path(registerPath)
                .headers(headers)
                .body(body.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private Http2Request buildDeleteRequest() {
        Map<String, String> headers = baseHeaders(null);
        return Http2Request.builder()
                .method("DELETE")
                .path(registerPath)
                .headers(headers)
                .build();
    }

    private Map<String, String> baseHeaders(String contentType) {
        Map<String, String> headers = new HashMap<>();
        if (Objects.nonNull(contentType)) {
            headers.put(Constants.CONTENT_TYPE_HEADER, contentType);
        }
        String bearer = bearerHeaderSupplier.get();
        if (Objects.nonNull(bearer) && !bearer.isBlank()) {
            headers.put(Constants.AUTHORIZATION_HEADER, bearer);
        }
        return headers;
    }

    private Integer extractHeartbeatTimer(Http2Response response) {
        try {
            byte[] body = response.getBody();
            if (Objects.isNull(body) || body.length == 0) {
                return null;
            }
            NFProfile returned = Converter.stringToObject(new String(body, StandardCharsets.UTF_8), NFProfile.class);
            return Objects.isNull(returned) ? null : returned.getHeartBeatTimer();
        } catch (Exception ex) {
            log.debug("Could not parse NFProfile from NRF response for networkId={}: {}",
                    networkId, ex.getMessage());
            return null;
        }
    }
}
