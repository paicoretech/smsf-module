package com.paicbd.smsc.smsf.core.stack;

import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicbd.smsc.dto.Http2GatewayUDMSecurity;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.http2.Http2ClientNrfConnection;
import com.paicbd.smsc.smsf.common.http2.Http2HostUtils;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.common.processors.MessageTpduProcessor;
import com.paicbd.smsc.smsf.common.redis.SmsfSecurityToken;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore;
import com.paicbd.smsc.smsf.core.config.AppProperties;
import com.paicbd.smsc.smsf.core.http2.Http2ClientTokenConnection;
import com.paicbd.smsc.smsf.core.security.nrf.NfProfileBuilder;
import com.paicbd.smsc.smsf.core.security.nrf.NrfRegistrationManager;
import com.paicbd.smsc.smsf.core.security.nrf.NrfRegistrationTimings;
import com.paicbd.smsc.smsf.core.security.outbound.OAuth2ClientCredentialsFetcher;
import com.paicbd.smsc.smsf.core.security.outbound.OAuth2Token;
import com.paicbd.smsc.smsf.core.security.outbound.OutboundTokenRefresher;
import com.paicbd.smsc.smsf.core.security.outbound.OutboundTokenTimings;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import lombok.Getter;
import com.paicbd.smsc.utils.Watcher;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-gateway stack owned by {@code smsf-core}: the NRF integration and the OAuth2 tokens.
 *
 * <p>This is the SBI/NRF slice of what used to be a single {@code Http2StackManager} holding the
 * whole SMSF. MT delivery towards the AMF now lives in {@code smsf-service-mt} and N21 forwarding
 * towards the UDM in {@code smsf-n21-registration}; each keeps its own per-gateway stack.</p>
 *
 * <p>The core still mints <b>both</b> tokens — the AMF-scoped one it does not use itself and the
 * UDM-scoped one — because it is the only module that talks to the NRF token endpoint. Every token
 * it obtains is republished to Redis through {@link SmsfTokenRedisStore} so the other two modules
 * can read it instead of opening a second flow against the NRF.</p>
 */
@Slf4j
@Getter
public class CoreStackManager {

    private final Http2Gateway gateway;
    private final List<Watcher> watchers = new ArrayList<>();
    private final AtomicInteger processMoPerSecond = new AtomicInteger(0);
    private final AppProperties appProperties;
    private final SmsfTokenRedisStore tokenStore;
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;

    private final Http2ClientNrfConnection nrfConnection;
    private final Http2ClientTokenConnection tokenConnection;
    /** Built here because the MO flow resolves it per gateway to decode and map incoming messages. */
    private final MessageFactory messageFactory;

    private OutboundTokenRefresher outboundTokenRefresher;
    private OutboundTokenRefresher udmTokenRefresher;
    private NrfRegistrationManager nrfRegistrationManager;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public CoreStackManager(Http2Gateway gateway, AppProperties appProperties,
                            SmsfTokenRedisStore tokenStore,
                            ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap) {
        this.gateway = gateway;
        this.appProperties = appProperties;
        this.tokenStore = tokenStore;
        this.errorCodeMappingConcurrentHashMap = errorCodeMappingConcurrentHashMap;
        this.nrfConnection = new Http2ClientNrfConnection(gateway);
        this.tokenConnection = new Http2ClientTokenConnection(gateway);
        this.messageFactory = new MessageFactory(gateway, errorCodeMappingConcurrentHashMap,
                new MessageTpduProcessor(new Ss7SmsTpduCodec()));
        this.addAndInitializeWatchers();
    }

    /**
     * Inbound MO throughput for this gateway. Started here, in the per-gateway manager, so each
     * tenant reports its own rate and the watcher dies with the gateway — the same shape the SS7
     * client uses, where the per-gateway layer manager owns its processors' watchers.
     *
     * <p>The Watcher constructor schedules the task; there is no explicit start.</p>
     */
    private void addAndInitializeWatchers() {
        this.watchers.add(new Watcher(
                "Process-MO-Message-NetworkId-" + gateway.getNetworkId(), processMoPerSecond, 1));
    }

    /** Incremented by the MO service, and only once the message is on Kafka. */
    public void incrementMoProcessed() {
        processMoPerSecond.incrementAndGet();
    }

    public int getProcessMoPerSecond() {
        return processMoPerSecond.get();
    }

    @Synchronized
    public void start() {
        if (!started.compareAndSet(false, true)) {
            log.warn("CoreStackManager for networkId {} is already started", gateway.getNetworkId());
            return;
        }
        log.info("Starting SMSF core stack for networkId {}", gateway.getNetworkId());
        try {
            // Before anything else: without the identity assigned by smsc-management-be the gateway
            // can neither register on the NRF nor ask for a token, so nothing is started at all.
            NfProfileBuilder.requireNfInstanceId(gateway);
            this.nrfConnection.connect();
            startOutboundTokenRefresher();
            startUdmTokenRefresher();
            startNrfRegistration();
            log.info("SMSF core stack ready for networkId {}", gateway.getNetworkId());
        } catch (Exception ex) {
            log.error("Failed to start the SMSF core stack for networkId {}: {}",
                    gateway.getNetworkId(), ex.getMessage(), ex);
            started.set(false);
            stopInternal();
            throw ex;
        }
    }

    @Synchronized
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            log.warn("CoreStackManager for networkId {} is already stopped", gateway.getNetworkId());
            // The watchers start with the manager, not with start(), so one that was built and
            // never started still owns a live watcher thread. stopInternal is idempotent.
            stopInternal();
            return;
        }
        log.info("Stopping SMSF core stack for networkId {}", gateway.getNetworkId());
        stopInternal();
        log.info("SMSF core stack stopped for networkId {}", gateway.getNetworkId());
    }

    public boolean isStarted() {
        return started.get();
    }

    private void stopInternal() {
        stopNrfRegistration();
        stopRefresher(outboundTokenRefresher, Constants.NF_TYPE_AMF);
        outboundTokenRefresher = null;
        stopRefresher(udmTokenRefresher, Constants.NF_TYPE_UDM);
        udmTokenRefresher = null;
        nrfConnection.disconnect();
        tokenConnection.disconnect();
        watchers.forEach(Watcher::stopWatching);
    }

    // --- NRF registration -----------------------------------------------------

    /**
     * Builds the NF instance resource path of TS 29.510: {@code <nrf_register_path>/<nfInstanceId>}.
     *
     * <p>{@code nrf_register_path} configures the service path only — {@code /nnrf-nfm/v1/nf-instances}
     * — and never the identifier. Appending it here is what keeps the id in the URL and the id in the
     * profile body the same value, which the NRF requires: the URL names the resource being created
     * and the body describes it.</p>
     */
    static String registerPathFor(String configuredPath, UUID nfInstanceId) {
        String base = configuredPath.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/" + nfInstanceId;
    }

    private void startNrfRegistration() {
        Http2GatewayNrfSecurity nrf = gateway.getNrfSecurity();
        if (Objects.isNull(nrf) || Objects.isNull(nrf.getNrfRegisterPath()) || nrf.getNrfRegisterPath().isBlank()) {
            log.warn("NRF registration skipped for networkId {} — nrf_register_path is missing",
                    gateway.getNetworkId());
            return;
        }
        if (!nrfConnection.isConnected() || Objects.isNull(nrfConnection.getClient())) {
            log.warn("NRF registration skipped for networkId {} — NRF client is not connected",
                    gateway.getNetworkId());
            return;
        }

        NrfRegistrationManager manager = new NrfRegistrationManager(
                gateway.getNetworkId(),
                nrfConnection.getClient(),
                registerPathFor(nrf.getNrfRegisterPath(), gateway.getNfInstanceId()),
                NfProfileBuilder.build(gateway, appProperties),
                () -> Objects.isNull(outboundTokenRefresher)
                        ? null
                        : outboundTokenRefresher.currentBearerHeader().orElse(null),
                new NrfRegistrationTimings(appProperties.getNrfInitialRegisterTimeoutSeconds(), appProperties.getNrfRetryBackoffSeconds(), appProperties.getNrfHeartbeatDefaultSeconds()));
        try {
            manager.register();
            manager.start();
            this.nrfRegistrationManager = manager;
            log.info("NRF registration manager started for networkId {}", gateway.getNetworkId());
        } catch (Exception ex) {
            log.error("Failed to register against NRF for networkId {}: {}",
                    gateway.getNetworkId(), ex.getMessage(), ex);
            manager.stop();
            throw new IllegalStateException("NRF registration failed for networkId "
                    + gateway.getNetworkId(), ex);
        }
    }

    private void stopNrfRegistration() {
        if (Objects.nonNull(nrfRegistrationManager)) {
            try {
                nrfRegistrationManager.stop();
            } catch (Exception ex) {
                log.warn("Error stopping NRF registration manager for networkId {}: {}",
                        gateway.getNetworkId(), ex.getMessage());
            } finally {
                nrfRegistrationManager = null;
            }
        }
    }

    // --- OAuth2 tokens --------------------------------------------------------

    /**
     * Starts the AMF-audience refresher. The core does not talk to the AMF itself; it mints this
     * token because it owns the NRF credentials, uses it as the bearer for its own NRF
     * registration, and publishes it to Redis for {@code smsf-service-mt}.
     */
    private void startOutboundTokenRefresher() {
        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        if (Objects.isNull(outbound) || !outbound.isOauth2Enabled()) {
            return;
        }

        OAuth2ClientCredentialsFetcher fetcher = buildTokenFetcher(
                outbound, Constants.NF_TYPE_AMF, outbound.getScope());
        if (Objects.isNull(fetcher)) {
            return;
        }

        this.outboundTokenRefresher = startRefresher(fetcher, Constants.NF_TYPE_AMF, outbound.getScope(), true);
    }

    /**
     * Starts the UDM-audience refresher, whose token is consumed only by
     * {@code smsf-n21-registration}. Skipped when the gateway does not forward registrations or
     * does not require OAuth2 towards the UDM, so a deployment without N21 does not ask the NRF for
     * a token nobody reads.
     *
     * <p>Unlike the AMF one, a failure here is not fatal to the core: the SBI listener and the
     * ScyllaDB persistence keep working, and only the N21 forwarding is left without a credential.</p>
     */
    private void startUdmTokenRefresher() {
        Http2GatewayUDMSecurity udm = gateway.getUdmSecurity();
        if (Objects.isNull(udm) || !udm.isForwardRegistrationEnabled() || !udm.isOauth2Enabled()) {
            log.debug("UDM token refresher skipped for networkId {} — N21 forwarding or its OAuth2 is disabled",
                    gateway.getNetworkId());
            return;
        }
        Http2GatewayOutboundSecurity outbound = gateway.getOutboundSecurity();
        if (Objects.isNull(outbound)) {
            log.error("N21 OAuth2 is enabled for networkId {} but outbound_security is not configured — "
                    + "there are no client credentials to request a UDM access token with", gateway.getNetworkId());
            return;
        }

        String scope = appProperties.getN21Oauth2Scope();
        OAuth2ClientCredentialsFetcher fetcher = buildTokenFetcher(outbound, Constants.NF_TYPE_UDM, scope);
        if (Objects.isNull(fetcher)) {
            log.error("N21 OAuth2 is enabled for networkId {} but no usable token source is configured — "
                    + "enable using_nrf with an NRF token_path, or using_external_endpoint with a token_endpoint",
                    gateway.getNetworkId());
            return;
        }

        this.udmTokenRefresher = startRefresher(fetcher, Constants.NF_TYPE_UDM, scope, false);
    }

    /**
     * Bootstraps and starts a refresher that republishes every token it obtains to Redis.
     *
     * @param fatal when {@code true} a bootstrap failure aborts the gateway start; the AMF token is
     *              fatal because the core needs it to register against the NRF, the UDM token is not.
     */
    private OutboundTokenRefresher startRefresher(OAuth2ClientCredentialsFetcher fetcher,
                                                  String targetNfType, String scope, boolean fatal) {
        OutboundTokenRefresher refresher = new OutboundTokenRefresher(
                gateway.getNetworkId(),
                fetcher,
                new OutboundTokenTimings(
                        appProperties.getOutboundTokenRefreshMarginSeconds(),
                        appProperties.getOutboundTokenInitialFetchTimeoutSeconds(),
                        appProperties.getOutboundTokenRetryBackoffSeconds()),
                null,
                null,
                token -> publishToken(token, targetNfType, scope));
        try {
            refresher.bootstrap();
            refresher.start();
            log.info("Outbound token refresher started for networkId {} (targetNfType={}, scope={})",
                    gateway.getNetworkId(), targetNfType, scope);
            return refresher;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            refresher.stop();
            return handleRefresherFailure(targetNfType, fatal, ie);
        } catch (Exception ex) {
            refresher.stop();
            return handleRefresherFailure(targetNfType, fatal, ex);
        }
    }

    private OutboundTokenRefresher handleRefresherFailure(String targetNfType, boolean fatal, Exception ex) {
        String message = "Token bootstrap failed for networkId " + gateway.getNetworkId()
                + " (targetNfType=" + targetNfType + ")";
        if (fatal) {
            log.error("{}: {}", message, ex.getMessage(), ex);
            throw new IllegalStateException(message, ex);
        }
        log.error("{}: {} — the dependent module will have no credential until this recovers",
                message, ex.getMessage());
        return null;
    }

    /** Republishes a freshly obtained token to Redis for the modules that no longer call the NRF. */
    private void publishToken(OAuth2Token token, String targetNfType, String scope) {
        if (Objects.isNull(token)) {
            return;
        }
        tokenStore.publish(new SmsfSecurityToken(
                gateway.getNetworkId(),
                targetNfType,
                token.accessToken(),
                "Bearer",
                scope,
                token.issuedAt().getEpochSecond(),
                token.expiresAt().getEpochSecond()));
    }

    private void stopRefresher(OutboundTokenRefresher refresher, String targetNfType) {
        if (Objects.isNull(refresher)) {
            return;
        }
        try {
            refresher.stop();
        } catch (Exception ex) {
            log.warn("Error stopping the {} token refresher for networkId {}: {}",
                    targetNfType, gateway.getNetworkId(), ex.getMessage());
        } finally {
            // The entry must not outlive the gateway: a stopped gateway has no valid credential.
            tokenStore.evict(gateway.getNetworkId(), targetNfType);
        }
    }

    /**
     * Builds the OAuth2 token fetcher for the configured outbound source. The token may be obtained
     * either from the NRF (using the NRF client and its {@code token_path}) or from an external
     * token endpoint (using a dedicated HTTP/2 client). Returns {@code null} when the selected
     * source is not usable, in which case the reason is logged and the refresher is skipped.
     *
     * <p>{@code targetNfType} and {@code scope} are parameters because an access token is bound to
     * the audience it was minted for: the token used towards the AMF is not valid towards the UDM,
     * so each peer needs its own fetcher over the same credentials.</p>
     */
    private OAuth2ClientCredentialsFetcher buildTokenFetcher(Http2GatewayOutboundSecurity outbound,
                                                             String targetNfType, String scope) {
        if (outbound.isUsingNrf()) {
            return buildNrfTokenFetcher(outbound, targetNfType, scope);
        }
        if (outbound.isUsingExternalEndpoint()) {
            return buildExternalTokenFetcher(outbound, targetNfType, scope);
        }
        log.warn("Outbound token refresher skipped for networkId {} — neither NRF nor external endpoint is enabled",
                gateway.getNetworkId());
        return null;
    }

    private OAuth2ClientCredentialsFetcher buildNrfTokenFetcher(Http2GatewayOutboundSecurity outbound,
                                                                String targetNfType, String scope) {
        Http2GatewayNrfSecurity nrf = gateway.getNrfSecurity();
        if (Objects.isNull(nrf) || Objects.isNull(nrf.getTokenPath()) || nrf.getTokenPath().isBlank()) {
            log.warn("Outbound token refresher skipped for networkId {} — NRF token_path is missing",
                    gateway.getNetworkId());
            return null;
        }
        if (!nrfConnection.isConnected()) {
            log.warn("Outbound token refresher skipped for networkId {} — NRF client is not connected",
                    gateway.getNetworkId());
            return null;
        }
        return new OAuth2ClientCredentialsFetcher(
                nrfConnection.getClient(),
                nrf.getTokenPath(),
                outbound.getClientId(),
                outbound.getClientSecret(),
                scope,
                Objects.isNull(gateway.getNfInstanceId()) ? null : gateway.getNfInstanceId().toString(),
                Constants.NF_TYPE_SMSF,
                targetNfType);
    }

    private OAuth2ClientCredentialsFetcher buildExternalTokenFetcher(Http2GatewayOutboundSecurity outbound,
                                                                     String targetNfType, String scope) {
        String tokenEndpoint = outbound.getTokenEndpoint();
        if (Objects.isNull(tokenEndpoint) || tokenEndpoint.isBlank()) {
            log.warn("Outbound token refresher skipped for networkId {} — token_endpoint is missing",
                    gateway.getNetworkId());
            return null;
        }

        tokenConnection.connect();
        if (!tokenConnection.isConnected()) {
            log.warn("Outbound token refresher skipped for networkId {} — external token client is not connected",
                    gateway.getNetworkId());
            return null;
        }

        return new OAuth2ClientCredentialsFetcher(
                tokenConnection.getClient(),
                Http2HostUtils.parsePath(tokenEndpoint),
                outbound.getClientId(),
                outbound.getClientSecret(),
                scope,
                Objects.isNull(gateway.getNfInstanceId()) ? null : gateway.getNfInstanceId().toString(),
                Constants.NF_TYPE_SMSF,
                targetNfType);
    }
}
