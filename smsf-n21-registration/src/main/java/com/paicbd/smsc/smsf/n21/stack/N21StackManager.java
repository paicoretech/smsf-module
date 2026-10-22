package com.paicbd.smsc.smsf.n21.stack;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayUDMSecurity;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.http2.Http2ClientNrfConnection;
import com.paicbd.smsc.smsf.common.redis.CachedTokenAuthorizationProvider;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.n21.config.AppProperties;
import com.paicbd.smsc.utils.Converter;
import com.paicore.sba.stack.api.n21.N21Client;
import com.paicore.sba.stack.api.n21.N21Configuration;
import com.paicore.sba.stack.api.n21.SmsfContext;
import com.paicore.sba.stack.api.n21.SmsfRegistrationResponse;
import com.paicbd.smsc.smsf.common.event.SmsfRegistrationEvent;
import com.paicbd.smsc.smsf.common.event.RegistrationAction;
import com.paicore.sba.stack.openapi.model.UeSmsContextData;
import com.paicore.sba.stack.transport.http2.n21.N21ClientFactory;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-gateway N21 stack: consumes the registration events published by {@code smsf-core} and
 * forwards them to the UDM.
 *
 * <p>This is the N21 slice of what used to be a single {@code Http2StackManager}. The forwarding
 * logic, the {@code forward_registration_enabled} gate and the throttled restart of a client whose
 * startup failed are preserved as they were.</p>
 *
 * <p>Two things changed. The trigger is now a Kafka event rather than an in-process call from the
 * controller, so the AMF is answered without waiting for the UDM. And the access token comes from
 * Redis: this module no longer opens its own OAuth2 flow against the NRF, although it still needs
 * an NRF connection because UDM discovery cannot be served from Redis.</p>
 */
@Slf4j
@Getter
public class N21StackManager {

    private final Http2Gateway gateway;
    private final AppProperties appProperties;
    private final SmsfTokenCache tokenCache;
    private final ConsumerFactory<String, String> consumerFactory;

    private final Http2ClientNrfConnection nrfConnection;

    private N21Client n21Client;
    private long lastN21StartAttemptMs;
    private ConcurrentMessageListenerContainer<String, String> registrationContainer;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public N21StackManager(Http2Gateway gateway, AppProperties appProperties,
                           SmsfTokenCache tokenCache, ConsumerFactory<String, String> consumerFactory) {
        this.gateway = gateway;
        this.appProperties = appProperties;
        this.tokenCache = tokenCache;
        this.consumerFactory = consumerFactory;
        this.nrfConnection = new Http2ClientNrfConnection(gateway);
    }

    @Synchronized
    public void start() {
        if (!started.compareAndSet(false, true)) {
            log.warn("N21StackManager for networkId {} is already started", gateway.getNetworkId());
            return;
        }
        log.info("Starting SMSF N21 stack for networkId {}", gateway.getNetworkId());
        try {
            this.nrfConnection.connect();
            startN21Client();
            registerTokenTracking();
            log.info("SMSF N21 stack ready for networkId {}", gateway.getNetworkId());
        } catch (Exception ex) {
            log.error("Failed to start the SMSF N21 stack for networkId {}: {}",
                    gateway.getNetworkId(), ex.getMessage(), ex);
            started.set(false);
            stopInternal();
            throw ex;
        }
    }

    @Synchronized
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            log.warn("N21StackManager for networkId {} is already stopped", gateway.getNetworkId());
            return;
        }
        log.info("Stopping SMSF N21 stack for networkId {}", gateway.getNetworkId());
        stopInternal();
        log.info("SMSF N21 stack stopped for networkId {}", gateway.getNetworkId());
    }

    public boolean isStarted() {
        return started.get();
    }

    private void stopInternal() {
        unregisterTokenTracking();
        stopRegistrationConsumer();
        closeN21Client();
        nrfConnection.disconnect();
    }

    // --- Security -------------------------------------------------------------

    /** Gateways that do not use OAuth2 towards the UDM must not be gated on a token that never exists. */
    private boolean isOauth2Required() {
        Http2GatewayUDMSecurity udm = gateway.getUdmSecurity();
        return Objects.nonNull(udm) && udm.isOauth2Enabled();
    }

    /** {@code true} when a usable UDM token is cached, or when the gateway needs none. */
    private boolean hasRequiredCredential() {
        return !isOauth2Required() || tokenCache.isAvailable(gateway.getNetworkId(), Constants.NF_TYPE_UDM);
    }

    /**
     * Hands the credential's lifecycle to the shared cache, which gates the registration consumer.
     *
     * <p>A gateway that requires OAuth2 must not consume registration events it cannot forward:
     * doing so would take them off the topic and drop them. The consumer therefore stays down until
     * {@code smsf-core} publishes the token, and is stopped again if the credential is lost — the
     * events stay in Kafka in the meantime and are picked up on recovery.</p>
     */
    private void registerTokenTracking() {
        if (!isOauth2Required()) {
            startRegistrationConsumer();
            return;
        }
        tokenCache.register(gateway.getNetworkId(), Constants.NF_TYPE_UDM,
                this::onTokenAvailable, this::onTokenUnavailable);
    }

    private void unregisterTokenTracking() {
        if (isOauth2Required()) {
            tokenCache.unregister(gateway.getNetworkId(), Constants.NF_TYPE_UDM);
        }
    }

    private synchronized void onTokenAvailable() {
        if (Objects.isNull(registrationContainer)) {
            startRegistrationConsumer();
        }
    }

    private synchronized void onTokenUnavailable() {
        log.error("Gateway {} requires authentication towards the UDM but no usable token is available "
                        + "(Redis key {}) — the registration consumer is not started",
                gateway.getNetworkId(),
                com.paicbd.smsc.smsf.common.redis.SmsfTokenRedisStore.keyFor(
                        gateway.getNetworkId(), Constants.NF_TYPE_UDM));
        stopRegistrationConsumer();
    }

    // --- Kafka ----------------------------------------------------------------

    /**
     * One listener container per gateway on {@code <networkId>.smsf.udm.register}, so a gateway
     * being enabled or disabled starts and stops its own consumer.
     *
     * <p>Unlike the MT consumers this uses a plain Spring container rather than
     * {@code KafkaConsumerCustomImpl}: registrations carry no MPS throttling and the custom
     * consumer would drag a ScyllaDB dependency into a module that otherwise persists nothing.
     * Records that fail are simply not committed and are redelivered.</p>
     */
    private void startRegistrationConsumer() {
        String topic = gateway.getNetworkId() + KafkaTopicsConstants.SMSF_UDM_REGISTER_TOPIC_SUFFIX;
        String groupId = KafkaConsumerConstants.SMSF_UDM_REGISTER_GROUP_ID_PREFIX + gateway.getNetworkId();

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId(groupId);
        containerProperties.setMessageListener((MessageListener<String, String>) this::onRegistrationEvent);
        containerProperties.setShutdownTimeout(10000);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setConcurrency(Math.max(1, appProperties.getKafkaListenerConcurrency()));
        container.setBeanName("smsf-udm-register-" + gateway.getNetworkId());
        container.start();
        this.registrationContainer = container;
        log.info("Registration consumer started for networkId {} topic={} groupId={}",
                gateway.getNetworkId(), topic, groupId);
    }

    private void stopRegistrationConsumer() {
        if (Objects.nonNull(registrationContainer)) {
            try {
                registrationContainer.stop();
            } catch (Exception ex) {
                log.warn("Error stopping the registration consumer for networkId {}: {}",
                        gateway.getNetworkId(), ex.getMessage());
            } finally {
                registrationContainer = null;
            }
        }
    }

    private void onRegistrationEvent(ConsumerRecord<String, String> consumerRecord) {
        SmsfRegistrationEvent event;
        try {
            event = Converter.stringToObject(consumerRecord.value(), SmsfRegistrationEvent.class);
        } catch (Exception ex) {
            log.error("Discarding a malformed registration event on topic {}: {}", consumerRecord.topic(), ex.getMessage());
            return;
        }
        if (Objects.isNull(event) || Objects.isNull(event.getAction()) || Objects.isNull(event.getSupi())) {
            log.error("Discarding an incomplete registration event on topic {}", consumerRecord.topic());
            return;
        }
        if (RegistrationAction.DEREGISTER.equals(event.getAction())) {
            forwardDeregistration(event.getSupi());
            return;
        }
        if (Objects.isNull(event.getUeContext())) {
            log.error("Discarding a REGISTER event without ueContext for supi={}", event.getSupi());
            return;
        }
        forwardRegistration(event.getUeContext());
    }

    // --- N21 (SMSF -> UDM) ----------------------------------------------------

    /**
     * Starts the per-gateway N21 client from the 5g-sba-stack library, reusing the already-connected
     * NRF client for UDM discovery and the Redis-published UDM token for OAuth2. Best-effort: a
     * failure here disables forwarding but does not stop the gateway, and {@link #ensureN21Client()}
     * retries later.
     */
    private void startN21Client() {
        Http2GatewayUDMSecurity udm = gateway.getUdmSecurity();
        if (Objects.isNull(udm) || !udm.isForwardRegistrationEnabled()) {
            log.info("N21 UDM forwarding disabled for networkId {}", gateway.getNetworkId());
            return;
        }
        lastN21StartAttemptMs = System.currentTimeMillis();
        try {
            N21Configuration config = N21Configuration.builder()
                    .nrfDiscoveryEnabled(udm.isNrfDiscoveryEnabled())
                    .apiRoot(udm.getApiRoot())
                    .registrationPath(udm.getRegistrationPath())
                    .deregistrationPath(udm.getDeregistrationPath())
                    .subscriberQueryPath(udm.getSubscriberQueryPath())
                    .targetNfType(Constants.NF_TYPE_UDM)
                    .targetServiceName(Constants.SERVICE_NAME_NUDM_UECM)
                    .requesterNfType(Constants.NF_TYPE_SMSF)
                    .requesterNfInstanceId(Objects.toString(gateway.getNfInstanceId(), null))
                    .subscriberAwareDiscovery(appProperties.isN21SubscriberAwareDiscovery())
                    .discoveryCacheTtlSeconds(appProperties.getN21DiscoveryCacheTtlSeconds())
                    .maxEndpoints(appProperties.getN21MaxEndpoints())
                    .mtlsEnabled(udm.isMtlsEnabled())
                    .clientCertificatePath(udm.getClientCertificatePath())
                    .clientPrivateKeyPath(udm.getClientPrivateKeyPath())
                    .truststoreCaPath(udm.getTruststoreCaPath())
                    .oauth2Enabled(udm.isOauth2Enabled())
                    .authorizationHeaderName(udm.getAuthorizationHeader())
                    .connectionTimeoutMs(gateway.getConnectionTimeoutMs())
                    .requestTimeoutMs(gateway.getRequestTimeoutMs())
                    .idleTimeoutMs(gateway.getIdleTimeoutMs())
                    .build();

            SmsfContext smsfContext = SmsfContext.builder()
                    .smsfInstanceId(Objects.toString(gateway.getNfInstanceId(), null))
                    .smsfMapAddress(udm.getSmsfMapAddress())
                    .smsfDiameterAddress(udm.getSmsf_diameter_address())
                    .homePlmnMcc(appProperties.getN21HomePlmnMcc())
                    .homePlmnMnc(appProperties.getN21HomePlmnMnc())
                    // This SMSF delivers MT over SBI (HTTP/2 to the AMF), so the UDM must be
                    // told it can select us for SBI-based delivery.
                    .smsfSbiSupInd(true)
                    .build();

            N21Client client = N21ClientFactory.createWithProvider(config, smsfContext,
                    nrfConnection.getClient(),
                    new CachedTokenAuthorizationProvider(tokenCache, gateway.getNetworkId(),
                            Constants.NF_TYPE_UDM));
            client.connect();
            this.n21Client = client;
            log.info("N21 UDM client started for networkId {}", gateway.getNetworkId());
        } catch (Exception ex) {
            log.error("Failed to start N21 UDM client for networkId {}: {} — registration forwarding disabled",
                    gateway.getNetworkId(), ex.getMessage(), ex);
            closeN21Client();
        }
    }

    /**
     * Returns the N21 client, retrying a failed startup at most once every
     * {@code smsf.n21.restart-min-interval-seconds}. Without this a UDM that was down when the
     * gateway started would never be reached again, since the client is only built at startup.
     */
    private synchronized N21Client ensureN21Client() {
        if (Objects.nonNull(n21Client) || !started.get()) {
            return n21Client;
        }
        long minInterval = Math.max(1, appProperties.getN21RestartMinIntervalSeconds()) * 1000L;
        if (System.currentTimeMillis() - lastN21StartAttemptMs < minInterval) {
            return null;
        }
        log.info("Retrying N21 UDM client startup for networkId {}", gateway.getNetworkId());
        startN21Client();
        return n21Client;
    }

    /** Forwards an SMSF registration to the UDM over N21. Best-effort and non-fatal. */
    public void forwardRegistration(UeSmsContextData ueContext) {
        N21Client client = ensureN21Client();
        if (Objects.isNull(client)) {
            log.debug("N21 forwarding unavailable for networkId {} — skipping registration for supi={}",
                    gateway.getNetworkId(), ueContext.getSupi());
            return;
        }
        if (!hasRequiredCredential()) {
            log.error("Gateway {} requires authentication towards the UDM but no token was found in Redis "
                    + "— the registration for supi={} is not forwarded",
                    gateway.getNetworkId(), ueContext.getSupi());
            return;
        }
        try {
            SmsfRegistrationResponse res = client.registerSubscriber(
                    gateway.getUdmSecurity().getRegistrationPath(), ueContext);
            if (res.isSuccess()) {
                log.info("N21 registration OK supi={} networkId={} (HTTP {})",
                        ueContext.getSupi(), gateway.getNetworkId(), res.getStatusCode());
            } else {
                log.warn("N21 registration rejected supi={} networkId={} (HTTP {}): {}",
                        ueContext.getSupi(), gateway.getNetworkId(), res.getStatusCode(),
                        Objects.nonNull(res.getProblem()) ? res.getProblem().getDetail() : res.getRawBody());
            }
        } catch (Exception ex) {
            log.error("Could not perform the N21 registration request to the UDM for supi={} networkId={}: {}",
                    ueContext.getSupi(), gateway.getNetworkId(), ex.getMessage(), ex);
        }
    }

    /** Forwards an SMSF deregistration to the UDM over N21. Best-effort and non-fatal. */
    public void forwardDeregistration(String supi) {
        N21Client client = ensureN21Client();
        if (Objects.isNull(client)) {
            log.debug("N21 forwarding unavailable for networkId {} — skipping deregistration for supi={}",
                    gateway.getNetworkId(), supi);
            return;
        }
        if (!hasRequiredCredential()) {
            log.error("Gateway {} requires authentication towards the UDM but no token was found in Redis "
                    + "— the deregistration for supi={} is not forwarded",
                    gateway.getNetworkId(), supi);
            return;
        }
        try {
            UeSmsContextData ueContext = new UeSmsContextData();
            ueContext.setSupi(supi);
            SmsfRegistrationResponse res = client.deregisterSubscriber(
                    gateway.getUdmSecurity().getDeregistrationPath(), ueContext);
            if (res.isSuccess()) {
                log.info("N21 deregistration OK supi={} networkId={} (HTTP {})",
                        supi, gateway.getNetworkId(), res.getStatusCode());
            } else {
                log.warn("N21 deregistration rejected supi={} networkId={} (HTTP {})",
                        supi, gateway.getNetworkId(), res.getStatusCode());
            }
        } catch (Exception ex) {
            log.error("Could not perform the N21 deregistration request to the UDM for supi={} networkId={}: {}",
                    supi, gateway.getNetworkId(), ex.getMessage(), ex);
        }
    }

    private void closeN21Client() {
        if (Objects.nonNull(n21Client)) {
            try {
                n21Client.close();
            } catch (Exception ex) {
                log.warn("Error closing N21 UDM client for networkId {}: {}",
                        gateway.getNetworkId(), ex.getMessage());
            } finally {
                n21Client = null;
            }
        }
    }
}
