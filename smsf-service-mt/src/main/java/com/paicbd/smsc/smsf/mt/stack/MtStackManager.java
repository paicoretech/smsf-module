package com.paicbd.smsc.smsf.mt.stack;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaConsumerCustomImpl;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaConsumerHandler;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.common.processors.MessageTpduProcessor;
import com.paicbd.smsc.smsf.common.redis.SmsfTokenCache;
import com.paicbd.smsc.smsf.mt.config.AppProperties;
import com.paicbd.smsc.smsf.mt.http2.Http2ClientConnection;
import com.paicbd.smsc.smsf.mt.processors.MessageProcessor;
import com.paicbd.smsc.smsf.common.processors.MtTimings;
import com.paicbd.smsc.smsf.common.http2.Http2HostUtils;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.paicbd.smsc.kafka.KafkaConsumerConstants.SMSF_HIGH_MESSAGE_GROUP_ID_PREFIX;
import static com.paicbd.smsc.kafka.KafkaConsumerConstants.SMSF_LOW_MESSAGE_GROUP_ID_PREFIX;
import static com.paicbd.smsc.kafka.KafkaConsumerConstants.SMSF_MEDIUM_MESSAGE_GROUP_ID_PREFIX;
import static com.paicbd.smsc.kafka.KafkaTopicsConstants.SMSF_HIGH_MESSAGE_TOPIC_SUFFIX;
import static com.paicbd.smsc.kafka.KafkaTopicsConstants.SMSF_LOW_MESSAGE_TOPIC_SUFFIX;
import static com.paicbd.smsc.kafka.KafkaTopicsConstants.SMSF_MEDIUM_MESSAGE_TOPIC_SUFFIX;

/**
 * Per-gateway MT stack: the priority consumers, the poll loop and the AMF client.
 *
 * <p>This is the MT slice of what used to be a single {@code Http2StackManager}. The priority
 * behaviour is unchanged — three consumers weighted by {@code messages_per_second_*}, drained once
 * a second by {@link KafkaConsumerHandler#fetchMessages}.</p>
 *
 * <p>The one behavioural difference is where the credential comes from. The module no longer mints
 * its own token: {@link SmsfTokenCache} holds the AMF-audience token that {@code smsf-core}
 * publishes to Redis, refreshed in the background from its own expiry, so sending a message is a
 * memory read. The previous guarantee is preserved through the cache's callbacks — when the
 * credential is lost the consumers are paused instead of sending unauthenticated requests to the
 * AMF, and they resume as soon as a fresh token lands.</p>
 */
@Slf4j
@Getter
public class MtStackManager extends KafkaConsumerHandler {

    private final Http2Gateway gateway;
    private final AppProperties appProperties;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScyllaManager scyllaManager;
    private final SmsfTokenCache tokenCache;
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;

    private final Http2ClientConnection clientConnection;
    private final MessageProcessor messageProcessing;
    private final MessageFactory messageFactory;
    private final CdrProcessor cdrProcessor = new CdrProcessor();

    private KafkaConsumerCustomImpl kafkaConsumerHigh;
    private KafkaConsumerCustomImpl kafkaConsumerMedium;
    private KafkaConsumerCustomImpl kafkaConsumerLow;

    private ScheduledExecutorService processorScheduler;
    private ScheduledFuture<?> processorTask;
    private ExecutorService dispatchExecutor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean pausedForToken = new AtomicBoolean(false);
    private final AtomicBoolean pausedForConnection = new AtomicBoolean(false);

    private ScheduledExecutorService connectionScheduler;
    private ScheduledFuture<?> connectionTask;

    public MtStackManager(Http2Gateway gateway, AppProperties appProperties,
                          KafkaConsumerFactory kafkaConsumerFactory, KafkaTemplate<String, String> kafkaTemplate,
                          ScyllaManager scyllaManager, SmsfTokenCache tokenCache,
                          ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap) {
        this.gateway = gateway;
        this.appProperties = appProperties;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.scyllaManager = scyllaManager;
        this.tokenCache = tokenCache;
        this.errorCodeMappingConcurrentHashMap = errorCodeMappingConcurrentHashMap;
        this.clientConnection = new Http2ClientConnection(gateway);
        this.messageFactory = new MessageFactory(gateway, errorCodeMappingConcurrentHashMap,
                new MessageTpduProcessor(new Ss7SmsTpduCodec()));
        this.messageProcessing = new MessageProcessor(gateway, clientConnection, messageFactory, scyllaManager,
                kafkaTemplate, cdrProcessor, this::currentBearerHeader, buildFailureNotificationUri(),
                new MtTimings(appProperties.getCorrelationWaitSeconds()));
    }

    private String buildFailureNotificationUri() {
        String apiRoot = appProperties.getSbiServerApiRoot();
        if (Objects.isNull(apiRoot) || apiRoot.isBlank()) {
            log.warn("The N1N2 failure callback will not be advertised for networkId {}: smsf.sbi.server.apiroot is not set",
                    gateway.getNetworkId());
            return null;
        }
        return trimTrailingSlashes(apiRoot)
                + appProperties.getSbiServerBasePath()
                + "/" + gateway.getNetworkId()
                + Constants.NAMF_CALLBACK_PATH
                + Constants.N1N2_FAILURE_SUFFIX;
    }

    /**
     * Strips the trailing slashes the operator may have left on the configured apiRoot. Done by
     * hand rather than with a regular expression: a pattern anchored on a repeated character
     * backtracks over the whole tail on a long input.
     */
    private static String trimTrailingSlashes(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }

    @Synchronized
    public void start() {
        if (!started.compareAndSet(false, true)) {
            log.warn("MtStackManager for networkId {} is already started", gateway.getNetworkId());
            return;
        }
        log.info("Starting SMSF MT stack for networkId {}", gateway.getNetworkId());
        try {
            requireAbsoluteHost();
            this.clientConnection.connect();
            initializeConsumers();
            startSenderResources();
            startProcessor();
            registerTokenTracking();
            startConnectionWatchdog();
            log.info("SMSF MT stack ready for networkId {}", gateway.getNetworkId());
        } catch (Exception ex) {
            log.error("Failed to start the SMSF MT stack for networkId {}: {}",
                    gateway.getNetworkId(), ex.getMessage(), ex);
            started.set(false);
            stopInternal();
            throw ex;
        }
    }

    /**
     * Rejects a gateway host that carries no scheme.
     *
     * <p>The MT flow derives the request path from this value, and
     * {@link Http2HostUtils#parsePath(String)} can only tell the authority from the path when a
     * scheme is present. Given {@code amf.operator.com} it has no way to know that is a host, so it
     * returns the whole string as the path and every delivery goes out to a nonsensical URI. The
     * connection still opens — the port defaults — so without this check the misconfiguration
     * surfaces as the AMF rejecting every message, far from its cause.</p>
     */
    private void requireAbsoluteHost() {
        String host = gateway.getHost();
        if (Objects.isNull(host) || !host.contains("://")) {
            throw new IllegalArgumentException(String.format(
                    "The host of gateway %d must be an absolute URL including the scheme, but it is \"%s\". "
                            + "Expected something like https://amf.operator.com/nsmsf-sms/v2/ue-contexts/{supi}/sendsms",
                    gateway.getNetworkId(), host));
        }
    }

    @Synchronized
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            log.warn("MtStackManager for networkId {} is already stopped", gateway.getNetworkId());
            // Released anyway. The throughput watchers are started by the MessageProcessor
            // constructor, not by start(), so a manager that was built and never started still owns
            // two live watcher threads. Everything stopInternal touches is null-guarded and
            // idempotent, so running it twice is safe and running it here closes the leak.
            stopInternal();
            return;
        }
        log.info("Stopping SMSF MT stack for networkId {}", gateway.getNetworkId());
        stopInternal();
        log.info("SMSF MT stack stopped for networkId {}", gateway.getNetworkId());
    }

    public boolean isStarted() {
        return started.get();
    }

    private void stopInternal() {
        stopConnectionWatchdog();
        unregisterTokenTracking();
        stopProcessor();
        stopConsumers();
        stopSenderResources();
        messageProcessing.stopWatchers();
        clientConnection.disconnect();
    }

    // --- Security -------------------------------------------------------------

    /**
     * The AMF bearer, read from memory. {@link SmsfTokenCache} keeps it fresh in the background, so
     * the MT hot path never touches Redis.
     */
    private Optional<String> currentBearerHeader() {
        return tokenCache.authorizationHeader(gateway.getNetworkId(), Constants.NF_TYPE_AMF);
    }

    /** Gateways that do not use OAuth2 towards the AMF must not be gated on a token that never exists. */
    private boolean isOauth2Required() {
        return Objects.nonNull(gateway.getOutboundSecurity()) && gateway.getOutboundSecurity().isOauth2Enabled();
    }

    /**
     * Hands the credential's lifecycle to the shared cache. The consumers only run while a usable
     * token is cached: when the cache reports the credential lost — expired without smsf-core
     * publishing a replacement — they are paused rather than sending unauthenticated MT to the AMF,
     * and resumed as soon as a fresh token lands.
     */
    private void registerTokenTracking() {
        if (!isOauth2Required()) {
            log.info("Token tracking not needed for networkId {} — OAuth2 towards the AMF is disabled",
                    gateway.getNetworkId());
            startConsumers();
            return;
        }
        // Closed until the cache says otherwise: nothing is consumed before there is a credential.
        pausedForToken.set(true);
        tokenCache.register(gateway.getNetworkId(), Constants.NF_TYPE_AMF,
                this::onTokenAvailable, this::onTokenUnavailable);
    }

    private void unregisterTokenTracking() {
        if (isOauth2Required()) {
            tokenCache.unregister(gateway.getNetworkId(), Constants.NF_TYPE_AMF);
        }
        pausedForToken.set(false);
    }

    private void onTokenAvailable() {
        if (pausedForToken.compareAndSet(true, false)) {
            log.info("MT consumers enabled for networkId {} — a usable AMF token is cached",
                    gateway.getNetworkId());
        }
        resumeConsumersIfReady();
    }

    private void onTokenUnavailable() {
        if (pausedForToken.compareAndSet(false, true)) {
            log.warn("MT consumers paused for networkId {} — no usable AMF token", gateway.getNetworkId());
        }
        pauseConsumers();
    }

    // --- AMF connection -------------------------------------------------------

    /**
     * Watches the AMF connection for the life of the stack.
     *
     * <p>{@code connect()} is called once at startup, and until now nothing noticed if the link
     * later went away: the consumers kept draining Kafka and every delivery failed against a dead
     * client. A TLS 1.3 handshake makes that easy to hit — the client finishes before the AMF has
     * judged its certificate, so a rejected certificate looks like a successful connect and the
     * channel dies moments later.</p>
     *
     * <p>On a drop the consumers are paused so messages stay on their topics instead of being
     * consumed and lost, and {@code connect()} is retried at the configured interval until it
     * takes. Recovery needs no restart.</p>
     */
    private void startConnectionWatchdog() {
        long intervalSeconds = Math.max(1L, appProperties.getAmfConnectionCheckIntervalSeconds());
        this.connectionScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("Smsf-Mt-Amf-Watchdog-NetworkId-" + gateway.getNetworkId());
            thread.setDaemon(true);
            return thread;
        });
        this.connectionTask = connectionScheduler.scheduleWithFixedDelay(
                this::checkConnection, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    private void checkConnection() {
        try {
            if (!started.get()) {
                return;
            }
            if (clientConnection.isConnected()) {
                onConnectionRestored();
                return;
            }
            onConnectionLost();
            clientConnection.connect();
            if (clientConnection.isConnected()) {
                onConnectionRestored();
            }
        } catch (Exception ex) {
            log.error("AMF connection watchdog failed for networkId {}: {}",
                    gateway.getNetworkId(), ex.getMessage(), ex);
        }
    }

    /** Logged once per outage, not once per attempt, so a long outage does not flood the log. */
    private void onConnectionLost() {
        if (pausedForConnection.compareAndSet(false, true)) {
            log.error("The AMF connection of gateway {} is down — MT consumers are paused and the "
                            + "connection will be retried every {}s. Messages stay on their topics "
                            + "until it is restored.",
                    gateway.getNetworkId(), Math.max(1L, appProperties.getAmfConnectionCheckIntervalSeconds()));
            pauseConsumers();
        }
    }

    private void onConnectionRestored() {
        if (pausedForConnection.compareAndSet(true, false)) {
            log.info("The AMF connection of gateway {} is back — resuming MT consumers",
                    gateway.getNetworkId());
            resumeConsumersIfReady();
        }
    }

    private void stopConnectionWatchdog() {
        if (Objects.nonNull(connectionTask)) {
            connectionTask.cancel(true);
            connectionTask = null;
        }
        if (Objects.nonNull(connectionScheduler)) {
            connectionScheduler.shutdownNow();
            connectionScheduler = null;
        }
        pausedForConnection.set(false);
    }

    // --- Consumers and dispatch ----------------------------------------------

    private void initializeConsumers() {
        int networkId = gateway.getNetworkId();
        int mpsHigh = gateway.getMessagesPerSecondHigh();
        int mpsMedium = gateway.getMessagesPerSecondMedium();
        int mpsLow = gateway.getMessagesPerSecondLow();
        int mps = Math.max(1, gateway.getMessagesPerSecond());

        if (mpsHigh > 0) {
            kafkaConsumerHigh = new KafkaConsumerCustomImpl(
                    networkId + SMSF_HIGH_MESSAGE_TOPIC_SUFFIX,
                    SMSF_HIGH_MESSAGE_GROUP_ID_PREFIX + networkId,
                    appProperties.getKafkaBootstrapServers(),
                    mps, kafkaConsumerFactory, scyllaManager);
        }
        if (mpsMedium > 0) {
            kafkaConsumerMedium = new KafkaConsumerCustomImpl(
                    networkId + SMSF_MEDIUM_MESSAGE_TOPIC_SUFFIX,
                    SMSF_MEDIUM_MESSAGE_GROUP_ID_PREFIX + networkId,
                    appProperties.getKafkaBootstrapServers(),
                    mps, kafkaConsumerFactory, scyllaManager);
        }
        if (mpsLow > 0) {
            kafkaConsumerLow = new KafkaConsumerCustomImpl(
                    networkId + SMSF_LOW_MESSAGE_TOPIC_SUFFIX,
                    SMSF_LOW_MESSAGE_GROUP_ID_PREFIX + networkId,
                    appProperties.getKafkaBootstrapServers(),
                    mps, kafkaConsumerFactory, scyllaManager);
        }
    }




    private void startSenderResources() {
        ThreadFactory factory = Thread.ofVirtual()
                .name("Smsf-Mt-Dispatch-NetworkId-" + gateway.getNetworkId() + "-", 1)
                .factory();
        this.dispatchExecutor = Executors.newThreadPerTaskExecutor(factory);
    }

    private synchronized void startConsumers() {
        if (Objects.nonNull(kafkaConsumerHigh)) kafkaConsumerHigh.startConsumer();
        if (Objects.nonNull(kafkaConsumerMedium)) kafkaConsumerMedium.startConsumer();
        if (Objects.nonNull(kafkaConsumerLow)) kafkaConsumerLow.startConsumer();
    }

    /**
     * Consumption has two independent gates — a usable AMF token and a live AMF connection. Either
     * one can pause it, so neither may resume it on its own.
     *
     * <p>The token gate starts <em>closed</em> when OAuth2 is required, not open. It is only opened
     * by the cache reporting a usable token. Were it to start open, a cold start with nothing
     * published yet would let a reconnection start the consumers with no credential at all.</p>
     */
    private void resumeConsumersIfReady() {
        if (pausedForToken.get() || pausedForConnection.get()) {
            return;
        }
        startConsumers();
    }

    private synchronized void pauseConsumers() {
        if (Objects.nonNull(kafkaConsumerHigh)) kafkaConsumerHigh.stopConsumer();
        if (Objects.nonNull(kafkaConsumerMedium)) kafkaConsumerMedium.stopConsumer();
        if (Objects.nonNull(kafkaConsumerLow)) kafkaConsumerLow.stopConsumer();
    }

    private void startProcessor() {
        this.processorScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("Smsf-Mt-Processor-NetworkId-" + gateway.getNetworkId());
            t.setDaemon(true);
            return t;
        });
        this.processorTask = processorScheduler.scheduleAtFixedRate(this::pollMessages, 0, 1, TimeUnit.SECONDS);
    }

    private void pollMessages() {
        try {
            if (!started.get() || pausedForToken.get() || pausedForConnection.get()) return;
            int mps = Math.max(1, gateway.getMessagesPerSecond());
            var messages = fetchMessages(
                    mps,
                    gateway.getMessagesPerSecondHigh(),
                    gateway.getMessagesPerSecondMedium(),
                    gateway.getMessagesPerSecondLow(),
                    kafkaConsumerHigh, kafkaConsumerMedium, kafkaConsumerLow);
            if (messages.isEmpty()) return;
            dispatchAll(messages);
        } catch (Exception ex) {
            log.error("[networkId={}] Error polling kafka messages: {}", gateway.getNetworkId(), ex.getMessage(), ex);
        }
    }

    private void dispatchAll(List<MessageEvent> events) {
        for (MessageEvent event : events) {
            dispatchExecutor.execute(() -> messageProcessing.processOutgoingRequest(event));
        }
    }

    private void stopProcessor() {
        if (Objects.nonNull(processorTask)) {
            processorTask.cancel(true);
            processorTask = null;
        }
        if (Objects.nonNull(processorScheduler)) {
            processorScheduler.shutdownNow();
            processorScheduler = null;
        }
    }

    private void stopConsumers() {
        pauseConsumers();
        kafkaConsumerHigh = null;
        kafkaConsumerMedium = null;
        kafkaConsumerLow = null;
    }

    private void stopSenderResources() {
        if (Objects.nonNull(dispatchExecutor)) {
            dispatchExecutor.shutdownNow();
            dispatchExecutor = null;
        }
    }
}
