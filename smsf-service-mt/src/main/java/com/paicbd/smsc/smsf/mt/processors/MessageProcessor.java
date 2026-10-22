package com.paicbd.smsc.smsf.mt.processors;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.mt.http2.Http2ClientConnection;
import com.paicbd.smsc.smsf.common.http2.Http2HostUtils;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.common.processors.MtDeliveryCloser;
import com.paicbd.smsc.smsf.common.processors.MtOutcome;
import com.paicbd.smsc.smsf.common.processors.MtTimings;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.UtilsEnum;
import com.paicbd.smsc.utils.Watcher;
import com.paicore.sba.stack.api.error.ProblemDetailsResponse;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import com.paicore.sba.stack.openapi.model.N1N2MessageTransferError;
import com.paicore.sba.stack.openapi.model.N1N2MessageTransferRspData;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import com.paicore.sba.stack.utils.SdkConverter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.paicbd.smsc.smsf.common.constants.Constants.CAUSE_USER_NOT_FOUND;
import static com.paicbd.smsc.smsf.common.constants.Constants.SUPI_PLACEHOLDER;
import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;
import static com.paicore.sba.stack.transport.http2.util.Http2Constants.HEADER_CONTENT_TYPE;

@Slf4j
public class MessageProcessor implements RequestProcessor {

    private static final String AUTHORIZATION_HEADER = "authorization";
    private static final int HTTP2_SERVICE_UNAVAILABLE = 503;
    private static final int HTTP2_GATEWAY_TIMEOUT = 504;
    private static final int HTTP2_NOT_FOUND = 404;
    private static final int DLR_CORRELATION_TTL_SECONDS = 300;
    private static final int FAILURE_CORRELATION_TTL_MARGIN_SECONDS = 60;
    private static final String CAUSE_NO_FAILURE_NOTIFICATION = "NO_FAILURE_NOTIFICATION_RECEIVED";
    private static final int MAX_PENDING_CORRELATION_TIMERS = 50_000;

    private final Http2Gateway gateway;
    private final Http2ClientConnection clientConnection;
    private final MessageFactory messageFactory;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Supplier<Optional<String>> bearerHeaderSupplier;
    private final String failureNotificationUriBase;
    private final MtTimings timings;
    private final MtDeliveryCloser closer;

    private final List<Watcher> watchers = new ArrayList<>();
    private final AtomicInteger processMtPerSecond = new AtomicInteger(0);
    private final AtomicInteger processDlrPerSecond = new AtomicInteger(0);
    private final AtomicInteger pendingCorrelationTimers = new AtomicInteger(0);
    private final Executor completionExecutor;
    private final ScheduledExecutorService correlationWatchdog;

    public MessageProcessor(Http2Gateway gateway,
                            Http2ClientConnection clientConnection,
                            MessageFactory messageFactory,
                            ScyllaManager scyllaManager,
                            KafkaTemplate<String, String> kafkaTemplate,
                            CdrProcessor cdrProcessor,
                            Supplier<Optional<String>> bearerHeaderSupplier,
                            String failureNotificationUri,
                            MtTimings timings) {
        this(gateway, clientConnection, messageFactory, scyllaManager, kafkaTemplate, cdrProcessor,
                bearerHeaderSupplier, failureNotificationUri, timings, Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Runs the MT completion callback on a caller-supplied executor instead of
     * the default virtual thread pool. Used by the tests to execute the callback
     * on the calling thread so the response and CDR assertions stay deterministic.
     */
    public MessageProcessor(Http2Gateway gateway,
                            Http2ClientConnection clientConnection,
                            MessageFactory messageFactory,
                            ScyllaManager scyllaManager,
                            KafkaTemplate<String, String> kafkaTemplate,
                            CdrProcessor cdrProcessor,
                            Supplier<Optional<String>> bearerHeaderSupplier,
                            String failureNotificationUri,
                            MtTimings timings,
                            Executor completionExecutor) {
        this.completionExecutor = completionExecutor;
        this.timings = timings;
        this.correlationWatchdog = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "smsf-mt-correlation-watchdog-" + gateway.getNetworkId());
            thread.setDaemon(true);
            return thread;
        });
        this.failureNotificationUriBase = failureNotificationUri;
        this.gateway = gateway;
        this.clientConnection = clientConnection;
        this.messageFactory = messageFactory;
        this.scyllaManager = scyllaManager;
        this.kafkaTemplate = kafkaTemplate;
        this.bearerHeaderSupplier = Objects.requireNonNullElse(bearerHeaderSupplier, Optional::empty);
        this.closer = new MtDeliveryCloser(gateway, scyllaManager, kafkaTemplate, cdrProcessor,
                messageFactory);
        this.addAndInitializeWatchers();
    }

    /**
     * Two rates, not one. Both leave over the same MT path, but a delivery receipt is not a message
     * delivery: counting them together makes the MT rate read higher than the traffic actually
     * carried, and hides a DLR storm inside what looks like healthy throughput.
     */
    private void addAndInitializeWatchers() {
        int networkId = gateway.getNetworkId();
        this.watchers.add(new Watcher("Process-MT-Message-NetworkId-" + networkId, processMtPerSecond, 1));
        this.watchers.add(new Watcher("Process-MT-Dlr-NetworkId-" + networkId, processDlrPerSecond, 1));
    }

    @Override
    public void processIncomingRequest(Http2Request request) {
        log.debug("[networkId={}] Incoming HTTP/2 message request path={}",
                gateway.getNetworkId(), request.getPath());
    }

    @Override
    public void processOutgoingRequest(MessageEvent event) {
        if (!event.isDlr()
                && Objects.nonNull(event.getMessageParts())
                && !event.getMessageParts().isEmpty()) {
            processPartsMessage(event);
            return;
        }
        sendSingleRequest(event);
    }

    /**
     * Splits a concatenated (multipart) {@link MessageEvent} delivered as a
     * single event by the routing module into one MT {@code sendsms} request per
     * segment. Each part carries its own UDH so it is encoded and transmitted as
     * an independent HTTP/2 request, mirroring the platform delivery pattern.
     */
    private void processPartsMessage(MessageEvent event) {
        for (MessagePart msgPart : event.getMessageParts()) {
            MessageEvent partEvent = Converter.deepCopy(event, MessageEvent.class);
            partEvent.setMessageId(msgPart.getMessageId());
            partEvent.setShortMessage(msgPart.getShortMessage());
            partEvent.setMsgReferenceNumber(msgPart.getMsgReferenceNumber());
            partEvent.setTotalSegment(msgPart.getTotalSegment());
            partEvent.setSegmentSequence(msgPart.getSegmentSequence());
            partEvent.setOptionalParameters(msgPart.getOptionalParameters());
            partEvent.setMessageBytes(msgPart.getPartBytes());
            byte[] udhBytes = msgPart.getUdhBytes();
            partEvent.setUdhBytes(udhBytes);
            partEvent.setUdhLength(Objects.nonNull(udhBytes) ? udhBytes.length : 0);
            partEvent.setUdhRaw(msgPart.getUdhRaw());
            partEvent.setMessageParts(null);
            partEvent.setSplitForSmsc(true);
            if (!partEvent.isFinalSegmentForSplitSmsc()) {
                partEvent.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());
            }
            partEvent.addCustomParam(msgPart.getCustomParams());
            sendSingleRequest(partEvent);
        }
    }

    private void sendSingleRequest(MessageEvent event) {
        if (Objects.isNull(event.getDestination()) || event.getDestination().isBlank()) {
            event.setErrorCode(HTTP2_NOT_FOUND);
            deleteSubmitSmResult(event);
            processCdr(event, UtilsEnum.CdrStatus.FAILED, buildFailureComment(HTTP2_NOT_FOUND, CAUSE_USER_NOT_FOUND));
            log.warn("[networkId={}] MT dropped: destinationSupi not resolved for messageId={} destAddr={}",
                    gateway.getNetworkId(), event.getMessageId(), event.getDestinationAddr());
            return;
        }
        Http2Request request;
        try {
            request = buildRequest(event);
        } catch (RuntimeException ex) {
            event.setErrorCode(ErrorCodes.SYSTEM_ERROR);
            processCdr(event, UtilsEnum.CdrStatus.FAILED);
            log.error("[networkId={}] Failed to build MT request for messageId={}: {}",
                    gateway.getNetworkId(), event.getMessageId(), ex.getMessage(), ex);
            return;
        }

        persistSubmitSmResult(event);
        try {
            clientConnection.send(request)
                    .whenCompleteAsync((response, error) -> {
                        if (Objects.nonNull(error)) {
                            processError(error, event);
                        } else {
                            processResponse(response, event);
                        }
                    }, completionExecutor);
        } catch (RuntimeException ex) {
            processError(ex, event);
            return;
        }
        if (event.isDlr()) {
            processDlrPerSecond.incrementAndGet();
        } else {
            processMtPerSecond.incrementAndGet();
        }
    }

    /**
     * Stops the per-gateway throughput watchers. Invoked when the owning HTTP/2
     * stack is stopped so the scheduled watcher thread is released on gateway
     * teardown or restart.
     */
    @Override
    public void stopWatchers() {
        watchers.forEach(Watcher::stopWatching);
        correlationWatchdog.shutdownNow();
        if (completionExecutor instanceof ExecutorService executorService) {
            executorService.shutdown();
        }
    }

    public int getProcessMtPerSecond() {
        return processMtPerSecond.get();
    }

    public int getProcessDlrPerSecond() {
        return processDlrPerSecond.get();
    }

    @Override
    public void processResponse(Http2Response response, MessageEvent originalEvent) {
        if (Objects.isNull(response)) {
            handleFailure(originalEvent, ErrorCodes.SYSTEM_ERROR, null);
            return;
        }

        String cause = extractResponseCause(response);
        MtOutcome outcome = MtOutcome.fromCause(cause);

        log.debug("[networkId={}] HTTP/2 message response status={} cause={} outcome={} messageId={}",
                gateway.getNetworkId(), response.getStatus(), cause, outcome, originalEvent.getMessageId());

        if (MtOutcome.PENDING.equals(outcome)) {
            int waitSeconds = correlationWaitSeconds(extractMaxWaitingTime(response), originalEvent);
            persistFailureCorrelation(originalEvent, waitSeconds);
            scheduleCorrelationTimeout(originalEvent.getMessageId(), waitSeconds);
            log.info("[networkId={}] MT messageId={} destination={} left open: the AMF answered {} and has {}s to report back",
                    gateway.getNetworkId(), originalEvent.getMessageId(), originalEvent.getDestinationAddr(),
                    cause, waitSeconds);
            return;
        }

        if (MtOutcome.ABSENT.equals(outcome)) {
            handleAbsentSubscriber(originalEvent, cause);
            return;
        }

        if (MtOutcome.RETRYABLE.equals(outcome)) {
            int errorCode = MtOutcome.RETRYABLE.getErrorCode();
            handleFailure(originalEvent, errorCode, buildFailureComment(errorCode, cause));
            return;
        }

        if (response.getStatus() < 200 || response.getStatus() >= 300) {
            handleFailure(originalEvent, response.getStatus(), buildFailureComment(response.getStatus(), cause));
            return;
        }

        if (!MtOutcome.DELIVERED.equals(outcome)) {
            int errorCode = ErrorCodes.SMSF_UE_TEMPORARY_REJECT;
            log.warn("[networkId={}] MT messageId={} answered {} without a recognisable cause ({}); not counted as delivered",
                    gateway.getNetworkId(), originalEvent.getMessageId(), response.getStatus(), cause);
            handleFailure(originalEvent, errorCode, buildFailureComment(errorCode, cause));
            return;
        }

        if (!originalEvent.isDlr()) {
            originalEvent.setErrorCode(null);
        }
        processCdr(originalEvent, UtilsEnum.CdrStatus.SUCCESS);
    }


    /**
     * Keeps the correlation alive while the AMF is still trying to reach the UE. The row is written
     * even when the message did not ask for a delivery report: the failure callback needs the
     * original event to raise the CDR and to mark the subscriber, not only to answer a DLR.
     *
     * <p>The lifetime comes from the {@code maxWaitingTime} the AMF reported, plus a margin, so the
     * correlation outlives the AMF's own timer instead of expiring under it. Without that value the
     * default DLR lifetime is used.</p>
     */
    private void persistFailureCorrelation(MessageEvent event, int waitSeconds) {
        int ttl = waitSeconds + FAILURE_CORRELATION_TTL_MARGIN_SECONDS;
        try {
            scyllaManager.insertIntoTable(SMSF_SUBMIT_SM_RESULT_TABLE, event.getMessageId(),
                    Converter.valueAsString(event), ttl);
            log.debug("[networkId={}] N1N2 failure correlation stored messageId={} ttl={}s",
                    gateway.getNetworkId(), event.getMessageId(), ttl);
        } catch (RuntimeException ex) {
            log.warn("[networkId={}] Failed to store the N1N2 failure correlation messageId={}: {}",
                    gateway.getNetworkId(), event.getMessageId(), ex.getMessage(), ex);
        }
    }

    /**
     * How long to wait for the failure callback: the {@code maxWaitingTime} the AMF reported, or the
     * default, and never longer than the validity the message has left — waiting past it would close
     * a delivery the SMSC already considers expired.
     */
    int correlationWaitSeconds(Integer maxWaitingTime, MessageEvent event) {
        int wait = Objects.nonNull(maxWaitingTime) && maxWaitingTime > 0
                ? maxWaitingTime
                : timings.correlationWaitSeconds();
        long validity = event.getValidityPeriod();
        return validity > 0 && validity < wait ? (int) validity : wait;
    }

    /**
     * Scylla expiry is silent, so nothing would ever close a delivery the AMF accepted and then
     * never reported on. The timer fires a margin before the row expires and closes it with
     * {@code 525}, without deactivating: a callback that never arrives is missing information, not
     * evidence that the subscriber is absent.
     *
     * <p>The timer lives in memory: a module restart loses the pending ones and those rows expire on
     * their TTL without a CDR.</p>
     */
    private void scheduleCorrelationTimeout(String messageId, int waitSeconds) {
        if (pendingCorrelationTimers.get() >= MAX_PENDING_CORRELATION_TIMERS) {
            log.warn("[networkId={}] {} correlation timers already armed; messageId={} will expire on its TTL without a CDR",
                    gateway.getNetworkId(), MAX_PENDING_CORRELATION_TIMERS, messageId);
            return;
        }
        pendingCorrelationTimers.incrementAndGet();
        try {
            correlationWatchdog.schedule(() -> {
                pendingCorrelationTimers.decrementAndGet();
                closeOnCompletionExecutor(messageId);
            }, waitSeconds, TimeUnit.SECONDS);
        } catch (RejectedExecutionException ex) {
            pendingCorrelationTimers.decrementAndGet();
            log.warn("[networkId={}] Could not arm the correlation timeout for messageId={}: {}",
                    gateway.getNetworkId(), messageId, ex.getMessage());
        }
    }

    /**
     * The watchdog thread only fires: closing a delivery reads and writes Scylla and raises a CDR,
     * and a burst of correlations expiring at once would serialise all of that behind the single
     * timer thread. The work goes to the same executor the response callbacks use.
     */
    private void closeOnCompletionExecutor(String messageId) {
        try {
            completionExecutor.execute(() -> closeExpiredCorrelation(messageId));
        } catch (RejectedExecutionException ex) {
            log.warn("[networkId={}] The stack is shutting down: messageId={} was left open and will only expire on its TTL",
                    gateway.getNetworkId(), messageId);
        }
    }

    /**
     * Only the message reference is held until the timer fires: the event itself is already stored in
     * Scylla, and keeping a second copy per pending delivery in the scheduler queue would cost memory
     * proportional to the message rate. A row that is gone means the callback already closed it.
     */
    void closeExpiredCorrelation(String messageId) {
        MessageEvent event = readCorrelation(messageId);
        if (Objects.isNull(event)) {
            return;
        }
        if (!closer.claim(messageId)) {
            log.info("[networkId={}] The delivery of messageId={} was already closed by the failure notification",
                    gateway.getNetworkId(), messageId);
            return;
        }
        try {
            int errorCode = ErrorCodes.SMSF_N1N2_CORRELATION_TIMEOUT;
            log.warn("[networkId={}] No failure notification arrived for messageId={}, closing with {}",
                    gateway.getNetworkId(), messageId, errorCode);
            closer.closeAsFailed(event, errorCode, buildFailureComment(errorCode, CAUSE_NO_FAILURE_NOTIFICATION));
        } catch (RuntimeException ex) {
            log.error("[networkId={}] Failed to close the expired correlation for messageId={}: {}",
                    gateway.getNetworkId(), messageId, ex.getMessage(), ex);
        }
    }

    private MessageEvent readCorrelation(String messageId) {
        try {
            String stored = scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, messageId);
            if (Objects.isNull(stored) || stored.isBlank()) {
                return null;
            }
            return Converter.stringToObject(stored, MessageEvent.class);
        } catch (RuntimeException ex) {
            log.error("[networkId={}] Failed to read the N1N2 correlation for messageId={}: {}",
                    gateway.getNetworkId(), messageId, ex.getMessage(), ex);
            return null;
        }
    }

    Integer extractMaxWaitingTime(Http2Response response) {
        if (Objects.isNull(response) || Objects.isNull(response.getBody()) || response.getBody().length == 0) {
            return null;
        }
        try {
            N1N2MessageTransferError error = SdkConverter.stringToObject(
                    new String(response.getBody(), StandardCharsets.UTF_8), N1N2MessageTransferError.class);
            return Objects.nonNull(error) && Objects.nonNull(error.getErrInfo())
                    ? error.getErrInfo().getMaxWaitingTime()
                    : null;
        } catch (RuntimeException ex) {
            log.debug("[networkId={}] Could not parse the N1N2 maxWaitingTime from the MT response: {}",
                    gateway.getNetworkId(), ex.getMessage());
            return null;
        }
    }


    void handleAbsentSubscriber(MessageEvent event, String cause) {
        deleteSubmitSmResult(event);
        closer.closeAsAbsent(event, cause);
    }


    @Override
    public void processError(Throwable error, MessageEvent originalEvent) {
        int errorCode = resolveErrorCode(error);
        log.error("[networkId={}] HTTP/2 message error messageId={} errorCode={} cause={}",
                gateway.getNetworkId(), originalEvent.getMessageId(), errorCode,
                error.getMessage(), error);
        handleFailure(originalEvent, errorCode, null);
    }

    /**
     * A delivery that did not succeed must not simply disappear.
     *
     * <p>It is handed to the retry process when the gateway declares that error retryable, and only
     * written off as a failed CDR when it is not, or when the message has no life left. This is the
     * same decision the SMPP and HTTP clients make, and the per-gateway lists it reads
     * ({@code no_retry_error_code}, {@code auto_retry_error_code} and
     * {@code retry_alternate_destination_error_code}) already exist on {@code Http2Gateway} for
     * exactly this purpose.</p>
     *
     * <p>It matters most while the AMF link is down: those deliveries fail with 503, and without
     * this they would be consumed off Kafka and lost.</p>
     */
    private void handleFailure(MessageEvent event, int errorCode, String comment) {
        event.setErrorCode(errorCode);
        deleteSubmitSmResult(event);

        if (event.getValidityPeriod() == 0) {
            log.debug("[networkId={}] MT messageId={} has no validity period left, closing the transaction",
                    gateway.getNetworkId(), event.getMessageId());
            handlerFailedMessage(event, errorCode, comment);
            return;
        }
        if (event.isLastRetry()) {
            log.debug("[networkId={}] MT messageId={} reached its validity period, closing the transaction",
                    gateway.getNetworkId(), event.getMessageId());
            handlerFailedMessage(event, errorCode, comment);
            return;
        }
        sendToRetryProcess(event, errorCode, comment);
    }

    private void sendToRetryProcess(MessageEvent event, int errorCode, String comment) {
        if (errorContained(gateway.getNoRetryErrorCode(), errorCode)) {
            log.debug("[networkId={}] MT messageId={} error {} is marked no-retry",
                    gateway.getNetworkId(), event.getMessageId(), errorCode);
            handlerFailedMessage(event, errorCode, comment);
            return;
        }
        if (errorContained(gateway.getRetryAlternateDestinationErrorCode(), errorCode)) {
            republish(event, comment, KafkaUtils.getRoutingTopicPriority(event.getSmscMessagePriority()),
                    "alternate destination");
            return;
        }
        if (errorContained(gateway.getAutoRetryErrorCode(), errorCode)) {
            republish(event, comment, KafkaUtils.getRetriesTopicPriority(event.getSmscMessagePriority()),
                    "auto retry");
            return;
        }
        log.debug("[networkId={}] MT messageId={} not retried: gateway {} does not list error {}",
                gateway.getNetworkId(), event.getMessageId(), gateway.getName(), errorCode);
        handlerFailedMessage(event, errorCode, comment);
    }

    private void handlerFailedMessage(MessageEvent event, int errorCode, String comment) {
        closer.closeAsFailed(event, errorCode, comment);
    }


    /**
     * The CDR records the attempt that failed, so the copy that goes back on Kafka is a fresh try
     * with its error cleared and its retry counter advanced.
     */
    private void republish(MessageEvent event, String comment, String topic, String reason) {
        processCdr(event, UtilsEnum.CdrStatus.FAILED, comment);
        MessageEvent retry = Converter.deepCopy(event, MessageEvent.class);
        retry.setErrorCode(null);
        retry.setRetry(true);
        retry.setRetryNumber(Objects.isNull(retry.getRetryNumber()) ? 1 : retry.getRetryNumber() + 1);
        kafkaTemplate.send(topic, retry.toString());
        log.warn("[networkId={}] MT messageId={} sent to {} on topic {} (attempt {})",
                gateway.getNetworkId(), event.getMessageId(), reason, topic, retry.getRetryNumber());
    }

    /** Null-safe: a gateway may simply have no list configured for a given category. */
    private boolean errorContained(String codes, int errorCode) {
        if (Objects.isNull(codes) || codes.isBlank()) {
            return false;
        }
        return Arrays.stream(codes.split(","))
                .map(String::trim)
                .anyMatch(code -> code.equals(String.valueOf(errorCode)));
    }

    private void processCdr(MessageEvent event, UtilsEnum.CdrStatus status) {
        processCdr(event, status, null);
    }

    private void processCdr(MessageEvent event, UtilsEnum.CdrStatus status, String comment) {
        closer.publishCdr(event, status, comment);
    }

    private String buildFailureComment(int status, String cause) {
        return closer.buildFailureComment(status, cause);
    }

    String extractResponseCause(Http2Response response) {
        if (Objects.isNull(response) || Objects.isNull(response.getBody()) || response.getBody().length == 0) {
            return null;
        }
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        if (response.getStatus() >= 200 && response.getStatus() < 300) {
            return extractTransferCause(body);
        }
        String transferErrorCause = extractTransferErrorCause(body);
        if (Objects.nonNull(transferErrorCause)) {
            return transferErrorCause;
        }
        String problemCause = extractProblemCause(response);
        return Objects.nonNull(problemCause) ? problemCause : extractTransferCause(body);
    }

    /**
     * An AMF that is still trying to reach the UE answers 504 with an {@code N1N2MessageTransferError},
     * where the transfer cause travels inside {@code error.cause} instead of at the top level. Read
     * that shape first, or a temporary condition reads as a plain gateway timeout.
     */
    private String extractTransferErrorCause(String body) {
        try {
            N1N2MessageTransferError error = SdkConverter.stringToObject(body, N1N2MessageTransferError.class);
            return Objects.nonNull(error)
                    ? error.getError().getCause()
                    : null;
        } catch (RuntimeException ex) {
            log.debug("[networkId={}] Could not parse the N1N2 transfer error from the MT response: {}",
                    gateway.getNetworkId(), ex.getMessage());
            return null;
        }
    }

    private String extractTransferCause(String body) {
        try {
            N1N2MessageTransferRspData transfer = SdkConverter.stringToObject(body, N1N2MessageTransferRspData.class);
            return Objects.nonNull(transfer)
                    ? transfer.getCause().getValue()
                    : null;
        } catch (RuntimeException ex) {
            log.debug("[networkId={}] Could not parse the N1N2 transfer cause from the MT response: {}",
                    gateway.getNetworkId(), ex.getMessage());
            return null;
        }
    }

    private String extractProblemCause(Http2Response response) {
        if (Objects.isNull(response) || Objects.isNull(response.getBody()) || response.getBody().length == 0) {
            return null;
        }
        try {
            ProblemDetailsResponse problem = SdkConverter.stringToObject(
                    new String(response.getBody(), StandardCharsets.UTF_8), ProblemDetailsResponse.class);
            return Objects.nonNull(problem) ? problem.cause() : null;
        } catch (RuntimeException ex) {
            log.debug("[networkId={}] Could not parse ProblemDetails cause from MT response: {}",
                    gateway.getNetworkId(), ex.getMessage());
            return null;
        }
    }

    private int resolveErrorCode(Throwable error) {
        Throwable cause = error;
        while (Objects.nonNull(cause)) {
            if (cause instanceof TimeoutException || cause instanceof SocketTimeoutException || cause.getClass().getSimpleName().contains("Timeout")) {
                return HTTP2_GATEWAY_TIMEOUT;
            }
            if (cause instanceof ConnectException || cause instanceof IllegalStateException) {
                return HTTP2_SERVICE_UNAVAILABLE;
            }
            cause = cause.getCause();
        }
        return ErrorCodes.SYSTEM_ERROR;
    }

    private void persistSubmitSmResult(MessageEvent event) {
        if (!requiresDlrCorrelation(event)) {
            return;
        }
        try {
            scyllaManager.insertIntoTable(SMSF_SUBMIT_SM_RESULT_TABLE, event.getMessageId(),
                    Converter.valueAsString(event), DLR_CORRELATION_TTL_SECONDS);
        } catch (RuntimeException ex) {
            log.warn("[networkId={}] Failed to persist SMSF DLR correlation messageId={}: {}", gateway.getNetworkId(), event.getMessageId(), ex.getMessage(), ex);
        }
    }


    private void deleteSubmitSmResult(MessageEvent event) {
        if (!requiresDlrCorrelation(event)) {
            return;
        }
        try {
            scyllaManager.deleteFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, event.getMessageId());
        } catch (RuntimeException ex) {
            log.warn("[networkId={}] Failed to delete SMSF DLR correlation messageId={}: {}", gateway.getNetworkId(), event.getMessageId(), ex.getMessage(), ex);
        }
    }

    private boolean requiresDlrCorrelation(MessageEvent event) {
        return Objects.nonNull(event) && !event.isDlr() && event.isFinalSegmentForSplitSmsc() && Objects.equals(event.getRegisteredDelivery(), RequestDelivery.REQUEST_DLR.getValue());
    }

    private String failureNotificationUriFor(MessageEvent event) {
        if (Objects.isNull(failureNotificationUriBase) || failureNotificationUriBase.isBlank()) {
            return null;
        }
        return failureNotificationUriBase + "/" + event.getMessageId();
    }

    private Http2Request buildRequest(MessageEvent event) {
        MultipartRelatedEncoder.Encoded encoded = messageFactory.createMtEncodeData(
                event, gateway.getNetworkId(), failureNotificationUriFor(event));
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_CONTENT_TYPE, encoded.contentType());
        bearerHeaderSupplier.get().ifPresent(bearer -> headers.put(AUTHORIZATION_HEADER, bearer));
        String path = Http2HostUtils.parsePath(gateway.getHost())
                .replace(SUPI_PLACEHOLDER, event.getDestination());
        return Http2Request.builder()
                .method("POST")
                .path(path)
                .headers(headers)
                .body(encoded.body())
                .correlationId(event.getMessageId())
                .build();
    }
}
