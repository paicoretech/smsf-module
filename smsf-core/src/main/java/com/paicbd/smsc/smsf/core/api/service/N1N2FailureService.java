package com.paicbd.smsc.smsf.core.api.service;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.processors.MtDeliveryCloser;
import com.paicbd.smsc.smsf.common.processors.MtOutcome;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicore.sba.stack.openapi.model.N1N2MsgTxfrFailureNotification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;

/**
 * Closes an MT delivery the AMF accepted and then gave up on.
 *
 * <p>The AMF answers a transfer it cannot complete straight away with {@code ATTEMPTING_TO_REACH_UE}
 * and reports the outcome later, by posting to the callback URI the MT advertised. The delivery is
 * closed here, where the notification lands: the stored correlation carries the original message, so
 * the CDR, the receipt and the subscriber state can all be settled without another hop.
 *
 * <p>A notification is the AMF saying it gave up, so the delivery is never retried from here. The
 * retry paths belong to the synchronous side, which still owns the response it is given.</p>
 */
@Slf4j
@Service
public class N1N2FailureService {

    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConcurrentMap<Integer, CoreStackManager> gatewayStacks;
    private final CdrProcessor cdrProcessor;

    public N1N2FailureService(ScyllaManager scyllaManager,
                              KafkaTemplate<String, String> kafkaTemplate,
                              ConcurrentMap<Integer, CoreStackManager> gatewayStacks) {
        this.scyllaManager = scyllaManager;
        this.kafkaTemplate = kafkaTemplate;
        this.gatewayStacks = gatewayStacks;
        this.cdrProcessor = new CdrProcessor();
    }

    public void processFailureNotification(int networkId, String messageId,
                                           N1N2MsgTxfrFailureNotification notification) {
        String cause = Objects.nonNull(notification) && Objects.nonNull(notification.getCause())
                ? notification.getCause().getValue()
                : null;

        log.warn("N1N2 transfer failure received networkId={} messageId={} cause={}", networkId, messageId, cause);

        CoreStackManager stack = gatewayStacks.get(networkId);
        if (Objects.isNull(stack)) {
            throw new IllegalStateException("No HTTP/2 gateway stack registered for networkId " + networkId);
        }

        MessageEvent original = loadCorrelatedEvent(messageId);
        if (Objects.isNull(original)) {
            log.warn("No correlation found for the N1N2 failure of messageId={}; the transaction was already closed",
                    messageId);
            return;
        }

        MtDeliveryCloser closer = new MtDeliveryCloser(stack.getGateway(), scyllaManager, kafkaTemplate,
                cdrProcessor, stack.getMessageFactory());

        if (!closer.claim(messageId)) {
            log.warn("The delivery of messageId={} was already closed elsewhere; the notification changes nothing",
                    messageId);
            return;
        }

        if (MtOutcome.ABSENT.equals(MtOutcome.fromCause(cause))) {
            closer.closeAsAbsent(original, cause);
            return;
        }

        int errorCode = ErrorCodes.SMSF_N1N2_FAILURE_CALLBACK;
        closer.closeAsFailed(original, errorCode, closer.buildFailureComment(errorCode, cause));
    }

    private MessageEvent loadCorrelatedEvent(String messageId) {
        try {
            String stored = scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, messageId);
            if (Objects.isNull(stored) || stored.isBlank()) {
                return null;
            }
            return Converter.stringToObject(stored, MessageEvent.class);
        } catch (RuntimeException ex) {
            log.error("Failed to read the N1N2 correlation for messageId={}: {}", messageId, ex.getMessage(), ex);
            return null;
        }
    }
}
