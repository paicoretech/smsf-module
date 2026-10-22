package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.ChargingUtils;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.RegEventState;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.UtilsEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Objects;

import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;
import static com.paicbd.smsc.utils.GeneralSmscConstants.HTTP2_PROTOCOL;

/**
 * Closes an MT delivery that will not be completed: the CDR records the failure, the originator
 * gets a receipt if it asked for one, the charge is refunded, and a subscriber the AMF reported as
 * unreachable is marked so the routing stops choosing HTTP/2 for it.
 *
 * <p>Both modules close deliveries. {@code smsf-service-mt} closes on the response the AMF gives it
 * and when the wait for a failure notification runs out; {@code smsf-core} closes on the failure
 * notification the AMF posts. Keeping the closing here means the decision and its consequences are
 * written once.
 */
@Slf4j
public class MtDeliveryCloser {

    private final Http2Gateway gateway;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final CdrProcessor cdrProcessor;
    private final MessageFactory messageFactory;

    public MtDeliveryCloser(Http2Gateway gateway,
                            ScyllaManager scyllaManager,
                            KafkaTemplate<String, String> kafkaTemplate,
                            CdrProcessor cdrProcessor,
                            MessageFactory messageFactory) {
        this.gateway = gateway;
        this.scyllaManager = scyllaManager;
        this.kafkaTemplate = kafkaTemplate;
        this.cdrProcessor = cdrProcessor;
        this.messageFactory = messageFactory;
    }

    /**
     * Takes the delivery so only one closer acts on it.
     *
     * <p>A delivery the AMF accepted can be closed from two sides: the notification arriving at
     * {@code smsf-core}, or the wait running out in {@code smsf-service-mt}. They run in different
     * processes, so the stored correlation is the token: the conditional delete is atomic, and the
     * one that removes the row is the one that closes. The loser stops, and the originator gets one
     * CDR and one receipt instead of two.
     *
     * @return {@code true} when this caller may close the delivery
     */
    public boolean claim(String messageId) {
        try {
            return scyllaManager.deleteFromTableIfExists(SMSF_SUBMIT_SM_RESULT_TABLE, messageId);
        } catch (RuntimeException ex) {
            log.error("[networkId={}] Failed to claim the correlation for messageId={}: {}",
                    gateway.getNetworkId(), messageId, ex.getMessage(), ex);
            return false;
        }
    }

    /** The AMF named the terminal: the subscriber is marked and the delivery is closed for good. */
    public void closeAsAbsent(MessageEvent event, String cause) {
        int errorCode = Objects.requireNonNullElse(
                MtOutcome.errorCodeForCause(cause), ErrorCodes.SMSF_N1N2_FAILURE_CALLBACK);
        deactivateSubscriber(event, cause);
        closeAsFailed(event, errorCode, buildFailureComment(errorCode, cause));
    }

    /**
     * Closes the delivery: CDR, receipt if it was asked for, and refund if it was billed.
     *
     * <p>Every closed failure passes through here, from all three paths, so this is where the trace
     * is written: which message, to whom, with what code and for what reason. The CDR carries the
     * same facts, but a line in the log is what lets an operator answer "what happened to this
     * message" without querying anything.
     */
    public void closeAsFailed(MessageEvent event, int errorCode, String comment) {
        event.setErrorCode(errorCode);
        publishCdr(event, UtilsEnum.CdrStatus.FAILED, comment);
        boolean receiptSent = publishDlr(event, errorCode);
        refundIfBilled(event);
        log.warn("[networkId={}] MT closed as failed messageId={} destination={} supi={} errorCode={} reason={} receipt={}",
                gateway.getNetworkId(), event.getMessageId(), event.getDestinationAddr(), event.getDestination(),
                errorCode, Objects.requireNonNullElse(comment, "not reported by the AMF"),
                receiptSent ? "sent" : "not requested");
    }

    public void publishCdr(MessageEvent event, UtilsEnum.CdrStatus status, String comment) {
        event.setDestProtocol(HTTP2_PROTOCOL);
        UtilsEnum.MessageType messageType = event.isDlr() ? UtilsEnum.MessageType.DELIVER : UtilsEnum.MessageType.MESSAGE;
        if (Objects.nonNull(comment) && !comment.isBlank()) {
            cdrProcessor.publishCdr(event, "", UtilsEnum.Module.HTTP2_CLIENT, messageType, status, comment, kafkaTemplate);
        } else {
            cdrProcessor.publishCdr(event, UtilsEnum.Module.HTTP2_CLIENT, messageType, status, kafkaTemplate);
        }
    }

    public String buildFailureComment(int status, String cause) {
        if (Objects.isNull(cause) || cause.isBlank()) {
            return null;
        }
        return ErrorCodes.getErrorDescription(UtilsEnum.Module.HTTP2_CLIENT, status, null) + " - " + cause;
    }

    /**
     * Only for a message that asked for a receipt, and on a split message only once the final
     * segment fails, so the originator does not get one receipt per segment.
     */
    private boolean publishDlr(MessageEvent event, int errorCode) {
        Integer registeredDelivery = event.getRegisteredDelivery();
        if (Objects.isNull(registeredDelivery) || registeredDelivery != RequestDelivery.REQUEST_DLR.getValue()) {
            return false;
        }
        if (!event.isFinalSegmentForSplitSmsc()) {
            return false;
        }
        MessageEvent dlr = messageFactory.createDeliveryReceiptMessage(event, errorCode);
        kafkaTemplate.send(KafkaTopicsConstants.PRE_DELIVER_TOPIC, dlr.toString());
        return true;
    }

    private void refundIfBilled(MessageEvent event) {
        if (!ChargingUtils.checkMessageForRefund(event)) {
            return;
        }
        String topic = KafkaUtils.getChargingTopicPriority(event.getSmscMessagePriority());
        kafkaTemplate.send(topic, event.toString());
        log.debug("[networkId={}] Refund applied for messageId={} on topic {}",
                gateway.getNetworkId(), event.getMessageId(), topic);
    }

    private void deactivateSubscriber(MessageEvent event, String cause) {
        String supi = event.getDestination();
        if (Objects.isNull(supi) || supi.isBlank()) {
            log.warn("[networkId={}] Cannot deactivate the subscriber for messageId={}: the SUPI is not resolved",
                    gateway.getNetworkId(), event.getMessageId());
            return;
        }
        try {
            scyllaManager.updateSmsfRegEventState(supi, RegEventState.INACTIVE.name());
            log.warn("[networkId={}] Subscriber {} marked INACTIVE after cause {} on messageId={}. "
                            + "It stays that way until a new subscription arrives.",
                    gateway.getNetworkId(), supi, cause, event.getMessageId());
        } catch (RuntimeException ex) {
            log.error("[networkId={}] Failed to mark subscriber {} as INACTIVE: {}",
                    gateway.getNetworkId(), supi, ex.getMessage(), ex);
        }
    }
}
