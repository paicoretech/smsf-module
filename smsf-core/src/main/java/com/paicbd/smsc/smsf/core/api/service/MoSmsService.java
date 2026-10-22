package com.paicbd.smsc.smsf.core.api.service;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.smsf.common.processors.dto.MessageRequestDecodeDataDTO;
import com.paicbd.smsc.smsf.core.api.exception.DlrCorrelationNotFoundException;
import com.paicbd.smsc.smsf.common.processors.MessageTpduProcessor;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.core.processors.multipart.MultiPartsHandler;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpduType;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;

@Slf4j
@Service
public class MoSmsService {

    private final MessageTpduProcessor messageTpduProcessor;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MultiPartsHandler multiPartsHandler;
    private final ConcurrentMap<Integer, CoreStackManager> gatewayStacks;
    private final ScyllaManager scyllaManager;

    public MoSmsService(KafkaTemplate<String, String> kafkaTemplate,
                        MultiPartsHandler multiPartsHandler,
                        ConcurrentMap<Integer, CoreStackManager> gatewayStacks,
                        ScyllaManager scyllaManager) {
        this.kafkaTemplate = kafkaTemplate;
        this.multiPartsHandler = multiPartsHandler;
        this.gatewayStacks = gatewayStacks;
        this.scyllaManager = scyllaManager;
        this.messageTpduProcessor = new MessageTpduProcessor(new Ss7SmsTpduCodec());
    }

    public String processMoSms(int networkId, String supi, String contentType, byte[] body) {
        MessageRequestDecodeDataDTO decoded = messageTpduProcessor.decode(body, contentType, networkId);
        CoreStackManager stack = resolveStack(networkId);
        MessageFactory messageFactory = stack.getMessageFactory();
        if (SmsTpduType.SMS_STATUS_REPORT.equals(decoded.getMessageType())) {
            validateDlrCorrelation(networkId, decoded.getSmsRecordId());
            MessageEvent deliveryReport = messageFactory.createDeliveryReportEvent(decoded);
            publishHandingOffOnCancellation(KafkaTopicsConstants.PRE_DELIVER_TOPIC, Converter.valueAsString(deliveryReport));
            log.debug("SMS delivery report published networkId={} supi={} smsRecordId={} status={}", networkId, supi, decoded.getSmsRecordId(), deliveryReport.getStatus());
            stack.incrementMoProcessed();
            return decoded.getSmsRecordId();
        }

        MessageEvent event = messageFactory.createMessageEventFromMoMessage(decoded, supi);

        String segmentText = Objects.nonNull(decoded.getUserData())
                && Objects.nonNull(decoded.getUserData().getDecodedMessage())
                ? decoded.getUserData().getDecodedMessage()
                : event.getShortMessage();
        UtilsRecords.ConcatenatedMessageDetails multipartInfo =
                messageFactory.validateConcatenationViaUdh(event, segmentText);
        if (Objects.nonNull(multipartInfo)) {
            multiPartsHandler.messagePartsProcessor(event, multipartInfo, KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC);
            log.debug("MO SMS multipart part received networkId={} supi={} messageId={} ref={} seq={}/{}",
                    networkId, supi, event.getMessageId(),
                    multipartInfo.msgReferenceNumber(), multipartInfo.segmentSequence(), multipartInfo.totalSegments());
            stack.incrementMoProcessed();
            return event.getMessageId();
        }

        publishHandingOffOnCancellation(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC, Converter.valueAsString(event));
        log.debug("MO SMS published networkId={} supi={} messageId={}", networkId, supi, event.getMessageId());
        stack.incrementMoProcessed();
        return event.getMessageId();
    }

    private void publishHandingOffOnCancellation(String topic, String payload) {
        try {
            kafkaTemplate.send(topic, payload);
        } catch (KafkaException ex) {
            if (!(ex.getCause() instanceof InterruptedException)) {
                throw ex;
            }
            log.debug("Publish to {} interrupted by a cancelled request; the record was already handed to the producer", topic);
        }
    }

    private void validateDlrCorrelation(int networkId, String smsRecordId) {
        String storedRequest = scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, smsRecordId);
        if (Objects.isNull(storedRequest) || storedRequest.isBlank()) {
            log.warn("DLR correlation not found networkId={} smsRecordId={}", networkId, smsRecordId);
            throw new DlrCorrelationNotFoundException("DLR correlation not found for smsRecordId " + smsRecordId);
        }
    }

    private CoreStackManager resolveStack(int networkId) {
        CoreStackManager stack = gatewayStacks.get(networkId);
        if (Objects.isNull(stack)) {
            throw new IllegalStateException("No HTTP/2 gateway stack registered for networkId " + networkId);
        }
        return stack;
    }
}
