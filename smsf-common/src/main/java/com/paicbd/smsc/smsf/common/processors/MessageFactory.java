package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.Udh;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.processors.dto.MessageRequestDecodeDataDTO;
import com.paicbd.smsc.utils.CustomNumberingPlanIndicator;
import com.paicbd.smsc.utils.CustomTypeOfNumber;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.SignalingFactory;
import com.paicore.sba.stack.exceptions.TpduEncodeException;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import lombok.extern.slf4j.Slf4j;
import org.restcomm.protocols.ss7.map.api.MAPException;
import org.restcomm.protocols.ss7.map.api.smstpdu.AddressField;
import org.restcomm.protocols.ss7.map.api.smstpdu.DataCodingScheme;
import org.restcomm.protocols.ss7.map.api.smstpdu.NumberingPlanIdentification;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsSubmitTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpduType;
import org.restcomm.protocols.ss7.map.api.smstpdu.Status;
import org.restcomm.protocols.ss7.map.api.smstpdu.TypeOfNumber;
import org.restcomm.protocols.ss7.map.api.smstpdu.UserData;
import org.restcomm.protocols.ss7.map.smstpdu.DataCodingSchemeImpl;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class MessageFactory extends SignalingFactory {
    private final Http2Gateway gateway;
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    private final MessageTpduProcessor messageTpduProcessor;

    public MessageFactory(Http2Gateway gateway,
                          ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap,
                          MessageTpduProcessor messageTpduProcessor) {
        this.gateway = gateway;
        this.errorCodeMappingConcurrentHashMap = errorCodeMappingConcurrentHashMap;
        this.messageTpduProcessor = messageTpduProcessor;
    }

    public MessageEvent createMessageEventFromMoMessage(MessageRequestDecodeDataDTO decoded, String supi) {
        // The millisecond prefix stays: MessageEvent.parseSubmitDate does Long.parseLong on
        // id.split("-")[0] to derive the CDR's submit_date, dialog_duration and processing_time.
        String id = System.currentTimeMillis() + "-" + UUID.randomUUID();
        CustomTypeOfNumber sourceTon = CustomTypeOfNumber.fromSmsTpdu(TypeOfNumber.InternationalNumber);
        CustomNumberingPlanIndicator sourceNpi = CustomNumberingPlanIndicator.fromSmsTpdu(
                NumberingPlanIdentification.ISDNTelephoneNumberingPlan);

        MessageEvent event = MessageEvent.builder()
                .id(id)
                .messageId(id)
                .parentId(id)
                .originProtocol(Constants.ORIGIN_PROTOCOL_HTTP2)
                .originNetworkType(Constants.NETWORK_TYPE_GATEWAY)
                .originNetworkId(decoded.getNetworkId())
                .destNetworkType(Constants.NETWORK_TYPE_SERVICE_PROVIDER)
                .source(supi)
                .sourceAddr(decoded.getSourceAddr())
                .sourceAddrTon((int) sourceTon.getSmscValue().value())
                .sourceAddrNpi((int) sourceNpi.getSmscValue().value())
                .retryDestNetworkId("")
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .moMessage(true)
                .build();

        applyDestinationAddress(event, decoded.getAddressField());
        applyTpduDerivedFields(event, decoded);
        return event;
    }

    public MessageEvent createDeliveryReportEvent(MessageRequestDecodeDataDTO decoded) {
        if (!SmsTpduType.SMS_STATUS_REPORT.equals(decoded.getMessageType())) {
            throw new IllegalArgumentException("SMS status report is required");
        }
        if (Objects.isNull(decoded.getSmsRecordId()) || decoded.getSmsRecordId().isBlank()) {
            throw new IllegalArgumentException("smsRecordId is required for DLR correlation");
        }

        return MessageEvent.builder()
                .deliverSmId(decoded.getSmsRecordId())
                .originProtocol(Constants.ORIGIN_PROTOCOL_HTTP2)
                .status(mapTpStatus(decoded.getTpStatus()))
                .errorCode(Objects.requireNonNullElse(decoded.getTpStatus(), 0))
                .checkSubmitSmResponse(true)
                .build();
    }

    private String mapTpStatus(Integer tpStatus) {
        if (Objects.isNull(tpStatus)) {
            return "UNKNOWN";
        }
        if (tpStatus == 0x00) {
            return "DELIVRD";
        }
        if (tpStatus == 0x01 || tpStatus == 0x02) {
            return "ACCEPTD";
        }
        if (tpStatus >= 0x20 && tpStatus <= 0x3F) {
            return "ENROUTE";
        }
        return switch (tpStatus) {
            case Status.SMS_VALIDITY_PERIOD_EXPIRED -> "EXPIRED";
            case Status.SMS_DELETED_BY_ORIGINATING_SME,
                 Status.SMS_DELETED_BY_ADMINISTRATOR,
                 Status.SMS_DOES_NOT_EXIST -> "DELETED";
            case Status.SERVICE_REJECTED -> "REJECTD";
            default -> "UNDELIV";
        };
    }

    private void applyDestinationAddress(MessageEvent event, AddressField destination) {
        if (Objects.isNull(destination)) {
            return;
        }
        event.setDestinationAddr(destination.getAddressValue());
        CustomTypeOfNumber destTon = CustomTypeOfNumber.fromSmsTpdu(destination.getTypeOfNumber());
        CustomNumberingPlanIndicator destNpi = CustomNumberingPlanIndicator.fromSmsTpdu(
                destination.getNumberingPlanIdentification());
        event.setDestAddrTon((int) destTon.getSmscValue().value());
        event.setDestAddrNpi((int) destNpi.getSmscValue().value());
    }

    private void applyTpduDerivedFields(MessageEvent event, MessageRequestDecodeDataDTO decoded) {
        boolean isUdh = decoded.isUdhPresent();
        int dataCoding = decoded.getDataCoding();
        event.setDataCoding(dataCoding);
        event.setProtocolId((byte) decoded.getProtocolId());
        event.setEsmClass(isUdh ? Constants.ESM_CLASS_UDH_PRESENT : 0);

        SmsSubmitTpdu submit = decoded.getSmsSubmitTpdu();
        if (Objects.nonNull(submit)) {
            event.setRegisteredDelivery(submit.getStatusReportRequest() ? 1 : 0);
            setValidityPeriod(event, submit);
            event.addCustomParam(Constants.HTTP2_MESSAGE_REFERENCE, decoded.getMessageReference());
        }

        UserData userData = decoded.getUserData();
        if (Objects.nonNull(userData)) {
            processUdhAndShortMessage(event, userData, isUdh, dataCoding, true);
        }
    }

    /**
     * Builds the delivery receipt for a message that could not be delivered, translating the error
     * through the MNO's mapping so the originator sees the operator's own status rather than a raw
     * HTTP status. Mirrors what the SIP module does on its own failure path.
     *
     * @param originalMessage the message that failed
     * @param errorCode       the resolved error code for the failure
     */
    public MessageEvent createDeliveryReceiptMessage(MessageEvent originalMessage, int errorCode) {
        return originalMessage.createDeliveryReceiptMessage(getErrorCodeMapping(errorCode), null);
    }

    /**
     * An MNO with no mapping for this code still gets a receipt: an undelivered one carrying the
     * error as-is, which is better than no receipt at all.
     */
    private ErrorCodeMapping getErrorCodeMapping(int errorCode) {
        List<ErrorCodeMapping> mappings = errorCodeMappingConcurrentHashMap.get(String.valueOf(gateway.getMnoId()));
        return Optional.ofNullable(mappings)
                .flatMap(list -> list.stream().filter(mapping -> mapping.getErrorCode() == errorCode).findFirst())
                .orElseGet(() -> {
                    log.debug("No error code mapping for mno {} with error {}, defaulting to UNDELIV",
                            gateway.getMnoId(), errorCode);
                    ErrorCodeMapping fallback = new ErrorCodeMapping();
                    fallback.setErrorCode(errorCode);
                    fallback.setDeliveryErrorCode(errorCode);
                    fallback.setDeliveryStatus("UNDELIV");
                    return fallback;
                });
    }

    public UtilsRecords.ConcatenatedMessageDetails validateConcatenationViaUdh(MessageEvent message, String segmentText) {
        Collection<Udh> udhRaw = message.getUdhRaw();
        if (Objects.isNull(udhRaw) || udhRaw.isEmpty()) {
            return null;
        }
        Optional<Udh> maybeUdh = udhRaw.stream()
                .filter(udh -> "00".equals(udh.iei()) || "08".equals(udh.iei()))
                .findFirst();

        if (maybeUdh.isEmpty()) {
            return null;
        }

        Udh udh = maybeUdh.get();
        byte[] ieiContent = EncodingUtils.hexToBytes(udh.content());

        int referenceNumber;
        int totalSegments;
        int segmentSequence;

        switch (udh.iei()) {
            case "00" -> {
                if (ieiContent.length < 5) return null;
                referenceNumber = Byte.toUnsignedInt(ieiContent[2]);
                totalSegments = Byte.toUnsignedInt(ieiContent[3]);
                segmentSequence = Byte.toUnsignedInt(ieiContent[4]);
            }
            case "08" -> {
                if (ieiContent.length < 6) return null;
                referenceNumber = Byte.toUnsignedInt(ieiContent[2]) << 8 | Byte.toUnsignedInt(ieiContent[3]);
                totalSegments = Byte.toUnsignedInt(ieiContent[4]);
                segmentSequence = Byte.toUnsignedInt(ieiContent[5]);
            }
            default -> {
                return null;
            }
        }

        log.debug("Concatenated message: referenceNumber={}, totalSegments={}, segmentSequence={}",
                referenceNumber, totalSegments, segmentSequence);

        return new UtilsRecords.ConcatenatedMessageDetails(referenceNumber, totalSegments, segmentSequence, segmentText);
    }

    public MultipartRelatedEncoder.Encoded createMtEncodeData(MessageEvent event, int networkId,
                                                              String failureNotificationUri) {
        if (event.isDlr()) {
            return messageTpduProcessor.encodeDeliveryReport(event, networkId, failureNotificationUri);
        }
        DataCodingScheme scheme = new DataCodingSchemeImpl(event.getDataCoding());
        UserData userData = buildUserDataForMt(event, scheme);
        try {
            userData.encode();
        } catch (MAPException e) {
            log.error("[networkId={}] Failed to encode MT user data for messageId={}: {}",
                    networkId, event.getMessageId(), e.getMessage(), e);
            throw new TpduEncodeException("Failed to encode MT user data", e);
        }

        return messageTpduProcessor.encode(event, userData, networkId, failureNotificationUri);
    }
}
