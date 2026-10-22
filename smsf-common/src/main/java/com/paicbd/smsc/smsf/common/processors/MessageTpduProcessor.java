package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.smsf.common.processors.dto.MessageRequestDecodeDataDTO;
import com.paicbd.smsc.utils.CustomNumberingPlanIndicator;
import com.paicbd.smsc.utils.CustomTypeOfNumber;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.SignalingFactory;
import com.paicore.sba.stack.codec.EncodeTpduRequest;
import com.paicore.sba.stack.codec.MessageType;
import com.paicore.sba.stack.codec.SmsTpduCodec;
import com.paicore.sba.stack.dto.MoMessageRequest;
import com.paicore.sba.stack.dto.MtMessageRequest;
import com.paicore.sba.stack.exceptions.TpduDecodeException;
import com.paicore.sba.stack.exceptions.TpduEncodeException;
import com.paicore.sba.stack.mo.MoMultipartPayload;
import com.paicore.sba.stack.mo.MultipartRelatedDecoder;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import com.paicore.sba.stack.openapi.model.RefToBinaryData;
import com.paicore.sba.stack.openapi.model.N1MessageClass;
import com.paicore.sba.stack.openapi.model.N1MessageContainer;
import com.paicore.sba.stack.openapi.model.N1N2MessageTransferReqData;
import com.paicore.sba.stack.openapi.model.SmsRecordData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.restcomm.protocols.ss7.map.api.MAPException;
import org.restcomm.protocols.ss7.map.api.smstpdu.DataCodingScheme;
import org.restcomm.protocols.ss7.map.api.smstpdu.NumberingPlanIdentification;
import org.restcomm.protocols.ss7.map.api.smstpdu.ProtocolIdentifier;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsStatusReportTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsSubmitTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpduType;
import org.restcomm.protocols.ss7.map.api.smstpdu.Status;
import org.restcomm.protocols.ss7.map.api.smstpdu.StatusReportQualifier;
import org.restcomm.protocols.ss7.map.api.smstpdu.TypeOfNumber;
import org.restcomm.protocols.ss7.map.api.smstpdu.UserData;
import org.restcomm.protocols.ss7.map.smstpdu.AddressFieldImpl;
import org.restcomm.protocols.ss7.map.smstpdu.ProtocolIdentifierImpl;
import org.restcomm.protocols.ss7.map.smstpdu.SmsStatusReportTpduImpl;
import org.restcomm.protocols.ss7.map.smstpdu.SmsTpduImpl;
import org.restcomm.protocols.ss7.map.smstpdu.StatusImpl;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static com.paicbd.smsc.smsf.common.constants.Constants.MSISDN_PREFIX;
import static com.paicbd.smsc.smsf.common.constants.Constants.HTTP2_MESSAGE_REFERENCE;

/**
 * TPDU codec service for SMS messages exchanged through the SMSF HTTP/2
 * interface (3GPP TS29540 {@code sendsms}).
 *
 * <p>Decoding (Mobile Originated):
 * <ul>
 *   <li>Extract the encoded TPDU from the {@code multipart/related} request.</li>
 *   <li>Invoke the {@code 5g-sba-stack} TPDU codec ({@link SmsTpduCodec#decode(byte[])})
 *       to parse the raw 3GPP TS23.040 TPDU.</li>
 *   <li>Map the decoded TPDU onto the internal {@link MessageRequestDecodeDataDTO}.</li>
 * </ul>
 *
 * <p>Encoding (Mobile Terminated):
 * <ul>
 *   <li>Encode the TPDU through the codec ({@link SmsTpduCodec#encode}).</li>
 *   <li>Build the {@code SmsRecordData} root part and the multipart body.</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class MessageTpduProcessor extends SignalingFactory {

    private static final String SMS_CONTENT_ID = "sms";
    private static final String N1_SMS_CONTENT_ID = "n1-sms-payload";
    private static final int STATUS_REPORT_INDICATION_MASK = 0x20;

    private final SmsTpduCodec tpduCodec;

    /**
     * Decodes a raw {@code multipart/related} {@code sendsms} body: splits the
     * JSON ({@code SmsRecordData}) and binary (TPDU) parts using the library and
     * builds the internal DTO.
     *
     * @param body        the raw multipart/related request body
     * @param contentType the request {@code Content-Type} header (with boundary)
     * @param networkId   the network identifier from the request context
     * @return the decoded MO SMS data
     */
    public MessageRequestDecodeDataDTO decode(byte[] body, String contentType, int networkId) {
        MoMultipartPayload payload = MultipartRelatedDecoder.decode(body, contentType);
        return decode(payload.getJsonData(), payload.getBinaryData(), networkId);
    }

    /**
     * Decodes the TPDU contained in a {@link MoMessageRequest} together with its
     * binary TPDU part.
     *
     * @param request     the MO message request carrying the {@code SmsRecordData}
     * @param tpduBinary  the raw TPDU bytes referenced by the request
     * @param networkId   the network identifier from the request context
     * @return the decoded MO SMS data
     */
    public MessageRequestDecodeDataDTO decode(MoMessageRequest request, byte[] tpduBinary, int networkId) {
        Objects.requireNonNull(request, "MoMessageRequest cannot be null");
        return decode(request.getSmsRecord(), tpduBinary, networkId);
    }

    /**
     * Core decode flow: parses the TPDU through the codec, maps it onto the
     * internal DTO and resolves the source address from the GPSI (preferred) or
     * the TPDU Originating Address.
     *
     * @param smsRecord   the SMS record metadata (may carry the GPSI)
     * @param tpduBinary  the raw TPDU bytes
     * @param networkId   the network identifier from the request context
     * @return the decoded MO SMS data
     */
    public MessageRequestDecodeDataDTO decode(SmsRecordData smsRecord, byte[] tpduBinary, int networkId) {
        try {
            SmsTpdu tpdu = decodeTpdu(tpduBinary);
            MessageRequestDecodeDataDTO dto = mapTpdu(tpdu);
            dto.setNetworkId(networkId);
            dto.setSmsRecordId(Objects.nonNull(smsRecord) ? smsRecord.getSmsRecordId() : null);
            String gpsi = Objects.nonNull(smsRecord) ? smsRecord.getGpsi() : null;
            dto.setSourceAddr(resolveSourceAddr(gpsi));
            return dto;
        } catch (MAPException e) {
            log.error("[networkId={}] TPDU decoding failed: {}", networkId, e.getMessage(), e);
            throw new TpduDecodeException("Failed to decode TPDU user data", e);
        } catch (RuntimeException e) {
            log.error("[networkId={}] TPDU decoding failed: {}", networkId, e.getMessage(), e);
            throw e;
        }
    }

    private SmsTpdu decodeTpdu(byte[] tpduBinary) throws MAPException {
        if (Objects.nonNull(tpduBinary) && tpduBinary.length > 0 && (tpduBinary[0] & 0x03) == 0x02) {
            return SmsTpduImpl.createInstance(tpduBinary, false, StandardCharsets.ISO_8859_1);
        }
        return tpduCodec.decode(tpduBinary);
    }

    /**
     * Encodes a Mobile Terminated (MT) SMS into a 3GPP TS29540
     * {@code multipart/related} {@code sendsms} body: encodes the TPDU through
     * the {@code 5g-sba-stack} codec, builds the {@code SmsRecordData} root part
     * and assembles the multipart body referencing the binary TPDU part.
     *
     * @param event     the outbound message event consumed from Kafka
     * @param userData  the TPDU user data already built for the message
     * @param networkId the destination network identifier
     * @return the encoded multipart body together with its {@code Content-Type}
     */
    public MultipartRelatedEncoder.Encoded encode(MessageEvent event, UserData userData, int networkId,
                                                  String failureNotificationUri) {
        MtMessageRequest mtMessageRequest = buildMtMessageRequest(event, userData, networkId);
        return MultipartRelatedEncoder.encode(
                buildN1N2MessageTransferReqData(event, failureNotificationUri),
                N1_SMS_CONTENT_ID,
                mtMessageRequest.getTpduBinary());
    }

    public MultipartRelatedEncoder.Encoded encodeDeliveryReport(MessageEvent event, int networkId,
                                                                String failureNotificationUri) {
        try {
            int protocolId = event.getProtocolId() & 0xFF;
            int status = Objects.requireNonNullElse(event.getErrorCode(), 0) == 0 ? Status.SMS_RECEIVED : Status.ERROR_IN_SME;
            var recipientAddress = new AddressFieldImpl(
                    Objects.isNull(event.getSourceAddrTon())
                            ? TypeOfNumber.Unknown
                            : CustomTypeOfNumber.fromSmsc(event.getSourceAddrTon().byteValue()).getSmsTpduValue(),
                    Objects.isNull(event.getSourceAddrNpi())
                            ? NumberingPlanIdentification.Unknown
                            : CustomNumberingPlanIndicator.fromSmsc(event.getSourceAddrNpi().byteValue()).getSmsTpduValue(),
                    event.getSourceAddr());

            byte[] encodedTpdu = new SmsStatusReportTpduImpl(
                    false,
                    false,
                    StatusReportQualifier.SmsSubmitResult,
                    parseMessageReference(event),
                    recipientAddress,
                    getAbsoluteTimeStampImpl(),
                    getAbsoluteTimeStampImpl(),
                    new StatusImpl(status),
                    new ProtocolIdentifierImpl(protocolId),
                    null
            ).encodeData();

            return MultipartRelatedEncoder.encode(
                    buildN1N2MessageTransferReqData(event, failureNotificationUri),
                    N1_SMS_CONTENT_ID,
                    encodedTpdu);
        } catch (MAPException | RuntimeException e) {
            log.error("[networkId={}] Failed to encode delivery report for messageId={}: {}", networkId, Objects.nonNull(event) ? event.getMessageId() : null, e.getMessage(), e);
            throw new TpduEncodeException("Failed to encode SMS status report", e);
        }
    }

    /**
     * Builds the {@link MtMessageRequest} carrying the {@code SmsRecordData}
     * root part and the binary TPDU encoded from the outbound {@link MessageEvent}.
     *
     * @param event     the outbound message event consumed from Kafka
     * @param userData  the TPDU user data already built for the message
     * @param networkId the destination network identifier
     * @return the MT message request with the encoded TPDU attached
     */
    public MtMessageRequest buildMtMessageRequest(MessageEvent event, UserData userData, int networkId) {
        try {
            byte[] encodedTpdu = tpduCodec.encode(toEncodeTpduRequest(event, userData));
            if (Objects.equals(event.getRegisteredDelivery(), RequestDelivery.REQUEST_DLR.getValue()) && event.isFinalSegmentForSplitSmsc()) {
                encodedTpdu[0] |= STATUS_REPORT_INDICATION_MASK;
            }
            return MtMessageRequest.builder()
                    .correlationId(event.getMessageId())
                    .smsRecord(buildSmsRecordData(event))
                    .targetSupi(event.getDestination())
                    .tpduBinary(encodedTpdu)
                    .build();
        } catch (RuntimeException e) {
            log.error("[networkId={}] TPDU encoding failed for messageId={}: {}",
                    networkId, event.getMessageId(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Maps the outbound {@link MessageEvent} onto the {@code 5g-sba-stack} codec
     * request. The codec-specific types are confined to this method. The MT TPDU
     * is always an SMS-DELIVER whose originating address is the original sender.
     */
    private EncodeTpduRequest toEncodeTpduRequest(MessageEvent event, UserData userData) {
        TypeOfNumber originatingTon = CustomTypeOfNumber.fromSmsc(event.getSourceAddrTon().byteValue()).getSmsTpduValue();
        NumberingPlanIdentification originatingNpi = CustomNumberingPlanIndicator.fromSmsc(event.getSourceAddrNpi().byteValue()).getSmsTpduValue();

        TypeOfNumber destinationTon = CustomTypeOfNumber.fromSmsc(event.getDestAddrTon().byteValue()).getSmsTpduValue();
        NumberingPlanIdentification destinationNpi = CustomNumberingPlanIndicator.fromSmsc(event.getDestAddrNpi().byteValue()).getSmsTpduValue();

        return EncodeTpduRequest.builder()
                .messageType(MessageType.SMS_DELIVER)
                .originatingAddress(event.getSourceAddr())
                .originatingAddressTon(originatingTon.getCode())
                .originatingAddressNpi(originatingNpi.getCode())
                .destinationAddress(event.getDestinationAddr())
                .destinationAddressTon(destinationTon.getCode())
                .destinationAddressNpi(destinationNpi.getCode())
                .dataCoding(event.getDataCoding())
                .protocolId(event.getProtocolId() & 0xFF)
                .userData(userData.getEncodedData())
                .userDataLength(userData.getEncodedUserDataLength())
                .userDataHeaderPresent(userData.getEncodedUserDataHeaderIndicator())
                .build();
    }

    private N1N2MessageTransferReqData buildN1N2MessageTransferReqData(MessageEvent event, String failureNotificationUri) {
        return new N1N2MessageTransferReqData()
                .supi(event.getDestination())
                .gpsi(formatGpsi(event.getDestinationAddr()))
                .n1n2FailureTxfNotifURI(failureNotificationUri)
                .n1MessageContainer(new N1MessageContainer()
                        .n1MessageClass(N1MessageClass.SMS)
                        .serviceInstanceId(event.getMessageId())
                        .n1MessageContent(new RefToBinaryData().contentId(N1_SMS_CONTENT_ID)));
    }

    private SmsRecordData buildSmsRecordData(MessageEvent event) {
        return new SmsRecordData()
                .smsRecordId(event.getMessageId())
                .gpsi(formatGpsi(event.getDestinationAddr()))
                .smsPayload(new RefToBinaryData().contentId(SMS_CONTENT_ID));
    }

    private int parseMessageReference(MessageEvent event) {
        Object value = event.getFromCustomParam(HTTP2_MESSAGE_REFERENCE, 0);
        try {
            return Integer.parseInt(String.valueOf(value).trim()) & 0xFF;
        } catch (NumberFormatException e) {
            log.warn("Invalid HTTP2 message reference '{}' for messageId={}, using 0", value, event.getMessageId());
            return 0;
        }
    }

    /**
     * Formats a recipient address as a 3GPP GPSI by adding the {@code msisdn-}
     * prefix when an address is present and not already prefixed.
     */
    private String formatGpsi(String address) {
        if (Objects.isNull(address) || address.isBlank()) {
            return null;
        }
        return address.startsWith(MSISDN_PREFIX) ? address : MSISDN_PREFIX + address;
    }

    /**
     * Maps a RestComm {@link SmsTpdu} onto the internal DTO. The telecom-specific
     * types are confined to this method.
     */
    private MessageRequestDecodeDataDTO mapTpdu(SmsTpdu tpdu) throws MAPException {
        SmsTpduType smsTpduType = tpdu.getSmsTpduType();
        MessageRequestDecodeDataDTO.MessageRequestDecodeDataDTOBuilder builder = MessageRequestDecodeDataDTO.builder()
                .messageType(smsTpduType);

        switch (smsTpduType) {
            case SMS_SUBMIT -> {
                SmsSubmitTpdu submit = (SmsSubmitTpdu) tpdu;
                builder.addressField(submit.getDestinationAddress())
                        .udhPresent(submit.getUserDataHeaderIndicator())
                        .validityPeriod(submit.getValidityPeriod())
                        .validityPeriodFormat(submit.getValidityPeriodFormat())
                        .smsSubmitTpdu(submit)
                        .messageReference(submit.getMessageReference());
                applyCommon(builder, submit.getDataCodingScheme(), submit.getProtocolIdentifier(), submit.getUserData());
            }
            case SMS_STATUS_REPORT -> {
                SmsStatusReportTpdu report = (SmsStatusReportTpdu) tpdu;
                builder.addressField(report.getRecipientAddress())
                        .udhPresent(report.getUserDataHeaderIndicator())
                        .messageReference(report.getMessageReference())
                        .tpStatus(Objects.nonNull(report.getStatus()) ? report.getStatus().getCode() : null);
                applyCommon(builder, report.getDataCodingScheme(), report.getProtocolIdentifier(), report.getUserData());
            }
            default -> throw new TpduDecodeException("Unsupported TPDU type: " + tpdu.getSmsTpduType());
        }

        return builder.build();
    }

    private void applyCommon(MessageRequestDecodeDataDTO.MessageRequestDecodeDataDTOBuilder builder,
                             DataCodingScheme dcs, ProtocolIdentifier pid, UserData userData)
            throws MAPException {
        builder.dataCoding(dcs == null ? 0 : dcs.getCode())
                .protocolId(pid == null ? 0 : pid.getCode());

        if (userData != null) {
            userData.decode();
            builder.encodedData(userData.getEncodedData())
                    .userData(userData);
        }
    }

    /**
     * Resolves the source address from the GPSI, formatted by removing the
     * {@code msisdn-} prefix when present.
     */
    private String resolveSourceAddr(String gpsi) {
        if (Objects.nonNull(gpsi) && !gpsi.isBlank()) {
            return gpsi.startsWith(MSISDN_PREFIX) ? gpsi.substring(MSISDN_PREFIX.length()) : gpsi;
        }
        return gpsi;
    }
}
