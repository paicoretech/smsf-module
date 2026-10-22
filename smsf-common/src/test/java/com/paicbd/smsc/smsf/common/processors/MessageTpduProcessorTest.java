package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.smsf.common.processors.dto.MessageRequestDecodeDataDTO;
import com.paicore.sba.stack.codec.EncodeTpduRequest;
import com.paicore.sba.stack.codec.MessageType;
import com.paicore.sba.stack.codec.SmsTpduCodec;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import com.paicore.sba.stack.dto.MoMessageRequest;
import com.paicore.sba.stack.dto.MtMessageRequest;
import com.paicore.sba.stack.exceptions.InvalidTpduException;
import com.paicore.sba.stack.exceptions.TpduDecodeException;
import com.paicore.sba.stack.exceptions.TpduEncodeException;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import com.paicore.sba.stack.openapi.model.RefToBinaryData;
import com.paicore.sba.stack.openapi.model.SmsRecordData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.restcomm.protocols.ss7.map.api.MAPException;
import org.restcomm.protocols.ss7.map.api.smstpdu.AddressField;
import org.restcomm.protocols.ss7.map.api.smstpdu.DataCodingScheme;
import org.restcomm.protocols.ss7.map.api.smstpdu.ProtocolIdentifier;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsStatusReportTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsSubmitTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpdu;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpduType;
import org.restcomm.protocols.ss7.map.api.smstpdu.Status;
import org.restcomm.protocols.ss7.map.api.smstpdu.UserData;
import org.restcomm.protocols.ss7.map.api.smstpdu.ValidityPeriod;
import org.restcomm.protocols.ss7.map.api.smstpdu.ValidityPeriodFormat;
import org.restcomm.protocols.ss7.map.smstpdu.DataCodingSchemeImpl;
import org.restcomm.protocols.ss7.map.smstpdu.UserDataImpl;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageTpduProcessorTest {

    private static final int NETWORK_ID = 7;
    private static final String DESTINATION = "50212345678";
    private static final String MESSAGE_ID = "1783981200123-86234715900123";

    private final SmsTpduCodec codec = new Ss7SmsTpduCodec();
    private final MessageTpduProcessor processor = new MessageTpduProcessor(codec);

    private byte[] submitTpdu() {
        return codec.encode(EncodeTpduRequest.builder()
                .messageType(MessageType.SMS_SUBMIT)
                .destinationAddress(DESTINATION)
                .destinationAddressTon(1)
                .destinationAddressNpi(1)
                .message("Hello")
                .dataCoding(0x00)
                .build());
    }

    private AddressField mockAddress(String value) {
        AddressField address = mock(AddressField.class);
        when(address.getAddressValue()).thenReturn(value);
        return address;
    }

    private MessageTpduProcessor processorReturning(SmsTpdu tpdu) {
        SmsTpduCodec mockCodec = mock(SmsTpduCodec.class);
        when(mockCodec.decode(any())).thenReturn(tpdu);
        return new MessageTpduProcessor(mockCodec);
    }

    @Test
    @DisplayName("decode maps an SMS-SUBMIT and uses the GPSI (msisdn- stripped) as source address")
    void decodeSubmitUsesGpsiAsSourceAddr() {
        SmsRecordData recordData = new SmsRecordData().gpsi("msisdn-8613915900000");

        MessageRequestDecodeDataDTO dto = processor.decode(recordData, submitTpdu(), NETWORK_ID);

        assertEquals("8613915900000", dto.getSourceAddr());
        assertEquals(DESTINATION, dto.getAddressField().getAddressValue());
        assertEquals(0x00, dto.getDataCoding());
        assertEquals(SmsTpduType.SMS_SUBMIT, dto.getMessageType());
        assertFalse(dto.isUdhPresent());
        assertNotNull(dto.getEncodedData());
        assertTrue(dto.getEncodedData().length > 0);
        assertNull(dto.getValidityPeriod());
        assertEquals(NETWORK_ID, dto.getNetworkId());
        assertNotNull(dto.getSmsSubmitTpdu());
        assertNotNull(dto.getUserData());
    }

    @Test
    @DisplayName("decode keeps a GPSI that has no msisdn- prefix unchanged")
    void decodeKeepsGpsiWithoutMsisdnPrefix() {
        SmsRecordData recordData = new SmsRecordData().gpsi("extid-user@domain");

        MessageRequestDecodeDataDTO dto = processor.decode(recordData, submitTpdu(), NETWORK_ID);

        assertEquals("extid-user@domain", dto.getSourceAddr());
    }

    @Test
    @DisplayName("decode leaves the source address unset when the GPSI is blank")
    void decodeLeavesSourceAddrWhenGpsiBlank() {
        SmsRecordData recordData = new SmsRecordData().gpsi("  ");

        MessageRequestDecodeDataDTO dto = processor.decode(recordData, submitTpdu(), NETWORK_ID);

        assertEquals("  ", dto.getSourceAddr());
        assertEquals(DESTINATION, dto.getAddressField().getAddressValue());
    }

    @Test
    @DisplayName("decode from a MoMessageRequest extracts the embedded SmsRecordData")
    void decodeFromMoMessageRequest() {
        SmsRecordData recordData = new SmsRecordData().gpsi("msisdn-50211112222");
        MoMessageRequest request = MoMessageRequest.builder().smsRecord(recordData).build();

        MessageRequestDecodeDataDTO dto = processor.decode(request, submitTpdu(), NETWORK_ID);

        assertEquals("50211112222", dto.getSourceAddr());
        assertEquals(DESTINATION, dto.getAddressField().getAddressValue());
    }

    @Test
    @DisplayName("decode rejects a null MoMessageRequest")
    void decodeRejectsNullMoMessageRequest() {
        byte[] submitTpdu = submitTpdu();
        assertThrows(NullPointerException.class,
                () -> processor.decode((MoMessageRequest) null, submitTpdu, NETWORK_ID));
    }

    @Test
    @DisplayName("decode parses a multipart/related sendsms body end to end")
    void decodeMultipartRelatedBody() {
        SmsRecordData smsRecordData = new SmsRecordData()
                .smsRecordId("777c3edf-129f-486e-a3f8-c48e7b515605")
                .gpsi("msisdn-8613915900000")
                .smsPayload(new RefToBinaryData().contentId("sms"));
        MultipartRelatedEncoder.Encoded encoded = MultipartRelatedEncoder.encode(smsRecordData, submitTpdu());

        MessageRequestDecodeDataDTO dto = processor.decode(encoded.body(), encoded.contentType(), NETWORK_ID);

        assertEquals("8613915900000", dto.getSourceAddr());
        assertEquals(DESTINATION, dto.getAddressField().getAddressValue());
        assertEquals(SmsTpduType.SMS_SUBMIT, dto.getMessageType());
        assertEquals(NETWORK_ID, dto.getNetworkId());
    }

    @Test
    @DisplayName("decode maps an SMS-SUBMIT carrying UDH and a validity period")
    void decodeSubmitWithUdhAndValidity() {
        AddressField destination = mockAddress(DESTINATION);
        DataCodingScheme dcs = mock(DataCodingScheme.class);
        when(dcs.getCode()).thenReturn(0x08);
        ProtocolIdentifier pid = mock(ProtocolIdentifier.class);
        when(pid.getCode()).thenReturn(0x7F);
        ValidityPeriod validityPeriod = mock(ValidityPeriod.class);
        byte[] encoded = {0x05, 0x00, 0x03, (byte) 0xAA};
        UserData userData = mock(UserData.class);
        when(userData.getEncodedData()).thenReturn(encoded);

        SmsSubmitTpdu submit = mock(SmsSubmitTpdu.class);
        when(submit.getSmsTpduType()).thenReturn(SmsTpduType.SMS_SUBMIT);
        when(submit.getDestinationAddress()).thenReturn(destination);
        when(submit.getUserDataHeaderIndicator()).thenReturn(true);
        when(submit.getValidityPeriod()).thenReturn(validityPeriod);
        when(submit.getValidityPeriodFormat()).thenReturn(ValidityPeriodFormat.fieldPresentRelativeFormat);
        when(submit.getDataCodingScheme()).thenReturn(dcs);
        when(submit.getProtocolIdentifier()).thenReturn(pid);
        when(submit.getUserData()).thenReturn(userData);
        when(submit.getMessageReference()).thenReturn(42);

        SmsRecordData recordData = new SmsRecordData().gpsi("msisdn-50211112222");
        MessageRequestDecodeDataDTO dto = processorReturning(submit).decode(recordData, new byte[]{0x01}, NETWORK_ID);

        assertEquals(SmsTpduType.SMS_SUBMIT, dto.getMessageType());
        assertEquals(DESTINATION, dto.getAddressField().getAddressValue());
        assertTrue(dto.isUdhPresent());
        assertEquals(0x08, dto.getDataCoding());
        assertEquals(0x7F, dto.getProtocolId());
        assertArrayEquals(encoded, dto.getEncodedData());
        assertSame(validityPeriod, dto.getValidityPeriod());
        assertEquals(ValidityPeriodFormat.fieldPresentRelativeFormat, dto.getValidityPeriodFormat());
        assertSame(submit, dto.getSmsSubmitTpdu());
        assertSame(userData, dto.getUserData());
        assertEquals(42, dto.getMessageReference());
    }

    @Test
    @DisplayName("decode maps an SMS-STATUS-REPORT keeping the GPSI as source address")
    void decodeMapsStatusReport() {
        AddressField recipient = mockAddress("50298765432");
        SmsStatusReportTpdu report = mock(SmsStatusReportTpdu.class);
        Status status = mock(Status.class);
        when(report.getSmsTpduType()).thenReturn(SmsTpduType.SMS_STATUS_REPORT);
        when(report.getRecipientAddress()).thenReturn(recipient);
        when(report.getUserDataHeaderIndicator()).thenReturn(false);
        when(report.getUserData()).thenReturn(null);
        when(report.getMessageReference()).thenReturn(73);
        when(report.getStatus()).thenReturn(status);
        when(status.getCode()).thenReturn(Status.SMS_RECEIVED);

        SmsRecordData recordData = new SmsRecordData()
                .smsRecordId(MESSAGE_ID)
                .gpsi("msisdn-50211112222");
        MessageRequestDecodeDataDTO dto = processorReturning(report).decode(recordData, new byte[]{0x00}, NETWORK_ID);

        assertEquals(SmsTpduType.SMS_STATUS_REPORT, dto.getMessageType());
        assertEquals("50298765432", dto.getAddressField().getAddressValue());
        assertEquals("50211112222", dto.getSourceAddr());
        assertEquals(MESSAGE_ID, dto.getSmsRecordId());
        assertEquals(73, dto.getMessageReference());
        assertEquals(Status.SMS_RECEIVED, dto.getTpStatus());
        assertNull(dto.getEncodedData());
    }

    @Test
    @DisplayName("encodeDeliveryReport builds an SMS-STATUS-REPORT preserving correlation and message reference")
    void encodeDeliveryReportPreservesCorrelationData() {
        MessageEvent event = mtEvent();
        event.setMessageId(MESSAGE_ID);
        event.setErrorCode(0);
        event.setSourceAddrTon(1);
        event.setSourceAddrNpi(1);
        event.addCustomParam(com.paicbd.smsc.smsf.common.constants.Constants.HTTP2_MESSAGE_REFERENCE, 73);

        MultipartRelatedEncoder.Encoded encoded = processor.encodeDeliveryReport(event, NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure");
        MessageRequestDecodeDataDTO decoded = processor.decode(
                new SmsRecordData().smsRecordId(MESSAGE_ID).smsPayload(new RefToBinaryData().contentId("n1-sms-payload")),
                binaryPartOf(encoded), NETWORK_ID);

        assertEquals(SmsTpduType.SMS_STATUS_REPORT, decoded.getMessageType());
        assertEquals(73, decoded.getMessageReference());
        assertEquals(Status.SMS_RECEIVED, decoded.getTpStatus());
        assertEquals("12025550100", decoded.getAddressField().getAddressValue());
    }

    @Test
    @DisplayName("encodeDeliveryReport uses failure status and defaults invalid addressing metadata")
    void encodeDeliveryReportMapsFailureAndInvalidReference() {
        MessageEvent event = mtEvent();
        event.setMessageId(MESSAGE_ID);
        event.setErrorCode(Status.ERROR_IN_SME);
        event.setSourceAddrTon(null);
        event.setSourceAddrNpi(null);
        event.addCustomParam(com.paicbd.smsc.smsf.common.constants.Constants.HTTP2_MESSAGE_REFERENCE, "invalid");

        MultipartRelatedEncoder.Encoded encoded = processor.encodeDeliveryReport(event, NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure");
        MessageRequestDecodeDataDTO decoded = processor.decode(encoded.body(), encoded.contentType(), NETWORK_ID);

        assertEquals(0, decoded.getMessageReference());
        assertEquals(Status.ERROR_IN_SME, decoded.getTpStatus());
        assertEquals("12025550100", decoded.getAddressField().getAddressValue());
    }

    @Test
    @DisplayName("encodeDeliveryReport wraps invalid input")
    void encodeDeliveryReportWrapsInvalidInput() {
        TpduEncodeException error = assertThrows(TpduEncodeException.class, () -> processor.encodeDeliveryReport(null, NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure"));

        assertTrue(error.getCause() instanceof NullPointerException);
    }

    @Test
    @DisplayName("decode throws TpduDecodeException for an unsupported TPDU type")
    void decodeRejectsUnsupportedTpduType() {
        SmsTpdu deliver = mock(SmsTpdu.class);
        when(deliver.getSmsTpduType()).thenReturn(SmsTpduType.SMS_DELIVER);

        SmsRecordData recordData = new SmsRecordData().gpsi("msisdn-50211112222");
        MessageTpduProcessor unsupported = processorReturning(deliver);

        assertThrows(TpduDecodeException.class,
                () -> unsupported.decode(recordData, new byte[]{0x00}, NETWORK_ID));
    }

    @Test
    @DisplayName("decode wraps a user-data MAPException into a TpduDecodeException")
    void decodeWrapsMapException() throws MAPException {
        AddressField destination = mockAddress(DESTINATION);
        UserData userData = mock(UserData.class);
        doThrow(new MAPException("broken user data")).when(userData).decode();

        SmsSubmitTpdu submit = mock(SmsSubmitTpdu.class);
        when(submit.getSmsTpduType()).thenReturn(SmsTpduType.SMS_SUBMIT);
        when(submit.getDestinationAddress()).thenReturn(destination);
        when(submit.getUserData()).thenReturn(userData);

        SmsRecordData recordData = new SmsRecordData().gpsi("msisdn-50211112222");
        MessageTpduProcessor failing = processorReturning(submit);

        assertThrows(TpduDecodeException.class,
                () -> failing.decode(recordData, new byte[]{0x01}, NETWORK_ID));
    }

    @Test
    @DisplayName("decode propagates an InvalidTpduException for an empty TPDU")
    void decodePropagatesInvalidTpdu() {
        SmsRecordData recordData = new SmsRecordData().gpsi("msisdn-8613915900000");

        assertThrows(InvalidTpduException.class,
                () -> processor.decode(recordData, new byte[0], NETWORK_ID));
    }

    private MessageEvent mtEvent() {
        MessageEvent event = new MessageEvent();
        event.setMessageId("rec-1");
        event.setSourceAddr("12025550100");
        event.setSourceAddrTon(1);
        event.setSourceAddrNpi(1);
        event.setDestinationAddr("50212345678");
        event.setDestAddrTon(1);
        event.setDestAddrNpi(1);
        event.setDestination("imsi-50212345678");
        event.setDataCoding(0x04);
        event.setProtocolId((byte) 0x00);
        return event;
    }

    private UserData encodedUserData() {
        UserData userData = new UserDataImpl(
                "Hi", new DataCodingSchemeImpl(0x04), null, StandardCharsets.ISO_8859_1);
        assertDoesNotThrow(userData::encode);
        return userData;
    }

    @Test
    @DisplayName("encode builds a multipart/related body carrying the N1N2 request and the encoded TPDU")
    void encodeBuildsMultipartRelatedBody() {
        MultipartRelatedEncoder.Encoded encoded = processor.encode(mtEvent(), encodedUserData(), NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure");

        assertNotNull(encoded);
        assertTrue(encoded.contentType().startsWith("multipart/related"));

        String body = new String(encoded.body(), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(body.contains("\"n1MessageClass\":\"SMS\""));
        assertTrue(body.contains("\"contentId\":\"n1-sms-payload\""));
        assertTrue(body.contains("\"gpsi\":\"msisdn-50212345678\""));
        assertTrue(body.contains("\"n1n2FailureTxfNotifURI\":\"https://smsf.local/namf-cb/v1/4/n1n2-failure\""));
        assertTrue(body.contains("Content-Id: n1-sms-payload"));
    }

    @Test
    @DisplayName("buildMtMessageRequest encodes the TPDU and populates the SmsRecordData from the event")
    void buildMtMessageRequestEncodesTpduAndRecord() {
        UserData userData = encodedUserData();
        MtMessageRequest request = processor.buildMtMessageRequest(mtEvent(), userData, NETWORK_ID);

        assertEquals("rec-1", request.getCorrelationId());
        assertEquals("imsi-50212345678", request.getTargetSupi());
        assertEquals("rec-1", request.getSmsRecord().getSmsRecordId());
        assertEquals("msisdn-50212345678", request.getSmsRecord().getGpsi());
        assertEquals("sms", request.getSmsRecord().getSmsPayload().getContentId());
        assertNotNull(request.getTpduBinary());
        assertTrue(request.getTpduBinary().length > 0);
        assertArrayEquals(request.getTpduBinary(),
                codec.encode(EncodeTpduRequest.builder()
                        .messageType(MessageType.SMS_DELIVER)
                        .originatingAddress("12025550100")
                        .originatingAddressTon(1)
                        .originatingAddressNpi(1)
                        .destinationAddress("50212345678")
                        .destinationAddressTon(1)
                        .destinationAddressNpi(1)
                        .dataCoding(0x04)
                        .protocolId(0)
                        .userData(userData.getEncodedData())
                        .userDataLength(userData.getEncodedUserDataLength())
                        .userDataHeaderPresent(userData.getEncodedUserDataHeaderIndicator())
                        .build()));
    }

    @Test
    @DisplayName("buildMtMessageRequest rejects a null event or user data")
    void buildMtMessageRequestRejectsNull() {
        UserData userData = encodedUserData();
        assertThrows(NullPointerException.class,
                () -> processor.buildMtMessageRequest(null, userData, NETWORK_ID));

        MessageEvent event = mtEvent();
        assertThrows(NullPointerException.class,
                () -> processor.buildMtMessageRequest(event, null, NETWORK_ID));
    }

    private byte[] binaryPartOf(MultipartRelatedEncoder.Encoded encoded) {
        byte[] body = encoded.body();
        byte[] marker = "Content-Id: n1-sms-payload\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        int start = indexOf(body, marker, 0);
        assertTrue(start >= 0, "the binary part header was not found in the multipart body");
        start += marker.length;
        byte[] boundary = ("\r\n--" + encoded.contentType().split("boundary=\"")[1].split("\"")[0])
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        int end = indexOf(body, boundary, start);
        assertTrue(end > start, "the closing boundary was not found in the multipart body");
        return java.util.Arrays.copyOfRange(body, start, end);
    }

    private int indexOf(byte[] haystack, byte[] needle, int from) {
        outer:
        for (int i = from; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) continue outer;
            }
            return i;
        }
        return -1;
    }
}
