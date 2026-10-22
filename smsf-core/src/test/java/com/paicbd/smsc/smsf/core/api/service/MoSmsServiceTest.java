package com.paicbd.smsc.smsf.core.api.service;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.core.stack.CoreStackManager;
import com.paicbd.smsc.smsf.core.api.exception.DlrCorrelationNotFoundException;
import com.paicbd.smsc.smsf.common.processors.MessageTpduProcessor;
import com.paicbd.smsc.smsf.common.processors.MessageFactory;
import com.paicbd.smsc.smsf.core.processors.multipart.MultiPartsHandler;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicore.sba.stack.codec.EncodeTpduRequest;
import com.paicore.sba.stack.codec.MessageType;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import com.paicore.sba.stack.openapi.model.RefToBinaryData;
import com.paicore.sba.stack.openapi.model.SmsRecordData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.paicbd.smsc.scylla.ScyllaTablesConstants.SMSF_SUBMIT_SM_RESULT_TABLE;

class MoSmsServiceTest {

    private static final int NETWORK_ID = 11;
    private static final String SUPI = "imsi-460000203924736";
    private static final String DESTINATION = "50212345678";
    private static final String SOURCE_ADDR_RAW = "8613915900000";
    private static final String GPSI = "msisdn-" + SOURCE_ADDR_RAW;
    private static final String MESSAGE_ID = "1783981200123-86234715900123";

    private final Ss7SmsTpduCodec codec = new Ss7SmsTpduCodec();

    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> kafkaTemplate = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);

    private final MultiPartsHandler multiPartsHandler = mock(MultiPartsHandler.class);
    private final ScyllaManager scyllaManager = mock(ScyllaManager.class);

    private MoSmsService service;
    private MessageFactory messageFactory;
    private CoreStackManager stackManager;

    @BeforeEach
    void setUp() {
        Http2Gateway gateway = new Http2Gateway();
        gateway.setNetworkId(NETWORK_ID);
        messageFactory = new MessageFactory(
                gateway, new ConcurrentHashMap<>(), new MessageTpduProcessor(new Ss7SmsTpduCodec()));
        stackManager = mock(CoreStackManager.class);
        when(stackManager.getMessageFactory()).thenReturn(messageFactory);
        ConcurrentMap<Integer, CoreStackManager> gatewayStacks = new ConcurrentHashMap<>();
        gatewayStacks.put(NETWORK_ID, stackManager);
        service = new MoSmsService(kafkaTemplate, multiPartsHandler, gatewayStacks, scyllaManager);
    }

    private byte[] tpdu() {
        return codec.encode(EncodeTpduRequest.builder()
                .messageType(MessageType.SMS_SUBMIT)
                .destinationAddress(DESTINATION)
                .destinationAddressTon(1)
                .destinationAddressNpi(1)
                .message("Hello world")
                .dataCoding(0)
                .protocolId(0)
                .build());
    }

    private MultipartRelatedEncoder.Encoded encodedMultipart() {
        SmsRecordData smsRecordData = new SmsRecordData()
                .smsRecordId(MESSAGE_ID)
                .gpsi(GPSI)
                .smsPayload(new RefToBinaryData().contentId("sms"));
        return MultipartRelatedEncoder.encode(smsRecordData, tpdu());
    }

    @Test
    @DisplayName("an MO does not touch the subscriber state: only a registration brings one back")
    void moDoesNotTouchTheSubscriberState() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();

        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        verify(scyllaManager, never()).updateSmsfRegEventState(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString());
        verify(kafkaTemplate).send(org.mockito.ArgumentMatchers.eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC),
                org.mockito.ArgumentMatchers.anyString());
    }

    private MessageEvent capturePublishedEvent() {
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(
                org.mockito.ArgumentMatchers.eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC),
                payloadCaptor.capture());
        return Converter.stringToObject(payloadCaptor.getValue(), MessageEvent.class);
    }

    private MessageEvent capturePublishedDeliveryReport() {
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(org.mockito.ArgumentMatchers.eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), payloadCaptor.capture());
        return Converter.stringToObject(payloadCaptor.getValue(), MessageEvent.class);
    }

    @Test
    @DisplayName("the MO is counted on its own gateway's watcher, and only once it is on Kafka")
    void countsMoOnItsGatewayOnlyAfterPublishing() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();

        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        // The counter lives on the per-gateway stack, so each tenant reports its own MO rate.
        verify(stackManager).incrementMoProcessed();
        verify(kafkaTemplate).send(org.mockito.ArgumentMatchers.eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC),
                org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    @DisplayName("a message that never reaches Kafka is not counted as processed")
    void doesNotCountWhatItCouldNotPublish() {
        // The rate has to say what got through. Counting on entry would report a healthy throughput
        // while nothing actually left the module.
        doThrow(new IllegalStateException("kafka down"))
                .when(kafkaTemplate).send(org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString());
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();

        String contentType = encoded.contentType();
        byte[] body = encoded.body();

        assertThrows(IllegalStateException.class,
                () -> service.processMoSms(NETWORK_ID, SUPI, contentType, body));

        verify(stackManager, never()).incrementMoProcessed();
    }

    @Test
    @DisplayName("a publish interrupted by a cancelled request is still counted: the record already reached the producer")
    void countsPublishInterruptedByCancellation() {
        doThrow(new KafkaException("Interrupted", new InterruptedException()))
                .when(kafkaTemplate).send(org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString());
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();

        String messageId = service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        assertNotNull(messageId);
        verify(stackManager).incrementMoProcessed();
    }

    @Test
    @DisplayName("a KafkaException that is not an interruption still fails the request")
    void stillFailsOnRealKafkaErrors() {
        doThrow(new KafkaException("Send failed", new IllegalStateException("broker gone")))
                .when(kafkaTemplate).send(org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString());
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();

        String contentType = encoded.contentType();
        byte[] body = encoded.body();

        assertThrows(KafkaException.class,
                () -> service.processMoSms(NETWORK_ID, SUPI, contentType, body));

        verify(stackManager, never()).incrementMoProcessed();
    }

    @Test
    @DisplayName("processMoSms publishes the MessageEvent to the medium topic and returns its id")
    void processMoSmsPublishesToMediumTopic() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        String messageId = service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        assertNotNull(messageId);
        MessageEvent event = capturePublishedEvent();
        assertEquals(messageId, event.getMessageId());
        assertEquals(messageId, event.getId());
        assertEquals(messageId, event.getParentId());
    }

    @Test
    @DisplayName("processMoSms sets the base identifying fields for an HTTP2 MO")
    void processMoSmsSetsBaseFields() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        MessageEvent event = capturePublishedEvent();
        assertEquals(Constants.ORIGIN_PROTOCOL_HTTP2, event.getOriginProtocol());
        assertEquals(Constants.NETWORK_TYPE_GATEWAY, event.getOriginNetworkType());
        assertEquals(NETWORK_ID, event.getOriginNetworkId());
        assertEquals(Constants.NETWORK_TYPE_SERVICE_PROVIDER, event.getDestNetworkType());
        assertEquals(SUPI, event.getSource());
        assertEquals(SOURCE_ADDR_RAW, event.getSourceAddr());
        assertEquals("", event.getRetryDestNetworkId());
        assertEquals(GeneralSmscConstants.MEDIUM_PRIORITY, event.getSmscMessagePriority());
        assertTrue(event.isMoMessage());
    }

    @Test
    @DisplayName("processMoSms fills in destination address + TON/NPI from the TPDU")
    void processMoSmsFillsDestinationFromTpdu() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        MessageEvent event = capturePublishedEvent();
        assertEquals(DESTINATION, event.getDestinationAddr());
        assertNotNull(event.getDestAddrTon());
        assertNotNull(event.getDestAddrNpi());
    }

    @Test
    @DisplayName("processMoSms uses SignalingFactory to populate short message and udh fields")
    void processMoSmsProcessesUserData() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        MessageEvent event = capturePublishedEvent();
        assertEquals("Hello world", event.getShortMessage());
        assertNotNull(event.getMessageBytes());
        assertTrue(event.getMessageBytes().length > 0);
        assertEquals(0, event.getUdhLength());
        assertNotNull(event.getUdhBytes());
        assertEquals(0, event.getEsmClass());
    }

    @Test
    @DisplayName("processMoSms sets data coding and protocol id from the TPDU")
    void processMoSmsSetsDataCodingAndProtocolId() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        MessageEvent event = capturePublishedEvent();
        assertEquals(0, event.getDataCoding());
        assertEquals(0, event.getProtocolId());
    }

    @Test
    @DisplayName("processMoSms attaches the message reference as a custom parameter")
    void processMoSmsAttachesMessageReferenceCustomParam() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        MessageEvent event = capturePublishedEvent();
        assertNotNull(event.getCustomParams());
        Object ref = event.getCustomParams().get(Constants.HTTP2_MESSAGE_REFERENCE);
        assertNotNull(ref);
    }

    @Test
    @DisplayName("processMoSms publishes an incoming SMS status report as a correlated DLR")
    void processMoSmsPublishesIncomingStatusReportAsDlr() {
        MessageEvent outboundDlr = MessageEvent.builder()
                .messageId(MESSAGE_ID)
                .sourceAddr(SOURCE_ADDR_RAW)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destinationAddr(DESTINATION)
                .protocolId((byte) 0)
                .errorCode(0)
                .isDlr(true)
                .build();
        outboundDlr.addCustomParam(Constants.HTTP2_MESSAGE_REFERENCE, 73);
        MultipartRelatedEncoder.Encoded encoded = encodeIncomingReport(outboundDlr);
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID))
                .thenReturn(Converter.valueAsString(MessageEvent.builder().messageId(MESSAGE_ID).build()));

        String generatedId = service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        MessageEvent published = capturePublishedDeliveryReport();
        assertEquals(MESSAGE_ID, generatedId);
        assertNull(published.getMessageId());
        assertEquals(MESSAGE_ID, published.getDeliverSmId());
        assertEquals("DELIVRD", published.getStatus());
        assertTrue(published.getCheckSubmitSmResponse());
        InOrder correlationBeforePublish = inOrder(scyllaManager, kafkaTemplate);
        correlationBeforePublish.verify(scyllaManager).selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID);
        correlationBeforePublish.verify(kafkaTemplate).send(org.mockito.ArgumentMatchers.eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), org.mockito.ArgumentMatchers.anyString());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = " ")
    @DisplayName("processMoSms rejects an incoming SMS status report without a stored correlation")
    void processMoSmsRejectsIncomingStatusReportWithoutStoredCorrelation(String storedRequest) {
        MessageEvent outboundDlr = MessageEvent.builder()
                .messageId(MESSAGE_ID)
                .sourceAddr(SOURCE_ADDR_RAW)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destinationAddr(DESTINATION)
                .protocolId((byte) 0)
                .errorCode(0)
                .isDlr(true)
                .build();
        MultipartRelatedEncoder.Encoded encoded = encodeIncomingReport(outboundDlr);
        when(scyllaManager.selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID)).thenReturn(storedRequest);

        String contentType = encoded.contentType();
        byte[] body = encoded.body();

        DlrCorrelationNotFoundException error = assertThrows(DlrCorrelationNotFoundException.class,
                () -> service.processMoSms(NETWORK_ID, SUPI, contentType, body));

        assertTrue(error.getMessage().contains(MESSAGE_ID));
        verify(scyllaManager).selectFromTable(SMSF_SUBMIT_SM_RESULT_TABLE, MESSAGE_ID);
        verify(kafkaTemplate, never()).send(org.mockito.ArgumentMatchers.eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    @DisplayName("processMoSms with GSM7 non-udh sets esmClass to 0")
    void processMoSmsEsmClassZeroWhenNoUdh() {
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        MessageEvent event = capturePublishedEvent();
        assertEquals(0, event.getEsmClass());
    }

    @Test
    @DisplayName("processMoSms propagates decode failures when the payload is invalid")
    void processMoSmsPropagatesDecodeFailure() {
        byte[] garbage = "not a valid multipart".getBytes(StandardCharsets.UTF_8);
        assertThrows(RuntimeException.class, () -> service.processMoSms(NETWORK_ID, SUPI, "application/json", garbage));
    }

    @Test
    @DisplayName("processMoSms throws IllegalStateException when the networkId has no registered stack")
    void processMoSmsThrowsWhenGatewayMissing() {
        MoSmsService serviceWithoutStacks = new MoSmsService(kafkaTemplate, multiPartsHandler, new ConcurrentHashMap<>(), scyllaManager);
        MultipartRelatedEncoder.Encoded encoded = encodedMultipart();
        String contentType = encoded.contentType();
        byte[] body = encoded.body();

        assertThrows(IllegalStateException.class,
                () -> serviceWithoutStacks.processMoSms(NETWORK_ID, SUPI, contentType, body));
    }

    @Test
    @DisplayName("processMoSms delegates a concat MO to the multipart handler and does NOT publish to Kafka directly")
    void processMoSmsMultipartDelegatesAndDoesNotPublish() {
        MultipartRelatedEncoder.Encoded encoded = concatEncoded();

        service.processMoSms(NETWORK_ID, SUPI, encoded.contentType(), encoded.body());

        verify(multiPartsHandler).messagePartsProcessor(
                org.mockito.ArgumentMatchers.any(MessageEvent.class),
                org.mockito.ArgumentMatchers.any(com.paicbd.smsc.dto.UtilsRecords.ConcatenatedMessageDetails.class),
                org.mockito.ArgumentMatchers.eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC));
        verify(kafkaTemplate, never()).send(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(String.class));
    }

    private MultipartRelatedEncoder.Encoded concatEncoded() {
        byte[] udh = new byte[]{0x05, 0x00, 0x03, (byte) 0xF2, 0x02, 0x01};
        byte[] text = "hello ".getBytes(StandardCharsets.UTF_8);
        byte[] userData = new byte[udh.length + text.length];
        System.arraycopy(udh, 0, userData, 0, udh.length);
        System.arraycopy(text, 0, userData, udh.length, text.length);

        byte[] concatTpdu = codec.encode(EncodeTpduRequest.builder()
                .messageType(MessageType.SMS_SUBMIT)
                .destinationAddress(DESTINATION)
                .destinationAddressTon(1)
                .destinationAddressNpi(1)
                .userData(userData)
                .userDataLength(userData.length)
                .userDataHeaderPresent(true)
                .dataCoding(4)
                .protocolId(0)
                .build());

        SmsRecordData smsRecordData = new SmsRecordData()
                .smsRecordId(MESSAGE_ID)
                .gpsi(GPSI)
                .smsPayload(new RefToBinaryData().contentId("sms"));
        return MultipartRelatedEncoder.encode(smsRecordData, concatTpdu);
    }

    private MultipartRelatedEncoder.Encoded encodeIncomingReport(MessageEvent outboundDlr) {
        MultipartRelatedEncoder.Encoded outbound = messageFactory.createMtEncodeData(
                outboundDlr, NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure");
        SmsRecordData incoming = new SmsRecordData()
                .smsRecordId(MESSAGE_ID)
                .gpsi(GPSI)
                .smsPayload(new RefToBinaryData().contentId("sms"));
        return MultipartRelatedEncoder.encode(incoming, binaryPartOf(outbound));
    }

    private byte[] binaryPartOf(MultipartRelatedEncoder.Encoded encoded) {
        byte[] body = encoded.body();
        byte[] marker = "Content-Id: n1-sms-payload\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        int start = indexOf(body, marker, 0) + marker.length;
        byte[] boundary = ("\r\n--" + encoded.contentType().split("boundary=\"")[1].split("\"")[0])
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return java.util.Arrays.copyOfRange(body, start, indexOf(body, boundary, start));
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
