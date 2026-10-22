package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.Udh;
import com.paicbd.smsc.utils.CustomNumberingPlanIndicator;
import com.paicbd.smsc.utils.CustomTypeOfNumber;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.smsf.common.constants.Constants;
import com.paicbd.smsc.smsf.common.processors.dto.MessageRequestDecodeDataDTO;
import com.paicore.sba.stack.codec.Ss7SmsTpduCodec;
import com.paicore.sba.stack.exceptions.TpduEncodeException;
import com.paicore.sba.stack.mo.MoMultipartPayload;
import com.paicore.sba.stack.mo.MultipartRelatedDecoder;
import com.paicore.sba.stack.mo.MultipartRelatedEncoder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.restcomm.protocols.ss7.map.api.MAPException;
import org.restcomm.protocols.ss7.map.api.smstpdu.AddressField;
import org.restcomm.protocols.ss7.map.api.smstpdu.NumberingPlanIdentification;
import org.restcomm.protocols.ss7.map.api.smstpdu.TypeOfNumber;
import org.restcomm.protocols.ss7.map.api.smstpdu.UserData;
import org.restcomm.protocols.ss7.map.api.smstpdu.SmsTpduType;
import org.restcomm.protocols.ss7.map.api.smstpdu.Status;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageFactoryTest {

    private static final int NETWORK_ID = 7;
    private static final String MESSAGE_ID = "1783981200123-86234715900123";

    private final MessageFactory factory = new MessageFactory(
            null, new ConcurrentHashMap<>(),
            new MessageTpduProcessor(new Ss7SmsTpduCodec()));

    private MessageEvent mtEvent() {
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-1");
        event.setSourceAddr("12025550100");
        event.setSourceAddrTon(1);
        event.setSourceAddrNpi(1);
        event.setDestinationAddr("50212345678");
        event.setDestAddrTon(1);
        event.setDestAddrNpi(1);
        event.setDataCoding(0);
        event.setEsmClass(0);
        event.setProtocolId((byte) 0x00);
        event.setShortMessage("Hello");
        return event;
    }

    private MoMultipartPayload decode(MultipartRelatedEncoder.Encoded encoded) {
        assertNotNull(encoded);
        assertTrue(encoded.contentType().startsWith("multipart/related"));
        return MultipartRelatedDecoder.decode(encoded.body(), encoded.contentType());
    }

    @Test
    @DisplayName("createMtEncodeData builds a multipart/related body from a plain GSM7 event")
    void createMtEncodeDataBuildsMultipart() {
        MultipartRelatedEncoder.Encoded encoded = factory.createMtEncodeData(mtEvent(), NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure");
        String body = new String(encoded.body(), java.nio.charset.StandardCharsets.UTF_8);

        assertTrue(body.contains("\"n1MessageClass\":\"SMS\""));
        assertTrue(body.contains("\"gpsi\":\"msisdn-50212345678\""));
        assertTrue(body.contains("\"contentId\":\"n1-sms-payload\""));
        assertTrue(body.contains("Content-Id: n1-sms-payload"));
    }

    @Test
    @DisplayName("createMtEncodeData keeps an already msisdn-prefixed destination unchanged")
    void createMtEncodeDataKeepsPrefixedGpsi() {
        MessageEvent event = mtEvent();
        event.setDestinationAddr("msisdn-50212345678");

        MoMultipartPayload payload = decode(factory.createMtEncodeData(event, NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure"));

        assertEquals("msisdn-50212345678", payload.getJsonData().getGpsi());
    }

    @Test
    @DisplayName("createMtEncodeData leaves the gpsi null when the destination is blank")
    void createMtEncodeDataNullGpsiWhenDestinationBlank() {
        MessageEvent event = mtEvent();
        event.setDestinationAddr("  ");

        MoMultipartPayload payload = decode(factory.createMtEncodeData(event, NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure"));

        assertNull(payload.getJsonData().getGpsi());
    }

    @Test
    @DisplayName("validateConcatenationViaUdh returns null when the event has no UDH")
    void validateConcatenationNullWhenNoUdh() {
        MessageEvent event = new MessageEvent();
        event.setShortMessage("hello");

        assertNull(factory.validateConcatenationViaUdh(event, "hello"));
    }

    @Test
    @DisplayName("validateConcatenationViaUdh returns null when the UDH collection is empty")
    void validateConcatenationNullWhenUdhCollectionIsEmpty() {
        MessageEvent event = new MessageEvent();
        event.setUdhRaw(Collections.emptySet());
        event.setShortMessage("hello");

        assertNull(factory.validateConcatenationViaUdh(event, "hello"));
    }

    @Test
    @DisplayName("validateConcatenationViaUdh returns null when udhRaw only contains non-concat IEIs")
    void validateConcatenationNullWhenNoConcatIei() {
        MessageEvent event = new MessageEvent();
        event.setUdhRaw(setOf(new Udh("24", "0102")));
        event.setShortMessage("hello");

        assertNull(factory.validateConcatenationViaUdh(event, "hello"));
    }

    @Test
    @DisplayName("validateConcatenationViaUdh parses IEI 00 (8-bit reference) into (ref, total, seq)")
    void validateConcatenation8BitRef() {
        MessageEvent event = new MessageEvent();
        event.setUdhRaw(setOf(new Udh("00", "0003F20201")));
        event.setShortMessage("hello ");

        UtilsRecords.ConcatenatedMessageDetails details = factory.validateConcatenationViaUdh(event, "hello ");

        assertNotNull(details);
        assertEquals(0xF2, details.msgReferenceNumber());
        assertEquals(2, details.totalSegments());
        assertEquals(1, details.segmentSequence());
        assertEquals("hello ", details.text());
    }

    @Test
    @DisplayName("validateConcatenationViaUdh parses IEI 08 (16-bit reference) into (ref, total, seq)")
    void validateConcatenation16BitRef() {
        MessageEvent event = new MessageEvent();
        event.setUdhRaw(setOf(new Udh("08", "0804ABCD0302")));
        event.setShortMessage("world");

        UtilsRecords.ConcatenatedMessageDetails details = factory.validateConcatenationViaUdh(event, "world");

        assertNotNull(details);
        assertEquals(0xABCD, details.msgReferenceNumber());
        assertEquals(3, details.totalSegments());
        assertEquals(2, details.segmentSequence());
        assertEquals("world", details.text());
    }

    @Test
    @DisplayName("validateConcatenationViaUdh returns null when IEI 00 content is shorter than 5 bytes")
    void validateConcatenation8BitTruncatedContent() {
        MessageEvent event = new MessageEvent();
        event.setUdhRaw(setOf(new Udh("00", "0003F2")));

        assertNull(factory.validateConcatenationViaUdh(event, null));
    }

    @Test
    @DisplayName("validateConcatenationViaUdh returns null when IEI 08 content is shorter than 6 bytes")
    void validateConcatenation16BitTruncatedContent() {
        MessageEvent event = new MessageEvent();
        event.setUdhRaw(setOf(new Udh("08", "0804ABCD")));

        assertNull(factory.validateConcatenationViaUdh(event, null));
    }

    @Test
    @DisplayName("validateConcatenationViaUdh returns null on the defensive default switch branch")
    void validateConcatenationDefaultSwitchBranch() {
        Udh udh = mock(Udh.class);
        when(udh.iei()).thenReturn("00", "09");
        when(udh.content()).thenReturn("0003F20201");

        MessageEvent event = new MessageEvent();
        event.setUdhRaw(setOf(udh));

        assertNull(factory.validateConcatenationViaUdh(event, null));
    }

    @Test
    @DisplayName("createMessageEventFromMoMessage processes short message bytes when userData is present")
    void createMessageEventFromMoMessageProcessesShortMessageWhenUserDataPresent() {
        UserData userData = mock(UserData.class);
        when(userData.getEncodedData()).thenReturn("hello".getBytes(StandardCharsets.ISO_8859_1));
        when(userData.getDecodedMessage()).thenReturn("hello");

        MessageRequestDecodeDataDTO decoded = MessageRequestDecodeDataDTO.builder()
                .networkId(NETWORK_ID)
                .sourceAddr("12025550100")
                .dataCoding(0)
                .protocolId(0)
                .udhPresent(false)
                .userData(userData)
                .build();

        MessageEvent event = factory.createMessageEventFromMoMessage(decoded, "imsi-460000203924736");

        assertEquals("hello", event.getShortMessage());
        assertNotNull(event.getMessageBytes());
        assertEquals(0, event.getUdhLength());
    }

    @Test
    @DisplayName("createMessageEventFromMoMessage maps the destination address and translates TON/NPI")
    void createMessageEventFromMoMessageMapsDestinationAddress() {
        AddressField destination = mock(AddressField.class);
        when(destination.getAddressValue()).thenReturn("50212345678");
        when(destination.getTypeOfNumber()).thenReturn(TypeOfNumber.InternationalNumber);
        when(destination.getNumberingPlanIdentification())
                .thenReturn(NumberingPlanIdentification.ISDNTelephoneNumberingPlan);

        MessageRequestDecodeDataDTO decoded = MessageRequestDecodeDataDTO.builder()
                .networkId(NETWORK_ID)
                .sourceAddr("12025550100")
                .addressField(destination)
                .build();

        MessageEvent event = factory.createMessageEventFromMoMessage(decoded, "imsi-460000203924736");

        assertEquals("50212345678", event.getDestinationAddr());
        // The TPDU enums are translated to their SMPP numeric equivalents, not copied across.
        assertEquals(CustomTypeOfNumber.fromSmsTpdu(TypeOfNumber.InternationalNumber)
                .getSmscValue().value(), event.getDestAddrTon().byteValue());
        assertEquals(CustomNumberingPlanIndicator.fromSmsTpdu(
                        NumberingPlanIdentification.ISDNTelephoneNumberingPlan)
                .getSmscValue().value(), event.getDestAddrNpi().byteValue());
    }

    @Test
    @DisplayName("createMessageEventFromMoMessage leaves the destination untouched when the TPDU carries none")
    void createMessageEventFromMoMessageWithoutDestinationAddress() {
        MessageRequestDecodeDataDTO decoded = MessageRequestDecodeDataDTO.builder()
                .networkId(NETWORK_ID)
                .sourceAddr("12025550100")
                .build();

        MessageEvent event = factory.createMessageEventFromMoMessage(decoded, "imsi-460000203924736");

        assertNull(event.getDestinationAddr());
    }

    @Test
    @DisplayName("createDeliveryReportEvent creates a correlated HTTP2 DLR from an SMS status report")
    void createDeliveryReportEventCreatesCorrelatedDlr() {
        MessageRequestDecodeDataDTO decoded = MessageRequestDecodeDataDTO.builder()
                .messageType(SmsTpduType.SMS_STATUS_REPORT)
                .smsRecordId(MESSAGE_ID)
                .networkId(NETWORK_ID)
                .sourceAddr("50212345678")
                .messageReference(73)
                .tpStatus(Status.SMS_RECEIVED)
                .build();

        MessageEvent event = factory.createDeliveryReportEvent(decoded);

        assertTrue(event.getCheckSubmitSmResponse());
        assertEquals(MESSAGE_ID, event.getDeliverSmId());
        assertEquals(Constants.ORIGIN_PROTOCOL_HTTP2, event.getOriginProtocol());
        assertEquals("DELIVRD", event.getStatus());
        assertEquals(Status.SMS_RECEIVED, event.getErrorCode());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = " ")
    @DisplayName("createDeliveryReportEvent rejects a status report without correlation id")
    void createDeliveryReportEventRejectsMissingCorrelation(String smsRecordId) {
        MessageRequestDecodeDataDTO decoded = MessageRequestDecodeDataDTO.builder()
                .messageType(SmsTpduType.SMS_STATUS_REPORT)
                .smsRecordId(smsRecordId)
                .build();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> factory.createDeliveryReportEvent(decoded));

        assertTrue(error.getMessage().contains("smsRecordId"));
    }

    @Test
    @DisplayName("createDeliveryReportEvent rejects a TPDU that is not a status report")
    void createDeliveryReportEventRejectsNonStatusReport() {
        MessageRequestDecodeDataDTO decoded = MessageRequestDecodeDataDTO.builder()
                .messageType(SmsTpduType.SMS_SUBMIT)
                .smsRecordId(MESSAGE_ID)
                .build();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> factory.createDeliveryReportEvent(decoded));

        assertTrue(error.getMessage().contains("status report"));
    }

    @ParameterizedTest(name = "TP-Status {0} maps to {1}")
    @CsvSource(value = {
            "NULL, UNKNOWN",
            "1, ACCEPTD",
            "2, ACCEPTD",
            "32, ENROUTE",
            "70, EXPIRED",
            "71, DELETED",
            "72, DELETED",
            "73, DELETED",
            "99, REJECTD",
            "64, UNDELIV"
    }, nullValues = "NULL")
    void createDeliveryReportEventMapsTpStatus(Integer tpStatus, String expectedStatus) {
        MessageRequestDecodeDataDTO decoded = MessageRequestDecodeDataDTO.builder()
                .messageType(SmsTpduType.SMS_STATUS_REPORT)
                .smsRecordId(MESSAGE_ID)
                .tpStatus(tpStatus)
                .build();

        MessageEvent event = factory.createDeliveryReportEvent(decoded);

        assertEquals(expectedStatus, event.getStatus());
    }

    @Test
    @DisplayName("createMtEncodeData wraps MAP encode failures in TpduEncodeException")
    void createMtEncodeDataWrapsEncodeFailures() throws Exception {
        UserData userData = mock(UserData.class);
        doThrow(new MAPException("encode-failed")).when(userData).encode();

        MessageFactory factoryWithEncodeFailure = new MessageFactory(
                null, new ConcurrentHashMap<>(), mock(MessageTpduProcessor.class)) {
            @Override
            protected UserData buildUserDataForMt(MessageEvent event,
                                                  org.restcomm.protocols.ss7.map.api.smstpdu.DataCodingScheme dataCodingScheme) {
                return userData;
            }
        };
        MessageEvent event = mtEvent();

        TpduEncodeException ex = assertThrows(TpduEncodeException.class,
                () -> factoryWithEncodeFailure.createMtEncodeData(event, NETWORK_ID, "https://smsf.local/namf-cb/v1/4/n1n2-failure"));
        assertTrue(ex.getMessage().contains("TPDU encode failed"));
        assertTrue(ex.getCause() instanceof MAPException);
    }

    private Set<Udh> setOf(Udh... udhs) {
        Set<Udh> set = new LinkedHashSet<>();
        for (Udh udh : udhs) set.add(udh);
        return set;
    }

    // --- message id -----------------------------------------------------------

    @Test
    @DisplayName("the generated id keeps a millisecond prefix, which the CDR parses as submit_date")
    void generatedIdKeepsTheMillisecondPrefix() {
        // MessageEvent.parseSubmitDate does Long.parseLong(id.split("-")[0]). A bare UUID would
        // throw there and take down every CDR.
        long before = System.currentTimeMillis();
        MessageEvent event = factory.createMessageEventFromMoMessage(
                MessageRequestDecodeDataDTO.builder().networkId(NETWORK_ID).sourceAddr("12025550100").build(),
                "imsi-460000203924736");
        long after = System.currentTimeMillis();

        long prefix = Long.parseLong(event.getId().split("-")[0]);
        assertTrue(prefix >= before && prefix <= after,
                "the prefix must be the generation instant in millis, was " + prefix);
        assertEquals(event.getId(), event.getMessageId());
        assertEquals(event.getId(), event.getParentId());
    }

    @Test
    @DisplayName("ids stay unique under the concurrency the MO path actually runs at")
    void generatedIdsAreUniqueUnderConcurrency() throws Exception {
        // The MO path dispatches on an unbounded virtual-thread scheduler. The previous generator
        // (currentTimeMillis + nanoTime) collided on roughly 9% of ids at this concurrency.
        int threads = 32;
        int perThread = 2_000;
        Set<String> ids = ConcurrentHashMap.newKeySet();
        AtomicInteger collisions = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        MessageEvent event = factory.createMessageEventFromMoMessage(
                                MessageRequestDecodeDataDTO.builder().networkId(NETWORK_ID)
                                        .sourceAddr("12025550100").build(),
                                "imsi-460000203924736");
                        if (!ids.add(event.getMessageId())) {
                            collisions.incrementAndGet();
                        }
                    }
                    done.countDown();
                });
            }
            assertTrue(done.await(60, TimeUnit.SECONDS), "the id generation did not finish in time");
        }

        assertEquals(0, collisions.get(), "duplicate message ids were generated");
        assertEquals(threads * perThread, ids.size());
    }

}
