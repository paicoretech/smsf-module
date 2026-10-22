package com.paicbd.smsc.smsf.core.processors.multipart;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcatenationAppenderTest {

    private static final int TOTAL_PARTS = 3;

    private MessageEvent incomingEvent;
    private ConcatenationAppender appender;

    @BeforeEach
    void setUp() {
        incomingEvent = MessageEvent.builder()
                .id("id-1")
                .messageId("msg-1")
                .parentId("parent-1")
                .source("imsi-460000203924736")
                .build();
        appender = new ConcatenationAppender(TOTAL_PARTS, incomingEvent);
    }

    @Test
    @DisplayName("initialize + addMessagePart sets each incoming segment as ADDED and populates the parts buffer")
    void addMessagePartAddedResultAndPopulatesBuffer() {
        UtilsRecords.ConcatenatedMessageDetails first = details(1, "part-1");
        UtilsRecords.ConcatenatedMessageDetails second = details(2, "part-2");

        assertEquals(ConcatenationAppender.AppendResult.ADDED, appender.initialize(incomingEvent, first));
        assertEquals(ConcatenationAppender.AppendResult.ADDED, appender.addMessagePart(second, incomingEvent));

        assertEquals(2, appender.getPartsBuffer().size());
        assertEquals("part-1", appender.getPartsBuffer().get(0).getShortMessage());
        assertEquals("part-2", appender.getPartsBuffer().get(1).getShortMessage());
        assertFalse(appender.isComplete());
    }

    @Test
    @DisplayName("isComplete flips to true only when all N segments have been received")
    void isCompleteWhenAllSegmentsReceived() {
        appender.initialize(incomingEvent, details(1, "a"));
        appender.addMessagePart(details(2, "b"), incomingEvent);
        assertFalse(appender.isComplete());

        appender.addMessagePart(details(3, "c"), incomingEvent);

        assertTrue(appender.isComplete());
        assertEquals(3, appender.getPartsBuffer().size());
    }

    @Test
    @DisplayName("duplicate segment sequence returns DUPLICATE and does not grow the buffer")
    void duplicateSegmentReturnsDuplicate() {
        appender.initialize(incomingEvent, details(1, "part-1"));

        ConcatenationAppender.AppendResult result = appender.addMessagePart(details(1, "part-1-dup"), incomingEvent);

        assertEquals(ConcatenationAppender.AppendResult.DUPLICATE, result);
        assertEquals(1, appender.getPartsBuffer().size());
    }

    @Test
    @DisplayName("segment sequence below 1 returns INVALID_SEQUENCE")
    void segmentBelowRangeReturnsInvalidSequence() {
        ConcatenationAppender.AppendResult result = appender.addMessagePart(details(0, "x"), incomingEvent);

        assertEquals(ConcatenationAppender.AppendResult.INVALID_SEQUENCE, result);
        assertTrue(appender.getPartsBuffer().isEmpty());
    }

    @Test
    @DisplayName("segment sequence above totalParts returns INVALID_SEQUENCE")
    void segmentAboveRangeReturnsInvalidSequence() {
        ConcatenationAppender.AppendResult result = appender.addMessagePart(details(TOTAL_PARTS + 1, "x"), incomingEvent);

        assertEquals(ConcatenationAppender.AppendResult.INVALID_SEQUENCE, result);
        assertTrue(appender.getPartsBuffer().isEmpty());
    }

    @Test
    @DisplayName("appended segments are also propagated into MessageEvent.messageParts for downstream consumers")
    void segmentsPropagatedToIncomingEvent() {
        appender.initialize(incomingEvent, details(1, "a"));
        appender.addMessagePart(details(2, "b"), incomingEvent);

        assertNotNull(incomingEvent.getMessageParts());
        assertEquals(2, incomingEvent.getMessageParts().size());
        assertEquals("a", incomingEvent.getMessageParts().get(0).getShortMessage());
    }

    @Test
    @DisplayName("final segment propagates its delivery report request to the reassembled event")
    void finalSegmentPropagatesRegisteredDelivery() {
        MessageEvent finalSegment = MessageEvent.builder()
                .id("1720701000000-100003")
                .messageId("1720701000000-200003")
                .parentId("1720701000000-300003")
                .source("imsi-460000203924736")
                .registeredDelivery(1)
                .build();

        incomingEvent.setRegisteredDelivery(0);
        appender.initialize(incomingEvent, details(1, "a"));
        appender.addMessagePart(details(TOTAL_PARTS, "c"), finalSegment);

        assertEquals(1, incomingEvent.getRegisteredDelivery());
    }

    private UtilsRecords.ConcatenatedMessageDetails details(int seq, String text) {
        return new UtilsRecords.ConcatenatedMessageDetails(0xF2, TOTAL_PARTS, seq, text);
    }
}
