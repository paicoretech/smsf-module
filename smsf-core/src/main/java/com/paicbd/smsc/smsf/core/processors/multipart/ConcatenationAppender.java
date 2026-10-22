package com.paicbd.smsc.smsf.core.processors.multipart;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.dto.UtilsRecords;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class ConcatenationAppender {
    public enum AppendResult {
        ADDED,
        DUPLICATE,
        INVALID_SEQUENCE
    }

    @Getter
    private final List<MessagePart> partsBuffer = new ArrayList<>();
    private final Set<Integer> receivedSequences = new HashSet<>();

    @Getter
    private final int totalParts;
    @Getter
    private final MessageEvent incomingEvent;
    @Getter
    private boolean isComplete = false;

    public ConcatenationAppender(int totalParts, MessageEvent incomingEvent) {
        this.totalParts = totalParts;
        this.incomingEvent = incomingEvent;
        this.incomingEvent.setMessageParts(new ArrayList<>());
        log.debug("Initialized ConcatenationAppender with key: {} and total parts: {}",
                incomingEvent.getSource(), totalParts);
    }

    public AppendResult initialize(MessageEvent incomingEvent, UtilsRecords.ConcatenatedMessageDetails details) {
        return appendPart(details, incomingEvent);
    }

    public AppendResult addMessagePart(UtilsRecords.ConcatenatedMessageDetails details, MessageEvent event) {
        return appendPart(details, event);
    }

    private AppendResult appendPart(UtilsRecords.ConcatenatedMessageDetails details, MessageEvent event) {
        int segmentSequence = details.segmentSequence();

        if (segmentSequence < 1 || segmentSequence > this.totalParts) {
            log.warn("Ignoring multipart segment with invalid sequence {} for total parts {}", segmentSequence, this.totalParts);
            return AppendResult.INVALID_SEQUENCE;
        }

        if (!this.receivedSequences.add(segmentSequence)) {
            log.info("Ignoring duplicate multipart segment {} for parent {}", segmentSequence, this.incomingEvent.getParentId());
            return AppendResult.DUPLICATE;
        }

        if (segmentSequence == this.totalParts) {
            this.incomingEvent.setRegisteredDelivery(event.getRegisteredDelivery());
        }

        MessagePart segment = MessagePart.builder()
                .messageId(event.getMessageId())
                .shortMessage(details.text())
                .msgReferenceNumber(String.valueOf(details.msgReferenceNumber()))
                .totalSegment(details.totalSegments())
                .segmentSequence(details.segmentSequence())
                .optionalParameters(event.getOptionalParameters())
                .customParams(Objects.nonNull(event.getCustomParams()) ? new HashMap<>(event.getCustomParams()) : null)
                .partBytes(event.getMessageBytes())
                .udhBytes(event.getUdhBytes())
                .udhRaw(event.getUdhRaw())
                .build();

        this.partsBuffer.add(segment);
        this.incomingEvent.getMessageParts().add(segment);
        this.isComplete = this.receivedSequences.size() == this.totalParts;
        return AppendResult.ADDED;
    }
}
