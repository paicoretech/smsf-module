package com.paicbd.smsc.smsf.common.processors;

/**
 * How long to wait for the AMF failure callback when the AMF reported no {@code maxWaitingTime} of
 * its own, so an operator can tune it without a rebuild.
 */
public record MtTimings(int correlationWaitSeconds) {

    private static final int DEFAULT_CORRELATION_WAIT_SECONDS = 300;

    public static MtTimings defaults() {
        return new MtTimings(DEFAULT_CORRELATION_WAIT_SECONDS);
    }
}
