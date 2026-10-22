package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.utils.ErrorCodes;

import java.util.Map;
import java.util.Objects;

/**
 * The outcome of one MT delivery, drawn from the cause the AMF reported.
 *
 * <p>The twelve causes of {@code N1N2MessageTransferCause} (TS 29.518) are covered. Only the three
 * that name the terminal — it did not respond, it is not reachable for the session, the N1 message
 * was not transferred — deactivate the subscriber. {@code N2_MSG_NOT_TRANSFERRED} does not: the
 * message never got past the access network, which says nothing about the subscriber.
 */
public enum MtOutcome {

    DELIVERED(null, false, true),
    PENDING(null, false, false),
    ABSENT(null, true, true),
    RETRYABLE(ErrorCodes.SMSF_UE_TEMPORARY_REJECT, false, true),
    TRANSPORT_ERROR(null, false, true);

    private static final Map<String, MtOutcome> BY_CAUSE = Map.ofEntries(
            Map.entry("N1_N2_TRANSFER_INITIATED", DELIVERED),
            Map.entry("ATTEMPTING_TO_REACH_UE", PENDING),
            Map.entry("WAITING_FOR_ASYNCHRONOUS_TRANSFER", PENDING),
            Map.entry("UE_NOT_RESPONDING", ABSENT),
            Map.entry("UE_NOT_REACHABLE_FOR_SESSION", ABSENT),
            Map.entry("N1_MSG_NOT_TRANSFERRED", ABSENT),
            Map.entry("N2_MSG_NOT_TRANSFERRED", RETRYABLE),
            Map.entry("TEMPORARY_REJECT_REGISTRATION_ONGOING", RETRYABLE),
            Map.entry("TEMPORARY_REJECT_HANDOVER_ONGOING", RETRYABLE),
            Map.entry("REJECTION_DUE_TO_PAGING_RESTRICTION", RETRYABLE),
            Map.entry("AN_NOT_RESPONDING", RETRYABLE),
            Map.entry("FAILURE_CAUSE_UNSPECIFIED", RETRYABLE));

    private static final Map<String, Integer> ABSENT_ERROR_CODES = Map.of(
            "UE_NOT_RESPONDING", ErrorCodes.SMSF_UE_NOT_RESPONDING,
            "UE_NOT_REACHABLE_FOR_SESSION", ErrorCodes.SMSF_UE_NOT_REACHABLE_FOR_SESSION,
            "N1_MSG_NOT_TRANSFERRED", ErrorCodes.SMSF_N1_MSG_NOT_TRANSFERRED);

    private final Integer errorCode;
    private final boolean deactivatesSubscriber;
    private final boolean closesTransaction;

    MtOutcome(Integer errorCode, boolean deactivatesSubscriber, boolean closesTransaction) {
        this.errorCode = errorCode;
        this.deactivatesSubscriber = deactivatesSubscriber;
        this.closesTransaction = closesTransaction;
    }

    public static MtOutcome fromCause(String cause) {
        if (Objects.isNull(cause) || cause.isBlank()) {
            return null;
        }
        return BY_CAUSE.get(cause.trim().toUpperCase());
    }

    public static Integer errorCodeForCause(String cause) {
        if (Objects.isNull(cause)) {
            return null;
        }
        return ABSENT_ERROR_CODES.get(cause.trim().toUpperCase());
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public boolean deactivatesSubscriber() {
        return deactivatesSubscriber;
    }

    public boolean closesTransaction() {
        return closesTransaction;
    }
}
