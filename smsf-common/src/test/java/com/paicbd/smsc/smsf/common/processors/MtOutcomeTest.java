package com.paicbd.smsc.smsf.common.processors;

import com.paicbd.smsc.utils.ErrorCodes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MtOutcomeTest {

    @ParameterizedTest
    @CsvSource({
            "N1_N2_TRANSFER_INITIATED,DELIVERED",

            "ATTEMPTING_TO_REACH_UE,PENDING",
            "WAITING_FOR_ASYNCHRONOUS_TRANSFER,PENDING",
            "UE_NOT_RESPONDING,ABSENT",
            "UE_NOT_REACHABLE_FOR_SESSION,ABSENT",
            "N1_MSG_NOT_TRANSFERRED,ABSENT",
            "N2_MSG_NOT_TRANSFERRED,RETRYABLE",
            "TEMPORARY_REJECT_REGISTRATION_ONGOING,RETRYABLE",
            "TEMPORARY_REJECT_HANDOVER_ONGOING,RETRYABLE",
            "REJECTION_DUE_TO_PAGING_RESTRICTION,RETRYABLE",
            "AN_NOT_RESPONDING,RETRYABLE",
            "FAILURE_CAUSE_UNSPECIFIED,RETRYABLE"})
    @DisplayName("fromCause maps every one of the twelve N1N2MessageTransferCause values to an outcome")
    void fromCauseMapsEveryTransferCause(String cause, MtOutcome expected) {
        assertEquals(expected, MtOutcome.fromCause(cause));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "SOMETHING_THE_AMF_INVENTED"})
    @DisplayName("fromCause, when the cause is missing or unknown, then no outcome is decided")
    void fromCauseWhenMissingOrUnknownThenNull(String cause) {
        assertNull(MtOutcome.fromCause(cause));
    }

    @Test
    @DisplayName("fromCause accepts the cause as the AMF sends it, regardless of case or padding")
    void fromCauseIsCaseAndPaddingTolerant() {
        assertEquals(MtOutcome.ABSENT, MtOutcome.fromCause("  ue_not_responding  "));
    }

    @ParameterizedTest
    @CsvSource({
            "UE_NOT_RESPONDING,520",
            "UE_NOT_REACHABLE_FOR_SESSION,521",
            "N1_MSG_NOT_TRANSFERRED,522"})
    @DisplayName("errorCodeForCause gives each absent cause its own SMSC error code")
    void errorCodeForCauseResolvesTheAbsentCodes(String cause, int expected) {
        assertEquals(expected, MtOutcome.errorCodeForCause(cause));
    }

    @Test
    @DisplayName("errorCodeForCause, when the cause does not report absence, then there is no code to raise")
    void errorCodeForCauseWhenNotAbsentThenNull() {
        assertNull(MtOutcome.errorCodeForCause("AN_NOT_RESPONDING"));
        assertNull(MtOutcome.errorCodeForCause(null));
    }

    @Test
    @DisplayName("a message that never left the access network does not deactivate anyone")
    void n2NotTransferredDoesNotDeactivate() {
        assertEquals(MtOutcome.RETRYABLE, MtOutcome.fromCause("N2_MSG_NOT_TRANSFERRED"));
        assertNull(MtOutcome.errorCodeForCause("N2_MSG_NOT_TRANSFERRED"));
        assertFalse(MtOutcome.RETRYABLE.deactivatesSubscriber());
    }

    @Test
    @DisplayName("a cause that does not exist in TS 29.518 is not recognised")
    void inventedCausesAreNotRecognised() {
        assertNull(MtOutcome.fromCause("N2_MSG_TRANSFER_INITIATED"));
    }

    @Test
    @DisplayName("ABSENT is the only outcome that deactivates the subscriber")
    void onlyAbsentDeactivatesTheSubscriber() {
        assertTrue(MtOutcome.ABSENT.deactivatesSubscriber());
        assertFalse(MtOutcome.DELIVERED.deactivatesSubscriber());
        assertFalse(MtOutcome.PENDING.deactivatesSubscriber());
        assertFalse(MtOutcome.RETRYABLE.deactivatesSubscriber());
        assertFalse(MtOutcome.TRANSPORT_ERROR.deactivatesSubscriber());
    }

    @Test
    @DisplayName("PENDING is the only outcome that leaves the transaction open for the failure callback")
    void onlyPendingKeepsTheTransactionOpen() {
        assertFalse(MtOutcome.PENDING.closesTransaction());
        assertTrue(MtOutcome.DELIVERED.closesTransaction());
        assertTrue(MtOutcome.ABSENT.closesTransaction());
        assertTrue(MtOutcome.RETRYABLE.closesTransaction());
        assertTrue(MtOutcome.TRANSPORT_ERROR.closesTransaction());
    }

    @Test
    @DisplayName("RETRYABLE carries the temporary reject code so the message goes back to the retry flow")
    void retryableCarriesTheTemporaryRejectCode() {
        assertEquals(ErrorCodes.SMSF_UE_TEMPORARY_REJECT, MtOutcome.RETRYABLE.getErrorCode());
        assertNull(MtOutcome.DELIVERED.getErrorCode());
        assertNull(MtOutcome.ABSENT.getErrorCode());
    }
}
