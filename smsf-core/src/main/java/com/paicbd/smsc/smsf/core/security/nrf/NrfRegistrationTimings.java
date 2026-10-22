package com.paicbd.smsc.smsf.core.security.nrf;

/**
 * The waits that govern the NRF registration lifecycle. Configured together and only meaningful
 * read against each other, so they travel as one value.
 *
 * @param initialRegisterTimeoutSeconds how long the first registration is given before it is abandoned
 * @param retryBackoffSeconds how long to wait before retrying a registration that failed
 * @param defaultHeartbeatSeconds heartbeat period to use when the NRF reports none of its own
 */
public record NrfRegistrationTimings(
        long initialRegisterTimeoutSeconds,
        long retryBackoffSeconds,
        int defaultHeartbeatSeconds
) {
}
