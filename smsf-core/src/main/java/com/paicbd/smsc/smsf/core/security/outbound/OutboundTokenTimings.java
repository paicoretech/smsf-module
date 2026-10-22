package com.paicbd.smsc.smsf.core.security.outbound;

/**
 * The three waits that govern the outbound token lifecycle. They are configured together and only
 * make sense read against each other, so they travel as one value.
 *
 * @param refreshMarginSeconds how long before expiry the next token is fetched
 * @param initialFetchTimeoutSeconds how long the bootstrap fetch is given before it is abandoned
 * @param retryBackoffSeconds how long to wait before retrying a fetch that failed
 */
public record OutboundTokenTimings(
        long refreshMarginSeconds,
        long initialFetchTimeoutSeconds,
        long retryBackoffSeconds
) {
}
