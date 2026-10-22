package com.paicbd.smsc.smsf.common.redis;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Objects;

/**
 * NRF-derived OAuth2 access token as published to Redis by {@code smsf-core} and consumed by
 * {@code smsf-n21-registration} and {@code smsf-service-mt}.
 *
 * <p>A token is bound to the audience it was minted for, so an entry is identified by the pair
 * ({@code networkId}, {@code targetNfType}): the token issued for the AMF is not valid towards the
 * UDM.</p>
 *
 * <p>Both timestamps are carried explicitly. {@code expiresAtEpochSecond} lets a consumer reject a
 * stale entry without decoding the JWT, and drives when the refresh task wakes up.
 * {@code issuedAtEpochSecond} says when the core obtained it, which is what tells an operator
 * whether a token is being rotated or has simply been sitting there.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SmsfSecurityToken(
        int networkId,
        String targetNfType,
        String accessToken,
        String tokenType,
        String scope,
        long issuedAtEpochSecond,
        long expiresAtEpochSecond) {

    private static final String DEFAULT_TOKEN_TYPE = "Bearer";

    /** {@code true} when the token is already past its expiry, brought forward by {@code marginSeconds}. */
    @JsonIgnore
    public boolean isExpired(long marginSeconds) {
        return Instant.now().plusSeconds(Math.max(0, marginSeconds)).getEpochSecond() >= expiresAtEpochSecond;
    }

    /** Seconds left before expiry; never negative. */
    @JsonIgnore
    public long secondsToExpiry() {
        return Math.max(0, expiresAtEpochSecond - Instant.now().getEpochSecond());
    }

    /** The ready-to-use {@code Authorization} header value. */
    @JsonIgnore
    public String authorizationHeader() {
        String type = (Objects.isNull(tokenType) || tokenType.isBlank()) ? DEFAULT_TOKEN_TYPE : tokenType;
        return type + " " + accessToken;
    }

    /**
     * Whether this is a different credential from {@code other}, which is how the refresh task
     * detects that {@code smsf-core} has rotated the entry rather than left the old one in place.
     * A different expiry counts as a rotation even if the value happens to repeat.
     */
    @JsonIgnore
    public boolean isDifferentFrom(SmsfSecurityToken other) {
        if (Objects.isNull(other)) {
            return true;
        }
        return !Objects.equals(accessToken, other.accessToken())
                || expiresAtEpochSecond != other.expiresAtEpochSecond;
    }
}
