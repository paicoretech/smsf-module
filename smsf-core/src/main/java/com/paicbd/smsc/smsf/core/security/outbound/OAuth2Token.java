package com.paicbd.smsc.smsf.core.security.outbound;

import java.time.Instant;

public record OAuth2Token(String accessToken, long expiresInSeconds, Instant issuedAt) {

    public Instant expiresAt() {
        return issuedAt.plusSeconds(expiresInSeconds);
    }

    public boolean isExpired(long marginSeconds) {
        return Instant.now().plusSeconds(marginSeconds).isAfter(expiresAt());
    }
}
