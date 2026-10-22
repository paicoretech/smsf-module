package com.paicbd.smsc.smsf.core.security.outbound;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OAuth2TokenTest {

    @Test
    void expiresAtAddsExpiresInToIssuedAt() {
        Instant issuedAt = Instant.parse("2026-01-01T00:00:00Z");
        OAuth2Token token = new OAuth2Token("abc", 300L, issuedAt);
        assertEquals(issuedAt.plusSeconds(300), token.expiresAt());
    }

    @Test
    void isExpiredFalseWhenIssuedRecentlyAndZeroMargin() {
        OAuth2Token token = new OAuth2Token("abc", 600L, Instant.now());
        assertFalse(token.isExpired(0));
    }

    @Test
    void isExpiredTrueWhenIssuedLongAgo() {
        OAuth2Token token = new OAuth2Token("abc", 60L, Instant.now().minusSeconds(120));
        assertTrue(token.isExpired(0));
    }

    @Test
    void isExpiredHonoursMargin() {
        OAuth2Token token = new OAuth2Token("abc", 60L, Instant.now());
        assertTrue(token.isExpired(120));
        assertFalse(token.isExpired(10));
    }
}
