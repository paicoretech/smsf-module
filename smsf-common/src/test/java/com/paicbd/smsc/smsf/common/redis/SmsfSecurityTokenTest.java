package com.paicbd.smsc.smsf.common.redis;

import com.paicbd.smsc.utils.Converter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SmsfSecurityTokenTest {

    private static final String ACCESS_TOKEN = "eyJhbGciOiJSUzI1NiJ9.payload.signature";

    private static SmsfSecurityToken tokenExpiringIn(long seconds) {
        long now = Instant.now().getEpochSecond();
        return new SmsfSecurityToken(7, "AMF", ACCESS_TOKEN, "Bearer", "nsmsf-sms", now, now + seconds);
    }

    @Test
    @DisplayName("a token well inside its lifetime is not expired")
    void liveTokenIsNotExpired() {
        assertThat(tokenExpiringIn(300).isExpired(0)).isFalse();
        assertThat(tokenExpiringIn(300).secondsToExpiry()).isBetween(295L, 300L);
    }

    @Test
    @DisplayName("a token past its expiry is expired")
    void pastTokenIsExpired() {
        assertThat(tokenExpiringIn(-1).isExpired(0)).isTrue();
    }

    @Test
    @DisplayName("the margin brings the expiry forward, so a token dying in flight is rejected")
    void marginBringsExpiryForward() {
        SmsfSecurityToken token = tokenExpiringIn(20);

        assertThat(token.isExpired(0)).isFalse();
        assertThat(token.isExpired(10)).isFalse();
        assertThat(token.isExpired(30)).isTrue();
    }

    @Test
    @DisplayName("a negative margin is treated as no margin rather than extending the lifetime")
    void negativeMarginDoesNotExtendLifetime() {
        assertThat(tokenExpiringIn(-5).isExpired(-600)).isTrue();
    }

    @Test
    @DisplayName("secondsToExpiry never goes negative")
    void secondsToExpiryIsClampedAtZero() {
        assertThat(tokenExpiringIn(-120).secondsToExpiry()).isZero();
    }

    @Test
    @DisplayName("isDifferentFrom detects the rotation the refresh task waits for")
    void isDifferentFromDetectsRotation() {
        SmsfSecurityToken original = tokenExpiringIn(300);

        assertThat(original.isDifferentFrom(null)).isTrue();
        assertThat(original.isDifferentFrom(original)).isFalse();

        long now = Instant.now().getEpochSecond();
        assertThat(original.isDifferentFrom(
                new SmsfSecurityToken(7, "AMF", "other-value", "Bearer", "s", now, now + 300))).isTrue();
        // A new expiry counts as a rotation even if the value happens to repeat.
        assertThat(original.isDifferentFrom(
                new SmsfSecurityToken(7, "AMF", ACCESS_TOKEN, "Bearer", "s", now, now + 999))).isTrue();
    }

    @Test
    @DisplayName("the authorization header is the token type followed by the value")
    void authorizationHeaderUsesTheTokenType() {
        assertThat(tokenExpiringIn(60).authorizationHeader()).isEqualTo("Bearer " + ACCESS_TOKEN);
    }

    @Test
    @DisplayName("a missing or blank token type falls back to Bearer")
    void authorizationHeaderFallsBackToBearer() {
        long now = Instant.now().getEpochSecond();
        long exp = now + 60;
        assertThat(new SmsfSecurityToken(7, "AMF", ACCESS_TOKEN, null, "s", now, exp).authorizationHeader())
                .isEqualTo("Bearer " + ACCESS_TOKEN);
        assertThat(new SmsfSecurityToken(7, "AMF", ACCESS_TOKEN, "  ", "s", now, exp).authorizationHeader())
                .isEqualTo("Bearer " + ACCESS_TOKEN);
    }

    @Test
    @DisplayName("the token survives the JSON round trip smsf-core and its readers rely on")
    void survivesJsonRoundTrip() {
        SmsfSecurityToken original = tokenExpiringIn(120);

        SmsfSecurityToken parsed = Converter.stringToObject(
                Converter.valueAsString(original), SmsfSecurityToken.class);

        assertThat(parsed).isEqualTo(original);
    }

    @Test
    @DisplayName("an entry written by a newer publisher with extra fields is still readable")
    void toleratesUnknownFields() {
        String raw = """
                {"networkId":7,"targetNfType":"AMF","accessToken":"%s","tokenType":"Bearer",
                 "scope":"nsmsf-sms","issuedAtEpochSecond":%d,"expiresAtEpochSecond":%d,
                 "somethingAddedLater":"x"}
                """.formatted(ACCESS_TOKEN, Instant.now().getEpochSecond(),
                Instant.now().getEpochSecond() + 60);

        SmsfSecurityToken parsed = Converter.stringToObject(raw, SmsfSecurityToken.class);

        assertThat(parsed.accessToken()).isEqualTo(ACCESS_TOKEN);
        assertThat(parsed.targetNfType()).isEqualTo("AMF");
    }
}
