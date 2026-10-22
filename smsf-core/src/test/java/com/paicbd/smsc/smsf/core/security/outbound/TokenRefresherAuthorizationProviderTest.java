package com.paicbd.smsc.smsf.core.security.outbound;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TokenRefresherAuthorizationProviderTest {

    @Mock
    private OutboundTokenRefresher refresher;

    @Test
    @DisplayName("authorizationHeader, when a token is held, then it is returned")
    void authorizationHeaderWhenTokenHeldThenReturnsIt() {
        when(refresher.currentBearerHeader()).thenReturn(Optional.of("Bearer abc"));

        var provider = new TokenRefresherAuthorizationProvider(() -> refresher);

        assertEquals("Bearer abc", provider.authorizationHeader());
    }

    @Test
    @DisplayName("authorizationHeader, when no token is held, then null is returned")
    void authorizationHeaderWhenNoTokenThenReturnsNull() {
        when(refresher.currentBearerHeader()).thenReturn(Optional.empty());

        var provider = new TokenRefresherAuthorizationProvider(() -> refresher);

        assertNull(provider.authorizationHeader());
    }

    @Test
    @DisplayName("authorizationHeader, when there is no refresher yet, then null is returned")
    void authorizationHeaderWhenNoRefresherThenReturnsNull() {
        var provider = new TokenRefresherAuthorizationProvider(() -> null);

        assertNull(provider.authorizationHeader());
    }

    @Test
    @DisplayName("authorizationHeader, when the refresher appears later, then it is picked up")
    void authorizationHeaderWhenRefresherAppearsLaterThenPicksItUp() {
        AtomicReference<OutboundTokenRefresher> holder = new AtomicReference<>();
        var provider = new TokenRefresherAuthorizationProvider(holder::get);

        assertNull(provider.authorizationHeader());

        when(refresher.currentBearerHeader()).thenReturn(Optional.of("Bearer late"));
        holder.set(refresher);

        assertEquals("Bearer late", provider.authorizationHeader());
    }

    @Test
    @DisplayName("constructor, when the supplier is null, then the provider degrades to no credential")
    void constructorWhenSupplierNullThenNoCredential() {
        var provider = new TokenRefresherAuthorizationProvider(null);

        assertNull(provider.authorizationHeader());
    }
}
