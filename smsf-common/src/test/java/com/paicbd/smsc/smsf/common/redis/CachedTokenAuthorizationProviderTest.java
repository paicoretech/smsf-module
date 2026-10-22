package com.paicbd.smsc.smsf.common.redis;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachedTokenAuthorizationProviderTest {

    private static final int NETWORK_ID = 10;
    private static final String UDM = "UDM";

    @Mock
    private SmsfTokenCache tokenCache;

    private CachedTokenAuthorizationProvider provider() {
        return new CachedTokenAuthorizationProvider(tokenCache, NETWORK_ID, UDM);
    }

    @Test
    @DisplayName("the header comes from the cache for the configured gateway and audience")
    void returnsTheCachedHeader() {
        when(tokenCache.authorizationHeader(NETWORK_ID, UDM)).thenReturn(Optional.of("Bearer abc"));

        assertThat(provider().authorizationHeader()).isEqualTo("Bearer abc");
        verify(tokenCache).authorizationHeader(NETWORK_ID, UDM);
    }

    @Test
    @DisplayName("no cached token yields null, which the stack reads as 'send no credential'")
    void returnsNullWhenNothingCached() {
        when(tokenCache.authorizationHeader(NETWORK_ID, UDM)).thenReturn(Optional.empty());

        assertThat(provider().authorizationHeader()).isNull();
    }

    @Test
    @DisplayName("the header is resolved per call, so a token refreshed in the background is picked up")
    void resolvesPerCall() {
        when(tokenCache.authorizationHeader(NETWORK_ID, UDM))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of("Bearer refreshed"));

        CachedTokenAuthorizationProvider provider = provider();

        assertThat(provider.authorizationHeader()).isNull();
        assertThat(provider.authorizationHeader()).isEqualTo("Bearer refreshed");
        verify(tokenCache, times(2)).authorizationHeader(NETWORK_ID, UDM);
    }

    @Test
    @DisplayName("it only reads memory: the provider never reaches Redis on the request path")
    void neverTouchesRedis() {
        when(tokenCache.authorizationHeader(NETWORK_ID, UDM)).thenReturn(Optional.of("Bearer abc"));

        for (int i = 0; i < 100; i++) {
            provider().authorizationHeader();
        }

        // The cache is the only collaborator; there is no token store to hit from here.
        verify(tokenCache, times(100)).authorizationHeader(NETWORK_ID, UDM);
        verifyNoMoreInteractions(tokenCache);
    }

    @Test
    @DisplayName("each audience is asked for separately, so the AMF token is never sent to the UDM")
    void audienceIsPartOfTheLookup() {
        when(tokenCache.authorizationHeader(NETWORK_ID, "AMF")).thenReturn(Optional.of("Bearer amf"));
        when(tokenCache.authorizationHeader(NETWORK_ID, UDM)).thenReturn(Optional.of("Bearer udm"));

        assertThat(new CachedTokenAuthorizationProvider(tokenCache, NETWORK_ID, "AMF").authorizationHeader())
                .isEqualTo("Bearer amf");
        assertThat(provider().authorizationHeader()).isEqualTo("Bearer udm");
    }
}
