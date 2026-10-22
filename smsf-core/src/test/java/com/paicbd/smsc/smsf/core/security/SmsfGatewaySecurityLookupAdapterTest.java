package com.paicbd.smsc.smsf.core.security;

import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import com.paicore.sba.stack.security.auth.Authenticator;
import com.paicore.sba.stack.security.webfilter.GatewaySecurityContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SmsfGatewaySecurityLookupAdapterTest {

    @Mock
    private GatewaySecurityLookup delegate;

    private SmsfGatewaySecurityLookupAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new SmsfGatewaySecurityLookupAdapter(delegate);
    }

    @Test
    @DisplayName("findByNetworkId wraps the delegate bundle as SmsfGatewaySecurityContext")
    void findByNetworkIdWrapsBundle() {
        GatewaySecurityBundle bundle = bundle(11, true, true, "/etc/peer.pem");
        when(delegate.findByNetworkId(11)).thenReturn(Optional.of(bundle));

        Optional<GatewaySecurityContext> ctx = adapter.findByNetworkId(11);

        assertThat(ctx).isPresent();
        assertThat(ctx.get()).isInstanceOf(SmsfGatewaySecurityContext.class);
        assertThat(ctx.get().networkId()).isEqualTo(11);
        assertThat(ctx.get().isOauth2Enabled()).isTrue();
        assertThat(ctx.get().isRequireMtls()).isTrue();
        assertThat(ctx.get().expectedPeerCertificatePath()).isEqualTo("/etc/peer.pem");
    }

    @Test
    @DisplayName("findByNetworkId returns empty when the delegate has no entry")
    void findByNetworkIdMissing() {
        when(delegate.findByNetworkId(404)).thenReturn(Optional.empty());

        assertThat(adapter.findByNetworkId(404)).isEmpty();
    }

    @Test
    @DisplayName("authenticatorForNetworkId returns the delegate's authenticator")
    void authenticatorPresent() {
        Authenticator authenticator = mock(Authenticator.class);
        when(delegate.authenticatorForNetworkId(11)).thenReturn(Optional.of(authenticator));

        Optional<Authenticator> result = adapter.authenticatorForNetworkId(11);

        assertThat(result).containsSame(authenticator);
    }

    @Test
    @DisplayName("authenticatorForNetworkId returns empty when delegate has none")
    void authenticatorMissing() {
        when(delegate.authenticatorForNetworkId(404)).thenReturn(Optional.empty());

        assertThat(adapter.authenticatorForNetworkId(404)).isEmpty();
    }

    private static GatewaySecurityBundle bundle(int networkId, boolean oauth2, boolean requireMtls, String peer) {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(oauth2);
        incoming.setRequireMtls(requireMtls);
        incoming.setExpectedAmfCertificatePath(peer);
        return new GatewaySecurityBundle(networkId, incoming,
                new Http2GatewayNrfSecurity(), new Http2GatewayOutboundSecurity());
    }
}
