package com.paicbd.smsc.smsf.core.security;

import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SmsfGatewaySecurityContextTest {

    @Test
    @DisplayName("returns flags and peer cert from the wrapped bundle")
    void readsFromBundleIncoming() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(true);
        incoming.setRequireMtls(true);
        incoming.setExpectedAmfCertificatePath("/etc/amf.pem");

        SmsfGatewaySecurityContext ctx = new SmsfGatewaySecurityContext(
                new GatewaySecurityBundle(11, incoming,
                        new Http2GatewayNrfSecurity(), new Http2GatewayOutboundSecurity()));

        assertThat(ctx.networkId()).isEqualTo(11);
        assertThat(ctx.isOauth2Enabled()).isTrue();
        assertThat(ctx.isRequireMtls()).isTrue();
        assertThat(ctx.expectedPeerCertificatePath()).isEqualTo("/etc/amf.pem");
    }

    @Test
    @DisplayName("returns safe defaults when incoming security is null")
    void handlesNullIncoming() {
        SmsfGatewaySecurityContext ctx = new SmsfGatewaySecurityContext(
                new GatewaySecurityBundle(22, null,
                        new Http2GatewayNrfSecurity(), new Http2GatewayOutboundSecurity()));

        assertThat(ctx.networkId()).isEqualTo(22);
        assertThat(ctx.isOauth2Enabled()).isFalse();
        assertThat(ctx.isRequireMtls()).isFalse();
        assertThat(ctx.expectedPeerCertificatePath()).isNull();
    }

    @Test
    @DisplayName("returns false flags when incoming security has them disabled")
    void readsDisabledFlags() {
        Http2GatewayIncomingSecurity incoming = new Http2GatewayIncomingSecurity();
        incoming.setOauth2Enabled(false);
        incoming.setRequireMtls(false);

        SmsfGatewaySecurityContext ctx = new SmsfGatewaySecurityContext(
                new GatewaySecurityBundle(33, incoming,
                        new Http2GatewayNrfSecurity(), new Http2GatewayOutboundSecurity()));

        assertThat(ctx.isOauth2Enabled()).isFalse();
        assertThat(ctx.isRequireMtls()).isFalse();
        assertThat(ctx.expectedPeerCertificatePath()).isNull();
    }
}
