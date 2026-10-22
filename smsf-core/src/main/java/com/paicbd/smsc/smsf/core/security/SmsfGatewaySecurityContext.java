package com.paicbd.smsc.smsf.core.security;

import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicore.sba.stack.security.webfilter.GatewaySecurityContext;

import java.util.Objects;

public record SmsfGatewaySecurityContext(GatewaySecurityBundle bundle) implements GatewaySecurityContext {

    @Override
    public int networkId() {
        return bundle.networkId();
    }

    @Override
    public boolean isOauth2Enabled() {
        Http2GatewayIncomingSecurity incoming = bundle.incoming();
        return Objects.nonNull(incoming) && incoming.isOauth2Enabled();
    }

    @Override
    public boolean isRequireMtls() {
        Http2GatewayIncomingSecurity incoming = bundle.incoming();
        return Objects.nonNull(incoming) && incoming.isRequireMtls();
    }

    @Override
    public String expectedPeerCertificatePath() {
        Http2GatewayIncomingSecurity incoming = bundle.incoming();
        return Objects.isNull(incoming) ? null : incoming.getExpectedAmfCertificatePath();
    }
}
