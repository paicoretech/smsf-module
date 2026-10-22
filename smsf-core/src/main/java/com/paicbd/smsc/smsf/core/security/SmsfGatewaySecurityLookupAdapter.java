package com.paicbd.smsc.smsf.core.security;

import com.paicore.sba.stack.security.auth.Authenticator;
import com.paicore.sba.stack.security.webfilter.GatewaySecurityContext;
import com.paicore.sba.stack.security.webfilter.GatewaySecurityLookup;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

@RequiredArgsConstructor
public class SmsfGatewaySecurityLookupAdapter implements GatewaySecurityLookup {

    private final com.paicbd.smsc.smsf.core.security.GatewaySecurityLookup delegate;

    @Override
    public Optional<GatewaySecurityContext> findByNetworkId(int networkId) {
        return delegate.findByNetworkId(networkId).map(b -> (GatewaySecurityContext) new SmsfGatewaySecurityContext(b));
    }

    @Override
    public Optional<Authenticator> authenticatorForNetworkId(int networkId) {
        return delegate.authenticatorForNetworkId(networkId);
    }
}
