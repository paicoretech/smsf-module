package com.paicbd.smsc.smsf.core.security.outbound;

import com.paicore.sba.stack.api.security.AuthorizationProvider;
import lombok.RequiredArgsConstructor;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Bridges an {@link OutboundTokenRefresher} to the stack's {@link AuthorizationProvider}, so
 * the 5g-sba-stack clients read the current bearer at request time.
 *
 * <p>The refresher is resolved through a supplier rather than captured once, so a refresher
 * that is (re)started after this provider was handed to a client is still picked up.</p>
 */
@RequiredArgsConstructor
public class TokenRefresherAuthorizationProvider implements AuthorizationProvider {

    private final Supplier<OutboundTokenRefresher> refresherSupplier;

    @Override
    public String authorizationHeader() {
        OutboundTokenRefresher refresher = Objects.isNull(refresherSupplier) ? null : refresherSupplier.get();
        return Objects.isNull(refresher) ? null : refresher.currentBearerHeader().orElse(null);
    }
}
