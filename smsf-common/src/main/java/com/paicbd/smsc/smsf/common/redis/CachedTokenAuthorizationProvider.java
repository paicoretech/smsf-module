package com.paicbd.smsc.smsf.common.redis;

import com.paicore.sba.stack.api.security.AuthorizationProvider;
import lombok.RequiredArgsConstructor;

/**
 * Supplies the {@code Authorization} header to the 5g-sba-stack clients from {@link SmsfTokenCache}.
 *
 * <p>The stack calls this once per outbound request — and again per NF discovery lookup — so it has
 * to be a memory read. The Redis round trip happens in the cache's background refresh, not here.</p>
 *
 * <p>Returns {@code null} when no usable token is cached, which is what the stack treats as "send
 * no credential". Callers that must not proceed unauthenticated check availability first; the cache
 * also stops their Kafka consumption when the credential is lost, so this should not normally be
 * reached with an empty cache.</p>
 */
@RequiredArgsConstructor
public class CachedTokenAuthorizationProvider implements AuthorizationProvider {

    private final SmsfTokenCache tokenCache;
    private final int networkId;
    private final String targetNfType;

    @Override
    public String authorizationHeader() {
        return tokenCache.authorizationHeader(networkId, targetNfType).orElse(null);
    }
}
