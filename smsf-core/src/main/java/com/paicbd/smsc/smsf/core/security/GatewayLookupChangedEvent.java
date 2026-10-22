package com.paicbd.smsc.smsf.core.security;

public record GatewayLookupChangedEvent(int networkId, ChangeType type) {

    public enum ChangeType {
        REFRESHED,
        EVICTED
    }
}
