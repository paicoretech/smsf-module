package com.paicbd.smsc.smsf.core.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayLookupChangedEventTest {

    @Test
    @DisplayName("exposes the networkId and REFRESHED type via record accessors")
    void recordHoldsRefreshedType() {
        GatewayLookupChangedEvent event = new GatewayLookupChangedEvent(11,
                GatewayLookupChangedEvent.ChangeType.REFRESHED);

        assertThat(event.networkId()).isEqualTo(11);
        assertThat(event.type()).isEqualTo(GatewayLookupChangedEvent.ChangeType.REFRESHED);
    }

    @Test
    @DisplayName("exposes the networkId and EVICTED type via record accessors")
    void recordHoldsEvictedType() {
        GatewayLookupChangedEvent event = new GatewayLookupChangedEvent(22,
                GatewayLookupChangedEvent.ChangeType.EVICTED);

        assertThat(event.networkId()).isEqualTo(22);
        assertThat(event.type()).isEqualTo(GatewayLookupChangedEvent.ChangeType.EVICTED);
    }

    @Test
    @DisplayName("ChangeType.valueOf round-trips both enum constants")
    void enumValues() {
        assertThat(GatewayLookupChangedEvent.ChangeType.values()).containsExactly(
                GatewayLookupChangedEvent.ChangeType.REFRESHED,
                GatewayLookupChangedEvent.ChangeType.EVICTED);
        assertThat(GatewayLookupChangedEvent.ChangeType.valueOf("REFRESHED"))
                .isEqualTo(GatewayLookupChangedEvent.ChangeType.REFRESHED);
        assertThat(GatewayLookupChangedEvent.ChangeType.valueOf("EVICTED"))
                .isEqualTo(GatewayLookupChangedEvent.ChangeType.EVICTED);
    }
}
