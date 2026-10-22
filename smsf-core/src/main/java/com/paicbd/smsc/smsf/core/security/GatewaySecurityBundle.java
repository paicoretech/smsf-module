package com.paicbd.smsc.smsf.core.security;

import com.paicbd.smsc.dto.Http2GatewayIncomingSecurity;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;

public record GatewaySecurityBundle(
        int networkId,
        Http2GatewayIncomingSecurity incoming,
        Http2GatewayNrfSecurity nrf,
        Http2GatewayOutboundSecurity outbound
) {
}
