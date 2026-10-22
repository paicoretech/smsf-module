package com.paicbd.smsc.smsf.core.config;

import com.paicbd.smsc.smsf.core.components.CustomFrameHandler;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.ws.SocketClient;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static com.paicbd.smsc.utils.WebsocketConstants.CONNECT_HTTP2_GATEWAY_ENDPOINT;
import static com.paicbd.smsc.utils.WebsocketConstants.STOP_HTTP2_GATEWAY_ENDPOINT;
import static com.paicbd.smsc.utils.WebsocketConstants.UPDATE_ERROR_CODE_MAPPING;


@Slf4j
@Generated
@Configuration
@RequiredArgsConstructor
public class WebSocketConfig {
    /**
     * The three SMSF modules subscribe to the same gateway endpoints, so each registers under a
     * distinct name for the management panel to address them individually.
     */
    private static final String CLIENT_NAME = "SMSF-CORE";

    private final AppProperties appProperties;
    private final SocketSession socketSession;
    private final CustomFrameHandler customFrameHandler;

    @Bean
    public SocketClient socketClient() {
        List<String> topicsToSubscribe = List.of(
                UPDATE_ERROR_CODE_MAPPING,
                CONNECT_HTTP2_GATEWAY_ENDPOINT,
                STOP_HTTP2_GATEWAY_ENDPOINT
        );

        UtilsRecords.WebSocketConnectionParams wsp = new UtilsRecords.WebSocketConnectionParams(
                appProperties.isWsEnabled(),
                appProperties.getWsHost(),
                appProperties.getWsPort(),
                appProperties.getWsPath(),
                topicsToSubscribe,
                appProperties.getWsHeaderName(),
                appProperties.getWsHeaderValue(),
                appProperties.getWsRetryInterval(),
                CLIENT_NAME
        );
        return new SocketClient(customFrameHandler, wsp, socketSession);
    }
}
