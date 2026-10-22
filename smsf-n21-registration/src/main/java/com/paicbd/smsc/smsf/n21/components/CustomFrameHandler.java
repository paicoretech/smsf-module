package com.paicbd.smsc.smsf.n21.components;

import com.paicbd.smsc.smsf.n21.ConnectionManager;
import com.paicbd.smsc.ws.FrameHandler;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static com.paicbd.smsc.smsf.common.constants.Constants.RESPONSE_HTTP2_CLIENT_ENDPOINT;
import static com.paicbd.smsc.utils.WebsocketConstants.CONNECT_HTTP2_GATEWAY_ENDPOINT;
import static com.paicbd.smsc.utils.WebsocketConstants.STOP_HTTP2_GATEWAY_ENDPOINT;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomFrameHandler implements FrameHandler {
    private final SocketSession socketSession;
    private final ConnectionManager connectionManager;

    @Override
    public void handleFrameLogic(StompHeaders headers, Object payload) {
        String payloadId = payload.toString();
        String destination = headers.getDestination();
        Objects.requireNonNull(payloadId, "payload cannot be null");
        Objects.requireNonNull(destination, "Destination cannot be null");

        switch (destination) {
            case CONNECT_HTTP2_GATEWAY_ENDPOINT -> handleGatewayChange("Connecting", payloadId);
            case STOP_HTTP2_GATEWAY_ENDPOINT -> handleGatewayChange("Stopping", payloadId);
            default -> log.warn("Received notification for unknown destination {}", destination);
        }
    }

    private void handleGatewayChange(String action, String payload) {
        try {
            int networkId = Integer.parseInt(payload);
            log.info("{} HTTP/2 Gateway {}", action, networkId);
            this.connectionManager.updateHttp2Gateway(payload);
            this.socketSession.getStompSession().send(RESPONSE_HTTP2_CLIENT_ENDPOINT, "The HTTP/2 Gateway has been updated");
        } catch (NumberFormatException e) {
            log.error("Error on {} HTTP/2 Gateway {} is not a number", action.toLowerCase(), payload);
        }
    }
}
