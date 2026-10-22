package com.paicbd.smsc.smsf.core.components;

import com.paicbd.smsc.smsf.core.ConnectionManager;
import com.paicbd.smsc.ws.SocketSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;

import static com.paicbd.smsc.smsf.common.constants.Constants.RESPONSE_HTTP2_CLIENT_ENDPOINT;
import static com.paicbd.smsc.utils.WebsocketConstants.CONNECT_HTTP2_GATEWAY_ENDPOINT;
import static com.paicbd.smsc.utils.WebsocketConstants.STOP_HTTP2_GATEWAY_ENDPOINT;
import static com.paicbd.smsc.utils.WebsocketConstants.UPDATE_ERROR_CODE_MAPPING;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CustomFrameHandlerTest {
    @Mock
    private SocketSession socketSession;

    @Mock
    private StompHeaders stompHeaders;

    @Mock
    private StompSession stompSession;

    @Mock
    private ConnectionManager connectionManager;

    @InjectMocks
    CustomFrameHandler customFrameHandler;

    @Test
    @DisplayName("connect destination delegates to ConnectionManager and acknowledges via STOMP")
    void connectDelegatesToConnectionManager() {
        String payload = "4";
        when(socketSession.getStompSession()).thenReturn(stompSession);
        when(stompHeaders.getDestination()).thenReturn(CONNECT_HTTP2_GATEWAY_ENDPOINT);

        customFrameHandler.handleFrameLogic(stompHeaders, payload);

        verify(connectionManager).updateHttp2Gateway(payload);
        verify(stompSession).send(RESPONSE_HTTP2_CLIENT_ENDPOINT, "The HTTP/2 Gateway has been updated");
    }

    @Test
    @DisplayName("stop destination delegates to ConnectionManager and acknowledges via STOMP")
    void stopDelegatesToConnectionManager() {
        String payload = "4";
        when(socketSession.getStompSession()).thenReturn(stompSession);
        when(stompHeaders.getDestination()).thenReturn(STOP_HTTP2_GATEWAY_ENDPOINT);

        customFrameHandler.handleFrameLogic(stompHeaders, payload);

        verify(connectionManager).updateHttp2Gateway(payload);
        verify(stompSession).send(RESPONSE_HTTP2_CLIENT_ENDPOINT, "The HTTP/2 Gateway has been updated");
    }

    @Test
    @DisplayName("update error code mapping is delegated and acknowledged")
    void updateErrorCodeMapping() {
        String payload = "1";
        when(socketSession.getStompSession()).thenReturn(stompSession);
        when(stompHeaders.getDestination()).thenReturn(UPDATE_ERROR_CODE_MAPPING);

        customFrameHandler.handleFrameLogic(stompHeaders, payload);

        verify(connectionManager).updateErrorCodeMapping(payload);
        verify(stompSession).send(RESPONSE_HTTP2_CLIENT_ENDPOINT, "The error code mapping has been updated");
    }

    @Test
    @DisplayName("unknown destination does not trigger any side effect")
    void unknownDestinationIgnored() {
        String payload = "HTTP2";
        when(stompHeaders.getDestination()).thenReturn("/app/invalid/path");

        customFrameHandler.handleFrameLogic(stompHeaders, payload);

        verifyNoInteractions(connectionManager);
        verifyNoInteractions(stompSession);
    }

    @ParameterizedTest
    @DisplayName("non numeric payload does not propagate to the connection manager")
    @ValueSource(strings = {CONNECT_HTTP2_GATEWAY_ENDPOINT, STOP_HTTP2_GATEWAY_ENDPOINT, UPDATE_ERROR_CODE_MAPPING})
    void nonNumericPayloadDoesNothing(String endpoint) {
        when(stompHeaders.getDestination()).thenReturn(endpoint);
        String payload = "notNumber";

        customFrameHandler.handleFrameLogic(stompHeaders, payload);

        verify(connectionManager, never()).updateHttp2Gateway(payload);
        verify(connectionManager, never()).updateErrorCodeMapping(payload);
    }
}
