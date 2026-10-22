package com.paicbd.smsc.smsf.common.http2;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayNrfSecurity;
import com.paicbd.smsc.smsf.common.utils.Http2GatewayCreator;
import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import com.paicore.sba.stack.transport.http2.dto.Http2Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.paicbd.smsc.smsf.common.constants.Constants.ENABLED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockConstruction;

@ExtendWith(MockitoExtension.class)
class Http2ClientNrfConnectionTest {

    private Http2Gateway gatewayWithNrf;
    private Http2Gateway gatewayWithoutNrf;

    @BeforeEach
    void setUp() {
        gatewayWithNrf = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw-nrf", 1, "smsf.local:8443", ENABLED);
        gatewayWithoutNrf = Http2GatewayCreator.buildHttp2Gateway("smsf-gw", 2, "smsf.local:8443", ENABLED);
    }

    @Test
    @DisplayName("connect, when NRF security is null, then skip and remain disconnected")
    void connectWhenNrfNullThenSkip() {
        gatewayWithNrf.setNrfSecurity(null);
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class)) {
            connection.connect();
            assertTrue(mocked.constructed().isEmpty());
            assertFalse(connection.isConnected());
        }
    }

    @Test
    @DisplayName("connect, when NRF api root blank, then skip and remain disconnected")
    void connectWhenTokenEndpointBlankThenSkip() {
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setApiRoot(" ");
        gatewayWithoutNrf.setNrfSecurity(nrf);

        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithoutNrf);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class)) {
            connection.connect();
            assertTrue(mocked.constructed().isEmpty());
            assertFalse(connection.isConnected());
        }
    }

    @Test
    @DisplayName("connect, when NRF token endpoint configured with mTLS, then instantiates client successfully")
    void connectWhenNrfConfiguredThenInstantiatesClient() {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, ctx) -> when(mock.isConnected()).thenReturn(true))) {
            connection.connect();
            assertTrue(connection.isConnected());
            assertFalse(mocked.constructed().isEmpty());
            verify(mocked.constructed().getFirst()).connect();
        }
    }

    @Test
    @DisplayName("connect, when called twice while already connected, then does not reconnect")
    void connectWhenAlreadyConnectedThenSkip() throws Exception {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        when(client.isConnected()).thenReturn(true);
        injectClient(connection, client);

        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class)) {
            connection.connect();
            assertTrue(mocked.constructed().isEmpty());
        }
        assertTrue(connection.isConnected());
    }

    @Test
    @DisplayName("connect, when NettyHttp2Client.connect() throws, then remains disconnected and client closed")
    void connectWhenClientThrowsThenStaysDisconnected() {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, ctx) -> doThrow(new RuntimeException("boom")).when(mock).connect())) {
            assertDoesNotThrow(connection::connect);
            assertFalse(connection.isConnected());
            assertFalse(mocked.constructed().isEmpty());
            verify(mocked.constructed().getFirst()).close();
        }
    }

    @Test
    @DisplayName("connect, when NRF mTLS disabled, then settings TLS portion remains disabled")
    void connectWhenMtlsDisabledThenTlsDisabled() {
        Http2GatewayNrfSecurity nrf = new Http2GatewayNrfSecurity();
        nrf.setMtlsEnabled(false);
        nrf.setApiRoot("https://nrf.example.com:9090");
        nrf.setTokenPath("/oauth2/token");
        gatewayWithoutNrf.setNrfSecurity(nrf);

        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithoutNrf);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, ctx) -> when(mock.isConnected()).thenReturn(true))) {
            connection.connect();
            assertTrue(connection.isConnected());
            assertFalse(mocked.constructed().isEmpty());
        }
    }

    @Test
    @DisplayName("disconnect, when not connected, then no-op")
    void disconnectWhenNotConnectedThenNoOp() {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        assertDoesNotThrow(connection::disconnect);
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("disconnect, when connected, then closes client and clears state")
    void disconnectWhenConnectedThenClosesClient() throws Exception {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        injectClient(connection, client);

        connection.disconnect();

        verify(client).close();
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("disconnect, when client.close() throws, then swallows and clears state")
    void disconnectWhenCloseThrowsThenSwallowed() throws Exception {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        doThrow(new RuntimeException("close-failed")).when(client).close();
        injectClient(connection, client);

        assertDoesNotThrow(connection::disconnect);
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("send, when not connected, then returns failed future")
    void sendWhenNotConnectedThenFailedFuture() {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        Http2Request request = Http2Request.builder().method("POST").path("/token").build();

        CompletableFuture<Http2Response> future = connection.send(request);

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(IllegalStateException.class, ex.getCause());
    }

    @Test
    @DisplayName("send, when connected, then delegates to underlying client")
    void sendWhenConnectedThenDelegates() throws Exception {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        when(client.isConnected()).thenReturn(true);
        Http2Response response = Http2Response.builder().status(200).build();
        when(client.send(any())).thenReturn(CompletableFuture.completedFuture(response));
        injectClient(connection, client);

        Http2Request request = Http2Request.builder().method("POST").path("/token").build();
        CompletableFuture<Http2Response> future = connection.send(request);

        verify(client).send(request);
        assertTrue(future.isDone() && !future.isCompletedExceptionally());
    }

    @Test
    @DisplayName("getGateway, when accessed, then returns gateway provided")
    void getGatewayWhenAccessedThenReturnsGateway() {
        Http2ClientNrfConnection connection = new Http2ClientNrfConnection(gatewayWithNrf);
        assertSame(connection.getGateway(), gatewayWithNrf);
    }

    private void injectClient(Http2ClientNrfConnection connection, NettyHttp2Client mockClient)
            throws Exception {
        Field clientField = Http2ClientNrfConnection.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connection, mockClient);

        Field flagField = Http2ClientNrfConnection.class.getDeclaredField("connected");
        flagField.setAccessible(true);
        AtomicBoolean flag = (AtomicBoolean) flagField.get(connection);
        flag.set(true);
    }
}
