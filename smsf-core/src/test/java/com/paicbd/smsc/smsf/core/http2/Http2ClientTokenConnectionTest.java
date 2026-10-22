package com.paicbd.smsc.smsf.core.http2;

import com.paicbd.smsc.dto.Http2Gateway;
import com.paicbd.smsc.dto.Http2GatewayOutboundSecurity;
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
class Http2ClientTokenConnectionTest {

    private Http2Gateway gatewayWithMtls;
    private Http2Gateway gatewayWithoutMtls;

    @BeforeEach
    void setUp() {
        gatewayWithMtls = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw-token", 1, "smsf.local:8443", ENABLED);
        gatewayWithMtls.getOutboundSecurity().setTokenEndpoint("https://oauth.example.com:9443/oauth2/token");

        gatewayWithoutMtls = Http2GatewayCreator.buildHttp2Gateway("smsf-gw", 2, "smsf.local:8443", ENABLED);
    }

    @Test
    @DisplayName("connect, when outbound security is null, then skip and remain disconnected")
    void connectWhenOutboundNullThenSkip() {
        gatewayWithMtls.setOutboundSecurity(null);
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class)) {
            connection.connect();
            assertTrue(mocked.constructed().isEmpty());
            assertFalse(connection.isConnected());
        }
    }

    @Test
    @DisplayName("connect, when token endpoint blank, then skip and remain disconnected")
    void connectWhenTokenEndpointBlankThenSkip() {
        gatewayWithoutMtls.getOutboundSecurity().setTokenEndpoint(" ");

        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithoutMtls);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class)) {
            connection.connect();
            assertTrue(mocked.constructed().isEmpty());
            assertFalse(connection.isConnected());
        }
    }

    @Test
    @DisplayName("connect, when token endpoint configured with mTLS, then instantiates client successfully")
    void connectWhenTokenEndpointConfiguredThenInstantiatesClient() {
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
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
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
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
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, ctx) -> doThrow(new RuntimeException("boom")).when(mock).connect())) {
            assertDoesNotThrow(connection::connect);
            assertFalse(connection.isConnected());
            assertFalse(mocked.constructed().isEmpty());
            verify(mocked.constructed().getFirst()).close();
        }
    }

    @Test
    @DisplayName("connect, when mTLS disabled, then settings TLS portion remains disabled")
    void connectWhenMtlsDisabledThenTlsDisabled() {
        Http2GatewayOutboundSecurity outbound = gatewayWithoutMtls.getOutboundSecurity();
        outbound.setMtlsEnabled(false);
        outbound.setTokenEndpoint("https://oauth.example.com:9090/oauth2/token");

        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithoutMtls);
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
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        assertDoesNotThrow(connection::disconnect);
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("disconnect, when connected, then closes client and clears state")
    void disconnectWhenConnectedThenClosesClient() throws Exception {
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        injectClient(connection, client);

        connection.disconnect();

        verify(client).close();
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("disconnect, when client.close() throws, then swallows and clears state")
    void disconnectWhenCloseThrowsThenSwallowed() throws Exception {
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        doThrow(new RuntimeException("close-failed")).when(client).close();
        injectClient(connection, client);

        assertDoesNotThrow(connection::disconnect);
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("send, when not connected, then returns failed future")
    void sendWhenNotConnectedThenFailedFuture() {
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        Http2Request request = Http2Request.builder().method("POST").path("/oauth2/token").build();

        CompletableFuture<Http2Response> future = connection.send(request);

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(IllegalStateException.class, ex.getCause());
    }

    @Test
    @DisplayName("send, when connected, then delegates to underlying client")
    void sendWhenConnectedThenDelegates() throws Exception {
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        when(client.isConnected()).thenReturn(true);
        Http2Response response = Http2Response.builder().status(200).build();
        when(client.send(any())).thenReturn(CompletableFuture.completedFuture(response));
        injectClient(connection, client);

        Http2Request request = Http2Request.builder().method("POST").path("/oauth2/token").build();
        CompletableFuture<Http2Response> future = connection.send(request);

        verify(client).send(request);
        assertTrue(future.isDone() && !future.isCompletedExceptionally());
    }

    @Test
    @DisplayName("getGateway, when accessed, then returns gateway provided")
    void getGatewayWhenAccessedThenReturnsGateway() {
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        assertSame(connection.getGateway(), gatewayWithMtls);
    }

    @Test
    @DisplayName("getClient, when connected, then exposes underlying client")
    void getClientWhenConnectedThenExposesClient() throws Exception {
        Http2ClientTokenConnection connection = new Http2ClientTokenConnection(gatewayWithMtls);
        NettyHttp2Client client = mock(NettyHttp2Client.class);
        injectClient(connection, client);

        assertSame(client, connection.getClient());
    }

    private void injectClient(Http2ClientTokenConnection connection, NettyHttp2Client mockClient)
            throws Exception {
        Field clientField = Http2ClientTokenConnection.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connection, mockClient);

        Field flagField = Http2ClientTokenConnection.class.getDeclaredField("connected");
        flagField.setAccessible(true);
        AtomicBoolean flag = (AtomicBoolean) flagField.get(connection);
        flag.set(true);
    }
}
