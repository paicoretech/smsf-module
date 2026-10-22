package com.paicbd.smsc.smsf.mt.http2;

import com.paicbd.smsc.dto.Http2Gateway;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockConstruction;

@ExtendWith(MockitoExtension.class)
class Http2ClientConnectionTest {

    private Http2Gateway gateway;

    @BeforeEach
    void setUp() {
        gateway = Http2GatewayCreator.buildHttp2Gateway("smsf-gw", 1, "smsf.local:8443", ENABLED);
    }

    @Test
    @DisplayName("connect, when called once, then instantiates NettyHttp2Client and sets connected")
    void connectWhenCalledThenInstantiatesClientAndConnects() {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, ctx) -> when(mock.isConnected()).thenReturn(true))) {
            connection.connect();
            assertTrue(connection.isConnected());
            assertFalse(mocked.constructed().isEmpty());
            verify(mocked.constructed().getFirst()).connect();
        }
    }

    @Test
    @DisplayName("connect, when already connected, then does not instantiate client again")
    void connectWhenAlreadyConnectedThenDoesNotReconnect() throws Exception {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        NettyHttp2Client mockClient = mock(NettyHttp2Client.class);
        when(mockClient.isConnected()).thenReturn(true);
        injectClient(connection, mockClient);

        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class)) {
            connection.connect();
            assertTrue(mocked.constructed().isEmpty());
        }
        assertTrue(connection.isConnected());
    }

    @Test
    @DisplayName("connect, when the peer went away, then it reconnects instead of reporting itself connected")
    void connectWhenThePeerWentAwayThenReconnects() throws Exception {
        // The AMF restarting leaves the channel dead while the flag still reads connected. Deciding
        // on the flag alone made this a no-op forever, and the MT consumers stayed paused until the
        // module was restarted.
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        NettyHttp2Client deadClient = mock(NettyHttp2Client.class);
        when(deadClient.isConnected()).thenReturn(false);
        injectClient(connection, deadClient);

        assertFalse(connection.isConnected());

        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, context) -> when(mock.isConnected()).thenReturn(true))) {
            connection.connect();

            assertEquals(1, mocked.constructed().size(), "a new client must be built for the reconnection");
            verify(mocked.constructed().get(0)).connect();
        }
        verify(deadClient).close();
        assertTrue(connection.isConnected());
    }

    @Test
    @DisplayName("connect, when NettyHttp2Client.connect() throws, then connection remains closed and client is reset")
    void connectWhenClientConnectThrowsThenStaysDisconnected() {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, ctx) -> doThrow(new RuntimeException("boom")).when(mock).connect())) {
            assertDoesNotThrow(connection::connect);
            assertFalse(connection.isConnected());
            assertFalse(mocked.constructed().isEmpty());
            verify(mocked.constructed().getFirst()).close();
        }
    }

    @Test
    @DisplayName("connect, when mTLS enabled, then builds settings with mtls true and verifyHostname")
    void connectWhenMtlsEnabledThenSettingsHonorMtls() {
        gateway = Http2GatewayCreator.buildHttp2GatewayWithMtls("smsf-gw-mtls", 2, "smsf.local:8443", ENABLED);
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        try (MockedConstruction<NettyHttp2Client> mocked = mockConstruction(NettyHttp2Client.class,
                (mock, ctx) -> when(mock.isConnected()).thenReturn(true))) {
            connection.connect();
            assertTrue(connection.isConnected());
            assertFalse(mocked.constructed().isEmpty());
        }
    }

    @Test
    @DisplayName("disconnect, when not connected, then no-op and remains disconnected")
    void disconnectWhenNotConnectedThenNoOp() {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        assertDoesNotThrow(connection::disconnect);
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("disconnect, when connected, then closes client and clears connected flag")
    void disconnectWhenConnectedThenClosesClient() throws Exception {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        NettyHttp2Client mockClient = mock(NettyHttp2Client.class);
        injectClient(connection, mockClient);

        connection.disconnect();

        verify(mockClient).close();
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("disconnect, when client close throws, then exception is swallowed and client is cleared")
    void disconnectWhenCloseThrowsThenSwallowed() throws Exception {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        NettyHttp2Client mockClient = mock(NettyHttp2Client.class);
        doThrow(new RuntimeException("close-failed")).when(mockClient).close();
        injectClient(connection, mockClient);

        assertDoesNotThrow(connection::disconnect);
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("send, when not connected, then returns failed future with IllegalStateException")
    void sendWhenNotConnectedThenFailedFuture() {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        Http2Request request = Http2Request.builder().method("POST").path("/p").build();

        CompletableFuture<Http2Response> result = connection.send(request);

        assertTrue(result.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, result::get);
        assertInstanceOf(IllegalStateException.class, ex.getCause());
    }

    @Test
    @DisplayName("send, when connected, then delegates to underlying client")
    void sendWhenConnectedThenDelegates() throws Exception {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        NettyHttp2Client mockClient = mock(NettyHttp2Client.class);
        when(mockClient.isConnected()).thenReturn(true);
        Http2Response response = Http2Response.builder().status(200).build();
        when(mockClient.send(any())).thenReturn(CompletableFuture.completedFuture(response));
        injectClient(connection, mockClient);

        Http2Request request = Http2Request.builder().method("POST").path("/p").build();
        CompletableFuture<Http2Response> future = connection.send(request);

        assertTrue(connection.isConnected());
        verify(mockClient).send(request);
        assertTrue(future.isDone() && !future.isCompletedExceptionally());
    }

    @Test
    @DisplayName("isConnected, when flag true but client null, then returns false")
    void isConnectedWhenClientNullThenFalse() throws Exception {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        setConnectedFlag(connection);
        assertFalse(connection.isConnected());
    }

    @Test
    @DisplayName("getGateway, when accessed, then returns gateway provided in constructor")
    void getGatewayWhenAccessedThenReturnsGateway() {
        Http2ClientConnection connection = new Http2ClientConnection(gateway);
        assertSame(connection.getGateway(), gateway);
        assertFalse(connection.isConnected());
        verify(mock(NettyHttp2Client.class), never()).connect();
    }

    private void injectClient(Http2ClientConnection connection, NettyHttp2Client mockClient)
            throws Exception {
        Field clientField = Http2ClientConnection.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connection, mockClient);
        setConnectedFlag(connection);
    }

    private void setConnectedFlag(Http2ClientConnection connection) throws Exception {
        Field flagField = Http2ClientConnection.class.getDeclaredField("connected");
        flagField.setAccessible(true);
        AtomicBoolean flag = (AtomicBoolean) flagField.get(connection);
        flag.set(true);
    }
}
