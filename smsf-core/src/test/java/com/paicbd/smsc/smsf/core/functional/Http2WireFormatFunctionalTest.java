package com.paicbd.smsc.smsf.core.functional;

import com.paicore.sba.stack.transport.http2.NettyHttp2Client;
import com.paicore.sba.stack.transport.http2.config.ConnectionPoolSettings;
import com.paicore.sba.stack.transport.http2.config.Http2ClientSettings;
import com.paicore.sba.stack.transport.http2.config.TlsSettings;
import com.paicore.sba.stack.transport.http2.dto.Http2Request;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Pins down what the SMSF puts on the wire when it opens an HTTP/2 connection.
 *
 * <p>It matters beyond curiosity: the module speaks cleartext HTTP/2 on non-standard ports, and a
 * protocol analyser only recognises that if the connection opens with the HTTP/2 preface — prior
 * knowledge — rather than an HTTP/1.1 upgrade. If this ever regressed to an upgrade handshake, the
 * traffic would still work but would stop being readable as HTTP/2 in a capture.</p>
 */
class Http2WireFormatFunctionalTest {

    private static final String HTTP2_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

    @Test
    @DisplayName("the connection opens with the HTTP/2 preface, so h2c is recognisable on any port")
    void connectionOpensWithTheHttp2Preface() throws Exception {
        ServerSocket listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        int port = listener.getLocalPort();
        byte[] firstBytes = new byte[64];
        CompletableFuture<Integer> read = new CompletableFuture<>();

        Thread server = new Thread(() -> {
            try (listener; Socket socket = listener.accept();
                 InputStream in = socket.getInputStream()) {
                read.complete(in.read(firstBytes));
            } catch (Exception ex) {
                read.completeExceptionally(ex);
            }
        }, "wire-probe");
        server.setDaemon(true);
        server.start();

        try (NettyHttp2Client client = new NettyHttp2Client(Http2ClientSettings.builder()
                .host("127.0.0.1").port(port)
                .tls(TlsSettings.builder().enabled(false).verifyHostname(false).build())
                .pool(ConnectionPoolSettings.builder()
                        .connectTimeoutMillis(3000).readTimeoutMillis(3000).idleTimeoutSeconds(5).build())
                .build())) {
            client.connect();
            Map<String, String> headers = new HashMap<>();
            headers.put("content-type", "application/json");
            client.send(Http2Request.builder().method("POST").path("/probe").headers(headers)
                    .body("{}".getBytes(StandardCharsets.UTF_8)).build());

            int length = read.get(10, java.util.concurrent.TimeUnit.SECONDS);
            String asText = new String(firstBytes, 0, Math.max(0, length), StandardCharsets.ISO_8859_1);

            System.out.println("\n=== first bytes on the wire ===");
            System.out.println("  length : " + length);
            System.out.println("  ascii  : " + asText.replace("\r", "\\r").replace("\n", "\\n"));
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < Math.min(length, 24); i++) {
                hex.append(String.format("%02x ", firstBytes[i]));
            }
            System.out.println("  hex    : " + hex);
            System.out.println("  HTTP/2 preface present: " + asText.startsWith(HTTP2_PREFACE));
            System.out.println("=== end ===\n");

            assertThat(asText)
                    .as("the client must open with the HTTP/2 preface (prior knowledge), "
                            + "otherwise h2c is not recognisable on a non-standard port")
                    .startsWith(HTTP2_PREFACE);
        }
    }
}
