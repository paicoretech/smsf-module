package com.paicbd.smsc.smsf.common.e2e;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hands out the loopback ports the end-to-end tests listen on.
 *
 * <p>They come from a dedicated range instead of the ephemeral one for two reasons. It keeps the
 * tests away from every port the SMSC platform uses, so the suite can run on a host that already
 * has the whole stack up — PostgreSQL 5432, Redis 6379, ScyllaDB 9042, Kafka 9092-9094 and the
 * SMSF's own 9070 are all outside it. And it makes a packet capture predictable: everything the
 * tests do is inside {@value #FIRST_PORT}-{@value #LAST_PORT}, which is a single Wireshark filter.</p>
 *
 * <p>The range also sits below the kernel's ephemeral range (32768-60999 on Linux), so an outgoing
 * connection from any other process cannot land on one of these ports while a test holds it.</p>
 *
 * <p>Override with {@code -Dsmsf.e2e.port.first} when the default range is taken.</p>
 */
public final class SmsfE2EPorts {

    public static final int FIRST_PORT = Integer.getInteger("smsf.e2e.port.first", 18000);
    public static final int LAST_PORT = FIRST_PORT + 999;

    /**
     * The range is split in two so a protocol analyser can be told "these ports are HTTP/2" without
     * that also covering Kafka and Redis. Pointing the HTTP/2 dissector at a Kafka port makes every
     * Produce frame come out as a malformed HTTP/2 packet.
     */
    public static final int FIRST_INFRA_PORT = FIRST_PORT;
    public static final int LAST_INFRA_PORT = FIRST_PORT + 99;
    public static final int FIRST_HTTP2_PORT = FIRST_PORT + 100;
    public static final int LAST_HTTP2_PORT = LAST_PORT;

    /** Ports already handed out in this JVM, so two servers never get the same one. */
    private static final Set<Integer> RESERVED = ConcurrentHashMap.newKeySet();

    private SmsfE2EPorts() {
    }

    /** A port for an HTTP/2 endpoint: the SBI listener and the NRF, UDM and AMF simulators. */
    public static int allocate() {
        return allocateBetween(FIRST_HTTP2_PORT, LAST_HTTP2_PORT);
    }

    /** A port for infrastructure that is not HTTP/2: the Kafka and Redis containers. */
    public static int allocateInfrastructure() {
        return allocateBetween(FIRST_INFRA_PORT, LAST_INFRA_PORT);
    }

    /**
     * @return a free port in the given band, reserved for this JVM
     * @throws IllegalStateException when the band is taken, which means something else on the host
     *                               is using it and the range should be moved
     */
    private static synchronized int allocateBetween(int first, int last) {
        for (int port = first; port <= last; port++) {
            if (RESERVED.contains(port)) {
                continue;
            }
            if (isFree(port)) {
                RESERVED.add(port);
                return port;
            }
        }
        throw new IllegalStateException("No free port between " + first + " and " + last
                + ". Move the range with -Dsmsf.e2e.port.first");
    }

    /** Gives the port back, so a long test class does not exhaust the range. */
    public static void release(int port) {
        RESERVED.remove(port);
    }

    /** The Wireshark display filter covering everything the end-to-end tests listen on. */
    public static String captureFilter() {
        return "tcp.port >= " + FIRST_PORT + " && tcp.port <= " + LAST_PORT;
    }

    /**
     * The value for Wireshark's HTTP2 "TCP port(s)" preference. It deliberately excludes the
     * infrastructure band: forcing the HTTP/2 dissector onto Kafka or Redis produces a stream of
     * malformed packets.
     */
    public static String http2PortPreference() {
        return FIRST_HTTP2_PORT + "-" + LAST_HTTP2_PORT;
    }

    private static boolean isFree(int port) {
        try (ServerSocket socket = new ServerSocket(port, 1, InetAddress.getByName("127.0.0.1"))) {
            socket.setReuseAddress(true);
            return true;
        } catch (IOException ex) {
            return false;
        }
    }
}
