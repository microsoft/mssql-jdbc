/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.socketfactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
public class CustomSocketFactoryTest extends AbstractTest {
    private static List<String> dummyLog = new ArrayList<>();

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void beforeEach() {
        dummyLog.clear();
    }

    public static class DummySocketFactory extends SocketFactory {
        private final String arg;

        public DummySocketFactory(String arg) {
            this.arg = arg;
        }

        public DummySocketFactory() {
            this.arg = null;
        }

        private void logUsage() {
            dummyLog.add(arg);
        }

        @Override
        public Socket createSocket() throws IOException {
            logUsage();
            return SocketFactory.getDefault().createSocket();
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            logUsage();
            return SocketFactory.getDefault().createSocket(host, port);
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost,
                int localPort) throws IOException, UnknownHostException {
            logUsage();
            return new Socket(host, port, localHost, localPort);
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            logUsage();
            return new Socket(host, port);
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress,
                int localPort) throws IOException {
            logUsage();
            return new Socket(address, port, localAddress, localPort);
        }
    }

    public static class BlackholeSocketFactory extends SocketFactory {
        private static volatile boolean blackholeReads;

        static void setBlackholeReads(boolean enabled) {
            blackholeReads = enabled;
        }

        @Override
        public Socket createSocket() throws IOException {
            return new BlackholeSocket();
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            return new BlackholeSocket(host, port);
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost,
                int localPort) throws IOException, UnknownHostException {
            return new BlackholeSocket(host, port, localHost, localPort);
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            return new BlackholeSocket(host, port);
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress,
                int localPort) throws IOException {
            return new BlackholeSocket(address, port, localAddress, localPort);
        }
    }

    private static class BlackholeSocket extends Socket {
        BlackholeSocket() {
        }

        BlackholeSocket(String host, int port) throws IOException {
            super(host, port);
        }

        BlackholeSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            super(host, port, localHost, localPort);
        }

        BlackholeSocket(InetAddress host, int port) throws IOException {
            super(host, port);
        }

        BlackholeSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            super(address, port, localAddress, localPort);
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return new BlackholeInputStream(super.getInputStream(), this);
        }
    }

    private static class BlackholeInputStream extends InputStream {
        private final InputStream inputStream;
        private final Socket socket;

        BlackholeInputStream(InputStream inputStream, Socket socket) {
            this.inputStream = inputStream;
            this.socket = socket;
        }

        @Override
        public int read() throws IOException {
            awaitNetworkTimeout();
            return inputStream.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            awaitNetworkTimeout();
            return inputStream.read(bytes, offset, length);
        }

        private void awaitNetworkTimeout() throws IOException {
            while (BlackholeSocketFactory.blackholeReads) {
                int networkTimeout = socket.getSoTimeout();
                try {
                    if (0 == networkTimeout) {
                        Thread.sleep(100);
                    } else {
                        Thread.sleep(networkTimeout);
                        throw new SocketTimeoutException("Simulated silently dropped TCP connection");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
        }
    }

    /**
     * Connect with a custom socket factory.
     */
    @Test
    public void testCustomSocketFactory() throws Exception {
        String url = connectionString + ";socketFactoryClass=" + DummySocketFactory.class.getName();
        try (Connection con = PrepUtil.getConnection(url)) {
            Assert.assertTrue(con != null);
        }
    }

    /**
     * Connect with a custom socket factory.
     */
    @Test
    public void testCustomSocketFactoryWithArg() throws Exception {
        String constructorArg = "TEST-CUSTOM-ARG";
        String url = connectionString + ";socketFactoryClass=" + DummySocketFactory.class.getName()
                + ";socketFactoryConstructorArg=" + constructorArg;
        try (Connection con = PrepUtil.getConnection(url)) {
            Assert.assertTrue(con != null);
        }
        Assert.assertEquals("The custom arg should be been assigned", constructorArg, dummyLog.get(0));
    }

    @Test
    public void testIsValidTimesOutWhenSocketReadsAreSilentlyDropped() throws Exception {
        String url = connectionString + ";socketTimeout=0;socketFactoryClass=" + BlackholeSocketFactory.class.getName();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        BlackholeSocketFactory.setBlackholeReads(false);

        try (Connection con = PrepUtil.getConnection(url)) {
            try {
                BlackholeSocketFactory.setBlackholeReads(true);
                Future<Boolean> result = executor.submit(() -> con.isValid(1));

                Assert.assertFalse("isValid should honor its timeout", result.get(5, TimeUnit.SECONDS));
            } finally {
                BlackholeSocketFactory.setBlackholeReads(false);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testIsValidRestoresUnlimitedNetworkTimeout() throws Exception {
        String url = connectionString + ";socketTimeout=0";
        try (Connection con = PrepUtil.getConnection(url)) {
            Assert.assertTrue(con.isValid(1));
            Assert.assertEquals(0, con.getNetworkTimeout());
        }
    }

    /**
     * This class does not implement SocketFactory and the connection must fail when it is specified by the
     * socketFactoryClass property.
     */
    public static class InvalidSocketFactory {}

    /**
     * Test with a custom socket factory class that does not implement SocketFactory.
     */
    @Test
    public void testInvalidSocketFactory() throws Exception {
        String url = connectionString + ";socketFactoryClass=" + InvalidSocketFactory.class.getName();
        try (Connection con = PrepUtil.getConnection(url)) {
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                    "The class specified by the socketFactoryClass property must be assignable to javax.net.SocketFactory"));
        }
    }
}
