/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.socketfactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.net.SocketFactory;

import com.microsoft.sqlserver.jdbc.TestUtils;
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
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
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
