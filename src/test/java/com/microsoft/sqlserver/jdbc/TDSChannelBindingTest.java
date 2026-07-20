/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

class TDSChannelBindingTest {

    @Test
    void createChannelBindingInfoReturnsNullWhenSessionIsNull() {
        assertNull(TDSChannel.createChannelBindingInfo(null));
    }

    @Test
    void createChannelBindingInfoReturnsNullWhenSessionIsNotExtended() {
        assertNull(TDSChannel.createChannelBindingInfo(Mockito.mock(SSLSession.class)));
    }

    @Test
    void createChannelBindingInfoPrefixesTlsUniqueChannelBinding() throws ClassNotFoundException {
        assumeTrue(isTlsUniqueChannelBindingMethodAvailable(),
                "Skipping because getTlsUniqueChannelBinding is not available on this JDK");

        byte[] tlsUnique = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

        SSLSession session = createExtendedSessionMock(tlsUnique);
        byte[] channelBinding = TDSChannel.createChannelBindingInfo(session);

        assertArrayEquals(new byte[] {'t', 'l', 's', '-', 'u', 'n', 'i', 'q', 'u', 'e', ':', 1, 2, 3, 4, 5, 6, 7, 8,
                9, 10, 11, 12}, channelBinding);
    }

    @Test
    void createChannelBindingInfoReturnsNullWhenTlsUniqueAbsent() throws ClassNotFoundException {
        assumeTrue(isTlsUniqueChannelBindingMethodAvailable(),
                "Skipping because getTlsUniqueChannelBinding is not available on this JDK");

        SSLSession session = createExtendedSessionMock(null);
        assertNull(TDSChannel.createChannelBindingInfo(session));
    }

    @Test
    void createChannelBindingInfoReturnsNullWhenTlsUniqueIsEmpty() throws ClassNotFoundException {
        assumeTrue(isTlsUniqueChannelBindingMethodAvailable(),
                "Skipping because getTlsUniqueChannelBinding is not available on this JDK");

        SSLSession session = createExtendedSessionMock(new byte[0]);
        assertNull(TDSChannel.createChannelBindingInfo(session));
    }

    @Test
    void createChannelBindingInfoReturnsNullWhenJdkDoesNotSupportTlsUniqueChannelBinding() {
        if (isTlsUniqueChannelBindingMethodAvailable()) {
            return;
        }

        assertNull(TDSChannel.createChannelBindingInfo(Mockito.mock(SSLSession.class)));
    }

    @Test
    void ntlmAuthenticationUsesPerContextChannelBindingInfo() throws Exception {
        byte[] firstChannelBinding = new byte[] {'t', 'l', 's', '-', 'u', 'n', 'i', 'q', 'u', 'e', ':', 1, 2, 3};
        byte[] secondChannelBinding = new byte[] {'t', 'l', 's', '-', 'u', 'n', 'i', 'q', 'u', 'e', ':', 4, 5, 6};

        NTLMAuthentication first = new NTLMAuthentication(null, "DOMAIN", "user", new byte[16], "host",
                firstChannelBinding);
        NTLMAuthentication second = new NTLMAuthentication(null, "DOMAIN", "user", new byte[16], "host",
                secondChannelBinding);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<byte[]> firstHash = executor.submit(() -> calculateNtlmChannelBindingHash(first, ready, start));
            Future<byte[]> secondHash = executor.submit(() -> calculateNtlmChannelBindingHash(second, ready, start));

            ready.await();
            start.countDown();

            byte[] firstResult = firstHash.get();
            byte[] secondResult = secondHash.get();
            assertArrayEquals(expectedNtlmChannelBindingHash(firstChannelBinding), firstResult);
            assertArrayEquals(expectedNtlmChannelBindingHash(secondChannelBinding), secondResult);
            assertFalse(Arrays.equals(firstResult, secondResult));
        } finally {
            first.releaseClientContext();
            second.releaseClientContext();
            executor.shutdownNow();
        }
    }

    private static SSLSession createExtendedSessionMock(byte[] tlsUnique) throws ClassNotFoundException {
        Class<?> extendedSessionClass = Class.forName("javax.net.ssl.ExtendedSSLSession");
        Object mock = Mockito.mock(extendedSessionClass, invocation -> {
            if ("getTlsUniqueChannelBinding".equals(invocation.getMethod().getName())) {
                return tlsUnique;
            }
            return Answers.RETURNS_DEFAULTS.answer(invocation);
        });
        return (SSLSession) mock;
    }

    private static boolean isTlsUniqueChannelBindingMethodAvailable() {
        try {
            Class<?> extendedSessionClass = Class.forName("javax.net.ssl.ExtendedSSLSession");
            extendedSessionClass.getMethod("getTlsUniqueChannelBinding");
            return true;
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            return false;
        }
    }

    private static byte[] calculateNtlmChannelBindingHash(NTLMAuthentication authentication, CountDownLatch ready,
            CountDownLatch start) throws Exception {
        Method calculateHash = NTLMAuthentication.class.getDeclaredMethod("calculateChannelBindingMD5Hash");
        calculateHash.setAccessible(true);
        Field msvAvChannelBindings = NTLMAuthentication.class.getDeclaredField("msvAvChannelBindings");
        msvAvChannelBindings.setAccessible(true);

        ready.countDown();
        start.await();
        try {
            calculateHash.invoke(authentication);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
        return (byte[]) msvAvChannelBindings.get(authentication);
    }

    private static byte[] expectedNtlmChannelBindingHash(byte[] channelBindingInfo) throws Exception {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(new byte[4]);
        md5.update(new byte[4]);
        md5.update(new byte[4]);
        md5.update(new byte[4]);
        md5.update(new byte[] {(byte) channelBindingInfo.length, (byte) 0, (byte) 0, (byte) 0});
        md5.update(channelBindingInfo);
        return md5.digest();
    }
}