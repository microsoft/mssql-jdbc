/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

class TDSChannelChannelBindingTest {

    @Test
    void createChannelBindingInfoPrefixesTlsUniqueData() throws ClassNotFoundException {
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
        SSLSession session = createExtendedSessionMock(null);
        assertNull(TDSChannel.createChannelBindingInfo(session));
    }

    @Test
    void createChannelBindingInfoReturnsNullWhenJdkDoesNotSupportTlsUniqueChannelBinding() {
        if (isTlsUniqueChannelBindingMethodAvailable()) {
            return;
        }

        assertNull(TDSChannel.createChannelBindingInfo(Mockito.mock(SSLSession.class)));
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
}