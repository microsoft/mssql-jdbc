/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.ActiveDirectoryAuthentication;


class SQLServerTokenAcquisitionTest {
    public static class FailingCallback implements SQLServerAccessTokenCallback {
        static RuntimeException failure;

        @Override
        public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
            throw failure;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void callbackTimeoutPreservesCause(boolean useClass) throws Exception {
        TimeoutException timeout = new TimeoutException("callback timed out");
        assertCallbackFailure(useClass, new RuntimeException(timeout), "R_AADTokenAcquisitionTimeout", timeout);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void callbackWorkerInterruptionDoesNotInterruptCaller(boolean useClass) throws Exception {
        InterruptedException interrupted = new InterruptedException("callback worker interrupted");
        try {
            assertCallbackFailure(useClass, new RuntimeException(interrupted), "R_AADTokenAcquisitionInterrupted",
                    interrupted);
            assertFalse(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void callbackFailureIsNotAClassConstructionFailure(boolean useClass) throws Exception {
        RuntimeException failure = new IllegalStateException("{\"error\":\"callback failed\"}");
        assertCallbackFailure(useClass, failure, "R_AADTokenAcquisitionError", failure);
    }

    private void assertCallbackFailure(boolean useClass, RuntimeException failure, String errorKey,
            Throwable cause) throws Exception {
        try (SQLServerConnection connection = connection(SqlAuthentication.NOT_SPECIFIED)) {
            if (useClass) {
                FailingCallback.failure = failure;
                connection.setAccessTokenCallbackClass(FailingCallback.class.getName());
            } else {
                setField(connection, "accessTokenCallback", (SQLServerAccessTokenCallback) (spn, stsurl) -> {
                    throw failure;
                });
            }
            SQLServerException exception = assertThrows(SQLServerException.class,
                    () -> connection.onFedAuthInfo(fedAuthInfo(connection), null));
            assertEquals(SQLServerException.getErrString(errorKey), exception.getMessage());
            assertSame(cause, exception.getCause());
        } finally {
            FailingCallback.failure = null;
        }
    }

    @Test
    void invalidCallbackClassKeepsConstructionError() throws Exception {
        try (SQLServerConnection connection = connection(SqlAuthentication.NOT_SPECIFIED)) {
            connection.setAccessTokenCallbackClass("missing.Callback");
            SQLServerException exception = assertThrows(SQLServerException.class,
                    () -> connection.onFedAuthInfo(fedAuthInfo(connection), null));
            assertEquals(MessageFormat.format(SQLServerException.getErrString("R_InvalidAccessTokenCallbackClass"),
                    "missing.Callback"), exception.getMessage());
        }
    }

    @Test
    void callbackSuccessPreservesTokenAndArguments() throws Exception {
        try (SQLServerConnection connection = connection(SqlAuthentication.NOT_SPECIFIED)) {
            SQLServerConnection.SqlFedAuthInfo info = fedAuthInfo(connection);
            SqlAuthenticationToken token = new SqlAuthenticationToken("dummy-token", Long.MAX_VALUE);
            SQLServerAccessTokenCallback callback = (spn, stsurl) -> {
                assertEquals(info.spn, spn);
                assertEquals(info.stsurl, stsurl);
                return token;
            };
            Method method = SQLServerConnection.class.getDeclaredMethod("getAccessTokenFromCallback",
                    SQLServerAccessTokenCallback.class, SQLServerConnection.SqlFedAuthInfo.class);
            method.setAccessible(true);
            assertSame(token, method.invoke(connection, callback, info));
        }
    }

    @Test
    void nativeDllLoadFailurePreservesCause() throws Exception {
        DLLException failure = new DLLException("DLL unavailable", -1, 0, 1);
        SQLServerException exception = nativeFailure(failure, false, false);
        assertSame(failure, exception.getCause());
    }

    @Test
    void nativeTransientFailureAtDeadlineReportsTimeout() throws Exception {
        DLLException failure = new DLLException("transient failure",
                ActiveDirectoryAuthentication.GET_ACCESS_TOKEN_TRANSIENT_ERROR, 1, 1);
        SQLServerException exception = nativeFailure(failure, true, false);
        assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionTimeout"), exception.getMessage());
        assertSame(failure, exception.getCause());
    }

    @Test
    void nativeNonTransientFailureKeepsAuthenticationError() throws Exception {
        DLLException failure = new DLLException("invalid grant",
                ActiveDirectoryAuthentication.GET_ACCESS_TOKEN_INVALID_GRANT, 1, 1);
        SQLServerException exception = nativeFailure(failure, true, false);
        assertEquals(MessageFormat.format(SQLServerException.getErrString("R_MSALExecution"), "",
                SqlAuthentication.ACTIVE_DIRECTORY_INTEGRATED.toString()), exception.getMessage());
        assertSame(failure, exception.getCause().getCause());
    }

    @Test
    void nativeRetryInterruptionStopsAcquisition() throws Exception {
        DLLException failure = new DLLException("transient failure",
                ActiveDirectoryAuthentication.GET_ACCESS_TOKEN_TRANSIENT_ERROR, 1, 1);
        try {
            SQLServerException exception = nativeFailure(failure, false, true);
            assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"), exception.getMessage());
            assertTrue(exception.getCause() instanceof InterruptedException);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private SQLServerException nativeFailure(DLLException failure, boolean expired,
            boolean interrupt) throws Exception {
        boolean wasWindows = SQLServerConnection.isWindows;
        try (SQLServerConnection connection = connection(SqlAuthentication.ACTIVE_DIRECTORY_INTEGRATED);
                MockedStatic<AuthenticationJNI> nativeAuth = mockStatic(AuthenticationJNI.class)) {
            SQLServerConnection.isWindows = true;
            connection.timerExpire = expired ? 0 : System.currentTimeMillis() + 30000;
            nativeAuth.when(AuthenticationJNI::isDllLoaded).thenReturn(true);
            nativeAuth.when(() -> AuthenticationJNI.getAccessTokenForWindowsIntegrated(anyString(), anyString(),
                    anyString(), anyString(), anyLong())).thenAnswer(invocation -> {
                        if (interrupt) {
                            Thread.currentThread().interrupt();
                        }
                        throw failure;
                    }).thenReturn(new FedAuthDllInfo("unused".getBytes(StandardCharsets.UTF_16LE), 3600));

            Method method = SQLServerConnection.class.getDeclaredMethod("getFedAuthToken",
                    SQLServerConnection.SqlFedAuthInfo.class);
            method.setAccessible(true);
            InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                    () -> method.invoke(connection, fedAuthInfo(connection)));
            assertTrue(exception.getCause() instanceof SQLServerException);
            return (SQLServerException) exception.getCause();
        } finally {
            SQLServerConnection.isWindows = wasWindows;
        }
    }

    private static SQLServerConnection connection(SqlAuthentication authentication) throws Exception {
        SQLServerConnection connection = new SQLServerConnection("token acquisition test");
        connection.activeConnectionProperties = new Properties();
        connection.activeConnectionProperties.setProperty("user", "");
        connection.activeConnectionProperties.setProperty("password", "");
        setField(connection, "authenticationString", authentication.toString());
        setField(connection, "clientConnectionId", UUID.randomUUID());
        return connection;
    }

    private static SQLServerConnection.SqlFedAuthInfo fedAuthInfo(SQLServerConnection connection) {
        SQLServerConnection.SqlFedAuthInfo info = connection.new SqlFedAuthInfo();
        info.spn = "https://database.windows.net/";
        info.stsurl = "https://login.microsoftonline.com/test";
        return info;
    }

    private static void setField(SQLServerConnection connection, String name, Object value) throws Exception {
        Field field = SQLServerConnection.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(connection, value);
    }
}
