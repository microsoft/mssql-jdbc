/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;

import reactor.core.publisher.Mono;

public class SQLServerSecurityUtilityTest {
    private static final TokenRequestContext TOKEN_REQUEST_CONTEXT = new TokenRequestContext()
            .setScopes(Collections.singletonList("https://database.windows.net/.default"));

    @Test
    public void tokenIsConverted() throws SQLServerException {
        OffsetDateTime expiresAt = OffsetDateTime.now().plusHours(1);
        TokenCredential credential = context -> Mono.just(new AccessToken("token", expiresAt));

        SqlAuthenticationToken token = SQLServerSecurityUtility.getCredentialAuthToken(credential,
                TOKEN_REQUEST_CONTEXT, 1_000);

        assertEquals("token", token.getAccessToken());
        assertEquals(expiresAt.toInstant().toEpochMilli(), token.getExpiresOn().getTime());
    }

    @Test
    public void credentialFailureIsPreserved() {
        IllegalStateException credentialFailure = new IllegalStateException("credential failure");
        TokenCredential credential = context -> Mono.error(credentialFailure);

        try {
            SQLServerSecurityUtility.getCredentialAuthToken(credential, TOKEN_REQUEST_CONTEXT, 1_000);
            fail("Expected token acquisition to fail.");
        } catch (SQLServerException e) {
            assertEquals(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionError"), e.getMessage());
            assertSame(credentialFailure, e.getCause());
        }
    }

    @Test
    public void tokenAcquisitionTimesOut() {
        TokenCredential credential = context -> Mono.never();

        try {
            SQLServerSecurityUtility.getCredentialAuthToken(credential, TOKEN_REQUEST_CONTEXT, 1);
            fail("Expected token acquisition to time out.");
        } catch (SQLServerException e) {
            assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionTimeout"), e.getMessage());
            assertTrue(hasCause(e, java.util.concurrent.TimeoutException.class));
        }
    }

    @Test
    public void emptyTokenResponseUsesExistingError() {
        TokenCredential credential = context -> Mono.empty();

        try {
            SQLServerSecurityUtility.getCredentialAuthToken(credential, TOKEN_REQUEST_CONTEXT, 1_000);
            fail("Expected empty token acquisition to fail.");
        } catch (SQLServerException e) {
            assertEquals(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionFail"), e.getMessage());
        }
    }

    @Test
    public void tokenAcquisitionRestoresInterruptFlag() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        TokenCredential credential = context -> Mono.<AccessToken>never()
                .doOnSubscribe(ignored -> subscribed.countDown());
        AtomicReference<SQLServerException> exception = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();

        Thread tokenThread = new Thread(() -> {
            try {
                SQLServerSecurityUtility.getCredentialAuthToken(credential, TOKEN_REQUEST_CONTEXT, 10_000);
            } catch (SQLServerException e) {
                exception.set(e);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            }
        });

        tokenThread.start();
        assertTrue(subscribed.await(5, TimeUnit.SECONDS));
        tokenThread.interrupt();
        tokenThread.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(tokenThread.isAlive());
        assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"),
                exception.get().getMessage());
        assertTrue(hasCause(exception.get(), InterruptedException.class));
        assertTrue(interruptRestored.get());
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> expectedType) {
        while (null != throwable) {
            if (expectedType.isInstance(throwable)) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }
}