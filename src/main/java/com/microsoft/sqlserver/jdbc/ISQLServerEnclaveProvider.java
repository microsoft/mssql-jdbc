/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * Provides an interface to create an Enclave Session
 *
 */
public interface ISQLServerEnclaveProvider {
    byte[] getEnclavePackage(String userSQL, ArrayList<byte[]> enclaveCEKs) throws SQLServerException;

    /**
     * Returns the attestation parameters
     * @param createNewParameters
     *        indicates whether to create new parameters
     * @param url
     *        attestation url
     * @throws SQLServerException
     *         when an error occurs.
     */
    void getAttestationParameters(boolean createNewParameters, String url) throws SQLServerException;

    /**
     * Creates the enclave session
     * 
     * @param connection
     *        connection
     * @param userSql
     *        user sql
     * @param preparedTypeDefinitions
     *        preparedTypeDefinitions
     * @param params
     *        params
     * @param parameterNames
     *        parameterNames
     * @return
     *        list of enclave requested CEKs
     * @throws SQLServerException
     *         when an error occurs.
     */
    ArrayList<byte[]> createEnclaveSession(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException;

    /**
     * Invalidates an enclave session
     */
    void invalidateEnclaveSession();

    /**
     * Returns the enclave session
     * @return
     *         the enclave session
     */
    EnclaveSession getEnclaveSession();
}


abstract class BaseAttestationRequest {
    protected PrivateKey privateKey;

    byte[] getBytes() {
        return null;
    };
}


class EnclaveSession {
    private byte[] sessionID;
    private AtomicInteger counter;
    private byte[] sessionSecret;

    EnclaveSession(byte[] cs, byte[] b) {
        sessionID = cs;
        sessionSecret = b;
        counter = new AtomicInteger(0);
    }

    byte[] getSessionID() {
        return sessionID;
    }

    byte[] getSessionSecret() {
        return sessionSecret;
    }

    long getCounter() {
        return counter.getAndIncrement();
    }
}
