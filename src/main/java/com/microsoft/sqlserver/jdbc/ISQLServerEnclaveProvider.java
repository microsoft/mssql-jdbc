package com.microsoft.sqlserver.jdbc;

import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


public interface ISQLServerEnclaveProvider {
    byte[] getEnclavePackage(String userSQL, ArrayList<byte[]> enclaveCEKs) throws SQLServerException;

    void getAttestationParamters(boolean b, String s) throws SQLServerException;

    ArrayList<byte[]> createEnclaveSession(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException;

    void invalidateEnclaveSession();

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
