package com.microsoft.sqlserver.jdbc;

import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.util.ArrayList;


public interface ISQLServerEnclaveProvider {
    byte[] getEnclavePackage(String userSQL, ArrayList<byte[]> enclaveCEKs);

    void getAttestationParamters(boolean b, String s) throws NoSuchAlgorithmException;

    void createEnclaveSession(SQLServerConnection connection, String userSql, String preparedTypeDefinitions,
            Parameter[] params, ArrayList<String> parameterNames) throws SQLServerException;

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
    private long counter;
    private byte[] sessionSecret;

    // Don't allow default instantiation
    @SuppressWarnings("unused")
    private EnclaveSession() {};

    public EnclaveSession(byte[] cs, byte[] b) {
        sessionID = cs;
        sessionSecret = b;
        counter = 0;
    }

    byte[] getSessionID() {
        return sessionID;
    }

    byte[] getSessionSecret() {
        return sessionSecret;
    }

    long getCounter() {
        counter++;
        return counter-1;
    }
}
