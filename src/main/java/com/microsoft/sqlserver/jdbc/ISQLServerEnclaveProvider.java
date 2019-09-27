package com.microsoft.sqlserver.jdbc;

import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;


public interface ISQLServerEnclaveProvider {
    BaseAttestationRequest getAttestationParamters(boolean b, String s) throws NoSuchAlgorithmException;

    void createEnclaveSession(byte[] b) throws SQLServerException;

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
    private long sessionID;
    private byte[] sessionSecret;

    // Don't allow default instantiation
    @SuppressWarnings("unused")
    private EnclaveSession() {};

    public EnclaveSession(long l, byte[] b) {
        sessionID = l;
        sessionSecret = b;
    }
    
    long getSessionID() {
        return sessionID;
    }
    
    byte[] getSessionSecret() {
        return sessionSecret;
    }
}
