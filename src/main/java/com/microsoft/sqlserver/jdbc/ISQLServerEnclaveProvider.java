package com.microsoft.sqlserver.jdbc;

import java.security.KeyPair;

public interface ISQLServerEnclaveProvider {
    BaseAttestationRequest getAttestationParamters();

    void createEnclaveSession();

    void invalidateEnclaveSession();

    void getEnclaveSession();
}

abstract class BaseAttestationRequest {
    KeyPair key;
    
    byte[] attestationParameters() {
        return key.getPublic().getEncoded();
    }
}