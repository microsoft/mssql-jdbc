package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.security.KeyPair;


public interface ISQLServerEnclaveProvider {
    BaseAttestationRequest getAttestationParamters();

    void createEnclaveSession();

    void invalidateEnclaveSession();

    void getEnclaveSession();
}


abstract class BaseAttestationRequest {
    KeyPair key;

    byte[] getBytes() throws IOException {
        return null;
    };
}
