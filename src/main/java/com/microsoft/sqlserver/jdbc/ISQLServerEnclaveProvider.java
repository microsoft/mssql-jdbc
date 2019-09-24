package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;


public interface ISQLServerEnclaveProvider {
    BaseAttestationRequest getAttestationParamters();

    void createEnclaveSession();

    void invalidateEnclaveSession();

    void getEnclaveSession();
}


abstract class BaseAttestationRequest {
    protected ECPublicKey publicKey;
    protected ECPrivateKey privateKey;
    byte[] sharedsecret;

    byte[] getBytes() throws IOException {
        return null;
    };
}
