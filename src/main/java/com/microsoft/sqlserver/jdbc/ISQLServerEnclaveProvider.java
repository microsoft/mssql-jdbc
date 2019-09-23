package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.security.KeyPair;
import java.security.PublicKey;

import javax.crypto.KeyAgreement;


public interface ISQLServerEnclaveProvider {
    BaseAttestationRequest getAttestationParamters();

    void createEnclaveSession();

    void invalidateEnclaveSession();

    void getEnclaveSession();
}


abstract class BaseAttestationRequest {
    protected PublicKey publickey;
    KeyAgreement keyAgreement;
    byte[] sharedsecret;

    byte[] getBytes() throws IOException {
        return null;
    };
}
