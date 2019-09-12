package com.microsoft.sqlserver.jdbc;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;


public class SQLServerVSMEnclaveProvider implements ISQLServerEnclaveProvider {

    @Override
    public VSMAtttestationRequest getAttestationParamters() {
        return new VSMAtttestationRequest();
    }

    @Override
    public void createEnclaveSession() {

    }

    @Override
    public void invalidateEnclaveSession() {

    }

    @Override
    public void getEnclaveSession() {

    }

}


class VSMAtttestationRequest extends BaseAttestationRequest {
    public VSMAtttestationRequest() {
        KeyPairGenerator kpg;
        try {
            kpg = KeyPairGenerator.getInstance("EC");
            kpg.initialize(384);
            key = kpg.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
