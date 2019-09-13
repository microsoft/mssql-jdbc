package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

    byte enclaveType[] = new byte[] {0x3, 0x0, 0x0, 0x0};
    byte enclaveChallenge[] = new byte[] {0x0, 0x0, 0x0, 0x0};

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

    @Override
    byte[] getBytes() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.write(enclaveType);
        os.write(enclaveChallenge);
        os.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(key.getPublic().getEncoded().length).array());
        os.write(key.getPublic().getEncoded());
        return os.toByteArray();
    }
}
