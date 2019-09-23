package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.InvalidParameterSpecException;
import java.util.concurrent.ThreadLocalRandom;

import javax.crypto.KeyAgreement;
import javax.crypto.spec.DHParameterSpec;


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
            KeyPair kp = kpg.generateKeyPair();
            publickey = kp.getPublic();
            keyAgreement = KeyAgreement.getInstance("ECDH");
            keyAgreement.init(kp.getPrivate());
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private BigInteger randomBigInt() {
        return new BigInteger(512, ThreadLocalRandom.current());
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Override
    byte[] getBytes() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.writeBytes(enclaveType);
        os.writeBytes(enclaveChallenge);
        int keyLength = publickey.getEncoded().length;
        os.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(keyLength).array());
        os.writeBytes(publickey.getEncoded());
        System.out.println(bytesToHex(os.toByteArray()));
        return os.toByteArray();
    }
}
