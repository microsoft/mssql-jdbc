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
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.InvalidParameterSpecException;
import java.util.Arrays;
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

    byte ECDH_MAGIC[] = {0x45, 0x43, 0x4b, 0x33, 0x30, 0x00, 0x00, 0x00};
    byte enclaveType[] = new byte[] {0x3, 0x0, 0x0, 0x0};
    byte enclaveChallenge[] = new byte[] {0x0, 0x0, 0x0, 0x0};
    int publicKeyLength;
    byte[] x;
    byte[] y;

    public VSMAtttestationRequest() {
        KeyPairGenerator kpg;
        try {
            kpg = KeyPairGenerator.getInstance("EC");
            kpg.initialize(384);
            KeyPair kp = kpg.generateKeyPair();
            publicKey = (ECPublicKey)kp.getPublic();
            privateKey = (ECPrivateKey)kp.getPrivate();
            ECPoint w = publicKey.getW();
            x = w.getAffineX().abs().toByteArray();
            y = w.getAffineY().abs().toByteArray();
            if (x[0] == 0) {
                x = Arrays.copyOfRange(x,1,x.length);
            }
            if (y[0] == 0) {
                y = Arrays.copyOfRange(y,1,y.length);
            }
            publicKeyLength = ECDH_MAGIC.length + x.length + y.length;
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
        os.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(publicKeyLength).array());
        os.writeBytes(ECDH_MAGIC);
        os.writeBytes(x);
        os.writeBytes(y);
        return os.toByteArray();
    }
}
