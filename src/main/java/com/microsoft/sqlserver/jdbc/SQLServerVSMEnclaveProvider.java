package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Collection;

import javax.crypto.KeyAgreement;


public class SQLServerVSMEnclaveProvider implements ISQLServerEnclaveProvider {

    private VSMAttestationParameters vsmParams = null;
    private String attestationURL = null;
    EnclaveSession enclaveSession = null;

    @Override
    public VSMAttestationParameters getAttestationParamters(boolean createNewParameters,
            String url) throws NoSuchAlgorithmException {
        if (null == vsmParams || createNewParameters) {
            attestationURL = url;
            vsmParams = new VSMAttestationParameters();
        }
        return vsmParams;
    }

    @Override
    public void createEnclaveSession(byte[] b) throws SQLServerException {
        AttestationResponse ar = new AttestationResponse(b);
        try {
            byte[] attestationCerts = getAttestationCertificates();
            ar.validateCert(attestationCerts);
            enclaveSession = new EnclaveSession(ar.getSessionID(), vsmParams.createSessionSecret(ar.DHpublicKey));
        } catch (IOException | InvalidKeyException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
    }

    @Override
    public void invalidateEnclaveSession() {
        enclaveSession = null;
        vsmParams = null;
        attestationURL = null;
    }

    @Override
    public EnclaveSession getEnclaveSession() {
        return enclaveSession;
    }

    private byte[] getAttestationCertificates() throws IOException {
        java.net.URL url = new java.net.URL(attestationURL + "/attestationservice.svc/v2.0/signingCertificates/");
        java.net.URLConnection con = url.openConnection();
        String s = new String(con.getInputStream().readAllBytes());
        // omit the square brackets that come with the JSON
        String[] bytesString = s.substring(1, s.length() - 1).split(",");
        byte[] certData = new byte[bytesString.length];
        for (int i = 0; i < certData.length; i++) {
            certData[i] = (byte) (Integer.parseInt(bytesString[i]));
        }
        return certData;
    }
}


class VSMAttestationParameters extends BaseAttestationRequest {

    // Static byte[] for VSM ECDH
    static byte ECDH_MAGIC[] = {0x45, 0x43, 0x4b, 0x33, 0x30, 0x00, 0x00, 0x00};
    // Type 3 is VSM, sent as Little Endian 0x30000000
    static byte ENCLAVE_TYPE[] = new byte[] {0x3, 0x0, 0x0, 0x0};
    // VSM doesn't have a challenge
    static byte ENCLAVE_CHALLENGE[] = new byte[] {0x0, 0x0, 0x0, 0x0};
    static int ENCLAVE_LENGTH = 104;
    byte[] x;
    byte[] y;

    public VSMAttestationParameters() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg;
        kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(384);
        KeyPair kp = kpg.generateKeyPair();
        ECPublicKey publicKey = (ECPublicKey) kp.getPublic();
        privateKey = kp.getPrivate();
        ECPoint w = publicKey.getW();
        x = w.getAffineX().toByteArray();
        y = w.getAffineY().toByteArray();
        /*
         * For some reason toByteArray doesn't have an Signum option like the constructor. Manually remove leading 00
         * byte if it exists.
         */
        if (x[0] == 0 && x.length != 48) {
            x = Arrays.copyOfRange(x, 1, x.length);
        }
        if (y[0] == 0 && y.length != 48) {
            y = Arrays.copyOfRange(y, 1, y.length);
        }
    }

    @Override
    byte[] getBytes() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.writeBytes(ENCLAVE_TYPE);
        os.writeBytes(ENCLAVE_CHALLENGE);
        os.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(ENCLAVE_LENGTH).array());
        os.writeBytes(ECDH_MAGIC);
        os.writeBytes(x);
        os.writeBytes(y);
        return os.toByteArray();
    }

    byte[] createSessionSecret(
            byte[] serverResponse) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SQLServerException {
        if (serverResponse.length != 104) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_MalformedECDHPublicKey"), "0", false);
        }
        ByteBuffer sr = ByteBuffer.wrap(serverResponse);
        byte[] magic = new byte[8];
        sr.get(magic);
        if (magic != ECDH_MAGIC) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_MalformedECDHHeader"),
                    "0", false);
        }
        byte[] x = new byte[48];
        byte[] y = new byte[48];
        sr.get(x);
        sr.get(y);
        /*
         * Server returns X and Y coordinates, create a key using the point of the server and our key parameters.
         * Public/Private key parameters are the same.
         */
        ECPublicKeySpec keySpec = new ECPublicKeySpec(new ECPoint(new BigInteger(1, x), new BigInteger(1, y)),
                ((ECPrivateKey) privateKey).getParams());
        KeyAgreement ka = KeyAgreement.getInstance("ECDH");
        ka.init(privateKey);
        // Generate a PublicKey from the above key specifications and do an agreement with our PrivateKey
        ka.doPhase(KeyFactory.getInstance("EC").generatePublic(keySpec), true);
        // Generate a Secret from the agreement and hash with SHA-256 to create Session Secret
        return MessageDigest.getInstance("SHA-256").digest(ka.generateSecret());
    }
}


class AttestationResponse {
    int totalSize;
    int identitySize;
    int healthReportSize;
    int enclaveReportSize;

    byte[] enclavePK;
    byte[] healthReportCertificate;
    byte[] enclaveReportPackage;

    int sessionInfoSize;
    long sessionID;
    int DHPKsize;
    int DHPKSsize;
    byte[] DHpublicKey;
    byte[] publicKeySig;

    X509Certificate healthCert;

    AttestationResponse(byte[] b) throws SQLServerException {
        ByteBuffer response = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
        this.totalSize = response.getInt();
        this.identitySize = response.getInt();
        this.healthReportSize = response.getInt();
        this.enclaveReportSize = response.getInt();

        enclavePK = new byte[identitySize];
        healthReportCertificate = new byte[healthReportSize];
        enclaveReportPackage = new byte[enclaveReportSize];

        response.get(enclavePK, 0, identitySize);
        response.get(healthReportCertificate, 0, healthReportSize);
        response.get(enclaveReportPackage, 0, enclaveReportSize);

        this.sessionInfoSize = response.getInt();
        this.sessionID = response.getLong();
        this.DHPKsize = response.getInt();
        this.DHPKSsize = response.getInt();

        DHpublicKey = new byte[DHPKsize];
        publicKeySig = new byte[DHPKSsize];

        response.get(DHpublicKey, 0, DHPKsize);
        response.get(publicKeySig, 0, DHPKSsize);

        if (0 != response.remaining()) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_EnclaveResponseLengthError"), "0", false);
        }

        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            healthCert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(healthReportCertificate));
        } catch (CertificateException ce) {
            SQLServerException.makeFromDriverError(null, this, ce.getLocalizedMessage(), "0", false);
        }
    }

    @SuppressWarnings("unchecked")
    boolean validateCert(byte[] b) throws SQLServerException {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<X509Certificate> certs = (Collection<X509Certificate>) cf
                    .generateCertificates(new ByteArrayInputStream(b));
            for (X509Certificate cert : certs) {
                try {
                    healthCert.verify(cert.getPublicKey());
                    return true;
                } catch (SignatureException e) {
                    // Doesn't match, but continue looping through the rest of the certificates
                }
            }
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException | CertificateException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
        return false;
    }

    long getSessionID() {
        return sessionID;
    }
}
