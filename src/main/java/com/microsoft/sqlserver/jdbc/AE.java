/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Represents a single encrypted value for a CEK. It contains the encrypted CEK,the store type, name,the key path and
 * encryption algorithm.
 */
class EncryptionKeyInfo {
    EncryptionKeyInfo(byte[] encryptedKeyVal, int dbId, int keyId, int keyVersion, byte[] mdVersion, String keyPathVal,
            String keyStoreNameVal, String algorithmNameVal) {
        encryptedKey = encryptedKeyVal;
        databaseId = dbId;
        cekId = keyId;
        cekVersion = keyVersion;
        cekMdVersion = mdVersion;
        keyPath = keyPathVal;
        keyStoreName = keyStoreNameVal;
        algorithmName = algorithmNameVal;
    }

    public EncryptionKeyInfo(byte[] encryptedKeyVal, int dbId, int keyId, int keyVersion, byte[] mdVersion,
            String keyPathVal, String keyStoreNameVal, String algorithmNameVal, byte requestedByEnclave,
            byte[] enclaveCmkSignature) {
        encryptedKey = encryptedKeyVal;
        databaseId = dbId;
        cekId = keyId;
        cekVersion = keyVersion;
        cekMdVersion = mdVersion;
        keyPath = keyPathVal;
        keyStoreName = keyStoreNameVal;
        algorithmName = algorithmNameVal;
        this.requestedByEnclave = (requestedByEnclave == 0);
        this.enclaveCmkSignature = enclaveCmkSignature;
    }

    byte[] encryptedKey; // the encrypted "column encryption key"
    int databaseId;
    int cekId;
    int cekVersion;
    byte[] cekMdVersion;
    String keyPath;
    String keyStoreName;
    String algorithmName;
    byte normalizationRuleVersion;
    boolean requestedByEnclave;
    byte[] enclaveCmkSignature;
}


/**
 * Represents a unique CEK as an entry in the CekTable. A unique (plaintext is unique) CEK can have multiple encrypted
 * CEKs when using multiple CMKs. These encrypted CEKs are represented by a member ArrayList.
 */
class CekTableEntry {
    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.AE");

    List<EncryptionKeyInfo> columnEncryptionKeyValues;
    int ordinal;
    int databaseId;
    int cekId;
    int cekVersion;
    byte[] cekMdVersion;

    List<EncryptionKeyInfo> getColumnEncryptionKeyValues() {
        return columnEncryptionKeyValues;
    }

    int getOrdinal() {
        return ordinal;
    }

    int getDatabaseId() {
        return databaseId;
    }

    int getCekId() {
        return cekId;
    }

    int getCekVersion() {
        return cekVersion;
    }

    byte[] getCekMdVersion() {
        return cekMdVersion;
    }

    CekTableEntry(int ordinalVal) {
        ordinal = ordinalVal;
        databaseId = 0;
        cekId = 0;
        cekVersion = 0;
        cekMdVersion = null;
        columnEncryptionKeyValues = new ArrayList<>();
    }

    int getSize() {
        return columnEncryptionKeyValues.size();
    }

    void add(byte[] encryptedKey, int dbId, int keyId, int keyVersion, byte[] mdVersion, String keyPath,
            String keyStoreName, String algorithmName) {

        assert null != columnEncryptionKeyValues : "columnEncryptionKeyValues should already be initialized.";

        aeLogger.fine("Retrieving CEK values");

        EncryptionKeyInfo encryptionKey = new EncryptionKeyInfo(encryptedKey, dbId, keyId, keyVersion, mdVersion,
                keyPath, keyStoreName, algorithmName);
        columnEncryptionKeyValues.add(encryptionKey);

        if (0 == databaseId) {
            databaseId = dbId;
            cekId = keyId;
            cekVersion = keyVersion;
            cekMdVersion = mdVersion;
        } else {
            assert (databaseId == dbId);
            assert (cekId == keyId);
            assert (cekVersion == keyVersion);
            assert ((null != cekMdVersion) && (null != mdVersion) && (cekMdVersion.length == mdVersion.length));
        }
    }

    public void add(byte[] encryptedKey, int dbId, int keyId, int keyVersion, byte[] mdVersion, String keyPath,
            String keyStoreName, String algorithmName, byte requestedByEnclave, byte[] enclaveCMKSignature) {

        assert null != columnEncryptionKeyValues : "columnEncryptionKeyValues should already be initialized.";

        aeLogger.fine("Retrieving CEK values");

        EncryptionKeyInfo encryptionKey = new EncryptionKeyInfo(encryptedKey, dbId, keyId, keyVersion, mdVersion,
                keyPath, keyStoreName, algorithmName, requestedByEnclave, enclaveCMKSignature);
        columnEncryptionKeyValues.add(encryptionKey);

        if (0 == databaseId) {
            databaseId = dbId;
            cekId = keyId;
            cekVersion = keyVersion;
            cekMdVersion = mdVersion;
        } else {
            assert (databaseId == dbId);
            assert (cekId == keyId);
            assert (cekVersion == keyVersion);
            assert ((null != cekMdVersion) && (null != mdVersion) && (cekMdVersion.length == mdVersion.length));
        }
    }
}


/**
 * Contains all CEKs, each row represents one unique CEK (represented by CekTableEntry).
 */
class CekTable implements Serializable {
    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = -4568542970907052239L;

    CekTableEntry[] keyList;

    CekTable(int tableSize) {
        keyList = new CekTableEntry[tableSize];
    }

    int getSize() {
        return keyList.length;
    }

    CekTableEntry getCekTableEntry(int index) {
        return keyList[index];
    }

    void setCekTableEntry(int index, CekTableEntry entry) {
        keyList[index] = entry;
    }
}


/**
 * Represents Encryption related information of the cipher data.
 */
class CryptoMetadata {
    TypeInfo baseTypeInfo;
    CekTableEntry cekTableEntry;
    byte cipherAlgorithmId;
    String cipherAlgorithmName;
    SQLServerEncryptionType encryptionType;
    byte normalizationRuleVersion;
    SQLServerEncryptionAlgorithm cipherAlgorithm = null;
    EncryptionKeyInfo encryptionKeyInfo;
    short ordinal;

    CekTableEntry getCekTableEntry() {
        return cekTableEntry;
    }

    void setCekTableEntry(CekTableEntry cekTableEntryObj) {
        cekTableEntry = cekTableEntryObj;
    }

    TypeInfo getBaseTypeInfo() {
        return baseTypeInfo;
    }

    void setBaseTypeInfo(TypeInfo baseTypeInfoObj) {
        baseTypeInfo = baseTypeInfoObj;
    }

    SQLServerEncryptionAlgorithm getEncryptionAlgorithm() {
        return cipherAlgorithm;
    }

    void setEncryptionAlgorithm(SQLServerEncryptionAlgorithm encryptionAlgorithmObj) {
        cipherAlgorithm = encryptionAlgorithmObj;
    }

    EncryptionKeyInfo getEncryptionKeyInfo() {
        return encryptionKeyInfo;
    }

    void setEncryptionKeyInfo(EncryptionKeyInfo encryptionKeyInfoObj) {
        encryptionKeyInfo = encryptionKeyInfoObj;
    }

    byte getEncryptionAlgorithmId() {
        return cipherAlgorithmId;
    }

    String getEncryptionAlgorithmName() {
        return cipherAlgorithmName;
    }

    SQLServerEncryptionType getEncryptionType() {
        return encryptionType;
    }

    byte getNormalizationRuleVersion() {
        return normalizationRuleVersion;
    }

    short getOrdinal() {
        return ordinal;
    }

    CryptoMetadata(CekTableEntry cekTableEntryObj, short ordinalVal, byte cipherAlgorithmIdVal,
            String cipherAlgorithmNameVal, byte encryptionTypeVal,
            byte normalizationRuleVersionVal) throws SQLServerException {
        cekTableEntry = cekTableEntryObj;
        ordinal = ordinalVal;
        cipherAlgorithmId = cipherAlgorithmIdVal;
        cipherAlgorithmName = cipherAlgorithmNameVal;
        encryptionType = SQLServerEncryptionType.of(encryptionTypeVal);
        normalizationRuleVersion = normalizationRuleVersionVal;
        encryptionKeyInfo = null;
    }

    boolean IsAlgorithmInitialized() {
        return null != cipherAlgorithm;
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

    AttestationResponse(byte[] b) {
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
            // throw exception
        }

        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            healthCert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(healthReportCertificate));
        } catch (CertificateException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    boolean validateCert(byte[] b) throws CertificateException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Collection<X509Certificate> certs = (Collection<X509Certificate>) cf
                .generateCertificates(new ByteArrayInputStream(b));
        for (X509Certificate cert : certs) {
            try {
                healthCert.verify(cert.getPublicKey());
                return true;
            } catch (SignatureException e) {
                // Doesn't match, invalid
            } catch (java.security.GeneralSecurityException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return false;
    }
}


// Fields in the first resultset of "sp_describe_parameter_encryption"
// We expect the server to return the fields in the resultset in the same order as mentioned below.
// If the server changes the below order, then transparent parameter encryption will break.
enum DescribeParameterEncryptionResultSet1 {
    KeyOrdinal,
    DbId,
    KeyId,
    KeyVersion,
    KeyMdVersion,
    EncryptedKey,
    ProviderName,
    KeyPath,
    KeyEncryptionAlgorithm,
    KeyRequestedByEnclave,
    EnclaveCMKSignature;

    int value() {
        // Column indexing starts from 1;
        return ordinal() + 1;
    }
}


// Fields in the second resultset of "sp_describe_parameter_encryption"
// We expect the server to return the fields in the resultset in the same order as mentioned below.
// If the server changes the below order, then transparent parameter encryption will break.
enum DescribeParameterEncryptionResultSet2 {
    ParameterOrdinal,
    ParameterName,
    ColumnEncryptionAlgorithm,
    ColumnEncrytionType,
    ColumnEncryptionKeyOrdinal,
    NormalizationRuleVersion;

    int value() {
        // Column indexing starts from 1;
        return ordinal() + 1;
    }

}
