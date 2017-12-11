/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single encrypted value for a CEK. It contains the encrypted CEK,the store type, name,the key path and encryption algorithm.
 */
class EncryptionKeyInfo {
    EncryptionKeyInfo(byte[] encryptedKeyVal,
            int dbId,
            int keyId,
            int keyVersion,
            byte[] mdVersion,
            String keyPathVal,
            String keyStoreNameVal,
            String algorithmNameVal) {
        encryptedKey = encryptedKeyVal;
        databaseId = dbId;
        cekId = keyId;
        cekVersion = keyVersion;
        cekMdVersion = mdVersion;
        keyPath = keyPathVal;
        keyStoreName = keyStoreNameVal;
        algorithmName = algorithmNameVal;
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
}

/**
 * Represents a unique CEK as an entry in the CekTable. A unique (plaintext is unique) CEK can have multiple encrypted CEKs when using multiple CMKs.
 * These encrypted CEKs are represented by a member ArrayList.
 */
class CekTableEntry {
    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.AE");

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

    void add(byte[] encryptedKey,
            int dbId,
            int keyId,
            int keyVersion,
            byte[] mdVersion,
            String keyPath,
            String keyStoreName,
            String algorithmName) {

        assert null != columnEncryptionKeyValues : "columnEncryptionKeyValues should already be initialized.";

        if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
            aeLogger.fine("Retrieving CEK values");
        }

        EncryptionKeyInfo encryptionKey = new EncryptionKeyInfo(encryptedKey, dbId, keyId, keyVersion, mdVersion, keyPath, keyStoreName,
                algorithmName);
        columnEncryptionKeyValues.add(encryptionKey);

        if (0 == databaseId) {
            databaseId = dbId;
            cekId = keyId;
            cekVersion = keyVersion;
            cekMdVersion = mdVersion;
        }
        else {
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
class CekTable {
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

    void setCekTableEntry(int index,
            CekTableEntry entry) {
        keyList[index] = entry;
    }
}

/**
 * Represents Encryption related information of the cipher data.
 */
class CryptoMetadata
{
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
	
	CryptoMetadata(CekTableEntry cekTableEntryObj,
                                short ordinalVal,
                                byte cipherAlgorithmIdVal,
                                String cipherAlgorithmNameVal,
                                byte encryptionTypeVal,
                                byte normalizationRuleVersionVal) throws SQLServerException
   {
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
    KeyEncryptionAlgorithm;

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
