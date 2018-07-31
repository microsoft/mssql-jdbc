/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.List;

import com.microsoft.sqlserver.jdbc.dataclassification.ColumnSensitivity;
import com.microsoft.sqlserver.jdbc.dataclassification.InformationType;
import com.microsoft.sqlserver.jdbc.dataclassification.Label;
import com.microsoft.sqlserver.jdbc.dataclassification.SensitivityClassification;
import com.microsoft.sqlserver.jdbc.dataclassification.SensitivityProperty;


/**
 * StreamColumns stores the column meta data for a result set. StreamColumns parses the inbound TDS packet stream to
 * determine column meta data.
 */

final class StreamColumns extends StreamPacket {
    /** the set of columns */
    private Column[] columns;

    /* The CekTable. */
    private CekTable cekTable = null;

    private boolean shouldHonorAEForRead = false;

    /* Returns the CekTable */
    CekTable getCekTable() {
        return cekTable;
    }

    /**
     * Create a new stream columns
     */
    /* L0 */ StreamColumns() {
        super(TDS.TDS_COLMETADATA);
    }

    /**
     * Create a new stream columns with column encryption setting
     */
    StreamColumns(boolean honorAE) {
        super(TDS.TDS_COLMETADATA);
        shouldHonorAEForRead = honorAE;
    }

    /**
     * Parse a result set column meta data TDS stream for CEK table entry.
     *
     * @throws SQLServerException
     */
    CekTableEntry readCEKTableEntry(TDSReader tdsReader) throws SQLServerException {
        // Read the DB ID
        int databaseId = tdsReader.readInt();

        // Read the keyID
        int cekId = tdsReader.readInt();

        // Read the key version
        int cekVersion = tdsReader.readInt();

        // Read the key MD Version
        byte[] cekMdVersion = new byte[8];
        tdsReader.readBytes(cekMdVersion, 0, 8);

        // Read the value count
        int cekValueCount = tdsReader.readUnsignedByte();

        CekTableEntry cekTableEntry = new CekTableEntry(cekValueCount);

        for (int i = 0; i < cekValueCount; i++) {
            // Read individual CEK values
            short encryptedCEKlength = tdsReader.readShort();

            byte[] encryptedCek = new byte[encryptedCEKlength];

            // Read the actual encrypted CEK
            tdsReader.readBytes(encryptedCek, 0, encryptedCEKlength);

            // Read the length of key store name
            int keyStoreLength = tdsReader.readUnsignedByte();

            // And read the key store name now
            String keyStoreName = tdsReader.readUnicodeString(keyStoreLength);

            // Read the length of key Path
            int keyPathLength = tdsReader.readShort();

            // Read the key path string
            String keyPath = tdsReader.readUnicodeString(keyPathLength);

            // Read the length of the string carrying the encryption algo
            int algorithmLength = tdsReader.readUnsignedByte();

            // Read the string carrying the encryption algo (eg. RSA_PKCS_OAEP)
            String algorithmName = tdsReader.readUnicodeString(algorithmLength);

            // Add this encrypted CEK blob to our list of encrypted values for the CEK
            cekTableEntry.add(encryptedCek, databaseId, cekId, cekVersion, cekMdVersion, keyPath, keyStoreName,
                    algorithmName);
        }
        return cekTableEntry;
    }

    /**
     * Parse a result set column meta data TDS stream for CEK table.
     *
     * @throws SQLServerException
     */
    void readCEKTable(TDSReader tdsReader) throws SQLServerException {
        // Read count
        int tableSize = tdsReader.readShort();

        // CEK table will be sent if AE is enabled. If none of the columns are encrypted, the CEK table
        // size would be zero.
        if (0 != tableSize) {
            cekTable = new CekTable(tableSize);

            // Read individual entries
            for (int i = 0; i < tableSize; i++) {
                // SqlTceCipherInfoEntry entry;
                cekTable.setCekTableEntry(i, readCEKTableEntry(tdsReader));
            }
        }
    }

    /**
     * Parse a result set column meta data TDS stream for crypto metadata for AE.
     *
     * @throws SQLServerException
     */
    CryptoMetadata readCryptoMetadata(TDSReader tdsReader) throws SQLServerException {
        short ordinal = 0;
        // Read the ordinal into cipher table. For return values there is not cipher table and no ordinal.
        if (null != cekTable) {
            ordinal = tdsReader.readShort();
        }

        TypeInfo typeInfo = TypeInfo.getInstance(tdsReader, false);

        // Read the cipher algorithm Id
        byte algorithmId = (byte) tdsReader.readUnsignedByte();

        String algorithmName = null;
        if ((byte) TDS.CUSTOM_CIPHER_ALGORITHM_ID == algorithmId) {
            // Custom encryption algorithm, read the name
            int nameSize = tdsReader.readUnsignedByte();
            algorithmName = tdsReader.readUnicodeString(nameSize);
        }

        // Read Encryption Type.
        byte encryptionType = (byte) tdsReader.readUnsignedByte();

        // Read Normalization Rule Version.
        byte normalizationRuleVersion = (byte) tdsReader.readUnsignedByte();

        CryptoMetadata cryptoMeta = new CryptoMetadata((cekTable == null) ? null : cekTable.getCekTableEntry(ordinal),
                ordinal, algorithmId, algorithmName, encryptionType, normalizationRuleVersion);
        cryptoMeta.setBaseTypeInfo(typeInfo);

        return cryptoMeta;
    }

    /**
     * Parse a result set column meta data TDS stream.
     *
     * @throws SQLServerException
     */
    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_COLMETADATA != tdsReader.readUnsignedByte())
            assert false;

        int nTotColumns = tdsReader.readUnsignedShort();

        // Handle the magic NoMetaData value
        if (0xFFFF == nTotColumns)
            return;

        if (tdsReader.getServerSupportsColumnEncryption()) {
            readCEKTable(tdsReader);
        }

        this.columns = new Column[nTotColumns];

        for (int numColumns = 0; numColumns < nTotColumns; numColumns++) {
            // Column type info
            TypeInfo typeInfo = TypeInfo.getInstance(tdsReader, true);

            // Table name
            //
            // Table name is set at this point for TEXT/NTEXT/IMAGE columns only.
            // For other columns, table name may be set via COLUMNINFO and TABNAME tokens.
            SQLIdentifier tableName = new SQLIdentifier();
            if (SSType.TEXT == typeInfo.getSSType() || SSType.NTEXT == typeInfo.getSSType()
                    || SSType.IMAGE == typeInfo.getSSType()) {
                // Yukon and later, table names are returned as multi-part SQL identifiers.
                tableName = tdsReader.readSQLIdentifier();
            }

            CryptoMetadata cryptoMeta = null;
            if (tdsReader.getServerSupportsColumnEncryption() && typeInfo.isEncrypted()) {
                cryptoMeta = readCryptoMetadata(tdsReader);
                cryptoMeta.baseTypeInfo.setFlags(typeInfo.getFlagsAsShort());
                typeInfo.setSQLCollation(cryptoMeta.baseTypeInfo.getSQLCollation());
            }

            // Column name
            String columnName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());

            if (shouldHonorAEForRead) {
                this.columns[numColumns] = new Column(typeInfo, columnName, tableName, cryptoMeta);
            } else {
                // Set null for crypto metadata if column encryption setting is off at the connection or at the
                // statement level.
                this.columns[numColumns] = new Column(typeInfo, columnName, tableName, null);
            }
        }

        // Data Classification
        if (tdsReader.getServerSupportsDataClassification()
                && tdsReader.peekTokenType() == TDS.TDS_SQLDATACLASSIFICATION) {
            // Read and parse
            tdsReader.trySetSensitivityClassification(processDataClassification(tdsReader));
        }
    }

    private String readByteString(TDSReader tdsReader) throws SQLServerException {
        String value = "";
        int byteLen = (int) tdsReader.readUnsignedByte();
        value = tdsReader.readUnicodeString(byteLen);
        return value;
    }

    private Label readSensitivityLabel(TDSReader tdsReader) throws SQLServerException {
        String name = readByteString(tdsReader);
        String id = readByteString(tdsReader);
        return new Label(name, id);
    }

    private InformationType readSensitivityInformationType(TDSReader tdsReader) throws SQLServerException {
        String name = readByteString(tdsReader);
        String id = readByteString(tdsReader);
        return new InformationType(name, id);
    }

    private SensitivityClassification processDataClassification(TDSReader tdsReader) throws SQLServerException {
        if (!tdsReader.getServerSupportsDataClassification()) {
            tdsReader.throwInvalidTDS();
        }

        int dataClassificationToken = tdsReader.readUnsignedByte();
        assert dataClassificationToken == TDS.TDS_SQLDATACLASSIFICATION;

        SensitivityClassification sensitivityClassification = null;

        // get the label count
        int numLabels = tdsReader.readUnsignedShort();
        List<Label> labels = new ArrayList<Label>(numLabels);

        for (int i = 0; i < numLabels; i++) {
            labels.add(readSensitivityLabel(tdsReader));
        }

        // get the information type count
        int numInformationTypes = tdsReader.readUnsignedShort();

        List<InformationType> informationTypes = new ArrayList<InformationType>(numInformationTypes);
        for (int i = 0; i < numInformationTypes; i++) {
            informationTypes.add(readSensitivityInformationType(tdsReader));
        }

        // get the per column classification data (corresponds to order of output columns for query)
        int numResultColumns = tdsReader.readUnsignedShort();

        List<ColumnSensitivity> columnSensitivities = new ArrayList<ColumnSensitivity>(numResultColumns);
        for (int columnNum = 0; columnNum < numResultColumns; columnNum++) {

            // get sensitivity properties for all the different sources which were used in generating the column output
            int numSources = tdsReader.readUnsignedShort();
            List<SensitivityProperty> sensitivityProperties = new ArrayList<SensitivityProperty>(numSources);
            for (int sourceNum = 0; sourceNum < numSources; sourceNum++) {

                // get the label index and then lookup label to use for source
                int labelIndex = tdsReader.readUnsignedShort();
                Label label = null;
                if (labelIndex != Integer.MAX_VALUE) {
                    if (labelIndex >= labels.size()) {
                        tdsReader.throwInvalidTDS();
                    }
                    label = labels.get(labelIndex);
                }

                // get the information type index and then lookup information type to use for source
                int informationTypeIndex = tdsReader.readUnsignedShort();
                InformationType informationType = null;
                if (informationTypeIndex != Integer.MAX_VALUE) {
                    if (informationTypeIndex >= informationTypes.size()) {}
                    informationType = informationTypes.get(informationTypeIndex);
                }
                // add sensitivity properties for the source
                sensitivityProperties.add(new SensitivityProperty(label, informationType));
            }
            columnSensitivities.add(new ColumnSensitivity(sensitivityProperties));
        }
        sensitivityClassification = new SensitivityClassification(labels, informationTypes, columnSensitivities);
        return sensitivityClassification;
    }

    /**
     * Applies per-column table information derived from COLINFO and TABNAME tokens to the set of columns defined by
     * this COLMETADATA token to produce the complete set of column information.
     */
    Column[] buildColumns(StreamColInfo colInfoToken, StreamTabName tabNameToken) throws SQLServerException {
        if (null != colInfoToken && null != tabNameToken)
            tabNameToken.applyTo(columns, colInfoToken.applyTo(columns));

        return columns;
    }
}
