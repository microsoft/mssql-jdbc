/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamRetValue represents a TDS return value.
 */
final class StreamRetValue extends StreamPacket {
    private String paramName;

    /*
     * TDS 7.2: Indicates ordinal position of the OUTPUT parameter in the original RPC call TDS 7.1: Indicates the length of the return value
     */
    private int ordinalOrLength;

    final int getOrdinalOrLength() {
        return ordinalOrLength;
    }

    /*
     * Status: 0x01 if the return value is an OUTPUT parameter of a stored procedure 0x02 if the return value is from a User Defined Function
     */
    private int status;

    StreamRetValue() {
        super(TDS.TDS_RETURN_VALUE);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_RETURN_VALUE != tdsReader.readUnsignedByte())
            assert false;
        ordinalOrLength = tdsReader.readUnsignedShort();
        paramName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
        status = tdsReader.readUnsignedByte();
    }

    CryptoMetadata getCryptoMetadata(TDSReader tdsReader) throws SQLServerException {
        CryptoMetadata cryptoMeta = (new StreamColumns()).readCryptoMetadata(tdsReader);

        return cryptoMeta;
    }
}
