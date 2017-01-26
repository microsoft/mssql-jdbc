/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamLoginAck represents a TDS login ack.
 *
 */

final class StreamLoginAck extends StreamPacket {
    String sSQLServerVersion;
    int tdsVersion;

    StreamLoginAck() {
        super(TDS.TDS_LOGIN_ACK);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_LOGIN_ACK != tdsReader.readUnsignedByte())
            assert false;
        tdsReader.readUnsignedShort(); // length of this token stream
        tdsReader.readUnsignedByte(); // SQL version accepted by the server
        tdsVersion = tdsReader.readIntBigEndian(); // TDS version accepted by the server
        tdsReader.readUnicodeString(tdsReader.readUnsignedByte()); // Program name
        int serverMajorVersion = tdsReader.readUnsignedByte();
        int serverMinorVersion = tdsReader.readUnsignedByte();
        int serverBuildNumber = (tdsReader.readUnsignedByte() << 8) | tdsReader.readUnsignedByte();

        sSQLServerVersion = serverMajorVersion + "." + ((serverMinorVersion <= 9) ? "0" : "") + serverMinorVersion + "." + serverBuildNumber;
    }
}
