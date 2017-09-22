/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamSSPi represents a TDS SSPI processing.
 *
 */

final class StreamSSPI extends StreamPacket {
    byte sspiBlob[];

    StreamSSPI() {
        super(TDS.TDS_SSPI);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_SSPI != tdsReader.readUnsignedByte())
            assert false;
        int blobLength = tdsReader.readUnsignedShort();
        sspiBlob = new byte[blobLength];
        tdsReader.readBytes(sspiBlob, 0, blobLength);
    }
}
