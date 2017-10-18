/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

final class StreamInfo extends StreamPacket {
    final StreamError msg = new StreamError();

    StreamInfo() {
        super(TDS.TDS_MSG);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_MSG != tdsReader.readUnsignedByte())
            assert false;
        msg.setContentsFromTDS(tdsReader);
    }
}
