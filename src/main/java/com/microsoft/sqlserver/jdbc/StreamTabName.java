//---------------------------------------------------------------------------------------------------------------------------------
// File: StreamTabName.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 

package com.microsoft.sqlserver.jdbc;

/**
* StreamTabName interprets the data stream from a TABNAME TDS token
*
*/

final class StreamTabName extends StreamPacket
{
    private TDSReader tdsReader;
    private TDSReaderMark tableNamesMark;

    StreamTabName()
    {
        super(TDS.TDS_TABNAME);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException
    {
        if (TDS.TDS_TABNAME != tdsReader.readUnsignedByte())
            assert false : "Not a TABNAME token";

        this.tdsReader = tdsReader;
        int tokenLength = tdsReader.readUnsignedShort();
        tableNamesMark = tdsReader.mark();
        tdsReader.skip(tokenLength);
    }

    void applyTo(Column[] columns, int numTables) throws SQLServerException
    {
        TDSReaderMark currentMark = tdsReader.mark();
        tdsReader.reset(tableNamesMark);

        // Read in all of the multi-part table names.  The number of table
        // names to expect is determined in advance.  It is computed as a side
        // effect of processing the COLINFO token that preceeds this TABNAME token.
        SQLIdentifier[] tableNames = new SQLIdentifier[numTables];
        for (int i = 0; i < numTables; i++)
            tableNames[i] = tdsReader.readSQLIdentifier();

        // Apply the table names to their appropriate columns
        for (int i = 0; i < columns.length; i++)
        {
            Column col = columns[i];

            if (col.getTableNum() > 0)
                col.setTableName(tableNames[col.getTableNum()-1]);
        }

        tdsReader.reset(currentMark);
        return;
    }
}
