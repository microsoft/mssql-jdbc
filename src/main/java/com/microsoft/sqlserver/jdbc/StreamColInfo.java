//---------------------------------------------------------------------------------------------------------------------------------
// File: StreamColInfo.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
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
* StreamColInfo interprets the data stream from a COLINFO TDS token
*/

final class StreamColInfo extends StreamPacket
{
    private TDSReader tdsReader;
    private TDSReaderMark colInfoMark;

    StreamColInfo()
    {
        super(TDS.TDS_COLINFO);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException
    {
        if (TDS.TDS_COLINFO != tdsReader.readUnsignedByte())
            assert false : "Not a COLINFO token";

        this.tdsReader = tdsReader;
        int tokenLength = tdsReader.readUnsignedShort();
        colInfoMark = tdsReader.mark();
        tdsReader.skip(tokenLength);
    }

    int applyTo(Column[] columns) throws SQLServerException
    {
        int numTables = 0;

        // Read and apply the column info for each column
        TDSReaderMark currentMark = tdsReader.mark();
        tdsReader.reset(colInfoMark);
        for (int i = 0; i < columns.length; i++)
        {
            Column col = columns[i];

            // Ignore the column number, per TDS spec.
            // Column info is returned for each column, ascending by column index,
            // so iterating through the column info is sufficient.
            tdsReader.readUnsignedByte();

            // Set the column's table number, keeping track of the maximum table number
            // representing the number of tables encountered.
            col.setTableNum(tdsReader.readUnsignedByte());
            if (col.getTableNum() > numTables)
                numTables = col.getTableNum();

            // Set the other column info
            col.setInfoStatus(tdsReader.readUnsignedByte());
            if (col.hasDifferentName())
                col.setBaseColumnName(tdsReader.readUnicodeString(tdsReader.readUnsignedByte()));
        }

        tdsReader.reset(currentMark);
        return numTables;
    }
}
