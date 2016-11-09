//---------------------------------------------------------------------------------------------------------------------------------
// File: UDTTDSHeader.java
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
* UDTTDSHeader is helper class used to read and write the UDT TDS 7.2x header from a TDS stream.
* 
* Typical UDT header->
*
* UDT header with schema/type/assembly information
*
* F0
* 21 00 <- MaxLen (2 byte length)
* 03|54 00 44 00 53 00 <- DB_NAME (1 byte length in UNICODE chars)
* 03|64 00 62 00 6F 00 <- SCHEMA_NAME (1 byte length in UNICODE chars)
* 12|4D 00 79 00 43 00 68 00 75 00 6E 00 6B 00 79 <- TYPE_NAME (1 byte length in UNICODE chars)
* 00 46 00 75 00 6E 00 6B 00 79 00 54 00 79 00 70 00 65 00 32 00 
* 63 00|4D 00 79 00 43 00 68 00 75 00 6E 00 6B 00	<- ASSEMBLY_QUALIFIED_NAME (2 byte length in UNICODE chars)
* 79 00 46 00 75 00 6E 00 6B 00 79 00 54 00 79 00 
* 70 00 65 00 32 00 2C 00 20 00 53 00 71 00 6C 00 
* 
*/

final class UDTTDSHeader
{
	private final int maxLen;						// MaxLen read from UDT type (not used when writing).
	private final String databaseName;			// Database name where UDT type resides.
	private final String schemaName;				// Schema where UDT resides.
	private final String typeName;				// Type name of UDT.
	private final String assemblyQualifiedName;	// Assembly qualified name of UDT.
	
    UDTTDSHeader(TDSReader tdsReader) throws SQLServerException
    {
		maxLen = tdsReader.readUnsignedShort();
    	databaseName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
        schemaName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
        typeName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
        assemblyQualifiedName = tdsReader.readUnicodeString(tdsReader.readUnsignedShort());
    }

    int getMaxLen() { return maxLen; }
	String getTypeName() { return typeName; }
}
