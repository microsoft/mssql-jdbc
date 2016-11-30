//---------------------------------------------------------------------------------------------------------------------------------
// File: StreamRetValue.java
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
* StreamRetValue represents a TDS return value.
*/
final class StreamRetValue extends StreamPacket
{
  private String paramName;

  /*
   * TDS 7.2: Indicates ordinal position of the OUTPUT parameter in the original RPC call
   * TDS 7.1: Indicates the length of the return value
   */
  private int ordinalOrLength;
  final int getOrdinalOrLength() { return ordinalOrLength; }

  /*
   * Status:
   * 0x01 if the return value is an OUTPUT parameter of a stored procedure
   * 0x02 if the return value is from a User Defined Function
   */
  private int status;

  StreamRetValue()
  {
    super(TDS.TDS_RETURN_VALUE);
  }

  void setFromTDS(TDSReader tdsReader) throws SQLServerException
  { 
    if (TDS.TDS_RETURN_VALUE != tdsReader.readUnsignedByte()) assert false;
    ordinalOrLength = tdsReader.readUnsignedShort();
    paramName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
    status = tdsReader.readUnsignedByte();
  }  

	CryptoMetadata getCryptoMetadata(TDSReader tdsReader) throws SQLServerException
	{ 
		CryptoMetadata cryptoMeta = (new StreamColumns()).readCryptoMetadata(tdsReader);

		return cryptoMeta;
	}  
}
