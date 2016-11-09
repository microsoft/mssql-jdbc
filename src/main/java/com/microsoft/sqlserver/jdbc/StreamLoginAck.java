//---------------------------------------------------------------------------------------------------------------------------------
// File: StreamLoginAck.java
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
* StreamLoginAck represents a TDS login ack.
*
*/

final class StreamLoginAck extends StreamPacket
{
  String sSQLServerVersion;
  int tdsVersion;

  StreamLoginAck()
  {
    super(TDS.TDS_LOGIN_ACK);
  }

  void setFromTDS(TDSReader tdsReader) throws SQLServerException
  {
    if (TDS.TDS_LOGIN_ACK != tdsReader.readUnsignedByte()) assert false;
    tdsReader.readUnsignedShort(); // length of this token stream
    tdsReader.readUnsignedByte(); // SQL version accepted by the server
    tdsVersion = tdsReader.readIntBigEndian(); // TDS version accepted by the server
    tdsReader.readUnicodeString(tdsReader.readUnsignedByte()); // Program name
    int serverMajorVersion = tdsReader.readUnsignedByte();
    int serverMinorVersion = tdsReader.readUnsignedByte();
    int serverBuildNumber  = (tdsReader.readUnsignedByte() << 8) | tdsReader.readUnsignedByte();

    sSQLServerVersion =
      serverMajorVersion + "." +
      ((serverMinorVersion <= 9) ? "0": "") + serverMinorVersion + "." +
      serverBuildNumber;
  }
}
