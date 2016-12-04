//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerEncryptionAlgorithmFactory.java
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
 * 
 * Abstract base class for all the encryption algorithm factory classes.
 *
 */
 abstract class SQLServerEncryptionAlgorithmFactory {
	
     /**
      * 
      * @param columnEncryptionKey key which will be used in encryption/decryption
      * @param encryptionType  specifies kind of encryption
      * @param encryptionAlgorithm name of encryption algorithm 
      * @return created SQLServerEncryptionAlgorithm instance 
      * @throws SQLServerException when an error occurs
      */
	 abstract SQLServerEncryptionAlgorithm create (SQLServerSymmetricKey columnEncryptionKey,SQLServerEncryptionType encryptionType,String encryptionAlgorithm) throws SQLServerException;

}
