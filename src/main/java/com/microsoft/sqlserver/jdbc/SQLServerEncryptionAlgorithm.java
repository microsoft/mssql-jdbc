//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerEncryptionAlgorithm.java
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

/*
 * Abstract base class for all AE encryption algorithms. It exposes two functions
 * 	1. encryptData - This function is used by the driver under the covers to transparently encrypt AE enabled column data.
 * 	2. decryptData - This function is used by the driver under the covers to transparently decrypt AE enabled column data.
 */
abstract class SQLServerEncryptionAlgorithm {

    /**
     * Perform encryption of the plain text
     *
     * @param plainText data to be encrypted
     * @return cipher text
     */
    abstract byte[] encryptData(byte[] plainText) throws SQLServerException;

    /**
     * Decrypt cipher text to plain text
     *
     * @param cipherText
     * @return plain text after decryption
     */
    abstract byte[] decryptData(byte[] cipherText) throws SQLServerException;
}
