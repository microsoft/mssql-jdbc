//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerAeadAes256CbcHmac256Factory.java
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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.DatatypeConverter;

/**
 * Factory for SQLServerAeadAes256CbcHmac256Algorithm
 */
class SQLServerAeadAes256CbcHmac256Factory extends SQLServerEncryptionAlgorithmFactory {
    // In future we can have more
    private byte algorithmVersion = 0x1;
    private ConcurrentHashMap<String, SQLServerAeadAes256CbcHmac256Algorithm> encryptionAlgorithms = new ConcurrentHashMap<String, SQLServerAeadAes256CbcHmac256Algorithm>();

    @Override
    SQLServerEncryptionAlgorithm create(
            SQLServerSymmetricKey columnEncryptionKey,
            SQLServerEncryptionType encryptionType, String encryptionAlgorithm) throws SQLServerException {

        assert (columnEncryptionKey != null);
        if (encryptionType != SQLServerEncryptionType.Deterministic && encryptionType != SQLServerEncryptionType.Randomized) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidEncryptionType"));
            Object[] msgArgs = {encryptionType, encryptionAlgorithm, "'" + SQLServerEncryptionType.Deterministic + "," + SQLServerEncryptionType.Randomized + "'"};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);

        }
        String factoryKey = "";

        try {
            StringBuffer factoryKeyBuilder = new StringBuffer();
            factoryKeyBuilder.append(
                    DatatypeConverter.printBase64Binary(
                            new String(
                                    columnEncryptionKey.getRootKey(),
                                    "UTF-8"
                            ).getBytes()
                    )
            );

            factoryKeyBuilder.append(":");
            factoryKeyBuilder.append(encryptionType);
            factoryKeyBuilder.append(":");
            factoryKeyBuilder.append(algorithmVersion);

            factoryKey = factoryKeyBuilder.toString();

            SQLServerAeadAes256CbcHmac256Algorithm aesAlgorithm;

            if (!encryptionAlgorithms.containsKey(factoryKey)) {
                SQLServerAeadAes256CbcHmac256EncryptionKey encryptedKey = new SQLServerAeadAes256CbcHmac256EncryptionKey(columnEncryptionKey.getRootKey(), SQLServerAeadAes256CbcHmac256Algorithm.algorithmName);
                aesAlgorithm = new SQLServerAeadAes256CbcHmac256Algorithm(encryptedKey, encryptionType, algorithmVersion);
                encryptionAlgorithms.putIfAbsent(factoryKey, aesAlgorithm);
            }


        } catch (UnsupportedEncodingException e) {
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_unsupportedEncoding"));
            Object[] msgArgs = {"UTF-8"};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        return encryptionAlgorithms.get(factoryKey);
    }

}
