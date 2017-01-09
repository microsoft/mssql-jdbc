// ---------------------------------------------------------------------------------------------------------------------------------
// File: BulkCopyTestSetUp.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------
package com.microsoft.sqlserver.jdbc.bulkCopy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;;

@RunWith(JUnitPlatform.class)
public class BulkCopyTestSetUp extends AbstractTest {

    static DBTable sourceTable;
    
    @BeforeAll
    static void setUpSourceTable() {
        DBConnection con = null;
        DBStatement stmt = null;
        try {
            con = new DBConnection(connectionString);
            stmt = con.createStatement();
            sourceTable = new DBTable(true);
            stmt.createTable(sourceTable);
            stmt.populateTable(sourceTable);
        }
        finally {
            con.close();
        }
    }

    @AfterAll
    static void dropSourceTable() {
        DBConnection con = null;
        DBStatement stmt = null;
        try {
            con = new DBConnection(connectionString);
            stmt = con.createStatement();
            stmt.dropTable(sourceTable);
        }
        finally {
            con.close();
        }
    }

}
