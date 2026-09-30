/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

public class VectorProbeTest extends AbstractTest {

    @Test
    public void probeVectorSupport() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {

            DatabaseMetaData md = con.getMetaData();
            System.out.println(">>> Connected to: " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion());

            // Try creating a table with VECTOR(3)
            TestUtils.dropTableIfExists("probe_vector_table", stmt);
            try {
                stmt.execute("CREATE TABLE probe_vector_table (id INT, v VECTOR(3))");
                System.out.println(">>> SUCCESS: VECTOR data type IS supported in this database!");
                TestUtils.dropTableIfExists("probe_vector_table", stmt);
            } catch (Exception e) {
                System.out.println(">>> FAILED: VECTOR data type is NOT supported in this instance: " + e.getMessage());
            }
        }
    }
}
