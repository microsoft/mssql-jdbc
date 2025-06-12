/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.resiliency.BasicConnectionTest;
import com.microsoft.sqlserver.jdbc.resiliency.ResiliencyUtils;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.fedAuth)
public class FedauthResiliencyTest extends FedauthCommon {


    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testConnectionResiliency() throws Exception {

        String connectionString = "jdbc:sqlserver://" + azureServer +
                ";database=" + azureDatabase + ";accessToken=" + accessToken + ";user=" + azureUserName
                + ";trustServerCertificate=true;loginTimeout=1200;socketTimeout=1200;";
        try {
            for (int i1 = 0; i1 < 2; i1++) {
                try (Connection c = ResiliencyUtils.getConnection(connectionString)) {
                    for (int i2 = 0; i2 < 3; i2++) {
                        try (Statement s = c.createStatement()) {
                            ResiliencyUtils.killConnection(c, connectionString, 0);
                            s.executeQuery("SELECT 1");
                        }
                    }
                }
        }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }    
    }
}
