/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fips;

import java.sql.Connection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Test class for testing FIPS property settings.
 */
@RunWith(JUnitPlatform.class)
public class FipsTest {

    private static String connectionString;
    private static String[] dataSourceProps;

    @BeforeAll
    public static void init() {
        connectionString = Utils.getConfiguredProperty("mssql_jdbc_test_connection_properties");
        dataSourceProps = getDataSourceProperties();
    }

    /**
     * Tests after removing all FIPS related properties.
     * 
     * @throws Exception
     */
    @Test
    public void fipsDataSourcePropertyTest() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        setDataSourceProperties(ds);
        ds.setEncrypt(false);
        ds.setTrustStoreType("JKS");
        Connection con = ds.getConnection();
        Assertions.assertTrue(!StringUtils.isEmpty(con.getSchema()));
        con.close();
        con = null;
    }

    /**
     * Setting appropriate data source properties including FIPS
     * @param ds
     */
    private void setDataSourceProperties(SQLServerDataSource ds) {
        ds.setServerName(dataSourceProps[0]);

        if (dataSourceProps[1] != null && StringUtils.isInteger(dataSourceProps[1])) {
            ds.setPortNumber(Integer.valueOf(dataSourceProps[1]));
        }

        ds.setUser(dataSourceProps[2]);
        ds.setPassword(dataSourceProps[3]);
        ds.setDatabaseName(dataSourceProps[4]);

        // Set all properties for FIPS
        ds.setEncrypt(true);
        ds.setTrustServerCertificate(false);
        ds.setIntegratedSecurity(false);
        ds.setTrustStoreType("PKCS12");
    }

    /**
     * It will return String array. [dbServer,username,password,dbname/database]
     * 
     * -ea -Dmssql_jdbc_test_connection_properties=jdbc:sqlserver://SQL-2K16-01.galaxy.ad;userName=sa;password=Moonshine4me;database=test;
     * -Djava.library.path=C:\Downloads\sqljdbc_6.0.7728.100_enu.tar\sqljdbc_6.0\enu\auth\x64
     * 
     * @param connectionProperty
     * @return
     */
    private static String[] getDataSourceProperties() {
        String[] params = connectionString.split(";");
        String[] dataSoureParam = new String[5];

        for (String strParam : params) {
            if (strParam.startsWith("jdbc:sqlserver")) {
                dataSoureParam[0] = strParam.replace("jdbc:sqlserver://", "");
                String[] hostPort = dataSoureParam[0].split(":");
                dataSoureParam[0] = hostPort[0];
                if (hostPort.length > 1) {
                    dataSoureParam[1] = hostPort[1];
                }
            }
            // Actually this is specifically did for Travis.
            else if(strParam.startsWith("port")) {
                strParam = strParam.toLowerCase();
                if(strParam.startsWith("portnumber")) {
                    dataSoureParam[1] = strParam.replace("portnumber=", "");
                } else {
                    dataSoureParam[1] = strParam.replace("port=", "");
                }
            }
            
            else if (strParam.startsWith("user")) {
                strParam = strParam.toLowerCase();
                if (strParam.startsWith("username")) {
                    dataSoureParam[2] = strParam.replace("username=", "");
                }
                else {
                    dataSoureParam[2] = strParam.replace("user=", "");
                }
            }
            else if (strParam.startsWith("password")) {
                dataSoureParam[3] = strParam.replace("password=", "");
            }
            else if (strParam.startsWith("database")) {
                strParam = strParam.toLowerCase();
                if (strParam.startsWith("databasename")) {
                    dataSoureParam[4] = strParam.replace("databasename=", "");
                }
                else {
                    dataSoureParam[4] = strParam.replace("database=", "");
                }
            }

        }

        return dataSoureParam;
    }

}
