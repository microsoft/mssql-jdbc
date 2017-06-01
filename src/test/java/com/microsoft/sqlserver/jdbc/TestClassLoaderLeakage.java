/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.PrepUtil;
import com.microsoft.sqlserver.testframework.Utils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import se.jiderhamn.classloader.leak.JUnitClassloaderRunner;
import se.jiderhamn.classloader.leak.Leaks;

/**
 * This class is using JUnit 4. This is dedicated to see any class leakage issues.
 * 
 * CAUTION: IN PROGRESS. Able to detect leakage issues but work is going on.
 */
@RunWith(JUnitClassloaderRunner.class)
public class TestClassLoaderLeakage {

    /**
     * This test case detects leakage...
     */
    @Leaks(dumpHeapOnError = true)
    @Test
    public void testClassLoaderForMSSQL() throws Exception {
        loadOnlyDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }

    /**
     * It will just load driver & deregister it.
     * 
     * @param driverClass
     * @throws Exception
     */
    private void loadOnlyDriver(String driverClass) throws Exception {

        Class.forName(driverClass);

        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = (Driver) drivers.nextElement();
            System.out.println(driver.toString());
            DriverManager.deregisterDriver(driver);
        }
    }

    /**
     * This test case detects leakage...
     */
    @Leaks(dumpHeapOnError = true)
    @Test
    public void testClassLoaderLeakage() {
        try {
            String connectionString = Utils.getConfiguredProperty("mssql_jdbc_test_connection_properties");

            System.out.println("Method Connection " + connectionString);

            Connection con = PrepUtil.getConnection(connectionString);
            System.out.println("Catalog...." + con.getCatalog());
            con.close();
            con = null;

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This test case detects leakage...
     * 
     * @throws Exception
     */
    @Leaks(dumpHeapOnError = true)
    @Test
    public void testConnectionPool() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        final int POOL_SIZE = 10;
        String connectionString = Utils.getConfiguredProperty("mssql_jdbc_test_connection_properties");
        Connection[] con = new Connection[10];
        HikariConfig config = new HikariConfig();
        config.setMaximumPoolSize(POOL_SIZE);
        config.setJdbcUrl(connectionString);
        HikariDataSource ds = new HikariDataSource(config);

        for (int i = 0; i < POOL_SIZE; i++) {
            con[i] = ds.getConnection();
        }

        for (int i = 0; i < POOL_SIZE; i++) {
            con[i].close();
            con[i] = null;
        }
        ds.close();
        ds = null;
        config = null;

    }

}
