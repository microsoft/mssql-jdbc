/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;

/**
 * Test BulkCopy Connection Constructor and BulkCopyOption
 */
@RunWith(JUnitPlatform.class)
@DisplayName("BulkCopy Connection Test")
public class BulkCopyConnectionTest extends BulkCopyTestSetUp {

    /**
     * Generate dynamic tests to test all SQLServerBulkCopy constructor
     * 
     * @return
     */
    @TestFactory
    Stream<DynamicTest> generateBulkCopyConstructorTest() {
        List<BulkCopyTestWrapper> testData = createTestDatatestBulkCopyConstructor();
        // had to avoid using lambdas as we need to test against java7
        return testData.stream().map(new Function<BulkCopyTestWrapper, DynamicTest>() {
            @Override
            public DynamicTest apply(final BulkCopyTestWrapper datum) {
                return DynamicTest.dynamicTest("Testing " + datum.testName, new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        BulkCopyTestUtil.performBulkCopy(datum, sourceTable);
                    }
                });
            }
        });
    }

    /**
     * Generate dynamic tests to test with various SQLServerBulkCopyOptions
     * 
     * @return
     */
    @TestFactory
    Stream<DynamicTest> generateBulkCopyOptionsTest() {
        List<BulkCopyTestWrapper> testData = createTestDatatestBulkCopyOption();
        return testData.stream().map(new Function<BulkCopyTestWrapper, DynamicTest>() {
            @Override
            public DynamicTest apply(final BulkCopyTestWrapper datum) {
                return DynamicTest.dynamicTest("Testing " + datum.testName, new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        BulkCopyTestUtil.performBulkCopy(datum, sourceTable);
                    }
                });
            }
        });
    }

    /**
     * BulkCopy:test uninitialized Connection
     */
    @Test
    @DisplayName("BulkCopy:test uninitialized Connection")
    void testInvalidConnection1() {
        assertThrows(SQLException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLException {
                try(Connection con = null;
                	SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
        			//do nothing
                }
            }
        });
    }

    /**
     * BulkCopy:test uninitialized SQLServerConnection
     */
    @Test
    @DisplayName("BulkCopy:test uninitialized SQLServerConnection")
    void testInvalidConnection2() {
        assertThrows(SQLException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLException {
                try(SQLServerConnection con = null;
                	SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                	//do nothing
                }
            }
        });
    }

    /**
     * BulkCopy:test empty connenction string
     */
    @Test
    @DisplayName("BulkCopy:test empty connenction string")
    void testInvalidConnection3() {
        assertThrows(SQLException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLException {
                String connectionUrl = " ";
                try(SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connectionUrl)) {
                	//do nothing
                }
            }
        });
    }

    /**
     * BulkCopy:test null connenction string
     */
    @Test
    @DisplayName("BulkCopy:test null connenction string")
    void testInvalidConnection4() {
        assertThrows(SQLException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLException {
                String connectionUrl = null;
                try(SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connectionUrl)) {
                	//do nothing
                }
            }
        });
    }

    /**
     * BulkCopy:test null SQLServerBulkCopyOptions
     */
    @Test
    @DisplayName("BulkCopy:test null SQLServerBulkCopyOptions")
    void testEmptyBulkCopyOptions() {
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        SQLServerBulkCopyOptions option = null;
        bulkWrapper.useBulkCopyOptions(true);
        bulkWrapper.setBulkOptions(option);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable);
    }

    /**
     * Generate BulkCopyTestWrapper objects with data for testing BulkCopyConstructor
     * 
     * @return
     */
    List<BulkCopyTestWrapper> createTestDatatestBulkCopyConstructor() {
        String testCaseName = "BulkCopyConstructor ";
        List<BulkCopyTestWrapper> testData = new ArrayList<>();
        BulkCopyTestWrapper bulkWrapper1 = new BulkCopyTestWrapper(connectionString);
        bulkWrapper1.testName = testCaseName;
        bulkWrapper1.setUsingConnection(true);
        testData.add(bulkWrapper1);

        BulkCopyTestWrapper bulkWrapper2 = new BulkCopyTestWrapper(connectionString);
        bulkWrapper2.testName = testCaseName;
        bulkWrapper2.setUsingConnection(false);
        testData.add(bulkWrapper2);

        return testData;
    }

    /**
     * Generate BulkCopyTestWrapper objects with data for testing BulkCopyOption
     * 
     * @return
     */
    private List<BulkCopyTestWrapper> createTestDatatestBulkCopyOption() {
        String testCaseName = "BulkCopyOption ";
        List<BulkCopyTestWrapper> testData = new ArrayList<>();

        Class<SQLServerBulkCopyOptions> bulkOptions = SQLServerBulkCopyOptions.class;
        Method[] methods = bulkOptions.getDeclaredMethods();
        for (Method method : methods) {

            // set bulkCopy Option if return is void and input is boolean
            if (0 != method.getParameterTypes().length && boolean.class == method.getParameterTypes()[0]) {
                try {

                    BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                    bulkWrapper.testName = testCaseName;
                    bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);

                    SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
                    if (!(method.getName()).equalsIgnoreCase("setUseInternalTransaction")
                            && !(method.getName()).equalsIgnoreCase("setAllowEncryptedValueModifications")) {
                        method.invoke(option, true);
                        bulkWrapper.useBulkCopyOptions(true);
                        bulkWrapper.setBulkOptions(option);
                        bulkWrapper.testName += method.getName() + ";";
                        testData.add(bulkWrapper);
                    }
                } catch (Exception ex) {
                    fail(ex.getMessage());
                }
            }

        }
        return testData;
    }
}
