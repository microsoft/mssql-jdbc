// ---------------------------------------------------------------------------------------------------------------------------------
// File: BulkCopyTestConnection.java
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.sql.Connection;
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
import com.microsoft.sqlserver.jdbc.SQLServerException;

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
        List<BulkCopyTestWrapper> testData = createTestData_testBulkCopyConstructor();
        // had to avoid using lambdas as we need to test against java7
        return testData.stream().map(new Function<BulkCopyTestWrapper, DynamicTest>() {
            @Override
            public DynamicTest apply(final BulkCopyTestWrapper datum) {
                return DynamicTest.dynamicTest("Testing " + datum.testName, new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        BulkCopyTestUtil.performBulkCopy(datum,sourceTable);
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
        List<BulkCopyTestWrapper> testData = createTestData_testBulkCopyOption();
        return testData.stream().map(new Function<BulkCopyTestWrapper, DynamicTest>() {
            @Override
            public DynamicTest apply(final BulkCopyTestWrapper datum) {
                return DynamicTest.dynamicTest("Testing " + datum.testName, new org.junit.jupiter.api.function.Executable() {
                    @Override
                    public void execute() {
                        BulkCopyTestUtil.performBulkCopy(datum,sourceTable);
                    }
                });
            }
        });
    }

    @Test
    @DisplayName("BulkCopy:test uninitialized Connection")
    void testInvalidConnection_1() {
        assertThrows(SQLServerException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLServerException {
                Connection con = null;
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
            }
        });
    }

    @Test
    @DisplayName("BulkCopy:test uninitialized SQLServerConnection")
    void testInvalidConnection_2() {
        assertThrows(SQLServerException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLServerException {
                SQLServerConnection con = null;
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
            }
        });
    }

    @Test
    @DisplayName("BulkCopy:test empty connenction string")
    void testInvalidConnection_3() {
        assertThrows(SQLServerException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLServerException {
                String connectionUrl = " ";
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connectionUrl);
            }
        });
    }

    @Test
    @DisplayName("BulkCopy:test null connenction string")
    void testInvalidConnection_4() {
        assertThrows(SQLServerException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLServerException {
                String connectionUrl = null;
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connectionUrl);
            }
        });
    }

    @Test
    @DisplayName("BulkCopy:test null SQLServerBulkCopyOptions")
    void testEmptyBulkCopyOptions() {
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        SQLServerBulkCopyOptions option = null;
        bulkWrapper.useBulkCopyOptions(true);
        bulkWrapper.setBulkOptions(option);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper,sourceTable);
    }
   
    List<BulkCopyTestWrapper> createTestData_testBulkCopyConstructor() {
        String testCaseName = "BulkCopyConstructor ";
        List<BulkCopyTestWrapper> testData = new ArrayList<BulkCopyTestWrapper>();
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

    private List<BulkCopyTestWrapper> createTestData_testBulkCopyOption() {
        String testCaseName = "BulkCopyOption ";
        List<BulkCopyTestWrapper> testData = new ArrayList<BulkCopyTestWrapper>();

        Class<SQLServerBulkCopyOptions> bulkOptions = SQLServerBulkCopyOptions.class;
        Method[] methods = bulkOptions.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {

            // set bulkCopy Option if return is void and input is boolean
            if (0 != methods[i].getParameterTypes().length && boolean.class == methods[i].getParameterTypes()[0]) {
                try {

                    BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                    bulkWrapper.testName = testCaseName;
                    bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);

                    SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
                    if (!(methods[i].getName()).equalsIgnoreCase("setUseInternalTransaction")
                            && !(methods[i].getName()).equalsIgnoreCase("setAllowEncryptedValueModifications")) {
                        methods[i].invoke(option, true);
                        bulkWrapper.useBulkCopyOptions(true);
                        bulkWrapper.setBulkOptions(option);
                        bulkWrapper.testName += methods[i].getName() + ";";
                        testData.add(bulkWrapper);
                    }
                }
                catch (Exception ex) {
                    fail(ex.getMessage());
                }
            }

        }
        return testData;
    }
}
