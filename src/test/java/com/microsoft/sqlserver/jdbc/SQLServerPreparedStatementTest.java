/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class SQLServerPreparedStatementTest extends AbstractTest {

    /**
     * This test case helps track the insertion process and ensures that encrypted columns can handle string values of varying sizes correctly.
     * @throws Exception
     */
    @Test
    public void testBuildPreparedStringsForVaryLength() throws Exception {
        // Create a mock of SQLServerConnection
        SQLServerConnection mockConnection = mock(SQLServerConnection.class);

        // Define the behavior of isAEv2()
        when(mockConnection.isAEv2()).thenReturn(true);

        // Call the method and assert the result
        boolean result = mockConnection.isAEv2();
        assertTrue(result, "isAEv2() should return true");

        // Create a mock or real instance of SQLServerPreparedStatement
        SQLServerPreparedStatement preparedStatement = mock(SQLServerPreparedStatement.class);

        // Use reflection to set the private 'connection' field in SQLServerPreparedStatement
        Field connectionField = SQLServerStatement.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(preparedStatement, mockConnection);

        // Use reflection to access the private method
        Method buildPreparedStringsMethod = SQLServerPreparedStatement.class.getDeclaredMethod(
                "buildPreparedStrings", Parameter[].class, boolean.class, boolean.class);
        buildPreparedStringsMethod.setAccessible(true);

        // Prepare the required parameters
        Parameter[] params = new Parameter[1];
        params[0] = mock(Parameter.class); // Mock a Parameter object
        
        // Mock behavior for dependent methods (if needed)
        when(params[0].getTypeDefinition(any(), any())).thenReturn("varchar(1)");

        boolean renewDefinition = false;
        boolean isInternalEncryptionQuery = false;
        // First invocation buildPreparedStringsMethod() method which will set the preparedTypeDefinitionsCache field
        result = (boolean) buildPreparedStringsMethod.invoke(preparedStatement, params, renewDefinition, isInternalEncryptionQuery);
        // Use reflection to access the private field 'preparedTypeDefinitionsCache'
        Field cacheFieldPreparedTypeDefinitionsCache = SQLServerPreparedStatement.class.getDeclaredField("preparedTypeDefinitionsCache");
        cacheFieldPreparedTypeDefinitionsCache.setAccessible(true);
        // Validate that the field is now non-null
        assertNotNull(cacheFieldPreparedTypeDefinitionsCache.get(preparedStatement), "The preparedTypeDefinitionsCache should not be null.");

        // Scond invocation buildPreparedStringsMethod() method which will reset the preparedTypeDefinitions with preparedTypeDefinitionsCache value and return true   
        renewDefinition = true;
        isInternalEncryptionQuery = false;
        when(params[0].getTypeDefinition(any(), any())).thenReturn("varchar(3)");
        // Invoke the private method
        result = (boolean) buildPreparedStringsMethod.invoke(preparedStatement, params, renewDefinition, isInternalEncryptionQuery);
        // Validate that the field is now null
        assertTrue(result, "buildPreparedStrings() should return true");
    }
    
}
