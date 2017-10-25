/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.bulkCopy.BulkCopyTestWrapper.ColumnMap;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.util.ComparisonUtil;

/**
 * Utility class
 */
class BulkCopyTestUtil {

    /**
     * perform bulk copy using source table and validate bulkcopy
     * 
     * @param wrapper
     * @param sourceTable
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable) {
        performBulkCopy(wrapper, sourceTable, true);
    }

    /**
     * perform bulk copy using source and destination tables and validate bulkcopy
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destTable) {
        performBulkCopy(wrapper, sourceTable, destTable, true);
    }

    /**
     * perform bulk copy using source table
     * 
     * @param wrapper
     * @param sourceTable
     * @param validateResult
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            boolean validateResult) {
        DBTable destinationTable = null;
        try (DBConnection con = new DBConnection(wrapper.getConnectionString());
        	 DBStatement stmt = con.createStatement()) {

            destinationTable = sourceTable.cloneSchema();
            stmt.createTable(destinationTable);
            try (DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
            	 SQLServerBulkCopy bulkCopy = wrapper.isUsingConnection() ? 
            			 new SQLServerBulkCopy((Connection) con.product()) : 
            				 new SQLServerBulkCopy(wrapper.getConnectionString())) {
	            if (wrapper.isUsingBulkCopyOptions()) {
	                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
	            }
	            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
	            if (wrapper.isUsingColumnMapping()) {
	                for (int i = 0; i < wrapper.cm.size(); i++) {
	                    ColumnMap currentMap = wrapper.cm.get(i);
	                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
	                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
	                    }
	                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
	                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
	                    }
	                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
	                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
	                    }
	                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
	                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
	                    }
	                }
	            }
	            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
	            if (validateResult) {
	                validateValues(con, sourceTable, destinationTable);
	            }
            }
            catch (SQLException ex) {
                fail(ex.getMessage());
            }
            finally {
                stmt.dropTable(destinationTable);
                con.close();
            }
        }
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * perform bulk copy using source and destination tables
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     * @param validateResult
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destinationTable,
            boolean validateResult) {
        try (DBConnection con = new DBConnection(wrapper.getConnectionString());
        	 DBStatement stmt = con.createStatement();
             DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
             SQLServerBulkCopy bulkCopy = wrapper.isUsingConnection() ? 
            		 new SQLServerBulkCopy((Connection) con.product()) : 
            			 new SQLServerBulkCopy(wrapper.getConnectionString())) {
            if (wrapper.isUsingBulkCopyOptions()) {
                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
            }
            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
            if (wrapper.isUsingColumnMapping()) {
                for (int i = 0; i < wrapper.cm.size(); i++) {
                    ColumnMap currentMap = wrapper.cm.get(i);
                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
                    }
                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
                    }
                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
                    }
                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
                    }
                }
            }
            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
            if (validateResult) {
                validateValues(con, sourceTable, destinationTable);
            }
    	}
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * perform bulk copy using source and destination tables
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     * @param validateResult
     * @param fail
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destinationTable,
            boolean validateResult,
            boolean fail) {
        try (DBConnection con = new DBConnection(wrapper.getConnectionString());
        	 DBStatement stmt = con.createStatement();
             DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
             SQLServerBulkCopy bulkCopy = wrapper.isUsingConnection() ?
            		 new SQLServerBulkCopy((Connection) con.product()) :
            			 new SQLServerBulkCopy(wrapper.getConnectionString())) {
            try {
            	if (wrapper.isUsingBulkCopyOptions()) {
	                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
	            }
	            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
	            if (wrapper.isUsingColumnMapping()) {
	                for (int i = 0; i < wrapper.cm.size(); i++) {
	                    ColumnMap currentMap = wrapper.cm.get(i);
	                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
	                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
	                    }
	                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
	                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
	                    }
	                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
	                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
	                    }
	                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
	                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
	                    }
	                }
	            }
	            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
	            if (fail)
	                fail("bulkCopy.writeToServer did not fail when it should have");
	            if (validateResult) {
	                validateValues(con, sourceTable, destinationTable);
	            }
	        } catch (SQLException ex) {
	            if (!fail) {
	                fail(ex.getMessage());
	            }
	        }
	        finally {
	            stmt.dropTable(destinationTable);
	            con.close();
	        }
        } catch (SQLException e) {
            if (!fail) {
                fail(e.getMessage());
            }
		}
    }

    /**
     * perform bulk copy using source and destination tables
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     * @param validateResult
     * @param fail
     * @param dropDest
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destinationTable,
            boolean validateResult,
            boolean fail,
            boolean dropDest) {
        try (DBConnection con = new DBConnection(wrapper.getConnectionString());
        	 DBStatement stmt = con.createStatement();
             DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
             SQLServerBulkCopy bulkCopy = wrapper.isUsingConnection() ?
            		new SQLServerBulkCopy((Connection) con.product()) :
            			new SQLServerBulkCopy(wrapper.getConnectionString())) {
            try {
            	if (wrapper.isUsingBulkCopyOptions()) {
	                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
	            }
	            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
	            if (wrapper.isUsingColumnMapping()) {
	                for (int i = 0; i < wrapper.cm.size(); i++) {
	                    ColumnMap currentMap = wrapper.cm.get(i);
	                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
	                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
	                    }
	                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
	                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
	                    }
	                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
	                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
	                    }
	                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
	                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
	                    }
	                }
	            }
	            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
	            if (fail)
	                fail("bulkCopy.writeToServer did not fail when it should have");
	            bulkCopy.close();
	            if (validateResult) {
	                validateValues(con, sourceTable, destinationTable);
	            }
	        }
	        catch (SQLException ex) {
	            if (!fail) {
	                fail(ex.getMessage());
	            }
	        }
	        finally {
	            if (dropDest) {
	                stmt.dropTable(destinationTable);
	            }
	            con.close();
	        }
        }
        catch (SQLException ex) {
            if (!fail) {
                fail(ex.getMessage());
            }
        }
    }

    /**
     * validate if same values are in both source and destination table
     * 
     * @param con
     * @param sourceTable
     * @param destinationTable
     * @throws SQLException
     */
    static void validateValues(DBConnection con,
            DBTable sourceTable,
            DBTable destinationTable) throws SQLException {
        try (DBStatement srcStmt = con.createStatement();
        	 DBStatement dstStmt = con.createStatement();
        	 DBResultSet srcResultSet = srcStmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
        	 DBResultSet dstResultSet = dstStmt.executeQuery("SELECT * FROM " + destinationTable.getEscapedTableName() + ";")) {
	        ResultSetMetaData destMeta = ((ResultSet) dstResultSet.product()).getMetaData();
	        int totalColumns = destMeta.getColumnCount();
	
	        // verify data from sourceType and resultSet
	        while (srcResultSet.next() && dstResultSet.next()) {
	            for (int i = 1; i <= totalColumns; i++) {
	                // TODO: check row and column count in both the tables
	
	                Object srcValue, dstValue;
	                srcValue = srcResultSet.getObject(i);
	                dstValue = dstResultSet.getObject(i);
	
	                ComparisonUtil.compareExpectedAndActual(destMeta.getColumnType(i), srcValue, dstValue);
	            }
	        }
        }
    }

    /**
     * 
     * @param bulkWrapper
     * @param srcData
     * @param dstTable
     */
    static void performBulkCopy(BulkCopyTestWrapper bulkWrapper,
            ISQLServerBulkRecord srcData,
            DBTable dstTable) {
        try (DBConnection con = new DBConnection(bulkWrapper.getConnectionString());
        	 DBStatement stmt = con.createStatement();
        	 SQLServerBulkCopy bc = new SQLServerBulkCopy(bulkWrapper.getConnectionString());) {
            bc.setDestinationTableName(dstTable.getEscapedTableName());
            bc.writeToServer(srcData);
            validateValues(con, srcData, dstTable);
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    /**
     * 
     * @param con
     * @param srcData
     * @param destinationTable
     * @throws Exception
     */
    static void validateValues(
            DBConnection con,
            ISQLServerBulkRecord srcData,
            DBTable destinationTable) throws Exception {
        
        try (DBStatement dstStmt = con.createStatement();
        	 DBResultSet dstResultSet = dstStmt.executeQuery("SELECT * FROM " + destinationTable.getEscapedTableName() + ";")) {
	        ResultSetMetaData destMeta = ((ResultSet) dstResultSet.product()).getMetaData();
	        int totalColumns = destMeta.getColumnCount();
	        
	        // reset the counter in ISQLServerBulkRecord, which was incremented during read by BulkCopy 
	        java.lang.reflect.Method method  = srcData.getClass().getMethod("reset");
	        method.invoke(srcData);
	        
	        // verify data from sourceType and resultSet
	        while (srcData.next() && dstResultSet.next())
	        {
	            Object[] srcValues = srcData.getRowData();
	            for (int i = 1; i <= totalColumns; i++) {
	
	                Object srcValue, dstValue;
	                srcValue = srcValues[i-1];
	                if(srcValue.getClass().getName().equalsIgnoreCase("java.lang.Double")){
	                    // in case of SQL Server type Float (ie java type double), in float(n) if n is <=24 ie precsion is <=7 SQL Server type Real is returned(ie java type float)
	                    if(destMeta.getPrecision(i) <8)
	                        srcValue = new Float(((Double)srcValue));
	                }
	                dstValue = dstResultSet.getObject(i);
	                int dstType = destMeta.getColumnType(i);
	                if(java.sql.Types.TIMESTAMP != dstType
	                        && java.sql.Types.TIME != dstType
	                        && microsoft.sql.Types.DATETIMEOFFSET != dstType){
	                    // skip validation for temporal types due to rounding eg 7986-10-21 09:51:15.114 is rounded as 7986-10-21 09:51:15.113 in server
	                    ComparisonUtil.compareExpectedAndActual(dstType, srcValue, dstValue);
	                }
	            }
	        }
        }
    }
}