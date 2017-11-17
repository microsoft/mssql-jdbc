/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;

/**
 * Test bulkcopy decimal sacle and precision
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test ISQLServerBulkRecord")
public class BulkCopyISQLServerBulkRecordTest extends AbstractTest {

    @Test
    void testISQLServerBulkRecord() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString);
        	 DBStatement stmt = con.createStatement()) {
        	DBTable dstTable = new DBTable(true);
	        stmt.createTable(dstTable);
	        BulkData Bdata = new BulkData(dstTable);
	
	        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
	        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
	        BulkCopyTestUtil.performBulkCopy(bulkWrapper, Bdata, dstTable);
        }
    }

    class BulkData implements ISQLServerBulkRecord {

        private class ColumnMetadata {
            String columnName;
            int columnType;
            int precision;
            int scale;

            ColumnMetadata(String name,
                    int type,
                    int precision,
                    int scale) {
                columnName = name;
                columnType = type;
                this.precision = precision;
                this.scale = scale;
            }
        }

        int totalColumn = 0;
        int counter = 0;
        int rowCount = 1;
        Map<Integer, ColumnMetadata> columnMetadata;
        List<Object[]> data;

        BulkData(DBTable dstTable) {
            columnMetadata = new HashMap<>();
            totalColumn = dstTable.totalColumns();

            // add metadata
            for (int i = 0; i < totalColumn; i++) {
                SqlType sqlType = dstTable.getSqlType(i);
                int precision = sqlType.getPrecision();
                if (JDBCType.TIMESTAMP == sqlType.getJdbctype()) {
                    // TODO: update the test to use correct precision once bulkCopy is fixed
                    precision = 50;
                }
                columnMetadata.put(i + 1,
                        new ColumnMetadata(sqlType.getName(), sqlType.getJdbctype().getVendorTypeNumber(), precision, sqlType.getScale()));
            }

            // add data
            rowCount = dstTable.getTotalRows();
            data = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                Object[] CurrentRow = new Object[totalColumn];
                for (int j = 0; j < totalColumn; j++) {
                    SqlType sqlType = dstTable.getSqlType(j);
                    if (JDBCType.BIT == sqlType.getJdbctype()) {
                        CurrentRow[j] = ((0 == ThreadLocalRandom.current().nextInt(2)) ? Boolean.FALSE : Boolean.TRUE);
                    }
                    else
                    {
                        CurrentRow[j] = sqlType.createdata();
                    }
                }
                data.add(CurrentRow);
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getColumnOrdinals()
         */
        @Override
        public Set<Integer> getColumnOrdinals() {
            return columnMetadata.keySet();
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getColumnName(int)
         */
        @Override
        public String getColumnName(int column) {
            return columnMetadata.get(column).columnName;
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getColumnType(int)
         */
        @Override
        public int getColumnType(int column) {
            return columnMetadata.get(column).columnType;
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getPrecision(int)
         */
        @Override
        public int getPrecision(int column) {
            return columnMetadata.get(column).precision;
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getScale(int)
         */
        @Override
        public int getScale(int column) {
            return columnMetadata.get(column).scale;
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#isAutoIncrement(int)
         */
        @Override
        public boolean isAutoIncrement(int column) {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getRowData()
         */
        @Override
        public Object[] getRowData() throws SQLServerException {
            return data.get(counter++);
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#next()
         */
        @Override
        public boolean next() throws SQLServerException {
            if (counter < rowCount)
                return true;
            return false;
        }

        /**
         * reset the counter when using the interface for validating the data
         */
        public void reset() {
            counter = 0;
        }
    }
}
