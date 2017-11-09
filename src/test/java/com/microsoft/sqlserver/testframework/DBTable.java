/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Hex;

import com.microsoft.sqlserver.testframework.sqlType.SqlType;
import com.microsoft.sqlserver.testframework.sqlType.VariableLengthType;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

/**
 * This class holds data for Table.
 */
public class DBTable extends AbstractSQLGenerator {

    public static final Logger log = Logger.getLogger("DBTable");
    String tableName;
    String escapedTableName;
    String tableDefinition;
    List<DBColumn> columns;
    int totalColumns;
    int totalRows = 3; // default row count set to 3
    DBSchema schema;

    /**
     * Initializes {@link DBTable} with tableName, schema, and {@link DBColumns}
     * 
     * @param autoGenerateSchema
     *            <code>true</code> : generates schema with all available dataTypes in SqlType class
     */
    public DBTable(boolean autoGenerateSchema) {
        this(autoGenerateSchema, false, false);
    }

    /**
     * Initializes {@link DBTable} with tableName, schema, and {@link DBColumns}
     * 
     * @param autoGenerateSchema
     *            <code>true</code>: generates schema with all available dataTypes in SqlType class
     * @param unicode
     *            <code>true</code>: sets unicode column names if autoGenerateSchema is also set to <code>true</code>
     */
    public DBTable(boolean autoGenerateSchema,
            boolean unicode) {
        this(autoGenerateSchema, unicode, false);
    }

    /**
     * Initializes {@link DBTable} with tableName, schema, and {@link DBColumns}
     * 
     * @param autoGenerateSchema
     *            <code>true</code>: generates schema with all available dataTypes in SqlType class
     * @param unicode
     *            <code>true</code>: sets unicode column names if autoGenerateSchema is also set to <code>true</code>
     * @param alternateShcema
     *            <code>true</code>: creates table with alternate schema
     */
    public DBTable(boolean autoGenerateSchema,
            boolean unicode,
            boolean alternateSchema) {

        this.tableName = RandomUtil.getIdentifier("table");
        this.escapedTableName = escapeIdentifier(tableName);
        this.schema = new DBSchema(autoGenerateSchema, alternateSchema);
        if (autoGenerateSchema) {
            if (unicode)
                addColumns(unicode);
            else
                addColumns();
        }
        else {
            this.columns = new ArrayList<>();
        }
        this.totalColumns = columns.size();
    }

    /**
     * Similar to {@link DBTable#DBTable(boolean)}, but uses existing list of columns Used internally to clone schema
     * 
     * @param DBTable
     */
    private DBTable(DBTable sourceTable) {
        this.tableName = RandomUtil.getIdentifier("table");
        this.escapedTableName = escapeIdentifier(tableName);
        this.columns = sourceTable.columns;
        this.totalColumns = columns.size();
        this.schema = sourceTable.schema;
    }

    /**
     * adds a columns for each SQL type in DBSchema
     */
    private void addColumns() {
        totalColumns = schema.getNumberOfSqlTypes();
        columns = new ArrayList<>(totalColumns);

        for (int i = 0; i < totalColumns; i++) {
            SqlType sqlType = schema.getSqlType(i);
            DBColumn column = new DBColumn(RandomUtil.getIdentifier(sqlType.getName()), sqlType);
            columns.add(column);
        }
    }

    /**
     * adds a columns for each SQL type in DBSchema
     */
    private void addColumns(boolean unicode) {
        totalColumns = schema.getNumberOfSqlTypes();
        columns = new ArrayList<>(totalColumns);

        for (int i = 0; i < totalColumns; i++) {
            SqlType sqlType = schema.getSqlType(i);
            DBColumn column;
            if (unicode)
                column = new DBColumn(RandomUtil.getIdentifier(sqlType.getName()) + "ĀĂŎՖએДЕЖЗИЙਟਖਞ", sqlType);
            else
                column = new DBColumn(RandomUtil.getIdentifier(sqlType.getName()), sqlType);
            columns.add(column);
        }
    }

    /**
     * gets table name of the {@link DBTable} object
     * 
     * @return {@link String} table name
     */
    public String getTableName() {
        return tableName;
    }

    public List<DBColumn> getColumns() {
        return this.columns;
    }

    /**
     * gets escaped table name of the {@link DBTable} object
     * 
     * @return {@link String} escaped table name
     */
    public String getEscapedTableName() {
        return escapedTableName;
    }

    public String getDefinitionOfColumns() {
        return tableDefinition;
    }

    /**
     * 
     * @return total rows in the table
     */
    public int getTotalRows() {
        return totalRows;
    }

    /**
     * 
     * @param totalRows
     *            set the number of rows in table, default value is 3
     */
    public void setTotalRows(int totalRows) {
        this.totalRows = totalRows;
    }

    /**
     * create table
     * 
     * @param dbstatement
     */
    boolean createTable(DBStatement dbstatement) {
        try {
            dropTable(dbstatement);
            String sql = createTableSql();
            return dbstatement.execute(sql);
        }
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
        return false;
    }

    String createTableSql() {
        StringJoiner sb = new StringJoiner(SPACE_CHAR);

        sb.add(CREATE_TABLE);
        sb.add(escapedTableName);
        sb.add(OPEN_BRACKET);

        StringJoiner sbDefinition = new StringJoiner(SPACE_CHAR);
        for (int i = 0; i < totalColumns; i++) {
            DBColumn column = getColumn(i);
            sbDefinition.add(escapeIdentifier(column.getColumnName()));
            sbDefinition.add(column.getSqlType().getName());
            // add precision and scale
            if (VariableLengthType.Precision == column.getSqlType().getVariableLengthType()) {
                sbDefinition.add(OPEN_BRACKET);
                sbDefinition.add("" + column.getSqlType().getPrecision());
                sbDefinition.add(CLOSE_BRACKET);
            }
            else if (VariableLengthType.Scale == column.getSqlType().getVariableLengthType()) {
                sbDefinition.add(OPEN_BRACKET);
                sbDefinition.add("" + column.getSqlType().getPrecision());
                sbDefinition.add(COMMA);
                sbDefinition.add("" + column.getSqlType().getScale());
                sbDefinition.add(CLOSE_BRACKET);
            }
            else if (VariableLengthType.ScaleOnly == column.getSqlType().getVariableLengthType()) {
                sbDefinition.add(OPEN_BRACKET);
                sbDefinition.add("" + column.getSqlType().getScale());
                sbDefinition.add(CLOSE_BRACKET);
            }
            sbDefinition.add(COMMA);
        }
        tableDefinition = sbDefinition.toString();

        // Remove the last comma
        int indexOfLastComma = tableDefinition.lastIndexOf(",");
        tableDefinition = tableDefinition.substring(0, indexOfLastComma);

        sb.add(tableDefinition);

        sb.add(CLOSE_BRACKET);
        return sb.toString();
    }

    /**
     * populate table with values
     * 
     * @param dbstatement
     * @return
     */
    boolean populateTable(DBStatement dbstatement) {
        try {
            populateValues();
            String sql = populateTableSql();
            return dbstatement.execute(sql);
        }
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
        return false;
    }

    /**
     * using prepared statement to populate table with values
     * 
     * @param dbstatement
     * @return
     */
    boolean populateTableWithPreparedStatement(DBPreparedStatement dbPStmt) {
        try {
            populateValues();

            // create the insertion query
            StringJoiner sb = new StringJoiner(SPACE_CHAR);
            sb.add("INSERT");
            sb.add("INTO");
            sb.add(escapedTableName);
            sb.add("VALUES");
            sb.add(OPEN_BRACKET);
            for (int colNum = 0; colNum < totalColumns; colNum++) {
                sb.add(QUESTION_MARK);

                if (colNum < totalColumns - 1) {
                    sb.add(COMMA);
                }
            }
            sb.add(CLOSE_BRACKET);
            String sql = sb.toString();

            dbPStmt.prepareStatement(sql);

            // insert data
            for (int i = 0; i < totalRows; i++) {
                for (int colNum = 0; colNum < totalColumns; colNum++) {
                    if (passDataAsHex(colNum)) {
                        ((PreparedStatement) dbPStmt.product()).setBytes(colNum + 1, ((byte[]) (getColumn(colNum).getRowValue(i))));
                    }
                    else {
                        dbPStmt.setObject(colNum + 1, String.valueOf(getColumn(colNum).getRowValue(i)));
                    }
                }
                dbPStmt.execute();
            }

            return true;
        }
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
        return false;
    }

    private void populateValues() {
        // generate values for all columns
        for (int i = 0; i < totalColumns; i++) {
            DBColumn column = getColumn(i);
            column.populateValues(totalRows);
        }
    }

    public SqlType getSqlType(int columnIndex) {
        return getColumn(columnIndex).getSqlType();
    }

    public String getColumnName(int columnIndex) {
        return getColumn(columnIndex).getColumnName();
    }

    public int totalColumns() {
        return totalColumns;
    }

    /**
     * 
     * @return new DBTable object with same schema
     */
    public DBTable cloneSchema() {

        DBTable clonedTable = new DBTable(this);
        return clonedTable;
    }

    /**
     * 
     * @return query to create table
     */
    String populateTableSql() {
        StringJoiner sb = new StringJoiner(SPACE_CHAR);

        sb.add("INSERT");
        sb.add("INTO");
        sb.add(escapedTableName);
        sb.add("VALUES");

        for (int i = 0; i < totalRows; i++) {
            if (i != 0) {
                sb.add(COMMA);
            }
            sb.add(OPEN_BRACKET);
            for (int colNum = 0; colNum < totalColumns; colNum++) {

                // TODO: consider how to enclose data in case of preparedStatemets
                if (passDataAsString(colNum)) {
                    sb.add("'" + String.valueOf(getColumn(colNum).getRowValue(i)) + "'");
                }
                else if (passDataAsHex(colNum)) {
                    sb.add("0X" + Hex.encodeHexString((byte[]) (getColumn(colNum).getRowValue(i))));
                }
                else {
                    sb.add(String.valueOf(getColumn(colNum).getRowValue(i)));
                }

                if (colNum < totalColumns - 1) {
                    sb.add(COMMA);
                }
            }
            sb.add(CLOSE_BRACKET);
        }

        return (sb.toString());
    }

    /**
     * Drop table from Database
     * 
     * @param dbstatement
     * @return true if table dropped
     */
    boolean dropTable(DBStatement dbstatement) {
        boolean result = false;
        try {
            String sql = dropTableSql();
            result = dbstatement.execute(sql);
            if (log.isLoggable(Level.FINE)) {
                log.fine("Table Deleted " + tableName);
            }
            else {
                log.fine("Table did not exist : " + tableName);
            }
        }
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
        return result;
    }

    /**
     * This will give you query for Drop Table.
     */
    String dropTableSql() {
        StringJoiner sb = new StringJoiner(SPACE_CHAR);
        sb.add("IF OBJECT_ID");
        sb.add(OPEN_BRACKET);
        sb.add(wrapName(tableName));
        sb.add(",");
        sb.add(wrapName("U"));
        sb.add(CLOSE_BRACKET);
        sb.add("IS NOT NULL");
        sb.add("DROP TABLE");
        sb.add(escapedTableName); // for drop table no need to wrap.
        return sb.toString();
    }

    /**
     * new column to add to DBTable based on the SqlType
     * 
     * @param sqlType
     */
    public void addColumn(SqlType sqlType) {
        schema.addSqlTpe(sqlType);
        DBColumn column = new DBColumn(RandomUtil.getIdentifier(sqlType.getName()), sqlType);
        columns.add(column);
        ++totalColumns;
    }

    /**
     * 
     * @param index
     * @return DBColumn
     */
    DBColumn getColumn(int index) {
        return columns.get(index);
    }

    /**
     * 
     * @param colIndex
     * @param rowIndex
     * @return
     */
    public Object getRowData(int colIndex,
            int rowIndex) {
        return columns.get(colIndex).getRowValue(rowIndex);
    }

    /**
     * 
     * @param colNum
     * @return <code>true</code> if value can be passed as String for the column
     */
    boolean passDataAsString(int colNum) {
        return (JDBCType.CHAR == getColumn(colNum).getJdbctype() || JDBCType.VARCHAR == getColumn(colNum).getJdbctype()
                || JDBCType.NCHAR == getColumn(colNum).getJdbctype() || JDBCType.NVARCHAR == getColumn(colNum).getJdbctype()
                || JDBCType.TIMESTAMP == getColumn(colNum).getJdbctype() || JDBCType.DATE == getColumn(colNum).getJdbctype()
                || JDBCType.TIME == getColumn(colNum).getJdbctype() || JDBCType.LONGVARCHAR == getColumn(colNum).getJdbctype()
                || JDBCType.LONGNVARCHAR == getColumn(colNum).getJdbctype());
    }

    /**
     * 
     * @param colNum
     * @return <code>true</code> if value can be passed as Hex for the column
     */

    boolean passDataAsHex(int colNum) {
        return (JDBCType.BINARY == getColumn(colNum).getJdbctype() || JDBCType.VARBINARY == getColumn(colNum).getJdbctype()
                || JDBCType.LONGVARBINARY == getColumn(colNum).getJdbctype());
    }
}