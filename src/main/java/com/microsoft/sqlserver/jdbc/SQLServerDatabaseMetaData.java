/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.text.MessageFormat;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;


/**
 * Provides the JDBC database meta data.
 *
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API
 * interfaces javadoc for those details.
 */
@SuppressWarnings("unused")
public final class SQLServerDatabaseMetaData implements java.sql.DatabaseMetaData, Serializable {
    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = -116977606028371577L;

    /** connection */
    private SQLServerConnection connection;

    static final String urlprefix = "jdbc:sqlserver://";

    static final private java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDatabaseMetaData");

    static final private java.util.logging.Logger loggerExternal = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.DatabaseMetaData");

    /**
     * Unique id generator for each instance (used for logging).
     */
    static private final AtomicInteger baseID = new AtomicInteger(0);

    /** trace ID */
    final private String traceID;

    // varbinary(max) https://msdn.microsoft.com/en-us/library/ms143432.aspx
    static final int MAXLOBSIZE = 2147483647;
    // uniqueidentifier https://msdn.microsoft.com/en-us/library/ms187942.aspx
    static final int uniqueidentifierSize = 36;

    enum CallableHandles {
        SP_COLUMNS("{ call sp_columns(?, ?, ?, ?, ?) }", "{ call sp_columns_100(?, ?, ?, ?, ?, ?) }"),
        SP_COLUMN_PRIVILEGES("{ call sp_column_privileges(?, ?, ?, ?)}", "{ call sp_column_privileges(?, ?, ?, ?)}"),
        SP_TABLES("{ call sp_tables(?, ?, ?, ?) }", "{ call sp_tables(?, ?, ?, ?) }"),
        SP_SPECIAL_COLUMNS("{ call sp_special_columns (?, ?, ?, ?, ?, ?, ?)}", "{ call sp_special_columns_100 (?, ?, ?, ?, ?, ?, ?)}"),
        SP_FKEYS("{ call sp_fkeys (?, ?, ?, ? , ? ,?)}", "{ call sp_fkeys (?, ?, ?, ? , ? ,?)}"),
        SP_STATISTICS("{ call sp_statistics(?,?,?,?,?, ?) }", "{ call sp_statistics_100(?,?,?,?,?, ?) }"),
        SP_SPROC_COLUMNS("{ call sp_sproc_columns(?, ?, ?,?,?) }", "{ call sp_sproc_columns_100(?, ?, ?,?,?) }"),
        SP_STORED_PROCEDURES("{call sp_stored_procedures(?, ?, ?) }", "{call sp_stored_procedures(?, ?, ?) }"),
        SP_TABLE_PRIVILEGES("{call sp_table_privileges(?,?,?) }", "{call sp_table_privileges(?,?,?) }"),
        SP_PKEYS("{ call sp_pkeys (?, ?, ?)}", "{ call sp_pkeys (?, ?, ?)}");

        // stored procs before Katmai ie SS10
        private final String preKatProc;
        // procs on or after katmai
        private final String katProc;

        private CallableHandles(String name, String katName) {
            this.preKatProc = name;
            this.katProc = katName;
        }

        CallableStatement prepare(SQLServerConnection conn) throws SQLServerException {
            return conn.prepareCall(conn.isKatmaiOrLater() ? katProc : preKatProc);
        }
    }

    final class HandleAssociation {
        Map<String, CallableStatement> statementMap;
        boolean nullCatalog = false;
        CallableStatement stmt;

        HandleAssociation() {
            if (null == statementMap) {
                statementMap = new HashMap<>();
            }
        }

        final void addToMap(String databaseName, CallableStatement stmt) {
            if (null != databaseName) {
                nullCatalog = false;
                statementMap.put(databaseName, stmt);
            } else {
                nullCatalog = true;
                this.stmt = stmt;
            }
        }

        final CallableStatement getMappedStatement(String databaseName) {
            if (null != databaseName) {
                if (null != statementMap && statementMap.containsKey(databaseName)) {
                    return statementMap.get(databaseName);
                }
                return null;
            } else {
                return stmt;
            }
        }
    }

    /** handle map */
    EnumMap<CallableHandles, HandleAssociation> handleMap = new EnumMap<>(CallableHandles.class);

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    /**
     * Provides a helper function to provide an ID string suitable for tracing.
     * 
     * @return traceID string
     */
    final public String toString() {
        return traceID;
    }

    /**
     * Constructs a SQLServerDatabaseMetaData database meta data
     * 
     * @param con
     *        the connection
     */
    public SQLServerDatabaseMetaData(SQLServerConnection con) {
        traceID = " SQLServerDatabaseMetaData:" + nextInstanceID();
        connection = con;
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine(toString() + " created by (" + connection.toString() + ")");
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        boolean f = iface.isInstance(this);
        return f;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        T t;
        try {
            t = iface.cast(this);
        } catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        return t;
    }

    private void checkClosed() throws SQLServerException {
        if (connection.isClosed()) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_connectionIsClosed"),
                    SQLServerException.EXCEPTION_XOPEN_CONNECTION_DOES_NOT_EXIST, false);
        }
    }

    private static final String ASC_OR_DESC = "ASC_OR_DESC";
    private static final String ATTR_NAME = "ATTR_NAME";
    private static final String ATTR_TYPE_NAME = "ATTR_TYPE_NAME";
    private static final String ATTR_SIZE = "ATTR_SIZE";
    private static final String ATTR_DEF = "ATTR_DEF";
    private static final String BASE_TYPE = "BASE_TYPE";
    private static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    private static final String CARDINALITY = "CARDINALITY";
    private static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    private static final String CLASS_NAME = "CLASS_NAME";
    private static final String COLUMN_DEF = "COLUMN_DEF";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String COLUMN_SIZE = "COLUMN_SIZE";
    private static final String COLUMN_TYPE = "COLUMN_TYPE";
    private static final String DATA_TYPE = "DATA_TYPE";
    private static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    private static final String DEFERRABILITY = "DEFERRABILITY";
    private static final String DELETE_RULE = "DELETE_RULE";
    private static final String FILTER_CONDITION = "FILTER_CONDITION";
    private static final String FK_NAME = "FK_NAME";
    private static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";
    private static final String FKTABLE_CAT = "FKTABLE_CAT";
    private static final String FKTABLE_NAME = "FKTABLE_NAME";
    private static final String FKTABLE_SCHEM = "FKTABLE_SCHEM";
    private static final String GRANTEE = "GRANTEE";
    private static final String GRANTOR = "GRANTOR";
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String INDEX_QUALIFIER = "INDEX_QUALIFIER";
    private static final String IS_GRANTABLE = "IS_GRANTABLE";
    private static final String IS_NULLABLE = "IS_NULLABLE";
    private static final String KEY_SEQ = "KEY_SEQ";
    private static final String LENGTH = "LENGTH";
    private static final String NON_UNIQUE = "NON_UNIQUE";
    private static final String NULLABLE = "NULLABLE";
    private static final String NUM_INPUT_PARAMS = "NUM_INPUT_PARAMS";
    private static final String NUM_OUTPUT_PARAMS = "NUM_OUTPUT_PARAMS";
    private static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
    private static final String NUM_RESULT_SETS = "NUM_RESULT_SETS";
    private static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    private static final String PAGES = "PAGES";
    private static final String PK_NAME = "PK_NAME";
    private static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
    private static final String PKTABLE_CAT = "PKTABLE_CAT";
    private static final String PKTABLE_NAME = "PKTABLE_NAME";
    private static final String PKTABLE_SCHEM = "PKTABLE_SCHEM";
    private static final String PRECISION = "PRECISION";
    private static final String PRIVILEGE = "PRIVILEGE";
    private static final String PROCEDURE_CAT = "PROCEDURE_CAT";
    private static final String PROCEDURE_NAME = "PROCEDURE_NAME";
    private static final String PROCEDURE_SCHEM = "PROCEDURE_SCHEM";
    private static final String PROCEDURE_TYPE = "PROCEDURE_TYPE";
    private static final String PSEUDO_COLUMN = "PSEUDO_COLUMN";
    private static final String RADIX = "RADIX";
    private static final String REMARKS = "REMARKS";
    private static final String SCALE = "SCALE";
    private static final String SCOPE = "SCOPE";
    private static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    private static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    private static final String SCOPE_TABLE = "SCOPE_TABLE";
    private static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    private static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    private static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    private static final String SS_DATA_TYPE = "SS_DATA_TYPE";
    private static final String SUPERTABLE_NAME = "SUPERTABLE_NAME";
    private static final String SUPERTYPE_CAT = "SUPERTYPE_CAT";
    private static final String SUPERTYPE_NAME = "SUPERTYPE_NAME";
    private static final String SUPERTYPE_SCHEM = "SUPERTYPE_SCHEM";
    private static final String TABLE_CAT = "TABLE_CAT";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TABLE_SCHEM = "TABLE_SCHEM";
    private static final String TABLE_TYPE = "TABLE_TYPE";
    private static final String TYPE = "TYPE";
    private static final String TYPE_CAT = "TYPE_CAT";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final String TYPE_SCHEM = "TYPE_SCHEM";
    private static final String UPDATE_RULE = "UPDATE_RULE";
    private static final String FUNCTION_CAT = "FUNCTION_CAT";
    private static final String FUNCTION_NAME = "FUNCTION_NAME";
    private static final String FUNCTION_SCHEM = "FUNCTION_SCHEM";
    private static final String FUNCTION_TYPE = "FUNCTION_TYPE";
    private static final String SS_IS_SPARSE = "SS_IS_SPARSE";
    private static final String SS_IS_COLUMN_SET = "SS_IS_COLUMN_SET";
    private static final String SS_UDT_CATALOG_NAME = "SS_UDT_CATALOG_NAME";
    private static final String SS_UDT_SCHEMA_NAME = "SS_UDT_SCHEMA_NAME";
    private static final String SS_UDT_ASSEMBLY_TYPE_NAME = "SS_UDT_ASSEMBLY_TYPE_NAME";
    private static final String SS_XML_SCHEMACOLLECTION_CATALOG_NAME = "SS_XML_SCHEMACOLLECTION_CATALOG_NAME";
    private static final String SS_XML_SCHEMACOLLECTION_SCHEMA_NAME = "SS_XML_SCHEMACOLLECTION_SCHEMA_NAME";
    private static final String SS_XML_SCHEMACOLLECTION_NAME = "SS_XML_SCHEMACOLLECTION_NAME";

    private static final String IS_GENERATEDCOLUMN = "IS_GENERATEDCOLUMN";
    private static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    private static final String SQL_KEYWORDS = createSqlKeyWords();

    // Use LinkedHashMap to force retrieve elements in order they were inserted
    private static LinkedHashMap<Integer, String> getColumnsDWColumns = null;
    private static volatile LinkedHashMap<Integer, String> getImportedKeysDWColumns;
    private static final Lock LOCK = new ReentrantLock();

    /**
     * Returns the result from a simple query. This is to be used only for internal queries without any user input.
     * 
     * @param catalog
     *        catalog the query to be made in
     * @param query
     *        to execute
     * @return Resultset from the execution
     * @throws SQLTimeoutException
     */
    private SQLServerResultSet getResultSetFromInternalQueries(String catalog,
            String query) throws SQLException, SQLTimeoutException {
        checkClosed();
        String orgCat = null;
        orgCat = switchCatalogs(catalog);
        SQLServerResultSet rs = null;
        try {
            SQLServerStatement statement = (SQLServerStatement) connection.createStatement();
            statement.closeOnCompletion();
            rs = statement.executeQueryInternal(query);
        } finally {
            if (null != orgCat) {
                connection.setCatalog(orgCat);
            }
        }
        return rs;
    }

    /**
     * Returns the CallableStatement handle. Note we pool the handles per object.
     */
    private CallableStatement getCallableStatementHandle(CallableHandles request,
            String catalog) throws SQLServerException {
        CallableStatement CS = null;
        HandleAssociation hassoc = handleMap.get(request);
        try {
            if (null == hassoc) {
                CS = request.prepare(connection);
                hassoc = new HandleAssociation();
                hassoc.addToMap(catalog, CS);
            } else { // hassoc != null
                CS = hassoc.getMappedStatement(catalog);
                // No Cached Statement yet
                if (null == CS || CS.isClosed()) {
                    CS = request.prepare(connection);
                    hassoc.addToMap(catalog, CS);
                }
            }
            handleMap.put(request, hassoc);
        } catch (SQLException e) {
            SQLServerException.makeFromDriverError(connection, CS, e.toString(), null, false);
        }
        return CS;
    }

    /**
     * Returns the result from the stored procedure call.
     * 
     * @param catalog
     *        catalog the query to be made in
     * @param procedure
     *        to execute
     * @param arguments
     *        for the stored procedure
     * @return Resultset from the execution
     * @throws SQLTimeoutException
     */
    private SQLServerResultSet getResultSetFromStoredProc(String catalog, CallableHandles procedure,
            String[] arguments) throws SQLServerException, SQLTimeoutException {
        checkClosed();
        assert null != arguments;
        String orgCat = null;
        orgCat = switchCatalogs(catalog);
        SQLServerResultSet rs = null;
        try {
            SQLServerCallableStatement call = (SQLServerCallableStatement) getCallableStatementHandle(procedure,
                    catalog);

            if (call != null) {
                for (int i = 1; i <= arguments.length; i++) {
                    // note individual arguments can be null.
                    call.setString(i, arguments[i - 1]);
                }
                rs = (SQLServerResultSet) call.executeQueryInternal();
            }
        } finally {
            if (null != orgCat) {
                connection.setCatalog(orgCat);
            }
        }

        return rs;
    }

    private SQLServerResultSet getResultSetWithProvidedColumnNames(String catalog, CallableHandles procedure,
            String[] arguments, String[] columnNames) throws SQLServerException, SQLTimeoutException {
        // Execute the query
        SQLServerResultSet rs = getResultSetFromStoredProc(catalog, procedure, arguments);

        // Rename the columns
        if (null != rs) {
            for (int i = 0; i < columnNames.length; i++)
                rs.setColumnName(1 + i, columnNames[i]);
        }
        return rs;
    }

    /**
     * Switches the database catalogs.
     * 
     * @param catalog
     *        the new catalog
     * @throws SQLServerException
     * @return the old catalog
     */
    private String switchCatalogs(String catalog) throws SQLServerException {
        if (null == catalog)
            return null;
        String sCurr = null;
        sCurr = connection.getCatalog().trim();
        String sNew = catalog.trim();
        if (sCurr.equals(sNew))
            return null;
        connection.setCatalog(sNew);
        if (null == sCurr || sCurr.length() == 0)
            return null;
        return sCurr;
    }

    /* -------------- JDBC Interface API starts here ---------------- */

    @Override
    public boolean allProceduresAreCallable() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        checkClosed();
        // driver supports retrieving generated keys
        return true;
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        checkClosed();
        return MAXLOBSIZE;
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsSharding() throws SQLException {
        if (!DriverJDBCVersion.checkSupportsJDBC43()) {
            throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
        }
        checkClosed();
        return false;
    }

    @Override
    public java.sql.ResultSet getCatalogs() throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        // Return the original case instead of CAPS.removed Upper().
        String s = "SELECT name AS TABLE_CAT FROM sys.databases order by name"; // Need
                                                                                // to
                                                                                // match
                                                                                // case
                                                                                // of
                                                                                // connection.getCatalog
        return getResultSetFromInternalQueries(null, s);
    }

    @Override
    public String getCatalogSeparator() throws SQLServerException {
        checkClosed();
        return ".";
    }

    @Override
    public String getCatalogTerm() throws SQLServerException {
        checkClosed();
        return "database";
    }

    private static final String[] getColumnPrivilegesColumnNames = { /* 1 */ TABLE_CAT, /* 2 */ TABLE_SCHEM,
            /* 3 */ TABLE_NAME, /* 4 */ COLUMN_NAME, /* 5 */ GRANTOR, /* 6 */ GRANTEE, /* 7 */ PRIVILEGE,
            /* 8 */ IS_GRANTABLE};

    @Override
    public java.sql.ResultSet getColumnPrivileges(String catalog, String schema, String table,
            String col) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        // column_privileges supports columns being escaped.
        col = EscapeIDName(col);
        /*
         * sp_column_privileges [ @table_name = ] 'table_name' [ , [ @table_owner = ] 'table_owner' ] [ ,
         * [ @table_qualifier = ] 'table_qualifier' ] [ , [ @column_name = ] 'column' ]
         */

        String[] arguments = new String[4];
        arguments[0] = table;
        arguments[1] = schema;
        arguments[2] = catalog;
        arguments[3] = col;
        return getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_COLUMN_PRIVILEGES, arguments,
                getColumnPrivilegesColumnNames);
    }

    private static final String[] getTablesColumnNames = { /* 1 */ TABLE_CAT, /* 2 */ TABLE_SCHEM, /* 3 */ TABLE_NAME,
            /* 4 */ TABLE_TYPE, /* 5 */ REMARKS};

    @Override
    public java.sql.ResultSet getTables(String catalog, String schema, String table,
            String types[]) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();

        // sp_tables supports table name and owner ie schema escaped.
        table = EscapeIDName(table);
        schema = EscapeIDName(schema);
        /*
         * sp_tables [ [ @table_name = ] 'name' ] [ , [ @table_owner = ] 'owner' ] [ , [ @table_qualifier = ]
         * 'qualifier' ] [ , [ @table_type = ] "type" ]
         */

        String[] arguments = new String[4];
        arguments[0] = table;
        arguments[1] = schema;
        arguments[2] = catalog;

        if (null != types) {
            final StringBuilder tableTypes = new StringBuilder("'");
            for (int i = 0; i < types.length; i++) {
                if (i > 0)
                    tableTypes.append(",");
                tableTypes.append("''").append(types[i]).append("''");
            }
            tableTypes.append("'");
            arguments[3] = tableTypes.toString();
        }
        return getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_TABLES, arguments, getTablesColumnNames);
    }

    static final char LEFT_BRACKET = '[';
    static final char RIGHT_BRACKET = ']';
    static final char ESCAPE = '\\';
    static final char PERCENT = '%';
    static final char UNDERSCORE = '_';
    static final char DOUBLE_RIGHT_BRACKET[] = {']', ']'};

    /**
     * Accepts a SQL identifier (such as a column name or table name) and escapes the identifier so sql 92 wild card
     * characters can be escaped properly to be passed to functions like sp_columns or sp_tables. Assumes that the
     * incoming identifier is un-escaped.
     * 
     * @inID input identifier to escape.
     * @return the escaped value.
     */
    private static String EscapeIDName(String inID) throws SQLServerException {
        if (null == inID)
            return inID;
        // SQL bracket escaping rules.
        // See Using Wildcard Characters As Literals in SQL BOL
        //
        // 5\% -> '5[%]'
        // \_n -> '[_]n'
        // \[ -> '[ [ ]'
        // \] -> ']'
        // \\ -> \
        // \x -> \x where x is any char other than the ones above.

        char ch;
        // Add 2 extra chars wild guess thinking at least one escape.
        StringBuilder outID = new StringBuilder(inID.length() + 2);

        for (int i = 0; i < inID.length(); i++) {
            ch = inID.charAt(i);
            if (ESCAPE == ch && (++i < inID.length())) {
                ch = inID.charAt(i);
                switch (ch) {
                    case PERCENT:
                    case UNDERSCORE:
                    case LEFT_BRACKET:
                        outID.append(LEFT_BRACKET);
                        outID.append(ch);
                        outID.append(RIGHT_BRACKET);
                        break;
                    case RIGHT_BRACKET:
                    case ESCAPE:
                        outID.append(ch);
                        break;
                    default:
                        outID.append(ESCAPE);
                        outID.append(ch);
                }

            } else {
                // no escape just copy
                outID.append(ch);
            }
        }
        return outID.toString();
    }

    @Override
    public java.sql.ResultSet getColumns(String catalog, String schema, String table, String col) throws SQLException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        String originalCatalog = switchCatalogs(catalog);
        if (!this.connection.isAzureDW()) {
            String spColumnsSql = "DECLARE @mssqljdbc_temp_sp_columns_result TABLE(TABLE_QUALIFIER SYSNAME, TABLE_OWNER SYSNAME,"
                    + "TABLE_NAME SYSNAME, COLUMN_NAME SYSNAME, DATA_TYPE SMALLINT, TYPE_NAME SYSNAME, PRECISION INT,"
                    + "LENGTH INT, SCALE SMALLINT, RADIX SMALLINT, NULLABLE SMALLINT, REMARKS VARCHAR(254), COLUMN_DEF NVARCHAR(4000),"
                    + "SQL_DATA_TYPE SMALLINT, SQL_DATETIME_SUB SMALLINT, CHAR_OCTET_LENGTH INT, ORDINAL_POSITION INT,"
                    + "IS_NULLABLE VARCHAR(254), SS_IS_SPARSE SMALLINT, SS_IS_COLUMN_SET SMALLINT, SS_IS_COMPUTED SMALLINT,"
                    + "SS_IS_IDENTITY SMALLINT, SS_UDT_CATALOG_NAME NVARCHAR(128), SS_UDT_SCHEMA_NAME NVARCHAR(128),"
                    + "SS_UDT_ASSEMBLY_TYPE_NAME NVARCHAR(max), SS_XML_SCHEMACOLLECTION_CATALOG_NAME NVARCHAR(128),"
                    + "SS_XML_SCHEMACOLLECTION_SCHEMA_NAME NVARCHAR(128), SS_XML_SCHEMACOLLECTION_NAME NVARCHAR(128),"
                    + "SS_DATA_TYPE TINYINT);"

                    + "INSERT INTO @mssqljdbc_temp_sp_columns_result EXEC sp_columns_100 ?,?,?,?,?,?;"

                    + "SELECT TABLE_QUALIFIER AS TABLE_CAT, TABLE_OWNER AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,"
                    + "TYPE_NAME, PRECISION AS COLUMN_SIZE, LENGTH AS BUFFER_LENGTH, SCALE AS DECIMAL_DIGITS, RADIX AS NUM_PREC_RADIX,"
                    + "NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE,"
                    + "NULL AS SCOPE_CATALOG, NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, SS_DATA_TYPE AS SOURCE_DATA_TYPE,"
                    + "CASE SS_IS_IDENTITY WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' WHEN '' THEN '' END AS IS_AUTOINCREMENT,"
                    + "CASE SS_IS_COMPUTED WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' WHEN '' THEN '' END AS IS_GENERATEDCOLUMN, "
                    + "SS_IS_SPARSE, SS_IS_COLUMN_SET, SS_UDT_CATALOG_NAME, SS_UDT_SCHEMA_NAME, SS_UDT_ASSEMBLY_TYPE_NAME,"
                    + "SS_XML_SCHEMACOLLECTION_CATALOG_NAME, SS_XML_SCHEMACOLLECTION_SCHEMA_NAME, SS_XML_SCHEMACOLLECTION_NAME "
                    + "FROM @mssqljdbc_temp_sp_columns_result ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;";
            SQLServerResultSet rs = null;
            PreparedStatement pstmt = (SQLServerPreparedStatement) this.connection.prepareStatement(spColumnsSql);
            pstmt.closeOnCompletion();
            try {
                pstmt.setString(1, (null != table && !table.isEmpty()) ? EscapeIDName(table) : "%");
                pstmt.setString(2, (null != schema && !schema.isEmpty()) ? EscapeIDName(schema) : "%");
                pstmt.setString(3, (null != catalog && !catalog.isEmpty()) ? catalog : this.connection.getCatalog());
                pstmt.setString(4, (null != col && !col.isEmpty()) ? EscapeIDName(col) : "%");
                pstmt.setInt(5, 2);// show sparse columns
                pstmt.setInt(6, 3);// odbc version

                rs = (SQLServerResultSet) pstmt.executeQuery();
                rs.getColumn(5).setFilter(new DataTypeFilter());
                rs.getColumn(7).setFilter(new ZeroFixupFilter());
                rs.getColumn(8).setFilter(new ZeroFixupFilter());
                rs.getColumn(16).setFilter(new ZeroFixupFilter());
            } catch (SQLException e) {
                if (null != pstmt) {
                    try {
                        pstmt.close();
                    } catch (SQLServerException ignore) {
                        if (loggerExternal.isLoggable(Level.FINER)) {
                            loggerExternal.finer(
                                    "getColumns() threw an exception when attempting to close PreparedStatement");
                        }
                    }
                }
                throw e;
            } finally {
                if (null != originalCatalog) {
                    connection.setCatalog(originalCatalog);
                }
            }

            return rs;
        } else {
            /**
             * Can't actually switchCatalogs on Azure DW. This is here to keep consistency in behavior with SQL Azure DB
             * when user provides a different catalog than the one they're currently connected to. Will throw exception
             * when it's different and do nothing if it's the same/null.
             */
            LOCK.lock();
            try {
                if (null == getColumnsDWColumns) {
                    getColumnsDWColumns = new LinkedHashMap<>();
                    getColumnsDWColumns.put(1, TABLE_CAT);
                    getColumnsDWColumns.put(2, TABLE_SCHEM);
                    getColumnsDWColumns.put(3, TABLE_NAME);
                    getColumnsDWColumns.put(4, COLUMN_NAME);
                    getColumnsDWColumns.put(5, DATA_TYPE);
                    getColumnsDWColumns.put(6, TYPE_NAME);
                    getColumnsDWColumns.put(7, COLUMN_SIZE);
                    getColumnsDWColumns.put(8, BUFFER_LENGTH);
                    getColumnsDWColumns.put(9, DECIMAL_DIGITS);
                    getColumnsDWColumns.put(10, NUM_PREC_RADIX);
                    getColumnsDWColumns.put(11, NULLABLE);
                    getColumnsDWColumns.put(12, REMARKS);
                    getColumnsDWColumns.put(13, COLUMN_DEF);
                    getColumnsDWColumns.put(14, SQL_DATA_TYPE);
                    getColumnsDWColumns.put(15, SQL_DATETIME_SUB);
                    getColumnsDWColumns.put(16, CHAR_OCTET_LENGTH);
                    getColumnsDWColumns.put(17, ORDINAL_POSITION);
                    getColumnsDWColumns.put(18, IS_NULLABLE);
                    /*
                     * Use negative value keys to indicate that this column doesn't exist in SQL Server and should just
                     * be queried as 'NULL'
                     */
                    getColumnsDWColumns.put(-1, SCOPE_CATALOG);
                    getColumnsDWColumns.put(-2, SCOPE_SCHEMA);
                    getColumnsDWColumns.put(-3, SCOPE_TABLE);
                    getColumnsDWColumns.put(29, SOURCE_DATA_TYPE);
                    getColumnsDWColumns.put(22, IS_AUTOINCREMENT);
                    getColumnsDWColumns.put(21, IS_GENERATEDCOLUMN);
                    getColumnsDWColumns.put(19, SS_IS_SPARSE);
                    getColumnsDWColumns.put(20, SS_IS_COLUMN_SET);
                    getColumnsDWColumns.put(23, SS_UDT_CATALOG_NAME);
                    getColumnsDWColumns.put(24, SS_UDT_SCHEMA_NAME);
                    getColumnsDWColumns.put(25, SS_UDT_ASSEMBLY_TYPE_NAME);
                    getColumnsDWColumns.put(26, SS_XML_SCHEMACOLLECTION_CATALOG_NAME);
                    getColumnsDWColumns.put(27, SS_XML_SCHEMACOLLECTION_SCHEMA_NAME);
                    getColumnsDWColumns.put(28, SS_XML_SCHEMACOLLECTION_NAME);
                }
            } finally {
                LOCK.unlock();
            }

            try (PreparedStatement storedProcPstmt = this.connection
                    .prepareStatement("EXEC sp_columns_100 ?,?,?,?,?,?;")) {
                storedProcPstmt.setString(1, (null != table && !table.isEmpty()) ? EscapeIDName(table) : "%");
                storedProcPstmt.setString(2, (null != schema && !schema.isEmpty()) ? EscapeIDName(schema) : "%");
                storedProcPstmt.setString(3,
                        (null != catalog && !catalog.isEmpty()) ? catalog : this.connection.getCatalog());
                storedProcPstmt.setString(4, (null != col && !col.isEmpty()) ? EscapeIDName(col) : "%");
                storedProcPstmt.setInt(5, 2);// show sparse columns
                storedProcPstmt.setInt(6, 3);// odbc version

                SQLServerResultSet userRs = null;
                PreparedStatement resultPstmt = null;
                try (ResultSet rs = storedProcPstmt.executeQuery()) {
                    StringBuilder azureDwSelectBuilder = new StringBuilder();
                    boolean isFirstRow = true; // less expensive than continuously checking isFirst()
                    while (rs.next()) {
                        if (!isFirstRow) {
                            azureDwSelectBuilder.append(" UNION ALL ");
                        }
                        azureDwSelectBuilder.append(generateAzureDWSelect(rs, getColumnsDWColumns));
                        isFirstRow = false;
                    }

                    if (0 == azureDwSelectBuilder.length()) {
                        azureDwSelectBuilder.append(generateAzureDWEmptyRS(getColumnsDWColumns));
                    } else {
                        azureDwSelectBuilder.append(" ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION ");
                    }

                    resultPstmt = (SQLServerPreparedStatement) this.connection
                            .prepareStatement(azureDwSelectBuilder.toString());
                    userRs = (SQLServerResultSet) resultPstmt.executeQuery();
                    resultPstmt.closeOnCompletion();
                    userRs.getColumn(5).setFilter(new DataTypeFilter());
                    userRs.getColumn(7).setFilter(new ZeroFixupFilter());
                    userRs.getColumn(8).setFilter(new ZeroFixupFilter());
                    userRs.getColumn(16).setFilter(new ZeroFixupFilter());
                } catch (SQLException e) {
                    if (null != resultPstmt) {
                        try {
                            resultPstmt.close();
                        } catch (SQLServerException ignore) {
                            if (loggerExternal.isLoggable(Level.FINER)) {
                                loggerExternal.finer(
                                        "getColumns() threw an exception when attempting to close PreparedStatement");
                            }
                        }
                    }
                    throw e;
                }
                return userRs;
            }
        }
    }

    private String generateAzureDWSelect(ResultSet rs, Map<Integer, String> columns) throws SQLException {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (Entry<Integer, String> p : columns.entrySet()) {
            if (p.getKey() < 0) {
                sb.append("NULL");
            } else {
                Object o = rs.getObject(p.getKey());
                if (null == o) {
                    sb.append("NULL");
                } else if (o instanceof Number) {
                    if ("IS_AUTOINCREMENT".equalsIgnoreCase(p.getValue())
                            || "IS_GENERATEDCOLUMN".equalsIgnoreCase(p.getValue())) {
                        sb.append("'").append(Util.escapeSingleQuotes(Util.zeroOneToYesNo(((Number) o).intValue())))
                                .append("'");
                    } else {
                        sb.append(o.toString());
                    }
                } else {
                    sb.append("'").append(Util.escapeSingleQuotes(o.toString())).append("'");
                }
            }
            sb.append(" AS ").append(p.getValue()).append(",");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private String generateAzureDWEmptyRS(Map<Integer, String> columns) throws SQLException {
        StringBuilder sb = new StringBuilder("SELECT TOP 0 ");
        for (Entry<Integer, String> p : columns.entrySet()) {
            sb.append("NULL AS ").append(p.getValue()).append(",");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private static final String[] getFunctionsColumnNames = { /* 1 */ FUNCTION_CAT, /* 2 */ FUNCTION_SCHEM,
            /* 3 */ FUNCTION_NAME, /* 4 */ NUM_INPUT_PARAMS, /* 5 */ NUM_OUTPUT_PARAMS, /* 6 */ NUM_RESULT_SETS,
            /* 7 */ REMARKS, /* 8 */ FUNCTION_TYPE};

    @Override
    public java.sql.ResultSet getFunctions(String catalog, String schemaPattern,
            String functionNamePattern) throws SQLException {
        checkClosed();

        /*
         * sp_stored_procedures [ [ @sp_name = ] 'name' ] [ , [ @sp_owner = ] 'schema'] [ , [ @sp_qualifier = ]
         * 'qualifier' ] [ , [@fUsePattern = ] 'fUsePattern' ]
         */ // use default ie use pattern matching.
        // catalog cannot be empty in sql server
        if (null != catalog && catalog.length() == 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
            Object[] msgArgs = {"catalog"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
        }

        String[] arguments = new String[3];
        arguments[0] = EscapeIDName(functionNamePattern);
        arguments[1] = EscapeIDName(schemaPattern);
        arguments[2] = catalog;
        return getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_STORED_PROCEDURES, arguments,
                getFunctionsColumnNames);
    }

    private static final String[] getFunctionsColumnsColumnNames = { /* 1 */ FUNCTION_CAT, /* 2 */ FUNCTION_SCHEM,
            /* 3 */ FUNCTION_NAME, /* 4 */ COLUMN_NAME, /* 5 */ COLUMN_TYPE, /* 6 */ DATA_TYPE, /* 7 */ TYPE_NAME,
            /* 8 */ PRECISION, /* 9 */ LENGTH, /* 10 */ SCALE, /* 11 */ RADIX, /* 12 */ NULLABLE, /* 13 */ REMARKS,
            /* 14 */ COLUMN_DEF, /* 15 */ SQL_DATA_TYPE, /* 16 */ SQL_DATETIME_SUB, /* 17 */ CHAR_OCTET_LENGTH,
            /* 18 */ ORDINAL_POSITION, /* 19 */ IS_NULLABLE};

    @Override
    public java.sql.ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        checkClosed();
        /*
         * sp_sproc_columns [[@procedure_name =] 'name'] [,[@procedure_owner =] 'owner'] [,[@procedure_qualifier =]
         * 'qualifier'] [,[@column_name =] 'column_name'] [,[@ODBCVer =] 'ODBCVer']
         */

        // catalog cannot be empty in sql server
        if (null != catalog && catalog.length() == 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
            Object[] msgArgs = {"catalog"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
        }

        String[] arguments = new String[5];

        // proc name supports escaping
        arguments[0] = EscapeIDName(functionNamePattern);
        // schema name supports escaping.
        arguments[1] = EscapeIDName(schemaPattern);
        arguments[2] = catalog;
        // col name supports escaping
        arguments[3] = EscapeIDName(columnNamePattern);
        arguments[4] = "3";
        SQLServerResultSet rs = getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_SPROC_COLUMNS,
                arguments, getFunctionsColumnsColumnNames);

        // Hook in a filter on the DATA_TYPE column of the result set we're
        // going to return that converts the ODBC values from sp_columns
        // into JDBC values. Also for the precision
        if (null != rs) {
            rs.getColumn(6).setFilter(new DataTypeFilter());

            if (connection.isKatmaiOrLater()) {
                rs.getColumn(8).setFilter(new ZeroFixupFilter());
                rs.getColumn(9).setFilter(new ZeroFixupFilter());
                rs.getColumn(17).setFilter(new ZeroFixupFilter());
            }
        }
        return rs;
    }

    @Override
    public java.sql.ResultSet getClientInfoProperties() throws SQLException {
        checkClosed();
        return getResultSetFromInternalQueries(null, "SELECT" +
        /* 1 */ " cast(NULL as char(1)) as NAME," +
        /* 2 */ " cast(0 as int) as MAX_LEN," +
        /* 3 */ " cast(NULL as char(1)) as DEFAULT_VALUE," +
        /* 4 */ " cast(NULL as char(1)) as DESCRIPTION " + " where 0 = 1");
    }

    private static final String[] getBestRowIdentifierColumnNames = { /* 1 */ SCOPE, /* 2 */ COLUMN_NAME,
            /* 3 */ DATA_TYPE, /* 4 */ TYPE_NAME, /* 5 */ COLUMN_SIZE, /* 6 */ BUFFER_LENGTH, /* 7 */ DECIMAL_DIGITS,
            /* 8 */ PSEUDO_COLUMN};

    @Override
    public java.sql.ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
            boolean nullable) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        /*
         * sp_special_columns [@table_name =] 'table_name' [,[@table_owner =] 'table_owner'] [,[@qualifier =]
         * 'qualifier'] [,[@col_type =] 'col_type'] [,[@scope =] 'scope'] [,[@nullable =] 'nullable'] [,[@ODBCVer =]
         * 'ODBCVer'] ;
         */
        String[] arguments = new String[7];
        arguments[0] = table;
        arguments[1] = schema;
        arguments[2] = catalog;
        arguments[3] = "R"; // coltype
        if (bestRowTemporary == scope)
            arguments[4] = "C"; // Scope is temporary C
        else
            arguments[4] = "T"; // Scope is for the transaction
        if (nullable)
            arguments[5] = "U"; // nullable
        else
            arguments[5] = "O"; // nullable
        arguments[6] = "3"; // Use 3 unless required otherwise
        SQLServerResultSet rs = getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_SPECIAL_COLUMNS,
                arguments, getBestRowIdentifierColumnNames);

        // Hook in a filter on the DATA_TYPE column of the result set we're
        // going to return that converts the ODBC values from sp_columns
        // into JDBC values.
        if (null != rs) {
            rs.getColumn(3).setFilter(new DataTypeFilter());
        }
        return rs;
    }

    @Override
    public java.sql.ResultSet getCrossReference(String cat1, String schem1, String tab1, String cat2, String schem2,
            String tab2) throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();

        /*
         * sp_fkeys [ @pktable_name = ] 'pktable_name' [ , [ @pktable_owner = ] 'pktable_owner' ] [ ,
         * [ @pktable_qualifier = ] 'pktable_qualifier' ] { , [ @fktable_name = ] 'fktable_name' } [ , [ @fktable_owner
         * = ] 'fktable_owner' ] [ , [ @fktable_qualifier = ] 'fktable_qualifier' ]
         */
        String[] arguments = {tab1, schem1, cat1, tab2, schem2, cat2};
        return executeSPFkeys(arguments);
    }

    @Override
    public String getDatabaseProductName() throws SQLServerException {
        checkClosed();
        return "Microsoft SQL Server";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLServerException {
        checkClosed();
        return connection.sqlServerVersion;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLServerException {
        checkClosed();
        return java.sql.Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public int getDriverMajorVersion() {
        return SQLJdbcVersion.major;
    }

    @Override
    public int getDriverMinorVersion() {
        return SQLJdbcVersion.minor;
    }

    @Override
    public String getDriverName() throws SQLServerException {
        checkClosed();
        return SQLServerDriver.PRODUCT_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLServerException {

        // driver version in the Major.Minor.MMDD.Revision form
        int n = getDriverMinorVersion();
        String s = getDriverMajorVersion() + ".";
        s += "" + n;
        s = s + ".";
        s = s + SQLJdbcVersion.patch;
        s = s + ".";
        s = s + SQLJdbcVersion.build;
        return s;
    }

    @Override
    public java.sql.ResultSet getExportedKeys(String cat, String schema,
            String table) throws SQLException, SQLTimeoutException {
        return getCrossReference(cat, schema, table, null, null, null);
    }

    @Override
    public String getExtraNameCharacters() throws SQLServerException {
        checkClosed();
        return "$#@";
    }

    @Override
    public String getIdentifierQuoteString() throws SQLServerException {
        checkClosed();
        return "\"";
    }

    @Override
    public java.sql.ResultSet getImportedKeys(String cat, String schema,
            String table) throws SQLException, SQLTimeoutException {
        return getCrossReference(null, null, null, cat, schema, table);
    }

    private ResultSet executeSPFkeys(String[] procParams) throws SQLException, SQLTimeoutException {
        if (!this.connection.isAzureDW()) {
            String tempTableName = "@jdbc_temp_fkeys_result";
            String sql = "DECLARE " + tempTableName + " table (PKTABLE_QUALIFIER sysname, " + "PKTABLE_OWNER sysname, "
                    + "PKTABLE_NAME sysname, " + "PKCOLUMN_NAME sysname, " + "FKTABLE_QUALIFIER sysname, "
                    + "FKTABLE_OWNER sysname, " + "FKTABLE_NAME sysname, " + "FKCOLUMN_NAME sysname, "
                    + "KEY_SEQ smallint, " + "UPDATE_RULE smallint, " + "DELETE_RULE smallint, " + "FK_NAME sysname, "
                    + "PK_NAME sysname, " + "DEFERRABILITY smallint);" + "INSERT INTO " + tempTableName
                    + " EXEC sp_fkeys ?,?,?,?,?,?;" + "SELECT  t.PKTABLE_QUALIFIER AS PKTABLE_CAT, "
                    + "t.PKTABLE_OWNER AS PKTABLE_SCHEM, " + "t.PKTABLE_NAME, " + "t.PKCOLUMN_NAME, "
                    + "t.FKTABLE_QUALIFIER AS FKTABLE_CAT, " + "t.FKTABLE_OWNER AS FKTABLE_SCHEM, " + "t.FKTABLE_NAME, "
                    + "t.FKCOLUMN_NAME, " + "t.KEY_SEQ, " + "CASE s.update_referential_action " + "WHEN 1 THEN 0 " +
                    // cascade - note that sp_fkey and sys.foreign_keys have flipped values for
                    // cascade and no action
                    "WHEN 0 THEN 3 " + // no action
                    "WHEN 2 THEN 2 " + // set null
                    "WHEN 3 THEN 4 " + // set default
                    "END as UPDATE_RULE, " + "CASE s.delete_referential_action " + "WHEN 1 THEN 0 " + "WHEN 0 THEN 3 "
                    + "WHEN 2 THEN 2 " + "WHEN 3 THEN 4 " + "END as DELETE_RULE, " + "t.FK_NAME, " + "t.PK_NAME, "
                    + "t.DEFERRABILITY " + "FROM " + tempTableName + " t "
                    + "LEFT JOIN sys.foreign_keys s ON t.FK_NAME = s.name COLLATE database_default AND schema_id(t.FKTABLE_OWNER) = s.schema_id "
                    + "ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";
            SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(sql);
            cstmt.closeOnCompletion();
            for (int i = 0; i < 6; i++) {
                cstmt.setString(i + 1, procParams[i]);
            }
            String currentDB = null;
            if (null != procParams[2] && !procParams[2].isEmpty()) {// pktable_qualifier
                currentDB = switchCatalogs(procParams[2]);
            } else if (null != procParams[5] && !procParams[5].isEmpty()) {// fktable_qualifier
                currentDB = switchCatalogs(procParams[5]);
            }
            ResultSet rs = cstmt.executeQuery();
            if (null != currentDB) {
                switchCatalogs(currentDB);
            }
            return rs;
        } else {
            // Azure DW does not support foreign keys, return an empty result set with correct metadata.
            ResultSet userRs = null;
            PreparedStatement pstmt = null;
            StringBuilder azureDwSelectBuilder = new StringBuilder();

            LinkedHashMap<Integer, String> importedKeysDWColumns = getImportedKeysDWColumns;
            if (null == importedKeysDWColumns) {
                LOCK.lock();
                try {
                    importedKeysDWColumns = getImportedKeysDWColumns;
                    if (null == importedKeysDWColumns) {
                        getImportedKeysDWColumns = importedKeysDWColumns = new LinkedHashMap<>(14, 1.0F);
                        importedKeysDWColumns.put(1, PKTABLE_CAT);
                        importedKeysDWColumns.put(2, PKTABLE_SCHEM);
                        importedKeysDWColumns.put(3, PKTABLE_NAME);
                        importedKeysDWColumns.put(4, PKCOLUMN_NAME);
                        importedKeysDWColumns.put(5, FKTABLE_CAT);
                        importedKeysDWColumns.put(6, FKTABLE_SCHEM);
                        importedKeysDWColumns.put(7, FKTABLE_NAME);
                        importedKeysDWColumns.put(8, FKCOLUMN_NAME);
                        importedKeysDWColumns.put(9, KEY_SEQ);
                        importedKeysDWColumns.put(10, UPDATE_RULE);
                        importedKeysDWColumns.put(11, DELETE_RULE);
                        importedKeysDWColumns.put(12, FK_NAME);
                        importedKeysDWColumns.put(13, PK_NAME);
                        importedKeysDWColumns.put(14, DEFERRABILITY);
                    }
                } finally {
                    LOCK.unlock();
                }
            }
            azureDwSelectBuilder.append(generateAzureDWEmptyRS(getImportedKeysDWColumns));
            try {
                pstmt = this.connection.prepareStatement(azureDwSelectBuilder.toString());
                userRs = pstmt.executeQuery();
                pstmt.closeOnCompletion();
                return userRs;
            } catch (SQLException e) {
                if (null != pstmt) {
                    try {
                        pstmt.close();
                    } catch (SQLServerException ignore) {
                        if (loggerExternal.isLoggable(Level.FINER)) {
                            loggerExternal.finer(
                                    "executeSPFkeys() threw an exception when attempting to close PreparedStatement");
                        }
                    }
                }
                throw e;
            }
        }
    }

    private static final String[] getIndexInfoColumnNames = { /* 1 */ TABLE_CAT, /* 2 */ TABLE_SCHEM,
            /* 3 */ TABLE_NAME, /* 4 */ NON_UNIQUE, /* 5 */ INDEX_QUALIFIER, /* 6 */ INDEX_NAME, /* 7 */ TYPE,
            /* 8 */ ORDINAL_POSITION, /* 9 */ COLUMN_NAME, /* 10 */ ASC_OR_DESC, /* 11 */ CARDINALITY, /* 12 */ PAGES,
            /* 13 */ FILTER_CONDITION};

    @Override
    public java.sql.ResultSet getIndexInfo(String cat, String schema, String table, boolean unique,
            boolean approximate) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        /*
         * sp_statistics [ @table_name = ] 'table_name' [ , [ @table_owner = ] 'owner' ] [ , [ @table_qualifier = ]
         * 'qualifier' ] [ , [ @index_name = ] 'index_name' ] [ , [ @is_unique = ] 'is_unique' ] [ , [ @accuracy = ]
         * 'accuracy' ]
         */
        String[] arguments = new String[6];
        arguments[0] = table;
        arguments[1] = schema;
        arguments[2] = cat;
        // use default for index name
        arguments[3] = "%"; // index name % is default
        if (unique)
            arguments[4] = "Y"; // is_unique
        else
            arguments[4] = "N";
        if (approximate)
            arguments[5] = "Q";
        else
            arguments[5] = "E";
        return getResultSetWithProvidedColumnNames(cat, CallableHandles.SP_STATISTICS, arguments,
                getIndexInfoColumnNames);
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLServerException {
        checkClosed();
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLServerException {
        checkClosed();
        return 128;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLServerException {
        checkClosed();
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLServerException {
        checkClosed();
        return 128;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLServerException {
        checkClosed();
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLServerException {
        checkClosed();
        return 16;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLServerException {
        checkClosed();
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLServerException {
        checkClosed();
        return 4096;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLServerException {
        checkClosed();
        return 1024;
    }

    @Override
    public int getMaxConnections() throws SQLException, SQLTimeoutException {
        checkClosed();
        try (SQLServerResultSet rs = getResultSetFromInternalQueries(null,
                "select maximum from sys.configurations where name = 'user connections'")) {
            if (!rs.next()) {
                return 0;
            }
            return rs.getInt("maximum");
        } catch (SQLServerException e) {
            // Try with sp_configure if users do not have privileges to execute sys.configurations
            try (SQLServerResultSet rs1 = getResultSetFromInternalQueries(null, "sp_configure 'user connections'")) {
                if (!rs1.next()) {
                    return 0;
                }
                return rs1.getInt("maximum");
            } catch (SQLServerException e1) {
                return 0;
            }
        }
    }

    @Override
    public int getMaxCursorNameLength() throws SQLServerException {
        checkClosed();
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLServerException {
        checkClosed();
        return 900;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLServerException {
        checkClosed();
        return 128;
    }

    @Override
    public int getMaxRowSize() throws SQLServerException {
        checkClosed();
        return 8060;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLServerException {
        checkClosed();
        return 128;
    }

    @Override
    public int getMaxStatementLength() throws SQLServerException {
        checkClosed();

        // SQL Server currently limits to 64K the number of TDS packets per
        // conversation.
        // This number multiplied by the size of each TDS packet yields the
        // maximum total
        // size of any request to the server, which is therefore an upper bound
        // to the
        // maximum SQL statement length.
        return 65536 * connection.getTDSPacketSize();
    }

    @Override
    public int getMaxStatements() throws SQLServerException {
        checkClosed();
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLServerException {
        checkClosed();
        return 128;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLServerException {
        checkClosed();
        return 256;
    }

    @Override
    public int getMaxUserNameLength() throws SQLServerException {
        checkClosed();
        return 128;
    }

    @Override
    public String getNumericFunctions() throws SQLServerException {
        checkClosed();
        return "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE";
    }

    private static final String[] getPrimaryKeysColumnNames = { /* 1 */ TABLE_CAT, /* 2 */ TABLE_SCHEM,
            /* 3 */ TABLE_NAME, /* 4 */ COLUMN_NAME, /* 5 */ KEY_SEQ, /* 6 */ PK_NAME};

    @Override
    public java.sql.ResultSet getPrimaryKeys(String cat, String schema,
            String table) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        /*
         * sp_pkeys [ @table_name = ] 'name' [ , [ @table_owner = ] 'owner' ] [ , [ @table_qualifier = ] 'qualifier' ]
         */
        String[] arguments = new String[3];
        arguments[0] = table;
        arguments[1] = schema;
        arguments[2] = cat;
        return getResultSetWithProvidedColumnNames(cat, CallableHandles.SP_PKEYS, arguments, getPrimaryKeysColumnNames);
    }

    private static final String[] getProcedureColumnsColumnNames = { /* 1 */ PROCEDURE_CAT, /* 2 */ PROCEDURE_SCHEM,
            /* 3 */ PROCEDURE_NAME, /* 4 */ COLUMN_NAME, /* 5 */ COLUMN_TYPE, /* 6 */ DATA_TYPE, /* 7 */ TYPE_NAME,
            /* 8 */ PRECISION, /* 9 */ LENGTH, /* 10 */ SCALE, /* 11 */ RADIX, /* 12 */ NULLABLE, /* 13 */ REMARKS,
            /* 14 */ COLUMN_DEF, /* 15 */ SQL_DATA_TYPE, /* 16 */ SQL_DATETIME_SUB, /* 17 */ CHAR_OCTET_LENGTH,
            /* 18 */ ORDINAL_POSITION, /* 19 */ IS_NULLABLE};

    @Override
    public java.sql.ResultSet getProcedureColumns(String catalog, String schema, String proc,
            String col) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        /*
         * sp_sproc_columns [[@procedure_name =] 'name'] [,[@procedure_owner =] 'owner'] [,[@procedure_qualifier =]
         * 'qualifier'] [,[@column_name =] 'column_name'] [,[@ODBCVer =] 'ODBCVer']
         */

        String[] arguments = new String[5];

        // proc name supports escaping
        proc = EscapeIDName(proc);
        arguments[0] = proc;
        arguments[1] = schema;
        arguments[2] = catalog;
        // col name supports escaping
        col = EscapeIDName(col);
        arguments[3] = col;
        arguments[4] = "3";
        SQLServerResultSet rs = getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_SPROC_COLUMNS,
                arguments, getProcedureColumnsColumnNames);

        // Hook in a filter on the DATA_TYPE column of the result set we're
        // going to return that converts the ODBC values from sp_columns
        // into JDBC values. Also for the precision
        if (null != rs) {
            rs.getColumn(6).setFilter(new DataTypeFilter());
            if (connection.isKatmaiOrLater()) {
                rs.getColumn(8).setFilter(new ZeroFixupFilter());
                rs.getColumn(9).setFilter(new ZeroFixupFilter());
                rs.getColumn(17).setFilter(new ZeroFixupFilter());
            }
        }
        return rs;
    }

    private static final String[] getProceduresColumnNames = { /* 1 */ PROCEDURE_CAT, /* 2 */ PROCEDURE_SCHEM,
            /* 3 */ PROCEDURE_NAME, /* 4 */ NUM_INPUT_PARAMS, /* 5 */ NUM_OUTPUT_PARAMS, /* 6 */ NUM_RESULT_SETS,
            /* 7 */ REMARKS, /* 8 */ PROCEDURE_TYPE};

    @Override
    public java.sql.ResultSet getProcedures(String catalog, String schema,
            String proc) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        checkClosed();
        /*
         * sp_stored_procedures [ [ @sp_name = ] 'name' ] [ , [ @sp_owner = ] 'schema'] [ , [ @sp_qualifier = ]
         * 'qualifier' ] [ , [@fUsePattern = ] 'fUsePattern' ]
         */
        String[] arguments = new String[3];
        arguments[0] = EscapeIDName(proc);
        arguments[1] = schema;
        arguments[2] = catalog;
        return getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_STORED_PROCEDURES, arguments,
                getProceduresColumnNames);
    }

    @Override
    public String getProcedureTerm() throws SQLServerException {
        checkClosed();
        return "stored procedure";
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        checkClosed();

        // SQL server does not support pseudo columns for identifiers
        // as per
        // http://msdn.microsoft.com/en-us/library/ms378445%28v=sql.110%29.aspx
        // so just return empty result set
        return getResultSetFromInternalQueries(catalog, "SELECT" +
        /* 1 */ " cast(NULL as char(1)) as TABLE_CAT," +
        /* 2 */ " cast(NULL as char(1)) as TABLE_SCHEM," +
        /* 3 */ " cast(NULL as char(1)) as TABLE_NAME," +
        /* 4 */ " cast(NULL as char(1)) as COLUMN_NAME," +
        /* 5 */ " cast(0 as int) as DATA_TYPE," +
        /* 6 */ " cast(0 as int) as COLUMN_SIZE," +
        /* 7 */ " cast(0 as int) as DECIMAL_DIGITS," +
        /* 8 */ " cast(0 as int) as NUM_PREC_RADIX," +
        /* 9 */ " cast(NULL as char(1)) as COLUMN_USAGE," +
        /* 10 */ " cast(NULL as char(1)) as REMARKS," +
        /* 11 */ " cast(0 as int) as CHAR_OCTET_LENGTH," +
        /* 12 */ " cast(NULL as char(1)) as IS_NULLABLE" + " where 0 = 1");
    }

    @Override
    public java.sql.ResultSet getSchemas() throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        return getSchemasInternal(null, null);

    }

    private java.sql.ResultSet getSchemasInternal(String catalog,
            String schemaPattern) throws SQLException, SQLTimeoutException {

        String s;
        // The schemas that return null for catalog name, these are prebuilt
        // schemas shipped by SQLServer, if SQLServer adds anymore of these
        // we need to add them here.
        String constSchemas = " ('dbo', 'guest','INFORMATION_SCHEMA','sys','db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin' "
                + " ,'db_backupoperator','db_datareader','db_datawriter','db_denydatareader','db_denydatawriter') ";

        String schema = "sys.schemas";
        String schemaName = "sys.schemas.name";
        if (null != catalog && catalog.length() != 0) {
            final String catalogId = Util.escapeSQLId(catalog);
            schema = catalogId + "." + schema;
            schemaName = catalogId + "." + schemaName;
        }

        // The common schemas need to be under null catalog name however the
        // schemas specific to the particular catalog has to have the current
        // catalog name
        // to achive this, first we figure out the common schemas by
        // intersecting current catalogs schemas with the const schemas (ie
        // builtinSchemas)
        s = "select " + schemaName + " 'TABLE_SCHEM',";
        if (null != catalog && catalog.length() == 0) {
            s += "null 'TABLE_CATALOG' ";
        } else {
            s += " CASE WHEN " + schemaName + "  IN " + constSchemas + " THEN null ELSE ";
            if (null != catalog && catalog.length() != 0) {
                s += "'" + catalog + "' ";
            } else
                s += " DB_NAME() ";

            s += " END 'TABLE_CATALOG' ";
        }
        s += "   from " + schema;

        // Handle the case when catalog is empty this means common schemas only
        //
        if (null != catalog && catalog.length() == 0) {
            if (null != schemaPattern)
                s += " where " + schemaName + " like ?  and ";
            else
                s += " where ";
            s += schemaName + " in " + constSchemas;
        } else if (null != schemaPattern)
            s += " where " + schemaName + " like ?  ";

        s += " order by 2, 1";
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine(toString() + " schema query (" + s + ")");
        }
        SQLServerResultSet rs;
        if (null == schemaPattern) {
            catalog = null;
            rs = getResultSetFromInternalQueries(catalog, s);
        } else {

            // The prepared statement is not closed after execution.
            // No we will not "leak a server handle" per execution
            // as the prepared statement will close as the resultset 'rs' is closed
            SQLServerPreparedStatement ps = (SQLServerPreparedStatement) connection.prepareStatement(s);
            ps.closeOnCompletion();
            ps.setString(1, schemaPattern);
            rs = (SQLServerResultSet) ps.executeQueryInternal();
        }
        return rs;
    }

    @Override
    public java.sql.ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        return getSchemasInternal(catalog, schemaPattern);
    }

    @Override
    public String getSchemaTerm() throws SQLServerException {
        checkClosed();
        return "schema";
    }

    @Override
    public String getSearchStringEscape() throws SQLServerException {
        checkClosed();
        return "\\";
    }

    @Override
    public String getSQLKeywords() throws SQLServerException {
        checkClosed();
        return SQL_KEYWORDS;
    }

    private static String createSqlKeyWords() {
        return "ADD,ALL,ALTER,AND,ANY,AS,ASC,AUTHORIZATION," + "BACKUP,BEGIN,BETWEEN,BREAK,BROWSE,BULK,BY,"
                + "CASCADE,CASE,CHECK,CHECKPOINT,CLOSE,CLUSTERED,COALESCE,COLLATE,COLUMN,COMMIT,"
                + "COMPUTE,CONSTRAINT,CONTAINS,CONTAINSTABLE,CONTINUE,CONVERT,CREATE,CROSS,CURRENT,"
                + "CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,"
                + "DATABASE,DBCC,DEALLOCATE,DECLARE,DEFAULT,DELETE,DENY,DESC,DISK,"
                + "DISTINCT,DISTRIBUTED,DOUBLE,DROP,DUMP,"
                + "ELSE,END,ERRLVL,ESCAPE,EXCEPT,EXEC,EXECUTE,EXISTS,EXIT,EXTERNAL,"
                + "FETCH,FILE,FILLFACTOR,FOR,FOREIGN,FREETEXT,FREETEXTTABLE,FROM,FULL,FUNCTION," + "GOTO,GRANT,GROUP,"
                + "HAVING,HOLDLOCK,"
                + "IDENTITY,IDENTITY_INSERT,IDENTITYCOL,IF,IN,INDEX,INNER,INSERT,INTERSECT,INTO,IS," + "JOIN,"
                + "KEY,KILL," + "LEFT,LIKE,LINENO,LOAD," + "MERGE," + "NATIONAL,NOCHECK,NONCLUSTERED,NOT,NULL,NULLIF,"
                + "OF,OFF,OFFSETS,ON,OPEN,OPENDATASOURCE,OPENQUERY," + "OPENROWSET,OPENXML,OPTION,OR,ORDER,OUTER,OVER,"
                + "PERCENT,PIVOT,PLAN,PRECISION,PRIMARY,PRINT,PROC,PROCEDURE,PUBLIC,"
                + "RAISERROR,READ,READTEXT,RECONFIGURE,REFERENCES,REPLICATION,RESTORE,RESTRICT,"
                + "RETURN,REVERT,REVOKE,RIGHT,ROLLBACK,ROWCOUNT,ROWGUIDCOL,RULE,"
                + "SAVE,SCHEMA,SECURITYAUDIT,SELECT,SEMANTICKEYPHRASETABLE,SEMANTICSIMILARITYDETAILSTABLE,"
                + "SEMANTICSIMILARITYTABLE,SESSION_USER,SET,SETUSER,SHUTDOWN,SOME,STATISTICS,SYSTEM_USER,"
                + "TABLE,TABLESAMPLE,TEXTSIZE,THEN,TO,TOP,TRAN,TRANSACTION,TRIGGER,TRUNCATE,TRY_CONVERT,TSEQUAL,"
                + "UNION,UNIQUE,UNPIVOT,UPDATE,UPDATETEXT,USE,USER," + "VALUES,VARYING,VIEW,"
                + "WAITFOR,WHEN,WHERE,WHILE,WITH,WITHIN GROUP,WRITETEXT";
    }

    @Override
    public String getStringFunctions() throws SQLServerException {
        checkClosed();
        return "ASCII,CHAR,CONCAT,DIFFERENCE,INSERT,LCASE,LEFT,LENGTH,LOCATE,LTRIM,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTRING,UCASE";
    }

    @Override
    public String getSystemFunctions() throws SQLServerException {
        checkClosed();
        return "DATABASE,IFNULL,USER"; // The functions no reinstated after the
                                       // CTS certification.
    }

    private static final String[] getTablePrivilegesColumnNames = { /* 1 */ TABLE_CAT, /* 2 */ TABLE_SCHEM,
            /* 3 */ TABLE_NAME, /* 4 */ GRANTOR, /* 5 */ GRANTEE, /* 6 */ PRIVILEGE, /* 7 */ IS_GRANTABLE};

    @Override
    public java.sql.ResultSet getTablePrivileges(String catalog, String schema,
            String table) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        table = EscapeIDName(table);
        schema = EscapeIDName(schema);
        /*
         * sp_table_privileges [ @table_name = ] 'table_name' [ , [ @table_owner = ] 'table_owner' ] [ ,
         * [ @table_qualifier = ] 'table_qualifier' ] [ , [@fUsePattern =] 'fUsePattern']
         */
        String[] arguments = new String[3];
        arguments[0] = table;
        arguments[1] = schema;
        arguments[2] = catalog;

        return getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_TABLE_PRIVILEGES, arguments,
                getTablePrivilegesColumnNames);
    }

    @Override
    public java.sql.ResultSet getTableTypes() throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        String s = "SELECT 'VIEW' 'TABLE_TYPE' UNION SELECT 'TABLE' UNION SELECT 'SYSTEM TABLE'";
        SQLServerResultSet rs = getResultSetFromInternalQueries(null, s);
        return rs;
    }

    @Override
    public String getTimeDateFunctions() throws SQLServerException {
        checkClosed();
        return "CURDATE,CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,HOUR,MINUTE,MONTH,MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR";
    }

    @Override
    public java.sql.ResultSet getTypeInfo() throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();

        SQLServerResultSet rs;
        // We support only sql2k5 and above
        if (connection.isKatmaiOrLater())
            rs = getResultSetFromInternalQueries(null, "sp_datatype_info_100 @ODBCVer=3");
        else
            rs = getResultSetFromInternalQueries(null, "sp_datatype_info @ODBCVer=3");

        rs.setColumnName(11, "FIXED_PREC_SCALE");
        // Hook in a filter on the DATA_TYPE column of the result set we're
        // going to return that converts the ODBC values from sp_columns
        // into JDBC values.
        rs.getColumn(2).setFilter(new DataTypeFilter());
        return rs;
    }

    @Override
    public String getURL() throws SQLServerException {
        checkClosed();
        // Build up the URL with the connection properties do not hand out user
        // ID and password
        StringBuilder url = new StringBuilder();
        // get the properties collection from the connection.
        Properties props = connection.activeConnectionProperties;
        DriverPropertyInfo[] info = SQLServerDriver.getPropertyInfoFromProperties(props);
        String serverName = null;
        String portNumber = null;
        String instanceName = null;

        // build the connection string without the server name, instance name
        // and port number as these go in the front
        int index = info.length;
        while (--index >= 0) {
            String name = info[index].name;

            // making sure no security info is exposed.
            if (!name.equals(SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.toString())
                    && !name.equals(SQLServerDriverStringProperty.USER.toString())
                    && !name.equals(SQLServerDriverStringProperty.PASSWORD.toString())
                    && !name.equals(SQLServerDriverStringProperty.KEY_STORE_SECRET.toString())
                    && !name.equals(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString())) {
                String val = info[index].value;
                // skip empty strings
                if (0 != val.length()) {
                    // special case these server name, instance name and port
                    // number as these go in the front
                    if (name.equals(SQLServerDriverStringProperty.SERVER_NAME.toString())) {
                        serverName = val;
                    } else if (name.equals(SQLServerDriverStringProperty.INSTANCE_NAME.toString())) {
                        instanceName = val;
                    } else if (name.equals(SQLServerDriverIntProperty.PORT_NUMBER.toString())) {
                        portNumber = val;
                    } else {
                        // build name value pairs separated by a semi colon
                        url.append(name);
                        url.append("=");
                        url.append(val);
                        url.append(";");
                    }
                }
            }

        }
        // insert the special items in the front in the reverse order.
        // This way we will get the expected form as below.
        // MYSERVER\INSTANCEFOO:1433
        // port number first, we should always have port number
        url.insert(0, ";");
        url.insert(0, portNumber);
        url.insert(0, ":");
        if (null != instanceName) {
            url.insert(0, instanceName);
            url.insert(0, "\\");
        }
        url.insert(0, serverName);

        url.insert(0, urlprefix); // insert the prefix at the front.
        return (url.toString());
    }

    @Override
    public String getUserName() throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        String result = "";
        try (SQLServerStatement s = (SQLServerStatement) connection.createStatement();
                SQLServerResultSet rs = s.executeQueryInternal("select system_user")) {
            // Select system_user will always return a row.
            boolean next = rs.next();
            assert next;
            result = rs.getString(1);
        }
        return result;
    }

    private static final String[] getVersionColumnsColumnNames = { /* 1 */ SCOPE, /* 2 */ COLUMN_NAME,
            /* 3 */ DATA_TYPE, /* 4 */ TYPE_NAME, /* 5 */ COLUMN_SIZE, /* 6 */ BUFFER_LENGTH, /* 7 */ DECIMAL_DIGITS,
            /* 8 */ PSEUDO_COLUMN};

    @Override
    public java.sql.ResultSet getVersionColumns(String catalog, String schema,
            String table) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        /*
         * sp_special_columns [@table_name =] 'table_name' [,[@table_owner =] 'table_owner'] [,[@qualifier =]
         * 'qualifier'] [,[@col_type =] 'col_type'] [,[@scope =] 'scope'] [,[@nullable =] 'nullable'] [,[@ODBCVer =]
         * 'ODBCVer'] ;
         */
        String[] arguments = new String[7];
        arguments[0] = table;
        arguments[1] = schema;
        arguments[2] = catalog;
        arguments[3] = "V"; // col type
        arguments[4] = "T"; // scope
        arguments[5] = "U"; // nullable
        arguments[6] = "3"; // odbc ver
        SQLServerResultSet rs = getResultSetWithProvidedColumnNames(catalog, CallableHandles.SP_SPECIAL_COLUMNS,
                arguments, getVersionColumnsColumnNames);

        // Hook in a filter on the DATA_TYPE column of the result set we're
        // going to return that converts the ODBC values from sp_columns
        // into JDBC values.
        if (null != rs) {
            rs.getColumn(3).setFilter(new DataTypeFilter());
        }
        return rs;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean isReadOnly() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLServerException {
        checkClosed();
        switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
            case SQLServerConnection.TRANSACTION_SNAPSHOT:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean supportsTransactions() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        switch (type) {
            case ResultSet.TYPE_FORWARD_ONLY:
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                // case SQLServerResultSet.TYPE_SS_SCROLL_STATIC: insensitive
                // synonym
                // case SQLServerResultSet.TYPE_SS_SCROLL_KEYSET: sensitive
                // synonym
            case SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY:
            case SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY:
            case SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        checkConcurrencyType(concurrency);
        switch (type) {
            case ResultSet.TYPE_FORWARD_ONLY:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                // case SQLServerResultSet.TYPE_SS_SCROLL_KEYSET: sensitive
                // synonym
            case SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC:
            case SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY:
                return true;
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                // case SQLServerResultSet.TYPE_SS_SCROLL_STATIC: sensitive
                // synonym
            case SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY:
                return (ResultSet.CONCUR_READ_ONLY == concurrency);
            default:
                // per spec if we do not know we do not support.
                return false;
        }
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return (type == SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC || SQLServerResultSet.TYPE_FORWARD_ONLY == type
                || SQLServerResultSet.TYPE_SCROLL_SENSITIVE == type || SQLServerResultSet.TYPE_SS_SCROLL_KEYSET == type
                || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == type);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return (type == SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC || SQLServerResultSet.TYPE_FORWARD_ONLY == type
                || SQLServerResultSet.TYPE_SCROLL_SENSITIVE == type || SQLServerResultSet.TYPE_SS_SCROLL_KEYSET == type
                || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == type);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return (type == SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC || SQLServerResultSet.TYPE_FORWARD_ONLY == type
                || SQLServerResultSet.TYPE_SCROLL_SENSITIVE == type || SQLServerResultSet.TYPE_SS_SCROLL_KEYSET == type
                || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == type);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return (type == SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC || SQLServerResultSet.TYPE_FORWARD_ONLY == type
                || SQLServerResultSet.TYPE_SCROLL_SENSITIVE == type || SQLServerResultSet.TYPE_SS_SCROLL_KEYSET == type
                || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == type);
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return (type == SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC || SQLServerResultSet.TYPE_FORWARD_ONLY == type
                || SQLServerResultSet.TYPE_SCROLL_SENSITIVE == type || SQLServerResultSet.TYPE_SS_SCROLL_KEYSET == type
                || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == type);
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return (type == SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC || SQLServerResultSet.TYPE_FORWARD_ONLY == type
                || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == type);
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return (SQLServerResultSet.TYPE_SS_SCROLL_KEYSET == type);
    }

    // Check the result types to make sure the user does not pass a bad value.
    private void checkResultType(int type) throws SQLServerException {
        switch (type) {
            case ResultSet.TYPE_FORWARD_ONLY:
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                // case SQLServerResultSet.TYPE_SS_SCROLL_STATIC: synonym
                // TYPE_SCROLL_INSENSITIVE
                // case SQLServerResultSet.TYPE_SS_SCROLL_KEYSET: synonym
                // TYPE_SCROLL_SENSITIVE
            case SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY:
            case SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY:
            case SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC:
                return;
            default:
                // if the value is outside of the valid values throw error.
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
                Object[] msgArgs = {type};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
        }
    }

    // Check the concurrency values and make sure the value is a supported
    // value.
    private void checkConcurrencyType(int type) throws SQLServerException {
        switch (type) {
            case ResultSet.CONCUR_READ_ONLY:
            case ResultSet.CONCUR_UPDATABLE:
                // case SQLServerResultSet.CONCUR_SS_OPTIMISTIC_CC: synonym
                // CONCUR_UPDATABLE
            case SQLServerResultSet.CONCUR_SS_SCROLL_LOCKS:
            case SQLServerResultSet.CONCUR_SS_OPTIMISTIC_CCVAL:
                return;
            default:
                // if the value is outside of the valid values throw error.
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
                Object[] msgArgs = {type};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
        }
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLServerException {
        checkClosed();
        checkResultType(type);
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public java.sql.ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
            int[] types) throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        return getResultSetFromInternalQueries(catalog, "SELECT" +
        /* 1 */ " cast(NULL as char(1)) as TYPE_CAT," +
        /* 2 */ " cast(NULL as char(1)) as TYPE_SCHEM," +
        /* 3 */ " cast(NULL as char(1)) as TYPE_NAME," +
        /* 4 */ " cast(NULL as char(1)) as CLASS_NAME," +
        /* 5 */ " cast(0 as int) as DATA_TYPE," +
        /* 6 */ " cast(NULL as char(1)) as REMARKS," +
        /* 7 */ " cast(0 as smallint) as BASE_TYPE" + " where 0 = 1");
    }

    @Override
    public java.sql.Connection getConnection() throws SQLServerException {
        checkClosed();
        return connection.getConnection();
    }

    /* JDBC 3.0 */

    @Override
    public int getSQLStateType() throws SQLServerException {
        checkClosed();
        if (null != connection && connection.xopenStates)
            return sqlStateXOpen;
        else
            return sqlStateSQL99;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLServerException {
        checkClosed();
        String s = connection.sqlServerVersion;
        int p = s.indexOf('.');
        if (p > 0)
            s = s.substring(0, p);
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLServerException {
        checkClosed();
        String s = connection.sqlServerVersion;
        int p = s.indexOf('.');
        int q = s.indexOf('.', p + 1);
        if (p > 0 && q > 0)
            s = s.substring(p + 1, q);
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLServerException {
        checkClosed();
        return DriverJDBCVersion.major;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLServerException {
        checkClosed();
        return DriverJDBCVersion.minor;
    }

    @Override
    public int getResultSetHoldability() throws SQLServerException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT; // Hold over commit is the
                                                   // default for SQL Server
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        checkClosed();
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLServerException {
        checkClosed();
        if (ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability || ResultSet.CLOSE_CURSORS_AT_COMMIT == holdability) {
            return true; // supported one a per connection level only, not
                         // statement by statement
        }

        // if the value is outside of the valid values throw error.
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
        Object[] msgArgs = {holdability};
        throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        return getResultSetFromInternalQueries(catalog, "SELECT" +
        /* 1 */ " cast(NULL as char(1)) as TYPE_CAT," +
        /* 2 */ " cast(NULL as char(1)) as TYPE_SCHEM," +
        /* 3 */ " cast(NULL as char(1)) as TYPE_NAME," +
        /* 4 */ " cast(NULL as char(1)) as ATTR_NAME," +
        /* 5 */ " cast(0 as int) as DATA_TYPE," +
        /* 6 */ " cast(NULL as char(1)) as ATTR_TYPE_NAME," +
        /* 7 */ " cast(0 as int) as ATTR_SIZE," +
        /* 8 */ " cast(0 as int) as DECIMAL_DIGITS," +
        /* 9 */ " cast(0 as int) as NUM_PREC_RADIX," +
        /* 10 */ " cast(0 as int) as NULLABLE," +
        /* 11 */ " cast(NULL as char(1)) as REMARKS," +
        /* 12 */ " cast(NULL as char(1)) as ATTR_DEF," +
        /* 13 */ " cast(0 as int) as SQL_DATA_TYPE," +
        /* 14 */ " cast(0 as int) as SQL_DATETIME_SUB," +
        /* 15 */ " cast(0 as int) as CHAR_OCTET_LENGTH," +
        /* 16 */ " cast(0 as int) as ORDINAL_POSITION," +
        /* 17 */ " cast(NULL as char(1)) as IS_NULLABLE," +
        /* 18 */ " cast(NULL as char(1)) as SCOPE_CATALOG," +
        /* 19 */ " cast(NULL as char(1)) as SCOPE_SCHEMA," +
        /* 20 */ " cast(NULL as char(1)) as SCOPE_TABLE," +
        /* 21 */ " cast(0 as smallint) as SOURCE_DATA_TYPE" + " where 0 = 1");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        return getResultSetFromInternalQueries(catalog, "SELECT" +
        /* 1 */ " cast(NULL as char(1)) as TYPE_CAT," +
        /* 2 */ " cast(NULL as char(1)) as TYPE_SCHEM," +
        /* 3 */ " cast(NULL as char(1)) as TYPE_NAME," +
        /* 4 */ " cast(NULL as char(1)) as SUPERTABLE_NAME" + " where 0 = 1");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
            String typeNamePattern) throws SQLException, SQLTimeoutException {
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        return getResultSetFromInternalQueries(catalog, "SELECT" +
        /* 1 */ " cast(NULL as char(1)) as TYPE_CAT," +
        /* 2 */ " cast(NULL as char(1)) as TYPE_SCHEM," +
        /* 3 */ " cast(NULL as char(1)) as TYPE_NAME," +
        /* 4 */ " cast(NULL as char(1)) as SUPERTYPE_CAT," +
        /* 5 */ " cast(NULL as char(1)) as SUPERTYPE_SCHEM," +
        /* 6 */ " cast(NULL as char(1)) as SUPERTYPE_NAME" + " where 0 = 1");
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLServerException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsSavepoints() throws SQLServerException {
        checkClosed();
        return true;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        checkClosed();
        return true;
    }

    /* -------------- MSSQL-JDBC Extension methods start here --------------- */

    /**
     * Returns the database compatibility level setting for the current database. This is useful if the database's
     * compatibility level is lower than the engine version. In this case the database will only support SQL commands at
     * its compatibility level, and not the wider set of commands accepted by the engine.
     *
     * @return the database compatibility level value (from sys.databases table).
     * @throws SQLException
     *         if error getting compatability level
     */
    public int getDatabaseCompatibilityLevel() throws SQLException {
        checkClosed();
        String database = connection.getCatalog();
        SQLServerResultSet rs = getResultSetFromInternalQueries(null,
                "select name, compatibility_level from sys.databases where name = '" + database + "'");
        if (!rs.next()) {
            return 0;
        }
        return rs.getInt("compatibility_level");
    }
}


/**
 * Provides filter to convert DATA_TYPE column values from the ODBC types returned by SQL Server to their equivalent
 * JDBC types.
 */
final class DataTypeFilter extends IntColumnFilter {
    private static final int ODBC_SQL_GUID = -11;
    private static final int ODBC_SQL_WCHAR = -8;
    private static final int ODBC_SQL_WVARCHAR = -9;
    private static final int ODBC_SQL_WLONGVARCHAR = -10;
    private static final int ODBC_SQL_FLOAT = 6;
    private static final int ODBC_SQL_TIME = -154;
    private static final int ODBC_SQL_XML = -152;
    private static final int ODBC_SQL_UDT = -151;

    int oneValueToAnother(int odbcType) {
        switch (odbcType) {
            case ODBC_SQL_FLOAT:
                return JDBCType.DOUBLE.asJavaSqlType();
            case ODBC_SQL_GUID:
                return JDBCType.CHAR.asJavaSqlType();
            case ODBC_SQL_WCHAR:
                return JDBCType.NCHAR.asJavaSqlType();
            case ODBC_SQL_WVARCHAR:
                return JDBCType.NVARCHAR.asJavaSqlType();
            case ODBC_SQL_WLONGVARCHAR:
                return JDBCType.LONGNVARCHAR.asJavaSqlType();
            case ODBC_SQL_TIME:
                return JDBCType.TIME.asJavaSqlType();
            case ODBC_SQL_XML:
                return SSType.XML.getJDBCType().asJavaSqlType();
            case ODBC_SQL_UDT:
                return SSType.UDT.getJDBCType().asJavaSqlType();
            default:
                return odbcType;
        }
    }
}


class ZeroFixupFilter extends IntColumnFilter {
    int oneValueToAnother(int precl) {
        if (0 == precl)
            return DataTypes.MAX_VARTYPE_MAX_BYTES;
        else
            return precl;
    }
}


/**
 * Converts one value to another solely based on the column integer value. Apply to integer columns only
 */
abstract class IntColumnFilter extends ColumnFilter {
    abstract int oneValueToAnother(int value);

    final Object apply(Object value, JDBCType asJDBCType) throws SQLServerException {
        if (null == value)
            return value;
        // Assumption: values will only be requested in integral or textual
        // format
        // (i.e. not as float, double, BigDecimal, Boolean or bytes). A request
        // to return
        // a value as anything else results in an exception being thrown.

        switch (asJDBCType) {
            case INTEGER:
                return oneValueToAnother((Integer) value);
            case SMALLINT: // small and tinyint returned as short
            case TINYINT:
                return (short) oneValueToAnother(((Short) value).intValue());
            case BIGINT:
                return (long) oneValueToAnother(((Long) value).intValue());
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                return Integer.toString(oneValueToAnother(Integer.parseInt((String) value)));
            default:
                DataTypes.throwConversionError("int", asJDBCType.toString());
                return value;
        }
    }

}


/**
 * Provides filter to convert int identity column values from 0,1 to YES, NO There is a mismatch between what the stored
 * proc returns and what the JDBC spec expects.
 */
class IntColumnIdentityFilter extends ColumnFilter {
    final Object apply(Object value, JDBCType asJDBCType) throws SQLServerException {
        if (null == value)
            return value;
        // Assumption: values will only be requested in integral or textual
        // format
        // (i.e. not as float, double, BigDecimal, Boolean or bytes). A request
        // to return
        // a value as anything else results in an exception being thrown.

        switch (asJDBCType) {
            case INTEGER:
            case SMALLINT:
                // This is a way for us to make getObject return a string, not
                // an
                // integer. What this means is that getInt/getShort also will
                // return a string.
                // However the identity column in the JDBC spec is supposed to
                // return a
                // string by default. To get to that default behavior right we
                // are deliberately breaking
                // the getInt/getShort behavior which should really error
                // anyways. Only thing is that
                // the user will get a cast exception in this case.
                assert (value instanceof Number);
                return Util.zeroOneToYesNo(((Number) value).intValue());
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                assert (value instanceof String);
                return Util.zeroOneToYesNo(Integer.parseInt((String) value));
            default:
                DataTypes.throwConversionError("char", asJDBCType.toString());
                return value;
        }
    }
}
