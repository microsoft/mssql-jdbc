/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQLServerParameterMetaData provides JDBC 3.0 meta data for prepared statement parameters.
 *
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API interfaces javadoc for those
 * details.
 *
 * Prepared statements are executed with SET FMT ONLY to retrieve column meta data Callable statements : sp_sp_sproc_columns is called to retrieve
 * names and meta data for the procedures params.
 */

public final class SQLServerParameterMetaData implements ParameterMetaData {

    private final static int SQL_SERVER_2012_VERSION = 11;

    private final SQLServerStatement stmtParent;
    private SQLServerConnection con;

    /* Used for callable statement meta data */
    private Statement stmtCall;
    private SQLServerResultSet rsProcedureMeta;

    static final private java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerParameterMetaData");

    static private final AtomicInteger baseID = new AtomicInteger(0);	// Unique id generator for each instance (used for logging).
    final private String traceID = " SQLServerParameterMetaData:" + nextInstanceID();
    boolean isTVP = false;

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    /**
     * This is a helper function to provide an ID string suitable for tracing.
     * 
     * @return traceID string
     */
    final public String toString() {
        return traceID;
    }

    /**
     * Parse the columns in a column set.
     * 
     * @param columnSet
     *            the list of columns
     * @param columnStartToken
     *            the token that prfixes the column set
     */
    /* L2 */ private String parseColumns(String columnSet,
            String columnStartToken) {
        StringTokenizer st = new StringTokenizer(columnSet, " =?<>!", true);
        final int START = 0;
        final int PARAMNAME = 1;
        final int PARAMVALUE = 2;

        int nState = 0;
        String sLastField = null;
        StringBuilder sb = new StringBuilder();

        while (st.hasMoreTokens()) {

            String sToken = st.nextToken();
            if (sToken.equalsIgnoreCase(columnStartToken)) {
                nState = PARAMNAME;
                continue;
            }
            if (nState == START)
                continue;
            if ((sToken.charAt(0) == '=') || sToken.equalsIgnoreCase("is") || (sToken.charAt(0) == '<') || (sToken.charAt(0) == '>')
                    || sToken.equalsIgnoreCase("like") || sToken.equalsIgnoreCase("not") || sToken.equalsIgnoreCase("in")
                    || (sToken.charAt(0) == '!')) {
                nState = PARAMVALUE;
                continue;
            }
            if (sToken.charAt(0) == '?' && sLastField != null) {
                if (sb.length() != 0) {
                    sb.append(", ");
                }
                sb.append(sLastField);
                nState = PARAMNAME;
                sLastField = null;
                continue;
            }
            if (nState == PARAMNAME) {
                // space get the next token.
                if (sToken.equals(" "))
                    continue;
                String paramN = escapeParse(st, sToken);
                if (paramN.length() > 0) {
                    sLastField = paramN;
                }
            }
        }

        return sb.toString();
    }

    /**
     * Parse the column set in an insert syntax.
     * 
     * @param sql
     *            the sql syntax
     * @param columnMarker
     *            the token that denotes the start of the column set
     */
    /* L2 */ private String parseInsertColumns(String sql,
            String columnMarker) {
        StringTokenizer st = new StringTokenizer(sql, " (),", true);
        int nState = 0;
        String sLastField = null;
        StringBuilder sb = new StringBuilder();

        while (st.hasMoreTokens()) {
            String sToken = st.nextToken();
            if (sToken.equalsIgnoreCase(columnMarker)) {
                nState = 1;
                continue;
            }
            if (nState == 0)
                continue;
            if (sToken.charAt(0) == '=') {
                nState = 2;
                continue;
            }
            if ((sToken.charAt(0) == ',' || sToken.charAt(0) == ')' || sToken.charAt(0) == ' ') && sLastField != null) {
                if (sb.length() != 0)
                    sb.append(", ");
                sb.append(sLastField);
                nState = 1;
                sLastField = null;
            }
            if (sToken.charAt(0) == ')') {
                nState = 0;
                break;
            }
            if (nState == 1) {
                if (sToken.trim().length() > 0) {
                    if (sToken.charAt(0) != ',')
                        sLastField = escapeParse(st, sToken);
                }
            }
        }

        return sb.toString();
    }

    /* Used for prepared statement meta data */
    class QueryMeta {
        String parameterClassName = null;
        int parameterType = 0;
        String parameterTypeName = null;
        int precision = 0;
        int scale = 0;
        int isNullable = ParameterMetaData.parameterNullableUnknown;
        boolean isSigned = false;
    }

    Map<Integer, QueryMeta> queryMetaMap = null;

    /*
     * Parse query metadata.
     */
    private void parseQueryMeta(ResultSet rsQueryMeta) throws SQLServerException {
        Pattern datatypePattern = Pattern.compile("(.*)\\((.*)(\\)|,(.*)\\))");
        try {
            while (rsQueryMeta.next()) {
                QueryMeta qm = new QueryMeta();
                SSType ssType = null;

                int paramOrdinal = rsQueryMeta.getInt("parameter_ordinal");
                String typename = rsQueryMeta.getString("suggested_system_type_name");

                if (null == typename) {
                    typename = rsQueryMeta.getString("suggested_user_type_name");
                    SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                            .prepareCall("select max_length, precision, scale, is_nullable from sys.assembly_types where name = ?");
                    pstmt.setNString(1, typename);
                    ResultSet assemblyRs = pstmt.executeQuery();
                    if (assemblyRs.next()) {
                        qm.parameterTypeName = typename;
                        qm.precision = assemblyRs.getInt("max_length");
                        qm.scale = assemblyRs.getInt("scale");
                        ssType = SSType.UDT;
                    }
                }
                else {
                    qm.precision = rsQueryMeta.getInt("suggested_precision");
                    qm.scale = rsQueryMeta.getInt("suggested_scale");

                    Matcher matcher = datatypePattern.matcher(typename);
                    if (matcher.matches()) {
                        // the datatype has some precision/scale defined explicitly.
                        ssType = SSType.of(matcher.group(1));
                        if (typename.equalsIgnoreCase("varchar(max)") || typename.equalsIgnoreCase("varbinary(max)")) {
                            qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE;
                        }
                        else if (typename.equalsIgnoreCase("nvarchar(max)")) {
                            qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE / 2;
                        }
                        else if (SSType.Category.CHARACTER == ssType.category || SSType.Category.BINARY == ssType.category
                                || SSType.Category.NCHARACTER == ssType.category) {
                            try {
                                // For character/binary data types "suggested_precision" is 0. So get the precision from the type itself.
                                qm.precision = Integer.parseInt(matcher.group(2));
                            }
                            catch (NumberFormatException e) {
                                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_metaDataErrorForParameter"));
                                Object[] msgArgs = {new Integer(paramOrdinal)};
                                SQLServerException.makeFromDriverError(con, stmtParent, form.format(msgArgs) + " " + e.toString(), null, false);
                            }
                        }
                    }
                    else
                        ssType = SSType.of(typename);

                    // For float and real types suggested_precision returns the number of bits, not digits.
                    if (SSType.FLOAT == ssType) {
                        // https://msdn.microsoft.com/en-CA/library/ms173773.aspx
                        // real is float(24) and is 7 digits. Float is 15 digits.
                        qm.precision = 15;
                    }
                    else if (SSType.REAL == ssType) {
                        qm.precision = 7;
                    }
                    else if (SSType.TEXT == ssType) {
                        qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE;
                    }
                    else if (SSType.NTEXT == ssType) {
                        qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE / 2;
                    }
                    else if (SSType.IMAGE == ssType) {
                        qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE;
                    }
                    else if (SSType.GUID == ssType) {
                        qm.precision = SQLServerDatabaseMetaData.uniqueidentifierSize;
                    }
                    else if (SSType.TIMESTAMP == ssType) {
                        qm.precision = 8;
                    }
                    else if (SSType.XML == ssType) {
                        qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE / 2;
                    }

                    qm.parameterTypeName = ssType.toString();
                }

                // Check if ssType is null. Was caught by static analysis.
                if (null == ssType) {
                    throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"), null);
                }

                JDBCType jdbcType = ssType.getJDBCType();
                qm.parameterClassName = jdbcType.className();
                qm.parameterType = jdbcType.getIntValue();
                // The parameter can be signed if it is a NUMERIC type (except bit or tinyint).
                qm.isSigned = ((SSType.Category.NUMERIC == ssType.category) && (SSType.BIT != ssType) && (SSType.TINYINT != ssType));
                queryMetaMap.put(paramOrdinal, qm);
            }
        }
        catch (SQLException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"), e);
        }
    }

    private void parseQueryMetaFor2008(ResultSet rsQueryMeta) throws SQLServerException {
        ResultSetMetaData md;

        try {
            md = rsQueryMeta.getMetaData();

            for (int i = 1; i <= md.getColumnCount(); i++) {
                QueryMeta qm = new QueryMeta();

                qm.parameterClassName = md.getColumnClassName(i);
                qm.parameterType = md.getColumnType(i);
                qm.parameterTypeName = md.getColumnTypeName(i);
                qm.precision = md.getPrecision(i);
                qm.scale = md.getScale(i);
                qm.isNullable = md.isNullable(i);
                qm.isSigned = md.isSigned(i);

                queryMetaMap.put(i, qm);
            }

        }
        catch (SQLException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"), e);
        }
    }

    /**
     * Escape parser, using the tokenizer tokenizes escaped strings properly e.g.[Table Name, ]
     * 
     * @param st
     *            string tokenizer
     * @param firstToken
     * @returns the full token
     */
    private String escapeParse(StringTokenizer st,
            String firstToken) {
        String nameFragment;
        String fullName;
        nameFragment = firstToken;
        // skip spaces
        while (nameFragment.equals(" ") && st.hasMoreTokens()) {
            nameFragment = st.nextToken();
        }
        fullName = nameFragment;
        if (nameFragment.charAt(0) == '[' && nameFragment.charAt(nameFragment.length() - 1) != ']') {
            while (st.hasMoreTokens()) {
                nameFragment = st.nextToken();
                fullName = fullName.concat(nameFragment);
                if (nameFragment.charAt(nameFragment.length() - 1) == ']') {
                    break;
                }

            }
        }
        fullName = fullName.trim();
        return fullName;
    }

    private class MetaInfo {
        String table;
        String fields;

        MetaInfo(String table,
                String fields) {
            this.table = table;
            this.fields = fields;
        }
    }

    /**
     * Parse a SQL syntax.
     * 
     * @param sql
     *            String
     * @param sTableMarker
     *            the location of the table in the syntax
     */
    private MetaInfo parseStatement(String sql,
            String sTableMarker) {
        StringTokenizer st = new StringTokenizer(sql, " ,\r\n", true);

        /* Find the table */

        String metaTable = null;
        String metaFields = "";
        while (st.hasMoreTokens()) {
            String sToken = st.nextToken().trim();

            if(sToken.contains("*/")){
                sToken = removeCommentsInTheBeginning(sToken, 0, 0, "/*", "*/");
            }

            if (sToken.equalsIgnoreCase(sTableMarker)) {
                if (st.hasMoreTokens()) {
                    metaTable = escapeParse(st, st.nextToken());
                    break;
                }
            }
        }

        if (null != metaTable) {
            if (sTableMarker.equalsIgnoreCase("UPDATE"))
                metaFields = parseColumns(sql, "SET"); // Get the set fields
            else if (sTableMarker.equalsIgnoreCase("INTO")) // insert
                metaFields = parseInsertColumns(sql, "("); // Get the value fields
            else
                metaFields = parseColumns(sql, "WHERE"); // Get the where fields

            return new MetaInfo(metaTable, metaFields);
        }

        return null;
    }

    /**
     * Parse a SQL syntax.
     * 
     * @param sql
     *            the syntax
     * @throws SQLServerException
     */
    private MetaInfo parseStatement(String sql) throws SQLServerException {
        StringTokenizer st = new StringTokenizer(sql, " ");
        if (st.hasMoreTokens()) {
            String sToken = st.nextToken().trim();

            // filter out multiple line comments in the beginning of the query
            if (sToken.contains("/*")) {
                String sqlWithoutCommentsInBeginning = removeCommentsInTheBeginning(sql, 0, 0, "/*", "*/");
                return parseStatement(sqlWithoutCommentsInBeginning);
            }

            // filter out single line comments in the beginning of the query
            if (sToken.contains("--")) {
                String sqlWithoutCommentsInBeginning = removeCommentsInTheBeginning(sql, 0, 0, "--", "\n");
                return parseStatement(sqlWithoutCommentsInBeginning);
            }

            if (sToken.equalsIgnoreCase("INSERT"))
                return parseStatement(sql, "INTO"); // INTO marks the table name

            if (sToken.equalsIgnoreCase("UPDATE"))
                return parseStatement(sql, "UPDATE");

            if (sToken.equalsIgnoreCase("SELECT"))
                return parseStatement(sql, "FROM");

            if (sToken.equalsIgnoreCase("DELETE"))
                return parseStatement(sql, "FROM");
        }

        return null;
    }
    
    private String removeCommentsInTheBeginning(String sql,
            int startCommentMarkCount,
            int endCommentMarkCount,
            String startMark,
            String endMark) {
        int startCommentMarkIndex = sql.indexOf(startMark);
        int endCommentMarkIndex = sql.indexOf(endMark);

        if (-1 == startCommentMarkIndex) {
            startCommentMarkIndex = Integer.MAX_VALUE;
        }
        if (-1 == endCommentMarkIndex) {
            endCommentMarkIndex = Integer.MAX_VALUE;
        }

        // Base case. startCommentMarkCount is guaranteed to be bigger than 0 because the method is called when /* occurs
        if (startCommentMarkCount == endCommentMarkCount) {
            if (startCommentMarkCount != 0 && endCommentMarkCount != 0) {
                return sql;
            }
        }

        // filter out first start comment mark
        if (startCommentMarkIndex < endCommentMarkIndex) {
            String sqlWithoutCommentsInBeginning = sql.substring(startCommentMarkIndex + startMark.length());
            return removeCommentsInTheBeginning(sqlWithoutCommentsInBeginning, ++startCommentMarkCount, endCommentMarkCount, startMark, endMark);
        }
        // filter out first end comment mark
        else {
            String sqlWithoutCommentsInBeginning = sql.substring(endCommentMarkIndex + endMark.length());
            return removeCommentsInTheBeginning(sqlWithoutCommentsInBeginning, startCommentMarkCount, ++endCommentMarkCount, startMark, endMark);
        }
    }

    String parseThreePartNames(String threeName) throws SQLServerException {
        int noofitems = 0;
        String procedureName = null;
        String procedureOwner = null;
        String procedureQualifier = null;
        StringTokenizer st = new StringTokenizer(threeName, ".", true);

        // parse left to right looking for three part name
        // note the user can provide three part, two part or one part name
        while (st.hasMoreTokens()) {
            String sToken = st.nextToken();
            String nextItem = escapeParse(st, sToken);
            if (nextItem.equals(".") == false) {
                switch (noofitems) {
                    case 2:
                        procedureQualifier = procedureOwner;
                        procedureOwner = procedureName;
                        procedureName = nextItem;
                        noofitems++;
                        break;
                    case 1:
                        procedureOwner = procedureName;
                        procedureName = nextItem;
                        noofitems++;
                        break;
                    case 0:
                        procedureName = nextItem;
                        noofitems++;
                        break;
                    default:
                        noofitems++;
                        break;
                }
            }
        }
        StringBuilder sb = new StringBuilder(100);

        if (noofitems > 3 && 1 < noofitems)
            SQLServerException.makeFromDriverError(con, stmtParent, SQLServerException.getErrString("R_noMetadata"), null, false);

        switch (noofitems) {
            case 3:
                sb.append("@procedure_qualifier =");
                sb.append(procedureQualifier);
                sb.append(", ");
                sb.append("@procedure_owner =");
                sb.append(procedureOwner);
                sb.append(", ");
                sb.append("@procedure_name =");
                sb.append(procedureName);
                sb.append(", ");
                break;

            case 2:
                sb.append("@procedure_owner =");
                sb.append(procedureOwner);
                sb.append(", ");
                sb.append("@procedure_name =");
                sb.append(procedureName);
                sb.append(", ");
                break;
            case 1:
                sb.append("@procedure_name =");
                sb.append(procedureName);
                sb.append(", ");
                break;
            default:
                break;
        }
        return sb.toString();

    }

    private void checkClosed() throws SQLServerException {
        stmtParent.checkClosed();
    }

    /**
     * Create new parameter meta data.
     * 
     * @param st
     *            the prepared statement
     * @param sProcString
     *            the pricedure name
     * @throws SQLServerException
     */
    SQLServerParameterMetaData(SQLServerStatement st,
            String sProcString) throws SQLServerException {

        assert null != st;
        stmtParent = st;
        con = st.connection;
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine(toString() + " created by (" + st.toString() + ")");
        }
        try {

            // If the CallableStatement/PreparedStatement is a stored procedure call
            // then we can extract metadata using sp_sproc_columns
            if (null != st.procedureName) {
                SQLServerStatement s = (SQLServerStatement) con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                String sProc = parseThreePartNames(st.procedureName);
                if (con.isKatmaiOrLater())
                    rsProcedureMeta = s.executeQueryInternal("exec sp_sproc_columns_100 " + sProc + " @ODBCVer=3");
                else
                    rsProcedureMeta = s.executeQueryInternal("exec sp_sproc_columns " + sProc + " @ODBCVer=3");
                // Sixth is DATA_TYPE
                rsProcedureMeta.getColumn(6).setFilter(new DataTypeFilter());
                if (con.isKatmaiOrLater()) {
                    rsProcedureMeta.getColumn(8).setFilter(new ZeroFixupFilter());
                    rsProcedureMeta.getColumn(9).setFilter(new ZeroFixupFilter());
                    rsProcedureMeta.getColumn(17).setFilter(new ZeroFixupFilter());
                }
            }

            // Otherwise we just have a parameterized statement.
            // if SQL server version is 2012 and above use stored
            // procedure "sp_describe_undeclared_parameters" to retrieve parameter meta data
            // if SQL server version is 2008, then use FMTONLY
            else {
                queryMetaMap = new HashMap<Integer, QueryMeta>();

                if (con.getServerMajorVersion() >= SQL_SERVER_2012_VERSION) {
                    // new implementation for SQL verser 2012 and above
                    String preparedSQL = con.replaceParameterMarkers(((SQLServerPreparedStatement) stmtParent).userSQL,
                            ((SQLServerPreparedStatement) stmtParent).inOutParam, ((SQLServerPreparedStatement) stmtParent).bReturnValueSyntax);

                    SQLServerCallableStatement cstmt = (SQLServerCallableStatement) con.prepareCall("exec sp_describe_undeclared_parameters ?");
                    cstmt.setNString(1, preparedSQL);
                    parseQueryMeta(cstmt.executeQueryInternal());
                    cstmt.close();
                }
                else {
                    // old implementation for SQL server 2008
                    MetaInfo metaInfo = parseStatement(sProcString);
                    if (null == metaInfo) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_cantIdentifyTableMetadata"));
                        Object[] msgArgs = {sProcString};
                        SQLServerException.makeFromDriverError(con, stmtParent, form.format(msgArgs), null, false);
                    }

                    if (metaInfo.fields.length() <= 0)
                        return;

                    Statement stmt = con.createStatement();
                    String sCom = "sp_executesql N'SET FMTONLY ON SELECT " + metaInfo.fields + " FROM " + metaInfo.table + " WHERE 1 = 2'";
                    ResultSet rs = stmt.executeQuery(sCom);
                    parseQueryMetaFor2008(rs);
                    stmt.close();
                    rs.close();
                }
            }
        }
        // Do not need to wrapper SQLServerException again
        catch (SQLServerException e) {
            throw e;
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
        }
        catch(StringIndexOutOfBoundsException e){
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        boolean f = iface.isInstance(this);
        return f;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        T t;
        try {
            t = iface.cast(this);
        }
        catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        return t;
    }

    /* L2 */ private void verifyParameterPosition(int param) throws SQLServerException {
        boolean bFound = false;
        try {
            if (((SQLServerPreparedStatement) stmtParent).bReturnValueSyntax && isTVP) {
                bFound = rsProcedureMeta.absolute(param);
            }
            else {
                bFound = rsProcedureMeta.absolute(param + 1);  // Note row 1 is the 'return value' meta data
            }
        }
        catch (SQLException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_metaDataErrorForParameter"));
            Object[] msgArgs = {new Integer(param)};
            SQLServerException.makeFromDriverError(con, stmtParent, form.format(msgArgs) + " " + e.toString(), null, false);
        }
        if (!bFound) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidParameterNumber"));
            Object[] msgArgs = {new Integer(param)};
            SQLServerException.makeFromDriverError(con, stmtParent, form.format(msgArgs), null, false);
        }
    }

    /* L2 */ private void checkParam(int n) throws SQLServerException {
        if (!queryMetaMap.containsKey(n)) {
            SQLServerException.makeFromDriverError(con, stmtParent, SQLServerException.getErrString("R_noMetadata"), null, false);
        }
    }

    /* L2 */ public String getParameterClassName(int param) throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement.
                checkParam(param);
                return queryMetaMap.get(param).parameterClassName;
            }
            else {
                verifyParameterPosition(param);
                JDBCType jdbcType = JDBCType.of(rsProcedureMeta.getShort("DATA_TYPE"));
                return jdbcType.className();
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return null;
        }
    }

    /* L2 */ public int getParameterCount() throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement
                return queryMetaMap.size();
            }
            else {
                rsProcedureMeta.last();
                int nCount = rsProcedureMeta.getRow() - 1;
                if (nCount < 0)
                    nCount = 0;
                return nCount;
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return 0;
        }
    }

    /* L2 */ public int getParameterMode(int param) throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                checkParam(param);
                // if it is not a stored proc, the param can only be input.
                return parameterModeIn;
            }
            else {
                verifyParameterPosition(param);
                int n = rsProcedureMeta.getInt("COLUMN_TYPE");
                switch (n) {
                    case 1:
                        return parameterModeIn;
                    case 2:
                        return parameterModeOut;
                    default:
                        return parameterModeUnknown;
                }
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return 0;
        }
    }

    /* L2 */ public int getParameterType(int param) throws SQLServerException {
        checkClosed();

        int parameterType;
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement.
                checkParam(param);
                parameterType = queryMetaMap.get(param).parameterType;
            }
            else {
                verifyParameterPosition(param);
                parameterType = rsProcedureMeta.getShort("DATA_TYPE");
            }

            switch (parameterType) {
                case microsoft.sql.Types.DATETIME:
                case microsoft.sql.Types.SMALLDATETIME:
                    parameterType = SSType.DATETIME2.getJDBCType().asJavaSqlType();
                    break;
                case microsoft.sql.Types.MONEY:
                case microsoft.sql.Types.SMALLMONEY:
                    parameterType = SSType.DECIMAL.getJDBCType().asJavaSqlType();
                    break;
                case microsoft.sql.Types.GUID:
                    parameterType = SSType.CHAR.getJDBCType().asJavaSqlType();
                    break;
            }

            return parameterType;
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return 0;
        }
    }

    /* L2 */ public String getParameterTypeName(int param) throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement.
                checkParam(param);
                return queryMetaMap.get(param).parameterTypeName;
            }
            else {
                verifyParameterPosition(param);
                return rsProcedureMeta.getString("TYPE_NAME");
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return null;
        }
    }

    /* L2 */ public int getPrecision(int param) throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement.
                checkParam(param);
                return queryMetaMap.get(param).precision;
            }
            else {
                verifyParameterPosition(param);
                int nPrec = rsProcedureMeta.getInt("PRECISION");
                return nPrec;
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return 0;
        }
    }

    /* L2 */ public int getScale(int param) throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement.
                checkParam(param);
                return queryMetaMap.get(param).scale;
            }
            else {
                verifyParameterPosition(param);
                int nScale = rsProcedureMeta.getInt("SCALE");
                return nScale;
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return 0;
        }
    }

    /* L2 */ public int isNullable(int param) throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement.
                checkParam(param);
                return queryMetaMap.get(param).isNullable;
            }
            else {
                verifyParameterPosition(param);
                int nNull = rsProcedureMeta.getInt("NULLABLE");
                if (nNull == 1)
                    return parameterNullable;
                if (nNull == 0)
                    return parameterNoNulls;
                return parameterNullableUnknown;
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return 0;
        }
    }

    /**
     * Verify a supplied parameter index is valid
     * 
     * @param param
     *            the param index
     * @throws SQLServerException
     *             when an error occurs
     * @return boolean
     */
    /* L2 */ public boolean isSigned(int param) throws SQLServerException {
        checkClosed();
        try {
            if (rsProcedureMeta == null) {
                // PreparedStatement.
                checkParam(param);
                return queryMetaMap.get(param).isSigned;
            }
            else {
                verifyParameterPosition(param);
                return JDBCType.of(rsProcedureMeta.getShort("DATA_TYPE")).isSigned();
            }
        }
        catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.toString(), null, false);
            return false;
        }
    }

    String getTVPSchemaFromStoredProcedure(int param) throws SQLServerException {
        checkClosed();
        verifyParameterPosition(param);
        return rsProcedureMeta.getString("SS_TYPE_SCHEMA_NAME");
    }
}
