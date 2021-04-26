/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Provides meta data for prepared statement parameters.
 *
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API
 * interfaces javadoc for those details.
 *
 * Prepared statements are executed with SET FMT ONLY to retrieve column meta data Callable statements :
 * sp_sp_sproc_columns is called to retrieve names and meta data for the procedures params.
 */

public final class SQLServerParameterMetaData implements ParameterMetaData {

    private final static int SQL_SERVER_2012_VERSION = 11;

    private final SQLServerPreparedStatement stmtParent;
    private SQLServerConnection con;

    private List<Map<String, Object>> procMetadata;

    boolean procedureIsFound = false;

    static final private java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerParameterMetaData");

    static private final AtomicInteger baseID = new AtomicInteger(0); // Unique id generator for each instance (used for
                                                                      // logging).
    final private String traceID = " SQLServerParameterMetaData:" + nextInstanceID();
    boolean isTVP = false;

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    /**
     * Provides a helper function to provide an ID string suitable for tracing.
     * 
     * @return traceID string
     */
    @Override
    final public String toString() {
        return traceID;
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
     * Parses query metadata.
     */
    private void parseQueryMeta(ResultSet rsQueryMeta) throws SQLServerException {
        Pattern datatypePattern = Pattern.compile("(.*)\\((.*)(\\)|,(.*)\\))");
        try {
            if (null != rsQueryMeta) {
                while (rsQueryMeta.next()) {
                    QueryMeta qm = new QueryMeta();
                    SSType ssType = null;

                    int paramOrdinal = rsQueryMeta.getInt("parameter_ordinal");
                    String typename = rsQueryMeta.getString("suggested_system_type_name");

                    if (null == typename) {
                        typename = rsQueryMeta.getString("suggested_user_type_name");
                        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(
                                "select max_length, precision, scale, is_nullable from sys.assembly_types where name = ?")) {
                            pstmt.setNString(1, typename);
                            try (ResultSet assemblyRs = pstmt.executeQuery()) {
                                if (assemblyRs.next()) {
                                    qm.parameterTypeName = typename;
                                    qm.precision = assemblyRs.getInt("max_length");
                                    qm.scale = assemblyRs.getInt("scale");
                                    ssType = SSType.UDT;
                                }
                            }
                        }
                    } else {
                        qm.precision = rsQueryMeta.getInt("suggested_precision");
                        qm.scale = rsQueryMeta.getInt("suggested_scale");

                        Matcher matcher = datatypePattern.matcher(typename);
                        if (matcher.matches()) {
                            // the datatype has some precision/scale defined explicitly.
                            ssType = SSType.of(matcher.group(1));
                            if ("varchar(max)".equalsIgnoreCase(typename)
                                    || "varbinary(max)".equalsIgnoreCase(typename)) {
                                qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE;
                            } else if ("nvarchar(max)".equalsIgnoreCase(typename)) {
                                qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE / 2;
                            } else if (SSType.Category.CHARACTER == ssType.category
                                    || SSType.Category.BINARY == ssType.category
                                    || SSType.Category.NCHARACTER == ssType.category) {
                                try {
                                    // For character/binary data types "suggested_precision" is 0. So get the precision
                                    // from
                                    // the type itself.
                                    qm.precision = Integer.parseInt(matcher.group(2));
                                } catch (NumberFormatException e) {
                                    MessageFormat form = new MessageFormat(
                                            SQLServerException.getErrString("R_metaDataErrorForParameter"));
                                    Object[] msgArgs = {paramOrdinal};
                                    SQLServerException.makeFromDriverError(con, stmtParent,
                                            form.format(msgArgs) + " " + e.getMessage(), null, false);
                                }
                            }
                        } else
                            ssType = SSType.of(typename);

                        // For float and real types suggested_precision returns the number of bits, not digits.
                        if (SSType.FLOAT == ssType) {
                            // https://msdn.microsoft.com/en-CA/library/ms173773.aspx
                            // real is float(24) and is 7 digits. Float is 15 digits.
                            qm.precision = 15;
                        } else if (SSType.REAL == ssType) {
                            qm.precision = 7;
                        } else if (SSType.TEXT == ssType) {
                            qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE;
                        } else if (SSType.NTEXT == ssType) {
                            qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE / 2;
                        } else if (SSType.IMAGE == ssType) {
                            qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE;
                        } else if (SSType.GUID == ssType) {
                            qm.precision = SQLServerDatabaseMetaData.uniqueidentifierSize;
                        } else if (SSType.TIMESTAMP == ssType) {
                            qm.precision = 8;
                        } else if (SSType.XML == ssType) {
                            qm.precision = SQLServerDatabaseMetaData.MAXLOBSIZE / 2;
                        }

                        qm.parameterTypeName = ssType.toString();
                    }

                    // Check if ssType is null. Was caught by static analysis.
                    if (null == ssType) {
                        throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"),
                                null);
                    }

                    JDBCType jdbcType = ssType.getJDBCType();
                    qm.parameterClassName = jdbcType.className();
                    qm.parameterType = jdbcType.getIntValue();
                    // The parameter can be signed if it is a NUMERIC type (except bit or tinyint).
                    qm.isSigned = ((SSType.Category.NUMERIC == ssType.category) && (SSType.BIT != ssType)
                            && (SSType.TINYINT != ssType));
                    queryMetaMap.put(paramOrdinal, qm);
                }
            }
        } catch (SQLException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"), e);
        }
    }

    private void parseFMTQueryMeta(ResultSetMetaData md, SQLServerFMTQuery f) throws SQLServerException {
        try {
            // Gets the list of parsed column names/targets
            List<String> columns = f.getColumns();
            // Gets VALUES(?,?,?...) list. The internal list corresponds to the parameters in the bracket after VALUES.
            List<List<String>> params = f.getValuesList();
            int valueListOffset = 0;
            int mdIndex = 1;
            int mapIndex = 1;
            for (int i = 0; i < columns.size(); i++) {
                /**
                 * For INSERT table VALUES(?,?,?...) scenarios where the column names are not specifically defined after
                 * the table name, the parser adds a '*' followed by '?'s equal to the number of parameters in the
                 * values bracket. The '*' will retrieve all values from the table and we'll use the '?'s to match their
                 * position here
                 */
                if ("*".equals(columns.get(i))) {
                    for (int j = 0; j < params.get(valueListOffset).size(); j++) {
                        if ("?".equals(params.get(valueListOffset).get(j))) {
                            if (!md.isAutoIncrement(mdIndex + j)) {
                                QueryMeta qm = getQueryMetaFromResultSetMetaData(md, mdIndex + j);
                                queryMetaMap.put(mapIndex++, qm);
                                i++;
                            }
                        }
                    }
                    mdIndex += params.get(valueListOffset).size();
                    valueListOffset++;
                } else {
                    /*
                     * If this is not a INSERT table VALUES(...) situation, just add the entry.
                     */
                    QueryMeta qm = getQueryMetaFromResultSetMetaData(md, mdIndex);
                    queryMetaMap.put(mapIndex++, qm);
                    mdIndex++;
                }
            }
        } catch (SQLException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"), e);
        }
    }

    private QueryMeta getQueryMetaFromResultSetMetaData(ResultSetMetaData md, int index) throws SQLException {
        QueryMeta qm = new QueryMeta();
        qm.parameterClassName = md.getColumnClassName(index);
        qm.parameterType = md.getColumnType(index);
        qm.parameterTypeName = md.getColumnTypeName(index);
        qm.precision = md.getPrecision(index);
        qm.scale = md.getScale(index);
        qm.isNullable = md.isNullable(index);
        qm.isSigned = md.isSigned(index);
        return qm;
    }

    String parseProcIdentifier(String procIdentifier) throws SQLServerException {
        ThreePartName threePartName = ThreePartName.parse(procIdentifier);
        StringBuilder sb = new StringBuilder();
        if (threePartName.getDatabasePart() != null) {
            sb.append("@procedure_qualifier=");
            sb.append(threePartName.getDatabasePart());
            sb.append(", ");
        }
        if (threePartName.getOwnerPart() != null) {
            sb.append("@procedure_owner=");
            sb.append(threePartName.getOwnerPart());
            sb.append(", ");
        }
        if (threePartName.getProcedurePart() != null) {
            sb.append("@procedure_name=");
            sb.append(threePartName.getProcedurePart());
        } else {
            SQLServerException.makeFromDriverError(con, stmtParent, SQLServerException.getErrString("R_noMetadata"),
                    null, false);
        }
        return sb.toString();
    }

    private void checkClosed() throws SQLServerException {
        // stmtParent does not seem to be re-used, should just verify connection is not closed.
        // stmtParent.checkClosed();
        con.checkClosed();
    }

    /**
     * Construct a SQLServerParameterMetaData parameter meta data.
     * 
     * @param st
     *        the prepared statement
     * @param sProcString
     *        the procedure name
     * @throws SQLServerException
     */
    @SuppressWarnings("serial")
    SQLServerParameterMetaData(SQLServerPreparedStatement st, String sProcString) throws SQLServerException {
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
                String sProc = parseProcIdentifier(st.procedureName);
                try (SQLServerStatement s = (SQLServerStatement) con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                        SQLServerResultSet rsProcedureMeta = s
                                .executeQueryInternal(con.isKatmaiOrLater()
                                                                            ? "exec sp_sproc_columns_100 " + sProc
                                                                                    + ", @ODBCVer=3, @fUsePattern=0"
                                                                            : "exec sp_sproc_columns " + sProc
                                                                                    + ", @ODBCVer=3, @fUsePattern=0")) {

                    // if rsProcedureMeta has next row, it means the stored procedure is found
                    if (rsProcedureMeta.next()) {
                        procedureIsFound = true;
                    } else {
                        procedureIsFound = false;
                    }

                    rsProcedureMeta.beforeFirst();

                    // Sixth is DATA_TYPE
                    rsProcedureMeta.getColumn(6).setFilter(new DataTypeFilter());
                    if (con.isKatmaiOrLater()) {
                        rsProcedureMeta.getColumn(8).setFilter(new ZeroFixupFilter());
                        rsProcedureMeta.getColumn(9).setFilter(new ZeroFixupFilter());
                        rsProcedureMeta.getColumn(17).setFilter(new ZeroFixupFilter());
                    }

                    procMetadata = new ArrayList<>();

                    // Process ResultSet Procedure Metadata for API usage
                    while (rsProcedureMeta.next()) {
                        procMetadata.add(new HashMap<String, Object>() {
                            {
                                put("DATA_TYPE", rsProcedureMeta.getShort("DATA_TYPE"));
                                put("COLUMN_TYPE", rsProcedureMeta.getInt("COLUMN_TYPE"));
                                put("TYPE_NAME", rsProcedureMeta.getString("TYPE_NAME"));
                                put("PRECISION", rsProcedureMeta.getInt("PRECISION"));
                                put("SCALE", rsProcedureMeta.getInt("SCALE"));
                                put("NULLABLE", rsProcedureMeta.getInt("NULLABLE"));
                                put("SS_TYPE_SCHEMA_NAME", rsProcedureMeta.getString("SS_TYPE_SCHEMA_NAME"));
                            }
                        });
                    }
                }
            }

            // Otherwise we just have a parameterized statement.
            // if SQL server version is 2012 and above use stored
            // procedure "sp_describe_undeclared_parameters" to retrieve parameter meta data
            // if SQL server version is 2008, then use FMTONLY
            else {
                queryMetaMap = new HashMap<>();
                if (con.getServerMajorVersion() >= SQL_SERVER_2012_VERSION && !st.getUseFmtOnly()) {
                    String preparedSQL = con.replaceParameterMarkers(stmtParent.userSQL,
                            stmtParent.userSQLParamPositions, stmtParent.inOutParam, stmtParent.bReturnValueSyntax);

                    try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) con
                            .prepareCall("exec sp_describe_undeclared_parameters ?")) {
                        cstmt.setNString(1, preparedSQL);
                        parseQueryMeta(cstmt.executeQueryInternal());
                    }
                } else {
                    SQLServerFMTQuery f = new SQLServerFMTQuery(sProcString);
                    try (SQLServerStatement stmt = (SQLServerStatement) con.createStatement();
                            ResultSet rs = stmt.executeQuery(f.getFMTQuery())) {
                        parseFMTQueryMeta(rs.getMetaData(), f);
                    }
                }
            }
        }
        // Do not need to wrapper SQLServerException again
        catch (SQLServerException e) {
            throw e;
        } catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.getMessage(), null, false);
        } catch (StringIndexOutOfBoundsException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.getMessage(), null, false);
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

    private Map<String, Object> getParameterInfo(int param) {
        if (stmtParent.bReturnValueSyntax && isTVP) {
            return procMetadata.get(param - 1);
        } else {
            // Note row 1 is the 'return value' meta data
            return procMetadata.get(param);
        }
    }

    private boolean isValidParamProc(int n) {
        // Note row 1 is the 'return value' meta data
        return ((stmtParent.bReturnValueSyntax && isTVP && procMetadata.size() >= n) || procMetadata.size() > n);
    }

    private boolean isValidParamQuery(int n) {
        return (null != queryMetaMap && queryMetaMap.containsKey(n));
    }

    /**
     * Checks if the @param passed is valid for either procedure metadata or query metadata.
     * 
     * @param param
     * @throws SQLServerException
     */
    private void checkParam(int param) throws SQLServerException {
        // Check if Procedure Metadata is not available
        if (null == procMetadata) {
            // Check if Query Metadata is also not available
            if (!isValidParamQuery(param)) {
                SQLServerException.makeFromDriverError(con, stmtParent, SQLServerException.getErrString("R_noMetadata"),
                        null, false);
            }
        } else if (!isValidParamProc(param)) {
            // Throw exception if @param index not found
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidParameterNumber"));
            Object[] msgArgs = {param};
            SQLServerException.makeFromDriverError(con, stmtParent, form.format(msgArgs), null, false);
        }
    }

    @Override
    public String getParameterClassName(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        try {
            if (null == procMetadata) {
                return queryMetaMap.get(param).parameterClassName;
            } else {
                return JDBCType.of((short) getParameterInfo(param).get("DATA_TYPE")).className();
            }
        } catch (SQLServerException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.getMessage(), null, false);
            return null;
        }
    }

    @Override
    public int getParameterCount() throws SQLServerException {
        checkClosed();
        if (null == procMetadata) {
            return queryMetaMap.size();
        } else {
            // Row 1 is Return Type metadata
            return (procMetadata.size() == 0 ? 0 : procMetadata.size() - 1);
        }
    }

    @Override
    public int getParameterMode(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        if (null == procMetadata) {
            // if it is not a stored procedure, the @param can only be input.
            return parameterModeIn;
        } else {
            int n = (int) getParameterInfo(param).get("COLUMN_TYPE");
            if (n == 1)
                return parameterModeIn;
            else if (n == 2)
                return parameterModeOut;
            else
                return parameterModeUnknown;
        }
    }

    @Override
    public int getParameterType(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        int parameterType = 0;
        if (null == procMetadata) {
            parameterType = queryMetaMap.get(param).parameterType;
        } else {
            parameterType = (short) getParameterInfo(param).get("DATA_TYPE");
        }
        if (0 != parameterType) {
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
                default:
                    break;
            }
        }
        return parameterType;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        if (null == procMetadata) {
            return queryMetaMap.get(param).parameterTypeName;
        } else {
            return getParameterInfo(param).get("TYPE_NAME").toString();
        }
    }

    @Override
    public int getPrecision(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        if (null == procMetadata) {
            return queryMetaMap.get(param).precision;
        } else {
            return (int) getParameterInfo(param).get("PRECISION");
        }
    }

    @Override
    public int getScale(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        if (null == procMetadata) {
            return queryMetaMap.get(param).scale;
        } else {
            return (int) getParameterInfo(param).get("SCALE");
        }
    }

    @Override
    public int isNullable(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        if (procMetadata == null) {
            return queryMetaMap.get(param).isNullable;
        } else {
            return (int) getParameterInfo(param).get("NULLABLE");
        }
    }

    /**
     * Returns if a supplied parameter index is valid.
     * 
     * @param param
     *        the @param index
     * @throws SQLServerException
     *         when an error occurs
     * @return boolean
     */
    @Override
    public boolean isSigned(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        try {
            if (null == procMetadata) {
                return queryMetaMap.get(param).isSigned;
            } else {
                return JDBCType.of((short) getParameterInfo(param).get("DATA_TYPE")).isSigned();
            }
        } catch (SQLException e) {
            SQLServerException.makeFromDriverError(con, stmtParent, e.getMessage(), null, false);
            return false;
        }
    }

    String getTVPSchemaFromStoredProcedure(int param) throws SQLServerException {
        checkClosed();
        checkParam(param);
        return (String) getParameterInfo(param).get("SS_TYPE_SCHEMA_NAME");
    }
}
