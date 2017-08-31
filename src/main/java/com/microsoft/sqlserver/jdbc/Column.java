/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.Calendar;

/**
 * Column represents a database column definition (meta data) within a result set.
 */

final class Column {
    private TypeInfo typeInfo;
    private CryptoMetadata cryptoMetadata;
    private SqlVariant internalVariant;   
    
    final void setInternalVariant(SqlVariant type){
        this.internalVariant = type;
    }
    
    final SqlVariant getInternalVariant(){
        return this.internalVariant;
    }
    
    final TypeInfo getTypeInfo() {
        return typeInfo;
    }

    private DTV updaterDTV;
    private final DTV getterDTV = new DTV();

    // updated if sendStringParametersAsUnicode=true for setNString, setNCharacterStream, and setNClob methods
    private JDBCType jdbcTypeSetByUser = null;

    // set length of value for variable length type (String)
    private int valueLength = 0;

    // The column name, which may be an alias, that is used with value setters and getters.
    private String columnName;

    final void setColumnName(String name) {
        columnName = name;
    }

    final String getColumnName() {
        return columnName;
    }

    // The base column name which is the actual column name in an underlying table.
    // This name must be used, rather than the column name above, when inserting or
    // updating rows in the table.
    private String baseColumnName;

    final void setBaseColumnName(String name) {
        baseColumnName = name;
    }

    final String getBaseColumnName() {
        return baseColumnName;
    }

    private int tableNum;

    final void setTableNum(int num) {
        tableNum = num;
    }

    final int getTableNum() {
        return tableNum;
    }

    private int infoStatus;

    final void setInfoStatus(int status) {
        infoStatus = status;
    }

    final boolean hasDifferentName() {
        return 0 != (infoStatus & TDS.COLINFO_STATUS_DIFFERENT_NAME);
    }

    final boolean isHidden() {
        return 0 != (infoStatus & TDS.COLINFO_STATUS_HIDDEN);
    }

    final boolean isKey() {
        return 0 != (infoStatus & TDS.COLINFO_STATUS_KEY);
    }

    final boolean isExpression() {
        return 0 != (infoStatus & TDS.COLINFO_STATUS_EXPRESSION);
    }

    final boolean isUpdatable() {
        return !isExpression() && !isHidden() && tableName.getObjectName().length() > 0;
    }

    private SQLIdentifier tableName;

    final void setTableName(SQLIdentifier name) {
        tableName = name;
    }

    final SQLIdentifier getTableName() {
        return tableName;
    }

    ColumnFilter filter;

    /**
     * Create a new column
     * 
     * @param typeInfo
     *            the column TYPE_INFO
     * @param columnName
     *            the column name
     * @param tableName
     *            the column's table name
     * @param cryptoMeta
     *            the column's crypto metadata
     */
    Column(TypeInfo typeInfo,
            String columnName,
            SQLIdentifier tableName,
            CryptoMetadata cryptoMeta) {
        this.typeInfo = typeInfo;
        this.columnName = columnName;
        this.baseColumnName = columnName;
        this.tableName = tableName;
        this.cryptoMetadata = cryptoMeta;
    }

    CryptoMetadata getCryptoMetadata() {
        return cryptoMetadata;
    }

    /**
     * Clears the values associated with this column.
     */
    final void clear() {
        getterDTV.clear();
    }

    /**
     * Skip this column.
     *
     * The column's value may or may not already be marked. If this column's value has not yet been marked, this function assumes that the value is
     * located at the current position in the response.
     */
    final void skipValue(TDSReader tdsReader,
            boolean isDiscard) throws SQLServerException {
        getterDTV.skipValue(typeInfo, tdsReader, isDiscard);
    }

    /**
     * Sets Null value on the getterDTV of a column
     */
    final void initFromCompressedNull() {
        getterDTV.initFromCompressedNull();
    }

    void setFilter(ColumnFilter filter) {
        this.filter = filter;
    }

    /**
     * Returns whether the value of this column is SQL NULL.
     *
     * If the column has not yet been read from the response then this method returns false.
     */
    final boolean isNull() {
        return getterDTV.isNull();
    }

    /**
     * Returns true if the column value is initialized to some value by reading the stream from server i.e. it returns true, if impl of getterDTV is
     * not set to null
     */
    final boolean isInitialized() {
        return getterDTV.isInitialized();
    }

    /**
     * Retrieves this colum's value.
     *
     * If the column has not yet been read from the response then this method reads it.
     */
    Object getValue(JDBCType jdbcType,
            InputStreamGetterArgs getterArgs,
            Calendar cal,
            TDSReader tdsReader) throws SQLServerException {
        Object value = getterDTV.getValue(jdbcType, typeInfo.getScale(), getterArgs, cal, typeInfo, cryptoMetadata, tdsReader);
        setInternalVariant(getterDTV.getInternalVariant());
        return (null != filter) ? filter.apply(value, jdbcType) : value;
    }

    int getInt(TDSReader tdsReader) throws SQLServerException {
        return (Integer) getValue(JDBCType.INTEGER, null, null, tdsReader);
    }

    void updateValue(JDBCType jdbcType,
            Object value,
            JavaType javaType,
            StreamSetterArgs streamSetterArgs,
            Calendar cal,
            Integer scale,
            SQLServerConnection con,
            SQLServerStatementColumnEncryptionSetting stmtColumnEncriptionSetting,
            Integer precision,
            boolean forceEncrypt,
            int parameterIndex) throws SQLServerException {
        SSType ssType = typeInfo.getSSType();

        if (null != cryptoMetadata) {
            if (SSType.VARBINARYMAX == cryptoMetadata.baseTypeInfo.getSSType() && JDBCType.BINARY == jdbcType) {
                jdbcType = cryptoMetadata.baseTypeInfo.getSSType().getJDBCType();
            }

            if (null != value) {
                // for encrypted tinyint, we need to convert short value to byte value, otherwise it would be sent as smallint
                if (JDBCType.TINYINT == cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType() && javaType == JavaType.SHORT) {
                    if (value instanceof Boolean) {
                        if (true == ((boolean) value)) {
                            value = 1;
                        }
                        else {
                            value = 0;
                        }
                    }
                    String stringValue = "" + value;
                    Short shortValue = Short.valueOf(stringValue);

                    if (shortValue >= 0 && shortValue <= 255) {
                        value = shortValue.byteValue();
                        javaType = JavaType.BYTE;
                        jdbcType = JDBCType.TINYINT;
                    }
                }
            }
            // if the column is encrypted and value is null, get the real column type instead of binary types
            else if (jdbcType.isBinary()) {
                jdbcType = cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType();
            }
        }

        if (null == scale && null != cryptoMetadata) {
            scale = cryptoMetadata.getBaseTypeInfo().getScale();
        }

        // if jdbcType is char or varchar, check if the column is actually char/varchar or nchar/nvarchar
        // in order to make updateString() work with encrypted Nchar typpes
        if (null != cryptoMetadata && (JDBCType.CHAR == jdbcType || JDBCType.VARCHAR == jdbcType)) {
            if (JDBCType.NVARCHAR == cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType()
                    || JDBCType.NCHAR == cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType()
                    || JDBCType.LONGNVARCHAR == cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType()) {
                jdbcType = cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType();
            }
        }

        if (Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, con)) {
            if ((null == cryptoMetadata) && true == forceEncrypt) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ForceEncryptionTrue_HonorAETrue_UnencryptedColumnRS"));
                Object[] msgArgs = {parameterIndex};

                throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
            else {
                setJdbcTypeSetByUser(jdbcType);

                this.valueLength = Util.getValueLengthBaseOnJavaType(value, javaType, precision, scale, jdbcType);

                // for update encrypted nchar or nvarchar value on result set, must double the value length,
                // otherwise, the data is truncated.
                if (null != cryptoMetadata) {
                    if (JDBCType.NCHAR == cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType()
                            || JDBCType.NVARCHAR == cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType()
                            || JDBCType.LONGNVARCHAR == cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType()) {
                        this.valueLength = valueLength * 2;
                    }
                }
            }
        }
        else {
            if (true == forceEncrypt) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ForceEncryptionTrue_HonorAEFalseRS"));
                Object[] msgArgs = {parameterIndex};

                throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
        }

        if (null != streamSetterArgs) {
            if (!streamSetterArgs.streamType.convertsTo(typeInfo))
                DataTypes.throwConversionError(streamSetterArgs.streamType.toString(), ssType.toString());
        }
        else {
            if (null != cryptoMetadata) {
                // For GUID, set the JDBCType before checking for conversion
                if ((JDBCType.UNKNOWN == jdbcType) && (value instanceof java.util.UUID)) {
                    javaType = JavaType.STRING;
                    jdbcType = JDBCType.GUID;
                    setJdbcTypeSetByUser(jdbcType);
                }

                SSType basicSSType = cryptoMetadata.baseTypeInfo.getSSType();
                if (!jdbcType.convertsTo(basicSSType))
                    DataTypes.throwConversionError(jdbcType.toString(), ssType.toString());

                JDBCType jdbcTypeFromSSType = getJDBCTypeFromBaseSSType(basicSSType, jdbcType);

                if (jdbcTypeFromSSType != jdbcType) {
                    setJdbcTypeSetByUser(jdbcTypeFromSSType);
                    jdbcType = jdbcTypeFromSSType;
                    this.valueLength = Util.getValueLengthBaseOnJavaType(value, javaType, precision, scale, jdbcType);
                }
            }
            else {
                if (!jdbcType.convertsTo(ssType))
                    DataTypes.throwConversionError(jdbcType.toString(), ssType.toString());
            }
        }

        // DateTimeOffset is not supported with SQL Server versions earlier than Katmai
        if ((JDBCType.DATETIMEOFFSET == jdbcType || JavaType.DATETIMEOFFSET == javaType) && !con.isKatmaiOrLater()) {
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET,
                    null);
        }

        // sendStringParametersAsUnicode
        // If set to true, this connection property tells the driver to send textual parameters
        // to the server as Unicode rather than MBCS. This is accomplished here by re-tagging
        // the value with the appropriate corresponding Unicode type.
        if ((null != cryptoMetadata) && (con.sendStringParametersAsUnicode())
                && (JavaType.STRING == javaType || JavaType.READER == javaType || JavaType.CLOB == javaType || JavaType.OBJECT == javaType)) {
            jdbcType = getSSPAUJDBCType(jdbcType);
        }

        // Cheesy checks determine whether updating is allowed, but do not determine HOW to do
        // the update (i.e. what JDBC type to use for the update). The JDBC type to use depends
        // on the SQL Server type of the column and the JDBC type requested.
        //
        // In most cases the JDBCType to use is just the requested JDBCType. But in some cases
        // a client side type conversion is necessary because SQL Server does not directly support
        // conversion from the requested JDBCType to the column SSType, or the driver needs to
        // provide special data conversion.

        // Update of Unicode SSType from textual JDBCType: Use Unicode.
        if ((SSType.NCHAR == ssType || SSType.NVARCHAR == ssType || SSType.NVARCHARMAX == ssType || SSType.NTEXT == ssType || SSType.XML == ssType) &&

                (JDBCType.CHAR == jdbcType || JDBCType.VARCHAR == jdbcType || JDBCType.LONGVARCHAR == jdbcType || JDBCType.CLOB == jdbcType)) {
            jdbcType = (JDBCType.CLOB == jdbcType) ? JDBCType.NCLOB : JDBCType.NVARCHAR;
        }

        // Update of binary SSType from textual JDBCType: Convert hex to binary.
        else if ((SSType.BINARY == ssType || SSType.VARBINARY == ssType || SSType.VARBINARYMAX == ssType || SSType.IMAGE == ssType
                || SSType.UDT == ssType) &&

                (JDBCType.CHAR == jdbcType || JDBCType.VARCHAR == jdbcType || JDBCType.LONGVARCHAR == jdbcType)) {
            jdbcType = JDBCType.VARBINARY;
        }

        // Update of textual SSType from temporal JDBCType requires
        // client-side conversion from temporal to textual.
        else if ((JDBCType.TIMESTAMP == jdbcType || JDBCType.DATE == jdbcType || JDBCType.TIME == jdbcType || JDBCType.DATETIMEOFFSET == jdbcType) &&

                (SSType.CHAR == ssType || SSType.VARCHAR == ssType || SSType.VARCHARMAX == ssType || SSType.TEXT == ssType || SSType.NCHAR == ssType
                        || SSType.NVARCHAR == ssType || SSType.NVARCHARMAX == ssType || SSType.NTEXT == ssType)) {
            jdbcType = JDBCType.NCHAR;
        }

        // Lazily create the updater DTV on first update of the column
        if (null == updaterDTV)
            updaterDTV = new DTV();

        // Set the column's value

        updaterDTV.setValue(typeInfo.getSQLCollation(), jdbcType, value, javaType, streamSetterArgs, cal, scale, con, false);
    }

    /**
     * Used when sendStringParametersAsUnicode=true to derive the appropriate National Character Set JDBC type corresponding to the specified JDBC
     * type.
     */
    private static JDBCType getSSPAUJDBCType(JDBCType jdbcType) {
        switch (jdbcType) {
            case CHAR:
                return JDBCType.NCHAR;
            case VARCHAR:
                return JDBCType.NVARCHAR;
            case LONGVARCHAR:
                return JDBCType.LONGNVARCHAR;
            case CLOB:
                return JDBCType.NCLOB;
            default:
                return jdbcType;
        }
    }

    private static JDBCType getJDBCTypeFromBaseSSType(SSType basicSSType,
            JDBCType jdbcType) {
        switch (jdbcType) {
            case TIMESTAMP:
                if (SSType.DATETIME == basicSSType)
                    return JDBCType.DATETIME;
                else if (SSType.SMALLDATETIME == basicSSType)
                    return JDBCType.SMALLDATETIME;
                return jdbcType;

            case NUMERIC:
            case DECIMAL:
                if (SSType.MONEY == basicSSType)
                    return JDBCType.MONEY;
                if (SSType.SMALLMONEY == basicSSType)
                    return JDBCType.SMALLMONEY;
                return jdbcType;

            case CHAR:
                if (SSType.GUID == basicSSType)
                    return JDBCType.GUID;
                if (SSType.VARCHARMAX == basicSSType)
                    return JDBCType.LONGVARCHAR;
                return jdbcType;

            default:
                return jdbcType;
        }
    }

    boolean hasUpdates() {
        return null != updaterDTV;
    }

    void cancelUpdates() {
        updaterDTV = null;
    }

    void sendByRPC(TDSWriter tdsWriter,
            SQLServerConnection conn) throws SQLServerException {
        // If the column has had no updates then there is nothing to send
        if (null == updaterDTV)
            return;
        try {
            // this is for updateRow() stuff
            updaterDTV.sendCryptoMetaData(cryptoMetadata, tdsWriter);
            updaterDTV.jdbcTypeSetByUser(getJdbcTypeSetByUser(), getValueLength());

            // Otherwise, send the updated value via RPC
            updaterDTV.sendByRPC(baseColumnName, typeInfo,
                    null != cryptoMetadata ? cryptoMetadata.getBaseTypeInfo().getSQLCollation() : typeInfo.getSQLCollation(),
                    null != cryptoMetadata ? cryptoMetadata.getBaseTypeInfo().getPrecision() : typeInfo.getPrecision(),
                    null != cryptoMetadata ? cryptoMetadata.getBaseTypeInfo().getScale() : typeInfo.getScale(), false, // isOutParameter (always false
                                                                                                                       // for column updates)
                    tdsWriter, conn);
        }
        finally {
            // this is for updateRow() stuff
            updaterDTV.sendCryptoMetaData(null, tdsWriter);
        }
    }

    JDBCType getJdbcTypeSetByUser() {
        return jdbcTypeSetByUser;
    }

    void setJdbcTypeSetByUser(JDBCType jdbcTypeSetByUser) {
        this.jdbcTypeSetByUser = jdbcTypeSetByUser;
    }

    int getValueLength() {
        return valueLength;
    }
}

abstract class ColumnFilter {
    abstract Object apply(Object value,
            JDBCType jdbcType) throws SQLServerException;
}
