/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Calendar;
import java.util.Locale;


/**
 * Parameter represents a JDBC parameter value that is supplied with a prepared or callable statement or an updatable result set. Parameter is JDBC
 * type specific and is capable of representing any Java native type as well as a number of Java object types including binary and character streams.
 */

final class Parameter {
    // Value type info for OUT parameters (excluding return status)
    private TypeInfo typeInfo;

    // For unencrypted paramters cryptometa will be null. For encrypted parameters it will hold encryption metadata.
    CryptoMetadata cryptoMeta = null;

    TypeInfo getTypeInfo() {
        return typeInfo;
    }

    final CryptoMetadata getCryptoMetadata() {
        return cryptoMeta;
    }

    private boolean shouldHonorAEForParameter = false;
    private boolean userProvidesPrecision = false;
    private boolean userProvidesScale = false;

    // The parameter type definition
    private String typeDefinition = null;
    boolean renewDefinition = false;

    // updated if sendStringParametersAsUnicode=true for setNString, setNCharacterStream, and setNClob methods
    private JDBCType jdbcTypeSetByUser = null;

    // set length of value for variable length type (String)
    private int valueLength = 0;

    private boolean forceEncryption = false;

    Parameter(boolean honorAE) {
        shouldHonorAEForParameter = honorAE;
    }

    // Flag set to true if this is a registered OUTPUT parameter.
    boolean isOutput() {
        return null != registeredOutDTV;
    }

    // Since a parameter can have only one type definition for both sending its value to the server (IN)
    // and getting its value from the server (OUT), we use the JDBC type of the IN parameter value if there
    // is one; otherwise we use the registered OUT param JDBC type.
    JDBCType getJdbcType() throws SQLServerException {
        return (null != inputDTV) ? inputDTV.getJdbcType() : JDBCType.UNKNOWN;
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

    // For parameters whose underlying type is not represented by a JDBC type
    // the transport type reflects how the value is sent to the
    // server (e.g. JDBCType.CHAR for GUID parameters).
    void registerForOutput(JDBCType jdbcType,
            SQLServerConnection con) throws SQLServerException {
        // DateTimeOffset is not supported with SQL Server versions earlier than Katmai
        if (JDBCType.DATETIMEOFFSET == jdbcType && !con.isKatmaiOrLater()) {
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET,
                    null);
        }

        // sendStringParametersAsUnicode
        // If set to true, this connection property tells the driver to send textual parameters
        // to the server as Unicode rather than MBCS. This is accomplished here by re-tagging
        // the value with the appropriate corresponding Unicode type.
        if (con.sendStringParametersAsUnicode()) {

            if (shouldHonorAEForParameter) {
                setJdbcTypeSetByUser(jdbcType);
            }

            jdbcType = getSSPAUJDBCType(jdbcType);
        }

        registeredOutDTV = new DTV();
        registeredOutDTV.setJdbcType(jdbcType);

        if (null == setterDTV)
            inputDTV = registeredOutDTV;

        resetOutputValue();
    }

    int scale = 0;

    // Scale requested for a DECIMAL and NUMERIC OUT parameter. If the OUT parameter
    // is also non-null IN parameter, the scale will be the larger of this value and
    // the value of the IN parameter's scale.
    private int outScale = 4;

    int getOutScale() {
        return outScale;
    }

    void setOutScale(int outScale) {
        this.outScale = outScale;
        userProvidesScale = true;
    }

    // The parameter name
    private String name;
    private String schemaName;

    /*
     * The different DTVs representing the parameter's value:
     *
     * getterDTV - The OUT value, if set, of the parameter after execution. This is the value retrieved by CallableStatement getter methods.
     *
     * registeredOutDTV - The "IN" value corresponding to a SQL NULL with a JDBC type that was passed to the CallableStatement.registerOutParameter
     * method. Since SQL Server does not directly support OUT-only parameters (just IN and IN/OUT), the driver sends a null IN value for an OUT
     * parameter, unless the application set an input value (setterDTV) as well.
     *
     * setterDTV - The IN value, if set, of the parameter. This is the value set by PreparedStatement and CallableStatement setter methods.
     *
     * inputDTV - If set, refers to either setterDTV or registeredOutDTV depending on whether the parameter is IN, IN/OUT, or OUT-only. If cleared
     * (i.e. set to null), it means that no value is set for the parameter and that execution of the PreparedStatement or CallableStatement should
     * throw a "parameter not set" exception.
     *
     * Note that if the parameter value is a stream, the driver consumes its contents it at execution and clears inputDTV and setterDTV so that the
     * application must reset the parameter prior to the next execution to avoid getting a "parameter not set" exception.
     */
    private DTV getterDTV;
    private DTV registeredOutDTV = null;
    private DTV setterDTV = null;
    private DTV inputDTV = null;

    /**
     * Clones this Parameter object for use in a batch.
     *
     * The clone method creates a shallow clone of the Parameter object. That is, the cloned instance references all of the same internal objects and
     * state as the original.
     *
     * Note: this method is purposely NOT the Object.clone() method, as that method has specific requirements and semantics that we don't need here.
     */
    final Parameter cloneForBatch() {
        Parameter clonedParam = new Parameter(shouldHonorAEForParameter);
        clonedParam.typeInfo = typeInfo;
        clonedParam.typeDefinition = typeDefinition;
        clonedParam.outScale = outScale;
        clonedParam.name = name;
        clonedParam.getterDTV = getterDTV;
        clonedParam.registeredOutDTV = registeredOutDTV;
        clonedParam.setterDTV = setterDTV;
        clonedParam.inputDTV = inputDTV;
        clonedParam.cryptoMeta = cryptoMeta;
        clonedParam.jdbcTypeSetByUser = jdbcTypeSetByUser;
        clonedParam.valueLength = valueLength;
        clonedParam.userProvidesPrecision = userProvidesPrecision;
        clonedParam.userProvidesScale = userProvidesScale;
        return clonedParam;
    }

    /**
     * Skip value.
     */
    final void skipValue(TDSReader tdsReader,
            boolean isDiscard) throws SQLServerException {
        if (null == getterDTV)
            getterDTV = new DTV();

        deriveTypeInfo(tdsReader);

        getterDTV.skipValue(typeInfo, tdsReader, isDiscard);
    }

    /**
     * Skip value.
     */
    final void skipRetValStatus(TDSReader tdsReader) throws SQLServerException {

        StreamRetValue srv = new StreamRetValue();
        srv.setFromTDS(tdsReader);
    }

    // Clear an INPUT parameter value
    void clearInputValue() {
        setterDTV = null;
        inputDTV = registeredOutDTV;
    }

    // reset output value for re -execution
    // if there was old value reset it to a new DTV
    void resetOutputValue() {
        getterDTV = null;
        typeInfo = null;
    }

    void deriveTypeInfo(TDSReader tdsReader) throws SQLServerException {
        if (null == typeInfo) {
            typeInfo = TypeInfo.getInstance(tdsReader, true);

            if (shouldHonorAEForParameter && typeInfo.isEncrypted()) {
                // In this case, method getCryptoMetadata(tdsReader) retrieves baseTypeInfo without cryptoMetadata,
                // so save cryptoMetadata first.
                CekTableEntry cekEntry = cryptoMeta.getCekTableEntry();
                cryptoMeta = (new StreamRetValue()).getCryptoMetadata(tdsReader);
                cryptoMeta.setCekTableEntry(cekEntry);
            }
        }
    }

    void setFromReturnStatus(int returnStatus,
            SQLServerConnection con) throws SQLServerException {
        if (null == getterDTV)
            getterDTV = new DTV();

        getterDTV.setValue(null, JDBCType.INTEGER, returnStatus, JavaType.INTEGER, null, null, null, con, getForceEncryption());
    }

    void setValue(JDBCType jdbcType,
            Object value,
            JavaType javaType,
            StreamSetterArgs streamSetterArgs,
            Calendar calendar,
            Integer precision,
            Integer scale,
            SQLServerConnection con,
            boolean forceEncrypt,
            SQLServerStatementColumnEncryptionSetting stmtColumnEncriptionSetting,
            int parameterIndex,
            String userSQL,
            String tvpName) throws SQLServerException {

        if (shouldHonorAEForParameter) {
            userProvidesPrecision = false;
            userProvidesScale = false;

            if (null != precision) {
                userProvidesPrecision = true;
            }

            if (null != scale) {
                userProvidesScale = true;
            }

            // for encrypted tinyint, we need to convert short value to byte value,
            // otherwise it would be sent as smallint
            // Also, for setters, we are able to send tinyint to smallint
            // However, for output parameter, it might cause error.
            if (!isOutput()) {
                if ((JavaType.SHORT == javaType) && ((JDBCType.TINYINT == jdbcType) || (JDBCType.SMALLINT == jdbcType))) {
                    // value falls in the TINYINT range
                    if (((Short) value) >= 0 && ((Short) value) <= 255) {
                        value = ((Short) value).byteValue();
                        javaType = JavaType.of(value);
                        jdbcType = javaType.getJDBCType(SSType.UNKNOWN, jdbcType);
                    }
                    // value falls outside tinyint range. Throw an error if the user intends to send as tinyint.
                    else {
                        // This is for cases like setObject(1, Short.valueOf("-1"), java.sql.Types.TINYINT);
                        if (JDBCType.TINYINT == jdbcType) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                            Object[] msgArgs = {javaType.toString().toLowerCase(Locale.ENGLISH), jdbcType.toString().toLowerCase(Locale.ENGLISH)};
                            throw new SQLServerException(form.format(msgArgs), null);
                        }
                    }
                }
            }
        }

        // forceEncryption is true, shouldhonorae is false
        if ((true == forceEncrypt) && (false == Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, con))) {

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ForceEncryptionTrue_HonorAEFalse"));
            Object[] msgArgs = {parameterIndex, userSQL};
            SQLServerException.makeFromDriverError(con, this, form.format(msgArgs), null, true);

        }

        // DateTimeOffset is not supported with SQL Server versions earlier than Katmai
        if ((JDBCType.DATETIMEOFFSET == jdbcType || JavaType.DATETIMEOFFSET == javaType) && !con.isKatmaiOrLater()) {
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET,
                    null);
        }

        if (JavaType.TVP == javaType) {
            TVP tvpValue;
            if (null == value) {
                tvpValue = new TVP(tvpName);
            }
            else if (value instanceof SQLServerDataTable) {
                tvpValue = new TVP(tvpName, (SQLServerDataTable) value);
            }
            else if (value instanceof ResultSet) {
                tvpValue = new TVP(tvpName, (ResultSet) value);
            }
            else if (value instanceof ISQLServerDataRecord) {
                tvpValue = new TVP(tvpName, (ISQLServerDataRecord) value);
            }
            else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_TVPInvalidValue"));
                Object[] msgArgs = {parameterIndex};
                throw new SQLServerException(form.format(msgArgs), null);
            }

            if (!tvpValue.isNull() && (0 == tvpValue.getTVPColumnCount())) {
                throw new SQLServerException(SQLServerException.getErrString("R_TVPEmptyMetadata"), null);
            }
            name = (tvpValue).getTVPName();
            schemaName = tvpValue.getOwningSchemaNameTVP();

            value = tvpValue;
        }

        // setting JDBCType and exact length needed for AE stored procedure
        if (shouldHonorAEForParameter) {
            setForceEncryption(forceEncrypt);

            // set it if it is not output parameter or jdbcTypeSetByUser is null
            if (!(this.isOutput() && this.jdbcTypeSetByUser != null)) {
                setJdbcTypeSetByUser(jdbcType);
            }

            // skip it if is (character types or binary type) & is output parameter && value is already set,
            if ((!(jdbcType.isTextual() || jdbcType.isBinary())) || !(this.isOutput()) || (this.valueLength == 0)) {
                this.valueLength = Util.getValueLengthBaseOnJavaType(value, javaType, precision, scale, jdbcType);
            }

            if (null != scale) {
                this.outScale = scale;
            }
        }

        // sendStringParametersAsUnicode
        // If set to true, this connection property tells the driver to send textual parameters
        // to the server as Unicode rather than MBCS. This is accomplished here by re-tagging
        // the value with the appropriate corresponding Unicode type.
        // JavaType.OBJECT == javaType when calling setNull()
        if (con.sendStringParametersAsUnicode()
                && (JavaType.STRING == javaType || JavaType.READER == javaType || JavaType.CLOB == javaType || JavaType.OBJECT == javaType)) {
            jdbcType = getSSPAUJDBCType(jdbcType);
        }

        DTV newDTV = new DTV();
        newDTV.setValue(con.getDatabaseCollation(), jdbcType, value, javaType, streamSetterArgs, calendar, scale, con, forceEncrypt);

        if (!con.sendStringParametersAsUnicode()) {
            newDTV.sendStringParametersAsUnicode = false;
        }

        inputDTV = setterDTV = newDTV;
    }

    boolean isNull() {
        if (null != getterDTV)
            return getterDTV.isNull();

        return false;
    }

    boolean isValueGotten() {
        return null != getterDTV;

    }

    Object getValue(JDBCType jdbcType,
            InputStreamGetterArgs getterArgs,
            Calendar cal,
            TDSReader tdsReader) throws SQLServerException {
        if (null == getterDTV)
            getterDTV = new DTV();

        deriveTypeInfo(tdsReader);
        // If the parameter is not encrypted or column encryption is turned off (either at connection or
        // statement level), cryptoMeta would be null.
        return getterDTV.getValue(jdbcType, outScale, getterArgs, cal, typeInfo, cryptoMeta, tdsReader);
    }

    int getInt(TDSReader tdsReader) throws SQLServerException {
        Integer value = (Integer) getValue(JDBCType.INTEGER, null, null, tdsReader);
        return null != value ? value : 0;
    }

    /**
     * DTV execute op to determine the parameter type definition.
     */
    final class GetTypeDefinitionOp extends DTVExecuteOp {
        private static final String NVARCHAR_MAX = "nvarchar(max)";
        private static final String NVARCHAR_4K = "nvarchar(4000)";
        private static final String NTEXT = "ntext";

        private static final String VARCHAR_MAX = "varchar(max)";
        private static final String VARCHAR_8K = "varchar(8000)";
        private static final String TEXT = "text";

        private static final String VARBINARY_MAX = "varbinary(max)";
        private static final String VARBINARY_8K = "varbinary(8000)";
        private static final String IMAGE = "image";

        private final Parameter param;
        private final SQLServerConnection con;

        GetTypeDefinitionOp(Parameter param,
                SQLServerConnection con) {
            this.param = param;
            this.con = con;
        }

        private void setTypeDefinition(DTV dtv) {
            switch (dtv.getJdbcType()) {
                case TINYINT:
                    param.typeDefinition = SSType.TINYINT.toString();
                    break;

                case SMALLINT:
                    param.typeDefinition = SSType.SMALLINT.toString();
                    break;

                case INTEGER:
                    param.typeDefinition = SSType.INTEGER.toString();
                    break;

                case BIGINT:
                    param.typeDefinition = SSType.BIGINT.toString();
                    break;

                case REAL:
                    // sp_describe_parameter_encryption must be queried as real for AE
                    if (param.shouldHonorAEForParameter && (null != jdbcTypeSetByUser)
                            && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        param.typeDefinition = SSType.REAL.toString();
                    }
                    else {
                        // use FLOAT if column is not encrypted
                        param.typeDefinition = SSType.FLOAT.toString();
                    }
                    break;
                    
                case FLOAT:
                case DOUBLE:
                    param.typeDefinition = SSType.FLOAT.toString();
                    break;

                case DECIMAL:
                case NUMERIC:
                    // First, bound the scale by the maximum allowed by SQL Server
                    if (scale > SQLServerConnection.maxDecimalPrecision)
                        scale = SQLServerConnection.maxDecimalPrecision;

                    // Next, prepare with the largest of:
                    // - the value's scale (initial value, as limited above)
                    // - the specified input scale (if any)
                    // - the registered output scale
                    Integer inScale = dtv.getScale();
                    if (null != inScale && scale < inScale)
                        scale = inScale;

                    if (param.isOutput() && scale < param.getOutScale())
                        scale = param.getOutScale();

                    if (param.shouldHonorAEForParameter && (null != jdbcTypeSetByUser)
                            && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        if (0 == valueLength) {
                            // for prepared statement and callable statement, There are only two cases where valueLength is 0:
                            // 1. when the parameter is output parameter
                            // 2. for input parameter, the value is null
                            // so, here, if the decimal parameter is encrypted and it is null and it is not outparameter
                            // then we set precision as the default precision instead of max precision
                            if (!isOutput()) {
                                param.typeDefinition = "decimal(" + SQLServerConnection.defaultDecimalPrecision + ", " + scale + ")";
                            }
                        }
                        else {
                            if (SQLServerConnection.defaultDecimalPrecision >= valueLength) {
                                param.typeDefinition = "decimal(" + SQLServerConnection.defaultDecimalPrecision + "," + scale + ")";

                                if (SQLServerConnection.defaultDecimalPrecision < (valueLength + scale)) {
                                    param.typeDefinition = "decimal(" + (SQLServerConnection.defaultDecimalPrecision + scale) + "," + scale + ")";
                                }
                            }
                            else {
                                param.typeDefinition = "decimal(" + SQLServerConnection.maxDecimalPrecision + "," + scale + ")";
                            }
                        }

                        if (isOutput()) {
                            param.typeDefinition = "decimal(" + SQLServerConnection.maxDecimalPrecision + ", " + scale + ")";
                        }

                        if (userProvidesPrecision) {
                            param.typeDefinition = "decimal(" + valueLength + "," + scale + ")";
                        }
                    }
                    else
                        param.typeDefinition = "decimal(" + SQLServerConnection.maxDecimalPrecision + "," + scale + ")";

                    break;

                case MONEY:
                    param.typeDefinition = SSType.MONEY.toString();
                    break;
                case SMALLMONEY:
                    param.typeDefinition = SSType.MONEY.toString();

                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        param.typeDefinition = SSType.SMALLMONEY.toString();
                    }

                    break;
                case BIT:
                case BOOLEAN:
                    param.typeDefinition = SSType.BIT.toString();
                    break;

                case LONGVARBINARY:
                case BLOB:
                    param.typeDefinition = VARBINARY_MAX;
                    break;

                case BINARY:
                case VARBINARY:
                    // To avoid the server side cost of re-preparing, once a "long" type, always a "long" type...
                    if (VARBINARY_MAX.equals(param.typeDefinition) || IMAGE.equals(param.typeDefinition))
                        break;
                    if (param.shouldHonorAEForParameter && (null != jdbcTypeSetByUser)
                            && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        if (0 == valueLength) {
                            // Workaround for the issue when inserting empty string and null into encrypted columns
                            param.typeDefinition = "varbinary(1)";
                            valueLength++;
                        }
                        else {
                            param.typeDefinition = "varbinary(" + valueLength + ")";
                        }

                        if (JDBCType.LONGVARBINARY == jdbcTypeSetByUser) {
                            param.typeDefinition = VARBINARY_MAX;
                        }
                    }
                    else
                        param.typeDefinition = VARBINARY_8K;
                    break;

                case DATE:
                    // Bind DATE values to pre-Katmai servers as DATETIME (which has no DATE-only type).
                    param.typeDefinition = con.isKatmaiOrLater() ? SSType.DATE.toString() : SSType.DATETIME.toString();
                    break;

                case TIME:
                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {

                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */

                        if (userProvidesScale) {
                            param.typeDefinition = (SSType.TIME.toString() + "(" + outScale + ")");
                        }
                        else {
                            param.typeDefinition = param.typeDefinition = SSType.TIME.toString() + "(" + valueLength + ")";
                        }
                    }
                    else {
                        param.typeDefinition = con.getSendTimeAsDatetime() ? SSType.DATETIME.toString() : SSType.TIME.toString();
                    }
                    break;

                case TIMESTAMP:
                    // Bind TIMESTAMP values to pre-Katmai servers as DATETIME. Bind TIMESTAMP values to
                    // Katmai and later servers as DATETIME2 to take advantage of increased precision.
                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        if (userProvidesScale) {
                            param.typeDefinition = con.isKatmaiOrLater() ? (SSType.DATETIME2.toString() + "(" + outScale + ")")
                                    : (SSType.DATETIME.toString());
                        }
                        else {
                            param.typeDefinition = con.isKatmaiOrLater() ? (SSType.DATETIME2.toString() + "(" + valueLength + ")")
                                    : SSType.DATETIME.toString();
                        }
                    }
                    else {
                        param.typeDefinition = con.isKatmaiOrLater() ? SSType.DATETIME2.toString() : SSType.DATETIME.toString();
                    }
                    break;

                case DATETIME:
                    // send as Datetime by default
                    param.typeDefinition = SSType.DATETIME2.toString();

                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        param.typeDefinition = SSType.DATETIME.toString();
                    }

                    if (!param.shouldHonorAEForParameter) {
                        // if AE is off and it is output parameter of stored procedure, sent it as datetime2(3)
                        // otherwise it returns incorrect milliseconds.
                        if (param.isOutput()) {
                            param.typeDefinition = SSType.DATETIME2.toString() + "(" + outScale + ")";
                        }
                    }
                    else {
                        // when AE is on, set it to Datetime by default,
                        // However, if column is not encrypted and it is output parameter of stored procedure,
                        // renew it to datetime2(3)
                        if (null == param.getCryptoMetadata() && param.renewDefinition) {
                            if (param.isOutput()) {
                                param.typeDefinition = SSType.DATETIME2.toString() + "(" + outScale + ")";
                            }
                            break;
                        }
                    }
                    break;

                case SMALLDATETIME:
                    param.typeDefinition = SSType.DATETIME2.toString();

                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        param.typeDefinition = SSType.SMALLDATETIME.toString();
                    }

                    break;

                case TIME_WITH_TIMEZONE:
                case TIMESTAMP_WITH_TIMEZONE:
                case DATETIMEOFFSET:
                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        if (userProvidesScale) {
                            param.typeDefinition = SSType.DATETIMEOFFSET.toString() + "(" + outScale + ")";
                        }
                        else {
                            param.typeDefinition = SSType.DATETIMEOFFSET.toString() + "(" + valueLength + ")";
                        }
                    }
                    else {
                        param.typeDefinition = SSType.DATETIMEOFFSET.toString();
                    }
                    break;

                case LONGVARCHAR:
                case CLOB:
                    param.typeDefinition = VARCHAR_MAX;
                    break;

                case CHAR:
                case VARCHAR:
                    // To avoid the server side cost of re-preparing, once a "long" type, always a "long" type...
                    if (VARCHAR_MAX.equals(param.typeDefinition) || TEXT.equals(param.typeDefinition))
                        break;

                    // Adding for case useColumnEncryption=true & sendStringParametersAsUnicode=false
                    if (param.shouldHonorAEForParameter && (null != jdbcTypeSetByUser)
                            && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        if (0 == valueLength) {
                            // Workaround for the issue when inserting empty string and null into encrypted columns
                            param.typeDefinition = "varchar(1)";
                            valueLength++;
                        }
                        else {
                            param.typeDefinition = "varchar(" + valueLength + ")";

                            if (DataTypes.SHORT_VARTYPE_MAX_BYTES <= valueLength) {
                                param.typeDefinition = VARCHAR_MAX;
                            }
                        }
                    }
                    else
                        param.typeDefinition = VARCHAR_8K;
                    break;

                case LONGNVARCHAR:
                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        if ((null != jdbcTypeSetByUser) && ((jdbcTypeSetByUser == JDBCType.VARCHAR) || (jdbcTypeSetByUser == JDBCType.CHAR)
                                || (jdbcTypeSetByUser == JDBCType.LONGVARCHAR))) {
                            if (0 == valueLength) {
                                // Workaround for the issue when inserting empty string and null into encrypted columns
                                param.typeDefinition = "varchar(1)";
                                valueLength++;
                            }
                            else if (DataTypes.SHORT_VARTYPE_MAX_BYTES < valueLength) {
                                param.typeDefinition = VARCHAR_MAX;
                            }
                            else {
                                param.typeDefinition = "varchar(" + valueLength + ")";
                            }

                            if (jdbcTypeSetByUser == JDBCType.LONGVARCHAR) {
                                param.typeDefinition = VARCHAR_MAX;
                            }
                        }
                        else if ((null != jdbcTypeSetByUser)
                                && (jdbcTypeSetByUser == JDBCType.NVARCHAR || jdbcTypeSetByUser == JDBCType.LONGNVARCHAR)) {
                            if (0 == valueLength) {
                                // Workaround for the issue when inserting empty string and null into encrypted columns
                                param.typeDefinition = "nvarchar(1)";
                                valueLength++;
                            }
                            else if (DataTypes.SHORT_VARTYPE_MAX_CHARS < valueLength) {
                                param.typeDefinition = NVARCHAR_MAX;
                            }
                            else {
                                param.typeDefinition = "nvarchar(" + valueLength + ")";
                            }

                            if (jdbcTypeSetByUser == JDBCType.LONGNVARCHAR) {
                                param.typeDefinition = NVARCHAR_MAX;
                            }
                        }
                        else { // used if setNull() is called with java.sql.Types.NCHAR
                            if (0 == valueLength) {
                                // Workaround for the issue when inserting empty string and null into encrypted columns
                                param.typeDefinition = "nvarchar(1)";
                                valueLength++;
                            }
                            else {
                                param.typeDefinition = "nvarchar(" + valueLength + ")";

                                if (DataTypes.SHORT_VARTYPE_MAX_BYTES <= valueLength) {
                                    param.typeDefinition = NVARCHAR_MAX;
                                }
                            }
                        }
                        break;
                    }
                    else
                        param.typeDefinition = NVARCHAR_MAX;
                    break;

                case NCLOB:
                    // do not need to check if AE is enabled or not,
                    // because NCLOB does not work with it
                    param.typeDefinition = NVARCHAR_MAX;
                    break;

                case NCHAR:
                case NVARCHAR:
                    // To avoid the server side cost of re-preparing, once a "long" type, always a "long" type...
                    if (NVARCHAR_MAX.equals(param.typeDefinition) || NTEXT.equals(param.typeDefinition))
                        break;

                    if (param.shouldHonorAEForParameter && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
                        /*
                         * This means AE is ON in the connection, and (1) this is either the first round to SQL Server to get encryption meta data, or
                         * (2) this is the second round of renewing meta data and parameter is encrypted In both of these cases we need to send
                         * specific type info, otherwise generic type info can be used as before.
                         */
                        if ((null != jdbcTypeSetByUser) && ((jdbcTypeSetByUser == JDBCType.VARCHAR) || (jdbcTypeSetByUser == JDBCType.CHAR)
                                || (JDBCType.LONGVARCHAR == jdbcTypeSetByUser))) {
                            if (0 == valueLength) {
                                // Workaround for the issue when inserting empty string and null into encrypted columns
                                param.typeDefinition = "varchar(1)";
                                valueLength++;
                            }
                            else {
                                param.typeDefinition = "varchar(" + valueLength + ")";

                                if (DataTypes.SHORT_VARTYPE_MAX_BYTES < valueLength) {
                                    param.typeDefinition = VARCHAR_MAX;
                                }
                            }

                            if (JDBCType.LONGVARCHAR == jdbcTypeSetByUser) {
                                param.typeDefinition = VARCHAR_MAX;
                            }
                        }
                        else if ((null != jdbcTypeSetByUser) && ((jdbcTypeSetByUser == JDBCType.NVARCHAR) || (jdbcTypeSetByUser == JDBCType.NCHAR)
                                || (JDBCType.LONGNVARCHAR == jdbcTypeSetByUser))) {
                            if (0 == valueLength) {
                                // Workaround for the issue when inserting empty string and null into encrypted columns
                                param.typeDefinition = "nvarchar(1)";
                                valueLength++;
                            }
                            else {
                                param.typeDefinition = "nvarchar(" + valueLength + ")";

                                if (DataTypes.SHORT_VARTYPE_MAX_BYTES <= valueLength) {
                                    param.typeDefinition = NVARCHAR_MAX;
                                }
                            }

                            if (JDBCType.LONGNVARCHAR == jdbcTypeSetByUser) {
                                param.typeDefinition = NVARCHAR_MAX;
                            }
                        }
                        else { // used if setNull() is called with java.sql.Types.NCHAR
                            if (0 == valueLength) {
                                // Workaround for the issue when inserting empty string and null into encrypted columns
                                param.typeDefinition = "nvarchar(1)";
                                valueLength++;
                            }
                            else {
                                param.typeDefinition = "nvarchar(" + valueLength + ")";

                                if (DataTypes.SHORT_VARTYPE_MAX_BYTES <= valueLength) {
                                    param.typeDefinition = NVARCHAR_MAX;
                                }
                            }
                        }
                        break;
                    }
                    else
                        param.typeDefinition = NVARCHAR_4K;
                    break;
                case SQLXML:
                    param.typeDefinition = SSType.XML.toString();
                    break;

                case TVP:
                    // definition should contain the TVP name and the keyword READONLY
                    String schema = param.schemaName;

                    if (null != schema) {
                        param.typeDefinition = "[" + schema + "].[" + param.name + "] READONLY";
                    }
                    else {
                        param.typeDefinition = "[" + param.name + "] READONLY";
                    }

                    break;

                case GUID:
                    param.typeDefinition = SSType.GUID.toString();
                    break;
                    
                case SQL_VARIANT:
                    param.typeDefinition = SSType.SQL_VARIANT.toString();
                    break;
                
                case GEOMETRY:
                    param.typeDefinition = SSType.GEOMETRY.toString();
                    break;
                    
                case GEOGRAPHY:
                    param.typeDefinition = SSType.GEOGRAPHY.toString();
                    break;
                default:
                    assert false : "Unexpected JDBC type " + dtv.getJdbcType();
                    break;
            }
        }

        void execute(DTV dtv,
                String strValue) throws SQLServerException {
            if (null != strValue && strValue.length() > DataTypes.SHORT_VARTYPE_MAX_CHARS)
                dtv.setJdbcType(JDBCType.LONGNVARCHAR);

            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Clob clobValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Byte byteValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Integer intValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                java.sql.Time timeValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                java.sql.Date dateValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                java.sql.Timestamp timestampValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                java.util.Date utildateValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                java.util.Calendar calendarValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                LocalDate localDateValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                LocalTime localTimeValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                LocalDateTime localDateTimeValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                OffsetTime offsetTimeValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                OffsetDateTime OffsetDateTimeValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                microsoft.sql.DateTimeOffset dtoValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Float floatValue) throws SQLServerException {
            scale = 4;
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Double doubleValue) throws SQLServerException {
            scale = 4;
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                BigDecimal bigDecimalValue) throws SQLServerException {
            if (null != bigDecimalValue) {
                scale = bigDecimalValue.scale();

                // BigDecimal in JRE 1.5 and later JVMs exposes an implementation detail
                // that allows representation of large values in small space by interpreting
                // a negative value for scale to imply scientific notation (e.g. 1 E 10^n)
                // would have a scale of -n. A BigDecimal value with a negative scale has
                // no fractional component.
                if (scale < 0)
                    scale = 0;
            }

            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Long longValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                java.math.BigInteger bigIntegerValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Short shortValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Boolean booleanValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                byte[] byteArrayValue) throws SQLServerException {
            if (null != byteArrayValue && byteArrayValue.length > DataTypes.SHORT_VARTYPE_MAX_BYTES)
                dtv.setJdbcType(dtv.getJdbcType().isBinary() ? JDBCType.LONGVARBINARY : JDBCType.LONGVARCHAR);

            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Blob blobValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                InputStream inputStreamValue) throws SQLServerException {
            StreamSetterArgs streamSetterArgs = dtv.getStreamSetterArgs();

            JDBCType jdbcType = dtv.getJdbcType();

            // If the JDBC type is currently a "short" type, then figure out if needs to be bumped up to a "long" type
            if (JDBCType.CHAR == jdbcType || JDBCType.VARCHAR == jdbcType || JDBCType.BINARY == jdbcType || JDBCType.VARBINARY == jdbcType) {
                // If we know the length is too long for a "short" type, then convert to a "long" type.
                if (streamSetterArgs.getLength() > DataTypes.SHORT_VARTYPE_MAX_BYTES)
                    dtv.setJdbcType(jdbcType.isBinary() ? JDBCType.LONGVARBINARY : JDBCType.LONGVARCHAR);

                // If the length of the value is unknown, then figure out whether it is at least longer
                // than what will fit into a "short" type.
                else if (DataTypes.UNKNOWN_STREAM_LENGTH == streamSetterArgs.getLength()) {
                    byte[] vartypeBytes = new byte[1 + DataTypes.SHORT_VARTYPE_MAX_BYTES];
                    BufferedInputStream bufferedStream = new BufferedInputStream(inputStreamValue, vartypeBytes.length);

                    int bytesRead = 0;

                    try {
                        bufferedStream.mark(vartypeBytes.length);

                        bytesRead = bufferedStream.read(vartypeBytes, 0, vartypeBytes.length);

                        if (-1 == bytesRead)
                            bytesRead = 0;

                        bufferedStream.reset();
                    }
                    catch (IOException e) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                        Object[] msgArgs = {e.toString()};
                        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
                    }

                    dtv.setValue(bufferedStream, JavaType.INPUTSTREAM);

                    // If the stream is longer than what can fit into the "short" type, then use the "long" type instead.
                    // Otherwise, we know the exact stream length since we reached end of stream before reading SHORT_VARTYPE_MAX_BYTES + 1
                    // bytes. So adjust the setter args to reflect the known length to avoid unnecessarily copying the
                    // stream again in SendByRPCOp.
                    if (bytesRead > DataTypes.SHORT_VARTYPE_MAX_BYTES)
                        dtv.setJdbcType(jdbcType.isBinary() ? JDBCType.LONGVARBINARY : JDBCType.LONGVARCHAR);
                    else
                        streamSetterArgs.setLength(bytesRead);
                }
            }

            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                Reader readerValue) throws SQLServerException {
            // If the JDBC type is currently a "short" type, then figure out if needs to be bumped up to a "long" type
            if (JDBCType.NCHAR == dtv.getJdbcType() || JDBCType.NVARCHAR == dtv.getJdbcType()) {
                StreamSetterArgs streamSetterArgs = dtv.getStreamSetterArgs();

                // If we know the length is too long for a "short" type, then convert to a "long" type.
                if (streamSetterArgs.getLength() > DataTypes.SHORT_VARTYPE_MAX_CHARS)
                    dtv.setJdbcType(JDBCType.LONGNVARCHAR);

                // If the length of the value is unknown, then figure out whether it is at least longer
                // than what will fit into a "short" type.
                else if (DataTypes.UNKNOWN_STREAM_LENGTH == streamSetterArgs.getLength()) {
                    char[] vartypeChars = new char[1 + DataTypes.SHORT_VARTYPE_MAX_CHARS];
                    BufferedReader bufferedReader = new BufferedReader(readerValue, vartypeChars.length);

                    int charsRead = 0;

                    try {
                        bufferedReader.mark(vartypeChars.length);

                        charsRead = bufferedReader.read(vartypeChars, 0, vartypeChars.length);

                        if (-1 == charsRead)
                            charsRead = 0;

                        bufferedReader.reset();
                    }
                    catch (IOException e) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                        Object[] msgArgs = {e.toString()};
                        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
                    }

                    dtv.setValue(bufferedReader, JavaType.READER);

                    if (charsRead > DataTypes.SHORT_VARTYPE_MAX_CHARS)
                        dtv.setJdbcType(JDBCType.LONGNVARCHAR);
                    else
                        streamSetterArgs.setLength(charsRead);
                }
            }

            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                SQLServerSQLXML xmlValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        void execute(DTV dtv,
                com.microsoft.sqlserver.jdbc.TVP tvpValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.DTVExecuteOp#execute(com.microsoft.sqlserver.jdbc.DTV, microsoft.sql.SqlVariant)
         */
        @Override
        void execute(DTV dtv,
                SqlVariant SqlVariantValue) throws SQLServerException {
            setTypeDefinition(dtv);
        }

    }

    /**
     * Returns a string used to define the parameter type for the server; null if no value for the parameter has been set or registered.
     */
    String getTypeDefinition(SQLServerConnection con,
            TDSReader tdsReader) throws SQLServerException {
        if (null == inputDTV)
            return null;

        inputDTV.executeOp(new GetTypeDefinitionOp(this, con));
        return typeDefinition;
    }

    void sendByRPC(TDSWriter tdsWriter,
            SQLServerConnection conn) throws SQLServerException {
        assert null != inputDTV : "Parameter was neither set nor registered";

        try {
            inputDTV.sendCryptoMetaData(this.cryptoMeta, tdsWriter);
            inputDTV.jdbcTypeSetByUser(getJdbcTypeSetByUser(), getValueLength());
            inputDTV.sendByRPC(name, null, conn.getDatabaseCollation(), valueLength, isOutput() ? outScale : scale, isOutput(), tdsWriter, conn);
        }
        finally {
            // reset the cryptoMeta in IOBuffer
            inputDTV.sendCryptoMetaData(null, tdsWriter);
        }
        // Per JDBC spec:
        // "If in the execution of a PreparedStatement object, the JDBC driver reads values set
        // for the parameter markers by the methods setAsciiStream, setBinaryStream, setCharacterStream,
        // setNCharacterStream, or setUnicodeStream, those parameters must be reset prior to the next
        // execution of the PreparedStatement object otherwise a SQLException will be thrown."
        //
        // Clear the input and setter DTVs to relinquish their hold on the stream resource and ensure
        // that the next call to execute will throw a SQLException (from getTypeDefinitionOp).
        // Don't clear the registered output DTV so that the parameter will still be an OUT (IN/OUT) parameter.
        if (JavaType.INPUTSTREAM == inputDTV.getJavaType() || JavaType.READER == inputDTV.getJavaType()) {
            inputDTV = setterDTV = null;
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

    void setValueLength(int valueLength) {
        this.valueLength = valueLength;
        userProvidesPrecision = true;
    }

    boolean getForceEncryption() {
        return forceEncryption;
    }

    void setForceEncryption(boolean forceEncryption) {
        this.forceEncryption = forceEncryption;
    }
}
