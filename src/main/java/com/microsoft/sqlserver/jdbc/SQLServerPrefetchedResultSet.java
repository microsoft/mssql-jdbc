package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Locale;
import java.util.UUID;

public class SQLServerPrefetchedResultSet extends SQLServerResultSet {
    private Object[] row = null;

    SQLServerPrefetchedResultSet(SQLServerStatement stmtIn) throws SQLServerException {
        super(stmtIn);
        this.row = new Object[this.columns.length];
    }

    private void loadRow() throws SQLServerException {
        initializeNullCompressedColumns();
        // Iterate on columns and get values and keep them in row
        for (int index = 1; index <= this.columns.length; ++index) {
            Column c = getColumn(index);
            SSType ssType = c.getTypeInfo().getSSType();
            
            // For temporal types, get as TIMESTAMP to preserve fractional seconds
            // except for DATETIMEOFFSET which needs to preserve timezone information
            JDBCType jdbcType = (ssType == SSType.TIME || ssType == SSType.DATETIME2) 
                               ? JDBCType.TIMESTAMP 
                               : ssType.getJDBCType();
            
            // Get the value from the TDS reader
            Object o = c.getValue(jdbcType, null, null, tdsReader, stmt);
            row[index - 1] = o;
        }
    }

    /**
     * Moves the cursor to the next row in the result set.
     *
     * @return true if the cursor is moved to a valid row, false if there are no more rows
     * @throws SQLServerException if a database access error occurs
     */
    @Override
    public boolean next() throws SQLServerException {
        super.checkClosed();
        boolean r = super.next();
        if (r) {
            loadRow();
        }
        return r;
    }

    /**
     * Updates the current row with the values in the row cache.
     *
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void updateRow() throws SQLException {
        super.updateRow();
        super.doRefreshRow(); // Refresh the row cache
        // Update the cached row with the latest values from the columns
        loadRow();
    }

    /**
     * Updates the current row with the values in the row cache.
     *
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void updateString(int columnIndex, String stringValue) throws SQLServerException {
        super.updateString(columnIndex, stringValue);
        row[columnIndex - 1] = stringValue;
    }

    /**
     * Moves the cursor to the specified row in the result set.
     *
     * @param row the row number to move to (1-based)
     * @return true if the cursor is moved to a valid row, false if the row is invalid
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean absolute(int row) throws SQLException {
        boolean r = super.absolute(row);
        if (r) {
            loadRow();
        }
        return r;
    }

    /**
     * Moves the cursor to the previous row in the result set.
     *
     * @return true if the cursor is moved to a valid row, false if there are no more rows
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean previous() throws SQLException {
        boolean r = super.previous();
        if (r) {
            loadRow();
        }
        return r;
    }

    /**
     * Moves the cursor to the first row in the result set.
     *
     * @return true if the cursor is moved to a valid row, false if there are no rows
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean first() throws SQLException {
        boolean r = super.first();
        if (r) {
            loadRow();
        }
        return r;
    }

    /**
     * Moves the cursor to the specified row in the result set.
     *
     * @param row the row number to move to (1-based)
     * @return true if the cursor is moved to a valid row, false if the row is invalid
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean relative(int rows) throws SQLException {
        boolean r = super.relative(rows);
        if (r) {
            loadRow();
        }
        return r;
    }

    @Override
    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLServerException {
        checkClosed();
        Object rawValue = row[columnIndex - 1];
        lastValueWasNull = (null == rawValue);
        
        if (rawValue == null) {
            return null;
        }
        
        if (rawValue instanceof byte[]) {
            return new java.io.ByteArrayInputStream((byte[]) rawValue);
        } else if (rawValue instanceof String) {
            // Convert hex string to bytes
            String hexStr = (String) rawValue;
            if (hexStr.length() % 2 != 0) {
                throw new SQLServerException("Invalid hex string length", null);
            }
            byte[] bytes = new byte[hexStr.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) Integer.parseInt(hexStr.substring(2 * i, 2 * i + 2), 16);
            }
            return new java.io.ByteArrayInputStream(bytes);
        } else {
            throw new SQLServerException("Cannot convert " + rawValue.getClass().getSimpleName() + " to InputStream", null);
        }
    }

    @Override
    public java.io.InputStream getBinaryStream(String columnName) throws SQLServerException {
        return getBinaryStream(findColumn(columnName));
    }

    @Override
    public java.io.Reader getCharacterStream(int columnIndex) throws SQLServerException {
        checkClosed();
        Object rawValue = row[columnIndex - 1];
        lastValueWasNull = (null == rawValue);
        
        if (rawValue == null) {
            return null;
        }
        
        if (rawValue instanceof String) {
            return new java.io.StringReader((String) rawValue);
        } else {
            // Convert to string first
            String strValue = rawValue.toString();
            return new java.io.StringReader(strValue);
        }
    }

    @Override
    public java.io.Reader getCharacterStream(String columnName) throws SQLServerException {
        return getCharacterStream(findColumn(columnName));
    }

    /**
     * Retrieves the value for the specified column index.
     *
     * @param columnIndex the column index (1-based)
     * @param jdbcType the JDBC type to convert to
     * @param getterArgs additional arguments for the getter
     * @param cal the calendar to use for date/time conversions
     * @return the value for the specified column index
     * @throws SQLServerException if a database access error occurs
     */
    @Override
    protected Object getValue(int columnIndex, JDBCType jdbcType, InputStreamGetterArgs getterArgs, Calendar cal)
            throws SQLServerException {
        Object rawValue = row[columnIndex - 1];
        lastValueWasNull = (null == rawValue);
        
        if (rawValue == null) {
            return null;
        }
        
        Column c = getColumn(columnIndex);
        SSType columnSSType = c.getTypeInfo().getSSType();

        // Special handling for temporal types that we store as TIMESTAMP for precision
        if ((columnSSType == SSType.TIME || columnSSType == SSType.DATETIME2) && 
            jdbcType == columnSSType.getJDBCType()) {
            // For TIME and DATETIME2 stored as Timestamp, convert back to the expected type
            if (columnSSType == SSType.TIME && jdbcType == JDBCType.TIME && rawValue instanceof java.sql.Timestamp) {
                return convertTemporalUsingDDC(rawValue, columnSSType, jdbcType, cal, c);
            } else if (columnSSType == SSType.DATETIME2 && jdbcType == JDBCType.TIMESTAMP) {
                return rawValue; // Already a Timestamp
            }
        }

        // If requesting same type as stored, return directly
        if (jdbcType == columnSSType.getJDBCType()) {
            return rawValue;
        }
        
        if (!columnSSType.convertsTo(jdbcType)) {
            DataTypes.throwConversionError(columnSSType.toString(), jdbcType.toString());
        }
        
        // Otherwise, use DDC to convert
        return convertUsingDDC(rawValue, jdbcType, columnSSType, getterArgs, cal, c);
    }
    
    private Object convertUsingDDC(Object rawValue, JDBCType targetJdbcType, SSType sourceSSType, 
                                   InputStreamGetterArgs getterArgs, Calendar cal, Column column) 
            throws SQLServerException {
        
        StreamType streamType = (getterArgs != null) ? getterArgs.streamType : StreamType.NONE;
        TypeInfo typeInfo = column.getTypeInfo();
        
        try {
            switch (sourceSSType) {
                case VARBINARYMAX:
                case VARCHARMAX:
                case NVARCHARMAX:
                case GEOMETRY:
                case GEOGRAPHY:
                case UDT: {
                    if (sourceSSType == SSType.VARCHARMAX && targetJdbcType == JDBCType.CLOB) {
                        try {
                            java.sql.Clob clob = stmt.connection.createClob();
                            String stringValue = (String) rawValue;
                            if (!stringValue.isEmpty()) {
                                clob.setString(1, stringValue);
                            }
                            return clob;
                        } catch (SQLException e) {
                            throw new SQLServerException("Failed to create CLOB from VARCHARMAX", e);
                        }
                    }
                    
                    // When NVARCHARMAX columns are accessed via getNClob() or similar methods
                    if (sourceSSType == SSType.NVARCHARMAX && targetJdbcType == JDBCType.NCLOB) {
                        try {
                            java.sql.NClob nclob = stmt.connection.createNClob();
                            String stringValue = (String) rawValue;
                            if (!stringValue.isEmpty()) {
                                nclob.setString(1, stringValue);
                            }
                            return nclob;
                        } catch (SQLException e) {
                            throw new SQLServerException("Failed to create NCLOB from NVARCHARMAX", e);
                        }
                    }

                    if (sourceSSType.category == SSType.Category.UDT) {
                        if (targetJdbcType == JDBCType.GEOMETRY) {
                            if (!typeInfo.getSSTypeName().equalsIgnoreCase(targetJdbcType.toString())) {
                                DataTypes.throwConversionError(typeInfo.getSSTypeName().toUpperCase(), targetJdbcType.toString());
                            }
                            return new Geometry((byte[]) rawValue);
                        } else if (targetJdbcType == JDBCType.GEOGRAPHY) {
                            if (!typeInfo.getSSTypeName().equalsIgnoreCase(targetJdbcType.toString())) {
                                DataTypes.throwConversionError(typeInfo.getSSTypeName().toUpperCase(), targetJdbcType.toString());
                            }
                            return new Geography((byte[]) rawValue);
                        }
                    }
                
                    // Handle BLOB conversion for VARBINARYMAX and other binary types
                    if (targetJdbcType == JDBCType.BLOB) {
                        // Use the deprecated constructor but it works for our case
                        @SuppressWarnings("deprecation")
                        SQLServerBlob blob = new SQLServerBlob(stmt.connection, (byte[]) rawValue);
                        return blob;
                    }

                    if (rawValue instanceof byte[]) {
                        return DDC.convertBytesToObject((byte[]) rawValue, targetJdbcType, typeInfo);
                    } else {
                        Charset charset = (column.getTypeInfo().getSQLCollation() != null) ? column.getTypeInfo().getSQLCollation().getCharset() : null;
                        return DDC.convertStringToObject((String) rawValue, charset, targetJdbcType, streamType);
                    }

                }

                case XML: {
                    if (rawValue instanceof String) {
                        return DDC.convertStringToObject((String) rawValue, null, targetJdbcType, streamType);
                    } else if (rawValue instanceof byte[]) {
                        return DDC.convertBytesToObject((byte[]) rawValue, targetJdbcType, typeInfo);
                    } else {
                        return DDC.convertStringToObject(rawValue.toString(), null, targetJdbcType, streamType);
                    }
                }

                // Convert other variable length native types
                // (CHAR/VARCHAR/TEXT/NCHAR/NVARCHAR/NTEXT/BINARY/VARBINARY/IMAGE) -> ANY jdbcType.
                case CHAR:
                case VARCHAR:
                case TEXT:
                case NCHAR:
                case NVARCHAR:
                case NTEXT:
                case IMAGE:
                case BINARY:
                case VARBINARY:
                case TIMESTAMP: // A special BINARY(8)
                {
                    if (rawValue instanceof String) {
                        Charset charset = (typeInfo.getSQLCollation() != null) 
                            ? typeInfo.getSQLCollation().getCharset() 
                            : null;
                        return DDC.convertStringToObject((String) rawValue, charset, targetJdbcType, streamType);
                    } else if (rawValue instanceof byte[]) {
                        return DDC.convertBytesToObject((byte[]) rawValue, targetJdbcType, typeInfo);
                    } else {
                        return DDC.convertStringToObject(rawValue.toString(), null, targetJdbcType, streamType);
                    }
                }

                // Convert BIT/TINYINT/SMALLINT/INTEGER/BIGINT native type -> ANY jdbcType.
                case BIT:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT: {
                    int valueLength = getValueLengthForInteger(sourceSSType);
                    switch (valueLength) {
                        case 8:
                            return DDC.convertLongToObject((Long) rawValue, targetJdbcType, sourceSSType, streamType);

                        case 4:
                            return DDC.convertIntegerToObject((Integer) rawValue, valueLength, targetJdbcType, streamType);

                        case 2:
                            return DDC.convertIntegerToObject((Short) rawValue, valueLength, targetJdbcType, streamType);

                        case 1:
                            return DDC.convertIntegerToObject(((Boolean) rawValue) ? 1 : 0, 1, targetJdbcType, streamType);

                        default:
                            assert false : "Unexpected valueLength" + valueLength;
                            throw new SQLServerException("jTDS Unexpected SSType " + sourceSSType, null, 0, null);
                    }
                }

                // Convert DECIMAL|NUMERIC native types -> ANY jdbcType.
                case DECIMAL:
                case NUMERIC:
                    return DDC.convertBigDecimalToObject((BigDecimal) rawValue, targetJdbcType, streamType);

                // Convert MONEY|SMALLMONEY native types -> ANY jdbcType.
                case MONEY:
                case SMALLMONEY:
                    int precision = (sourceSSType == SSType.MONEY) ? 8 : 4;
                    return DDC.convertMoneyToObject((BigDecimal) rawValue, targetJdbcType, streamType, precision);

                // Convert FLOAT native type -> ANY jdbcType.
                case FLOAT:
                    if (rawValue instanceof Double) {
                        return DDC.convertDoubleToObject((Double) rawValue, targetJdbcType, streamType);
                    } else {
                        return DDC.convertFloatToObject((Float) rawValue, targetJdbcType, streamType);
                    }
                   

                // Convert REAL native type -> ANY jdbcType.
                case REAL:
                    return DDC.convertFloatToObject((Float) rawValue, targetJdbcType, streamType);

                // Convert DATETIME|SMALLDATETIME native types -> ANY jdbcType.
                case DATETIME:
                case SMALLDATETIME:
                    return convertTemporalUsingDDC(rawValue, sourceSSType, targetJdbcType, cal, column);

                // Convert DATE native type -> ANY jdbcType.
                case DATE:
                    return convertTemporalUsingDDC(rawValue, sourceSSType, targetJdbcType, cal, column);
                // Convert TIME native type -> ANY jdbcType.
                case TIME:
                    return convertTemporalUsingDDC(rawValue, sourceSSType, targetJdbcType, cal, column);

                // Convert DATETIME2 native type -> ANY jdbcType.
                case DATETIME2:
                    return convertTemporalUsingDDC(rawValue, sourceSSType, targetJdbcType, cal, column);

                // Convert DATETIMEOFFSET native type -> ANY jdbcType.
                case DATETIMEOFFSET:
                    return convertTemporalUsingDDC(rawValue, sourceSSType, targetJdbcType, cal, column);

                // Convert GUID native type -> ANY jdbcType.
                case GUID:
                    switch (targetJdbcType) {
                        case BINARY:
                        case VARBINARY:
                        case LONGVARBINARY:{
                            return Util.asGuidByteArray(UUID.fromString(rawValue.toString()));
                        }
                        default:
                            if (StreamType.BINARY == streamType || StreamType.ASCII == streamType)
                                return new ByteArrayInputStream(rawValue.toString().getBytes(Encoding.UNICODE.charset()));
                            return rawValue;
                    }

                case SQL_VARIANT:
                    int valueLength = getValueLengthForSQLVariant(rawValue);
                    switch (valueLength) {
                        case 8:
                            return DDC.convertLongToObject((Long) rawValue, targetJdbcType, sourceSSType, streamType);

                        case 4:
                            return DDC.convertIntegerToObject((Integer) rawValue, valueLength, targetJdbcType, streamType);

                        case 2:
                            return DDC.convertIntegerToObject((Short) rawValue, valueLength, targetJdbcType, streamType);

                        case 1:
                            return DDC.convertIntegerToObject(((Boolean) rawValue) ? 1 : 0, 1, targetJdbcType, streamType);
                        
                        case -1:
                            if (rawValue instanceof String) {
                                return DDC.convertStringToObject((String) rawValue, null, targetJdbcType, streamType);
                            } else if (rawValue instanceof byte[]) {
                                return DDC.convertBytesToObject((byte[]) rawValue, targetJdbcType, typeInfo);
                            } else {
                                return DDC.convertStringToObject(rawValue.toString(), null, targetJdbcType, streamType);
                            }
                    }
                default:
                    assert false : "Unexpected SSType " + sourceSSType;
                    throw new SQLServerException("jTDS Unexpected SSType " + sourceSSType, null, 0, null);

            }
        } catch (java.io.UnsupportedEncodingException e) {
            throw new SQLServerException("Error converting string with charset", e);
        }
    }
    
    private int getValueLengthForInteger(SSType ssType) {
        switch (ssType) {
            case BIT:
                return 1;
            case SMALLINT:
            case TINYINT:
                return 2;
            case INTEGER:
                return 4;
            case BIGINT:
                return 8;
            default:
                return 4;//TO DO : error?
        }
    }

    private int getValueLengthForSQLVariant(Object rawValue) {
        if (rawValue instanceof Long) {
            return 8;
        } else if (rawValue instanceof Integer) {
            return 4;
        } else if (rawValue instanceof Short) {
            return 2;
        } else if (rawValue instanceof Byte || rawValue instanceof Boolean) {
            return 1;
        }
        return -1;
    }
    
    private Object convertTemporalUsingDDC(Object temporalValue, SSType sourceSSType, JDBCType targetJdbcType, Calendar cal, Column column) throws SQLServerException {
        switch (targetJdbcType) {
            case TIMESTAMP:
                if (temporalValue instanceof java.sql.Timestamp) {
                    return temporalValue;
                } else if (temporalValue instanceof java.sql.Date) {
                    return new java.sql.Timestamp(((java.sql.Date) temporalValue).getTime());
                } else if (temporalValue instanceof java.sql.Time) {
                    return new java.sql.Timestamp(((java.sql.Time) temporalValue).getTime());
                } else if (temporalValue instanceof microsoft.sql.DateTimeOffset) {
                    return ((microsoft.sql.DateTimeOffset) temporalValue).getTimestamp();
                }
                break;

            case DATE:
                if (temporalValue instanceof java.sql.Date) {
                    return temporalValue;
                } else if (temporalValue instanceof java.sql.Time) {
                    // TIME to DATE conversion - return epoch date (1970-01-01)
                    return new java.sql.Date(0);
                } else if (temporalValue instanceof java.sql.Timestamp) {
                    java.util.GregorianCalendar tempCal = new java.util.GregorianCalendar();
                    tempCal.setTime((java.util.Date) temporalValue);
                    tempCal.set(java.util.Calendar.HOUR_OF_DAY, 0);
                    tempCal.set(java.util.Calendar.MINUTE, 0);
                    tempCal.set(java.util.Calendar.SECOND, 0);
                    tempCal.set(java.util.Calendar.MILLISECOND, 0);
                    return new java.sql.Date(tempCal.getTime().getTime());
                } else if (temporalValue instanceof microsoft.sql.DateTimeOffset) {
                    microsoft.sql.DateTimeOffset dto = (microsoft.sql.DateTimeOffset) temporalValue;
                    java.util.GregorianCalendar tempCal = new java.util.GregorianCalendar();
                    tempCal.setTime(dto.getTimestamp());
                    tempCal.set(java.util.Calendar.HOUR_OF_DAY, 0);
                    tempCal.set(java.util.Calendar.MINUTE, 0);
                    tempCal.set(java.util.Calendar.SECOND, 0);
                    tempCal.set(java.util.Calendar.MILLISECOND, 0);
                    return new java.sql.Date(tempCal.getTime().getTime());
                }
                break;

            case TIME:
                if (temporalValue instanceof java.sql.Time) {
                    return temporalValue;
                } else if (temporalValue instanceof java.sql.Date) {
                    // DATE to TIME conversion - return midnight time
                    return new java.sql.Time(0);
                } else if (temporalValue instanceof java.sql.Timestamp) {
                    // Convert Timestamp to Time with proper rounding, following DDC logic
                    java.sql.Timestamp ts = (java.sql.Timestamp) temporalValue;
                    int subSecondNanos = ts.getNanos();
                    
                    // Follow DDC rounding logic: round if fractional nanos >= 0.5 milliseconds
                    if (subSecondNanos % 1_000_000 >= 500_000) { // Nanos.PER_MILLISECOND / 2
                        // Add 1 millisecond for rounding
                        long roundedTime = ts.getTime() + 1;
                        java.sql.Timestamp roundedTs = new java.sql.Timestamp(roundedTime);
                        java.util.Calendar tempCal = java.util.Calendar.getInstance();
                        tempCal.setTime(roundedTs);
                        
                        // Set date to epoch (1970-01-01) and keep only time
                        tempCal.set(java.util.Calendar.YEAR, 1970);
                        tempCal.set(java.util.Calendar.MONTH, 0);
                        tempCal.set(java.util.Calendar.DAY_OF_MONTH, 1);
                        
                        return new java.sql.Time(tempCal.getTimeInMillis());
                    } else {
                        // No rounding needed
                        java.util.Calendar tempCal = java.util.Calendar.getInstance();
                        tempCal.setTime(ts);
                        
                        // Set date to epoch (1970-01-01) and keep only time
                        tempCal.set(java.util.Calendar.YEAR, 1970);
                        tempCal.set(java.util.Calendar.MONTH, 0);
                        tempCal.set(java.util.Calendar.DAY_OF_MONTH, 1);
                        
                        return new java.sql.Time(tempCal.getTimeInMillis());
                    }
                } else if (temporalValue instanceof microsoft.sql.DateTimeOffset) {
                    microsoft.sql.DateTimeOffset dto = (microsoft.sql.DateTimeOffset) temporalValue;
                    java.sql.Timestamp ts = dto.getTimestamp();
                    int subSecondNanos = ts.getNanos();
                    
                    // Follow DDC rounding logic for DateTimeOffset to Time conversion too
                    if (subSecondNanos % 1_000_000 >= 500_000) { // Nanos.PER_MILLISECOND / 2
                        // Add 1 millisecond for rounding
                        long roundedTime = ts.getTime() + 1;
                        java.sql.Timestamp roundedTs = new java.sql.Timestamp(roundedTime);
                        java.util.Calendar tempCal2 = java.util.Calendar.getInstance();
                        tempCal2.setTime(roundedTs);
                        
                        // Set date to epoch (1970-01-01) and keep only time
                        tempCal2.set(java.util.Calendar.YEAR, 1970);
                        tempCal2.set(java.util.Calendar.MONTH, 0);
                        tempCal2.set(java.util.Calendar.DAY_OF_MONTH, 1);
                        
                        return new java.sql.Time(tempCal2.getTimeInMillis());
                    } else {
                        // No rounding needed
                        java.util.Calendar tempCal2 = java.util.Calendar.getInstance();
                        tempCal2.setTime(ts);
                        
                        // Set date to epoch (1970-01-01) and keep only time
                        tempCal2.set(java.util.Calendar.YEAR, 1970);
                        tempCal2.set(java.util.Calendar.MONTH, 0);
                        tempCal2.set(java.util.Calendar.DAY_OF_MONTH, 1);
                        
                        return new java.sql.Time(tempCal2.getTimeInMillis());
                    }
                }
                break;

            case LOCALDATETIME: {
                if (temporalValue instanceof java.sql.Timestamp) {
                    // Direct conversion without timezone adjustment for timestamp-related types
                    java.sql.Timestamp ts = (java.sql.Timestamp) temporalValue;
                    java.util.Calendar tempCal = java.util.Calendar.getInstance();
                    tempCal.setTime(ts);
                    return java.time.LocalDateTime.of(
                        tempCal.get(java.util.Calendar.YEAR),
                        tempCal.get(java.util.Calendar.MONTH) + 1,
                        tempCal.get(java.util.Calendar.DAY_OF_MONTH),
                        tempCal.get(java.util.Calendar.HOUR_OF_DAY),
                        tempCal.get(java.util.Calendar.MINUTE),
                        tempCal.get(java.util.Calendar.SECOND),
                        ts.getNanos()
                    );
                } else if (temporalValue instanceof microsoft.sql.DateTimeOffset) {
                    // For DateTimeOffset, check the connection setting for timezone behavior
                    microsoft.sql.DateTimeOffset dto = (microsoft.sql.DateTimeOffset) temporalValue;
                    if (stmt != null && stmt.connection != null && stmt.connection.getIgnoreOffsetOnDateTimeOffsetConversion()) {
                        // Ignore offset - use the original time components without timezone conversion
                        return dto.getOffsetDateTime().toLocalDateTime();
                    } else {
                        // Apply offset conversion - convert to system local timezone
                        return dto.getOffsetDateTime().atZoneSameInstant(java.time.ZoneId.systemDefault()).toLocalDateTime();
                    }
                } else if (temporalValue instanceof java.sql.Date) {
                    java.sql.Timestamp ts = new java.sql.Timestamp(((java.sql.Date) temporalValue).getTime());
                    return ts.toLocalDateTime();
                } else if (temporalValue instanceof java.sql.Time) {
                    java.sql.Timestamp ts = new java.sql.Timestamp(((java.sql.Time) temporalValue).getTime());
                    return ts.toLocalDateTime();
                }
                break;
            }

            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case CHAR:
            case NCHAR:
               return formatTemporalAsString(temporalValue, sourceSSType, column);
        }
        
        return temporalValue.toString();
    }
    
    /**
     * Format temporal values as strings with proper precision for DATETIME2 and other high-precision types.
     */
    private String formatTemporalAsString(Object temporalValue, SSType sourceSSType, Column column) {
        if (temporalValue instanceof java.sql.Timestamp) {
            java.sql.Timestamp ts = (java.sql.Timestamp) temporalValue;
            int subSecondNanos = ts.getNanos();
            
            // Get the actual scale from the column, default to 7 if not available
            int scale = (column != null) ? column.getTypeInfo().getScale() : 7;
            
            switch (sourceSSType) {
                case DATETIME2:
                    return String.format(Locale.US, "%1$tF %1$tT%2$s", ts, fractionalSecondsString(subSecondNanos, scale));
                case TIME:
                    return String.format(Locale.US, "%1$tT%2$s", ts, fractionalSecondsString(subSecondNanos, scale));
                case DATETIMEOFFSET:
                    return String.format(Locale.US, "%1$tF %1$tT%2$s", ts, fractionalSecondsString(subSecondNanos, scale));
                case DATETIME:
                case SMALLDATETIME:
                    return ts.toString();
                default:
                    return ts.toString();
            }
        } else if (temporalValue instanceof java.sql.Date) {
            return String.format(Locale.US, "%1$tF", (java.sql.Date) temporalValue);
        } else if (temporalValue instanceof java.sql.Time) {
            return String.format(Locale.US, "%1$tT", (java.sql.Time) temporalValue);
        } else if (temporalValue instanceof microsoft.sql.DateTimeOffset) {
            microsoft.sql.DateTimeOffset dto = (microsoft.sql.DateTimeOffset) temporalValue;
            // Get the actual scale from the column, default to 7 if not available
            int scale = (column != null) ? column.getTypeInfo().getScale() : 7;
            String dtoString = dto.toString();
            
            // If scale is 0, return as-is but remove any existing fractional seconds
            if (scale == 0) {
                // Remove fractional seconds if present
                int dotIndex = dtoString.indexOf('.');
                if (dotIndex > 0) {
                    int spaceIndex = dtoString.indexOf(' ', dotIndex);
                    if (spaceIndex > 0) {
                        return dtoString.substring(0, dotIndex) + dtoString.substring(spaceIndex);
                    }
                }
                return dtoString;
            }
            
            // Find where to insert/replace fractional seconds
            int dotIndex = dtoString.indexOf('.');
            int spaceIndex = dtoString.lastIndexOf(' ');
            
            if (dotIndex > 0 && spaceIndex > dotIndex) {
                // Has fractional seconds, replace them
                String prefix = dtoString.substring(0, dotIndex);
                String suffix = dtoString.substring(spaceIndex);
                java.sql.Timestamp ts = dto.getTimestamp();
                String fractional = fractionalSecondsString(ts.getNanos(), scale);
                return prefix + fractional + suffix;
            } else if (spaceIndex > 0) {
                // No fractional seconds, add them
                String prefix = dtoString.substring(0, spaceIndex);
                String suffix = dtoString.substring(spaceIndex);
                java.sql.Timestamp ts = dto.getTimestamp();
                String fractional = fractionalSecondsString(ts.getNanos(), scale);
                return prefix + fractional + suffix;
            } else {
                // Fallback - couldn't parse format
                return dtoString;
            }
        }
        
        // Fallback to default string conversion
        return temporalValue.toString();
    }
    
    // Copy of DDC.fractionalSecondsString method to match exact behavior
    private static String fractionalSecondsString(long subSecondNanos, int scale) {
        assert 0 <= subSecondNanos && subSecondNanos < 1_000_000_000; // Nanos.PER_SECOND
        assert 0 <= scale && scale <= 7; // TDS.MAX_FRACTIONAL_SECONDS_SCALE
        
        // Fast path for 0 scale (avoids creation of two BigDecimal objects and
        // two Strings when the answer is going to be "" anyway...)
        if (0 == scale)
            return "";
        
        return java.math.BigDecimal.valueOf(subSecondNanos % 1_000_000_000, 9).setScale(scale).toPlainString()
                .substring(1);
    }
}
