/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.EnumMap;
import java.util.EnumSet;


enum TDSType {
    // FIXEDLEN types
    BIT1(0x32), // 50
    INT8(0x7F), // 127
    INT4(0x38), // 56
    INT2(0x34), // 52
    INT1(0x30), // 48
    FLOAT4(0x3B), // 59
    FLOAT8(0x3E), // 62
    DATETIME4(0x3A), // 58
    DATETIME8(0x3D), // 61
    MONEY4(0x7A), // 122
    MONEY8(0x3C), // 60

    // BYTELEN types
    BITN(0x68), // 104
    INTN(0x26), // 38
    DECIMALN(0x6A), // 106
    NUMERICN(0x6C), // 108
    FLOATN(0x6D), // 109
    MONEYN(0x6E), // 110
    DATETIMEN(0x6F), // 111
    GUID(0x24), // 36
    DATEN(0x28), // 40
    TIMEN(0x29), // 41
    DATETIME2N(0x2a), // 42
    DATETIMEOFFSETN(0x2b), // 43

    // USHORTLEN type
    BIGCHAR(0xAF), // -81
    BIGVARCHAR(0xA7), // -89
    BIGBINARY(0xAD), // -83
    BIGVARBINARY(0xA5), // -91
    NCHAR(0xEF), // -17
    NVARCHAR(0xE7), // -15

    // PARTLEN types
    IMAGE(0x22), // 34
    TEXT(0x23), // 35
    NTEXT(0x63), // 99
    UDT(0xF0), // -16
    XML(0xF1), // -15

    // LONGLEN types
    SQL_VARIANT(0x62); // 98

    private final int intValue;

    private static final int MAXELEMENTS = 256;
    private static final TDSType[] VALUES = values();
    private static final TDSType valuesTypes[] = new TDSType[MAXELEMENTS];

    byte byteValue() {
        return (byte) intValue;
    }

    static {
        for (TDSType s : VALUES)
            valuesTypes[s.intValue] = s;
    }

    private TDSType(int intValue) {
        this.intValue = intValue;
    }

    static TDSType valueOf(int intValue) throws IllegalArgumentException {
        TDSType tdsType;

        if (!(0 <= intValue && intValue < valuesTypes.length) || null == (tdsType = valuesTypes[intValue])) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownSSType"));
            Object[] msgArgs = {intValue};
            throw new IllegalArgumentException(form.format(msgArgs));
        }

        return tdsType;
    }
}


enum SSLenType {
    FIXEDLENTYPE,
    BYTELENTYPE,
    USHORTLENTYPE,
    LONGLENTYPE,
    PARTLENTYPE
}


enum SSType {
    UNKNOWN(Category.UNKNOWN, "unknown", JDBCType.UNKNOWN),
    TINYINT(Category.NUMERIC, "tinyint", JDBCType.TINYINT),
    BIT(Category.NUMERIC, "bit", JDBCType.BIT),
    SMALLINT(Category.NUMERIC, "smallint", JDBCType.SMALLINT),
    INTEGER(Category.NUMERIC, "int", JDBCType.INTEGER),
    BIGINT(Category.NUMERIC, "bigint", JDBCType.BIGINT),
    FLOAT(Category.NUMERIC, "float", JDBCType.DOUBLE),
    REAL(Category.NUMERIC, "real", JDBCType.REAL),
    SMALLDATETIME(Category.DATETIME, "smalldatetime", JDBCType.SMALLDATETIME),
    DATETIME(Category.DATETIME, "datetime", JDBCType.DATETIME),
    DATE(Category.DATE, "date", JDBCType.DATE),
    TIME(Category.TIME, "time", JDBCType.TIME),
    DATETIME2(Category.DATETIME2, "datetime2", JDBCType.TIMESTAMP),
    DATETIMEOFFSET(Category.DATETIMEOFFSET, "datetimeoffset", JDBCType.DATETIMEOFFSET),
    SMALLMONEY(Category.NUMERIC, "smallmoney", JDBCType.SMALLMONEY),
    MONEY(Category.NUMERIC, "money", JDBCType.MONEY),
    CHAR(Category.CHARACTER, "char", JDBCType.CHAR),
    VARCHAR(Category.CHARACTER, "varchar", JDBCType.VARCHAR),
    VARCHARMAX(Category.LONG_CHARACTER, "varchar", JDBCType.LONGVARCHAR),
    TEXT(Category.LONG_CHARACTER, "text", JDBCType.LONGVARCHAR),
    NCHAR(Category.NCHARACTER, "nchar", JDBCType.NCHAR),
    NVARCHAR(Category.NCHARACTER, "nvarchar", JDBCType.NVARCHAR),
    NVARCHARMAX(Category.LONG_NCHARACTER, "nvarchar", JDBCType.LONGNVARCHAR),
    NTEXT(Category.LONG_NCHARACTER, "ntext", JDBCType.LONGNVARCHAR),
    BINARY(Category.BINARY, "binary", JDBCType.BINARY),
    VARBINARY(Category.BINARY, "varbinary", JDBCType.VARBINARY),
    VARBINARYMAX(Category.LONG_BINARY, "varbinary", JDBCType.LONGVARBINARY),
    IMAGE(Category.LONG_BINARY, "image", JDBCType.LONGVARBINARY),
    DECIMAL(Category.NUMERIC, "decimal", JDBCType.DECIMAL),
    NUMERIC(Category.NUMERIC, "numeric", JDBCType.NUMERIC),
    GUID(Category.GUID, "uniqueidentifier", JDBCType.GUID),
    SQL_VARIANT(Category.SQL_VARIANT, "sql_variant", JDBCType.SQL_VARIANT),
    UDT(Category.UDT, "udt", JDBCType.VARBINARY),
    XML(Category.XML, "xml", JDBCType.LONGNVARCHAR),
    TIMESTAMP(Category.TIMESTAMP, "timestamp", JDBCType.BINARY),
    GEOMETRY(Category.UDT, "geometry", JDBCType.GEOMETRY),
    GEOGRAPHY(Category.UDT, "geography", JDBCType.GEOGRAPHY);

    final Category category;
    private final String name;
    private final JDBCType jdbcType;
    private static final SSType[] VALUES = values();

    static final BigDecimal MAX_VALUE_MONEY = new BigDecimal("922337203685477.5807");
    static final BigDecimal MIN_VALUE_MONEY = new BigDecimal("-922337203685477.5808");
    static final BigDecimal MAX_VALUE_SMALLMONEY = new BigDecimal("214748.3647");
    static final BigDecimal MIN_VALUE_SMALLMONEY = new BigDecimal("-214748.3648");

    private SSType(Category category, String name, JDBCType jdbcType) {
        this.category = category;
        this.name = name;
        this.jdbcType = jdbcType;
    }

    public String toString() {
        return name;
    }

    final JDBCType getJDBCType() {
        return jdbcType;
    }

    static SSType of(String typeName) throws SQLServerException {
        for (SSType ssType : VALUES)
            if (ssType.name.equalsIgnoreCase(typeName))
                return ssType;

        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownSSType"));
        Object[] msgArgs = {typeName};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
        return SSType.UNKNOWN;
    }

    enum Category {
        BINARY,
        CHARACTER,
        DATE,
        DATETIME,
        DATETIME2,
        DATETIMEOFFSET,
        GUID,
        LONG_BINARY,
        LONG_CHARACTER,
        LONG_NCHARACTER,
        NCHARACTER,
        NUMERIC,
        UNKNOWN,
        TIME,
        TIMESTAMP,
        UDT,
        SQL_VARIANT,
        XML;

        private static final Category[] VALUES = values();
    }

    enum GetterConversion {
        NUMERIC(SSType.Category.NUMERIC, EnumSet.of(JDBCType.Category.NUMERIC, JDBCType.Category.CHARACTER,
                JDBCType.Category.BINARY)),

        DATETIME(SSType.Category.DATETIME, EnumSet.of(JDBCType.Category.DATE, JDBCType.Category.TIME,
                JDBCType.Category.TIMESTAMP, JDBCType.Category.CHARACTER, JDBCType.Category.BINARY)),

        DATETIME2(SSType.Category.DATETIME2, EnumSet.of(JDBCType.Category.DATE, JDBCType.Category.TIME,
                JDBCType.Category.TIMESTAMP, JDBCType.Category.CHARACTER)),

        DATE(SSType.Category.DATE, EnumSet.of(JDBCType.Category.DATE, JDBCType.Category.TIMESTAMP,
                JDBCType.Category.CHARACTER)),

        TIME(SSType.Category.TIME, EnumSet.of(JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP,
                JDBCType.Category.CHARACTER)),

        DATETIMEOFFSET(SSType.Category.DATETIMEOFFSET, EnumSet.of(JDBCType.Category.DATE, JDBCType.Category.TIME,
                JDBCType.Category.TIMESTAMP, JDBCType.Category.DATETIMEOFFSET, JDBCType.Category.CHARACTER)),

        CHARACTER(SSType.Category.CHARACTER, EnumSet.of(JDBCType.Category.NUMERIC, JDBCType.Category.DATE,
                JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP, JDBCType.Category.CHARACTER,
                JDBCType.Category.LONG_CHARACTER, JDBCType.Category.BINARY, JDBCType.Category.GUID)),

        LONG_CHARACTER(SSType.Category.LONG_CHARACTER, EnumSet.of(JDBCType.Category.NUMERIC, JDBCType.Category.DATE,
                JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP, JDBCType.Category.CHARACTER,
                JDBCType.Category.LONG_CHARACTER, JDBCType.Category.BINARY, JDBCType.Category.CLOB)),

        NCHARACTER(SSType.Category.NCHARACTER, EnumSet.of(JDBCType.Category.NUMERIC, JDBCType.Category.CHARACTER,
                JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER,
                JDBCType.Category.BINARY, JDBCType.Category.DATE, JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP)),

        LONG_NCHARACTER(SSType.Category.LONG_NCHARACTER, EnumSet.of(JDBCType.Category.NUMERIC,
                JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER,
                JDBCType.Category.LONG_NCHARACTER, JDBCType.Category.BINARY, JDBCType.Category.DATE,
                JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP, JDBCType.Category.CLOB, JDBCType.Category.NCLOB)),

        BINARY(SSType.Category.BINARY, EnumSet.of(JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY,
                JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER, JDBCType.Category.GUID)),

        LONG_BINARY(SSType.Category.LONG_BINARY, EnumSet.of(JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY,
                JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER, JDBCType.Category.BLOB)),

        TIMESTAMP(SSType.Category.TIMESTAMP, EnumSet.of(JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY,
                JDBCType.Category.CHARACTER)),

        XML(SSType.Category.XML, EnumSet.of(JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER,
                JDBCType.Category.CLOB, JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER,
                JDBCType.Category.NCLOB, JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY,
                JDBCType.Category.BLOB, JDBCType.Category.SQLXML)),

        UDT(SSType.Category.UDT, EnumSet.of(JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY,
                JDBCType.Category.CHARACTER, JDBCType.Category.GEOMETRY, JDBCType.Category.GEOGRAPHY)),

        GUID(SSType.Category.GUID, EnumSet.of(JDBCType.Category.BINARY, JDBCType.Category.CHARACTER)),

        SQL_VARIANT(SSType.Category.SQL_VARIANT, EnumSet.of(JDBCType.Category.CHARACTER, JDBCType.Category.SQL_VARIANT,
                JDBCType.Category.NUMERIC, JDBCType.Category.DATE, JDBCType.Category.TIME, JDBCType.Category.BINARY,
                JDBCType.Category.TIMESTAMP, JDBCType.Category.NCHARACTER, JDBCType.Category.GUID));

        private final SSType.Category from;
        private final EnumSet<JDBCType.Category> to;
        private static final GetterConversion[] VALUES = values();

        private GetterConversion(SSType.Category from, EnumSet<JDBCType.Category> to) {
            this.from = from;
            this.to = to;
        }

        private static final EnumMap<SSType.Category, EnumSet<JDBCType.Category>> conversionMap = new EnumMap<>(
                SSType.Category.class);

        static {
            for (SSType.Category category : SSType.Category.VALUES)
                conversionMap.put(category, EnumSet.noneOf(JDBCType.Category.class));

            for (GetterConversion conversion : VALUES)
                conversionMap.get(conversion.from).addAll(conversion.to);
        }

        static final boolean converts(SSType fromSSType, JDBCType toJDBCType) {
            return conversionMap.get(fromSSType.category).contains(toJDBCType.category);
        }
    }

    boolean convertsTo(JDBCType jdbcType) {
        return GetterConversion.converts(this, jdbcType);
    }
}


enum StreamType {
    NONE(JDBCType.UNKNOWN, "None"),
    ASCII(JDBCType.LONGVARCHAR, "AsciiStream"),
    BINARY(JDBCType.LONGVARBINARY, "BinaryStream"),
    CHARACTER(JDBCType.LONGVARCHAR, "CharacterStream"),
    NCHARACTER(JDBCType.LONGNVARCHAR, "NCharacterStream"),
    SQLXML(JDBCType.SQLXML, "SQLXML");

    // JDBC type most naturally associated with this type of stream
    private final JDBCType jdbcType;

    JDBCType getJDBCType() {
        return jdbcType;
    }

    // Display string to use when describing this stream type in traces and error messages
    private final String name;

    private StreamType(JDBCType jdbcType, String name) {
        this.jdbcType = jdbcType;
        this.name = name;
    }

    public String toString() {
        return name;
    }

    boolean convertsFrom(TypeInfo typeInfo) {
        // Special case handling for ASCII streams:
        if (ASCII == this) {
            // Conversion not allowed from XML to AsciiStream
            if (SSType.XML == typeInfo.getSSType())
                return false;

            // Conversion not allowed if collation doesn't cleanly convert to ASCII character set
            if (null != typeInfo.getSQLCollation() && !typeInfo.getSQLCollation().supportsAsciiConversion())
                return false;
        }

        return typeInfo.getSSType().convertsTo(jdbcType);
    }

    boolean convertsTo(TypeInfo typeInfo) {
        // Special case handling for ASCII streams:
        if (ASCII == this) {
            // Conversion not allowed to XML from AsciiStream
            if (SSType.XML == typeInfo.getSSType())
                return false;

            // Conversion not allowed if collation doesn't cleanly convert from ASCII character set
            if (null != typeInfo.getSQLCollation() && !typeInfo.getSQLCollation().supportsAsciiConversion())
                return false;
        }

        return jdbcType.convertsTo(typeInfo.getSSType());
    }
}


final class UserTypes {
    /* System defined UDTs */
    static final int TIMESTAMP = 0x0050;

    private UserTypes() {} // prevent instantiation
}


/**
 * Java class types that may be used as parameter or column values.
 *
 * Explicit external representation of the Java types eliminates multiple expensive calls to Class.isInstance (from DTV
 * and elsewhere) where the Java type of a parameter or column value needs to be known.
 *
 * !! IMPORTANT !! The tradeoff of using an external representation is that the driver must ensure that the JavaType
 * always reflects the Java type of the object instance, so as a general rule, any code that passes Object instances
 * around where the type must be later known, should always pass a JavaType as well.
 */
enum JavaType {
    // !! IMPORTANT !!
    // JavaType enumeration constants are arranged in the order checked
    // when determining the type of an arbitrary Object instance.
    //
    // Since the type determination process involves calls to Class.isInstance,
    // the following must be observed:
    //
    // 1. Constants MUST be arranged such that subclasses are listed first
    // to prevent an Object from being incorrectly determined as just an
    // instance of the superclass.
    //
    // 2. Notwithstanding the previous restriction, because the type determination
    // process involves a linear search, constants SHOULD be arranged
    // in order of decreasing probability of occurrence.
    //
    // 3. The last constant must be for the Object class to ensure that every
    // type of Object maps to some JavaType.
    INTEGER(Integer.class, JDBCType.INTEGER),
    STRING(String.class, JDBCType.CHAR),
    DATE(java.sql.Date.class, JDBCType.DATE),
    TIME(java.sql.Time.class, JDBCType.TIME),
    TIMESTAMP(java.sql.Timestamp.class, JDBCType.TIMESTAMP),
    UTILDATE(java.util.Date.class, JDBCType.TIMESTAMP),
    CALENDAR(java.util.Calendar.class, JDBCType.TIMESTAMP),
    LOCALDATE(getJavaClass("LocalDate"), JDBCType.DATE),
    LOCALTIME(getJavaClass("LocalTime"), JDBCType.TIME),
    LOCALDATETIME(getJavaClass("LocalDateTime"), JDBCType.TIMESTAMP),
    OFFSETTIME(getJavaClass("OffsetTime"), JDBCType.TIME_WITH_TIMEZONE),
    OFFSETDATETIME(getJavaClass("OffsetDateTime"), JDBCType.TIMESTAMP_WITH_TIMEZONE),
    DATETIMEOFFSET(microsoft.sql.DateTimeOffset.class, JDBCType.DATETIMEOFFSET),
    BOOLEAN(Boolean.class, JDBCType.BIT),
    BIGDECIMAL(BigDecimal.class, JDBCType.DECIMAL),
    DOUBLE(Double.class, JDBCType.DOUBLE),
    FLOAT(Float.class, JDBCType.REAL),
    SHORT(Short.class, JDBCType.SMALLINT),
    LONG(Long.class, JDBCType.BIGINT),
    BIGINTEGER(BigInteger.class, JDBCType.BIGINT),
    BYTE(Byte.class, JDBCType.TINYINT),
    BYTEARRAY(byte[].class, JDBCType.BINARY),
    // Check for NClob before checking for Clob, since NClob IS A Clob
    NCLOB(NClob.class, JDBCType.NCLOB),
    CLOB(Clob.class, JDBCType.CLOB),
    BLOB(Blob.class, JDBCType.BLOB),
    TVP(com.microsoft.sqlserver.jdbc.TVP.class, JDBCType.TVP),
    GEOMETRY(Geometry.class, JDBCType.GEOMETRY),
    GEOGRAPHY(Geography.class, JDBCType.GEOGRAPHY),

    INPUTSTREAM(InputStream.class, JDBCType.UNKNOWN) {
        // InputStreams are either ASCII or binary
        JDBCType getJDBCType(SSType ssType, JDBCType jdbcTypeFromApp) {
            JDBCType jdbcType;

            // When the backend type is known, the JDBC type is unknown.
            // That is, this method is being called from updateObject
            // rather than setObject.
            if (SSType.UNKNOWN != ssType) {
                // If the backend type is known to be textual then assume
                // that the stream is ASCII. Otherwise, assume that the
                // stream is binary. In this case XML is NOT considered
                // to be textual. When updating an XML column from an
                // InputStream through updateObject, the stream is assumed
                // to be binary, not ASCII.
                switch (ssType) {
                    case CHAR:
                    case VARCHAR:
                    case VARCHARMAX:
                    case TEXT:
                    case NCHAR:
                    case NVARCHAR:
                    case NVARCHARMAX:
                    case NTEXT:
                        jdbcType = JDBCType.LONGVARCHAR;
                        break;

                    case XML:
                    default:
                        jdbcType = JDBCType.LONGVARBINARY;
                        break;
                }
            }

            // When the backend type is unknown (i.e. the method is
            // being called from setObject rather than updateObject),
            // if the JDBC type is specified as something other
            // than textual, then assume the stream is binary.
            else {
                jdbcType = jdbcTypeFromApp.isTextual() ? JDBCType.LONGVARCHAR : JDBCType.LONGVARBINARY;
            }

            assert null != jdbcType;
            return jdbcType;
        }
    },

    READER(Reader.class, JDBCType.LONGVARCHAR),
    // Note: Only SQLServerSQLXML SQLXML instances are accepted by this driver
    SQLXML(SQLServerSQLXML.class, JDBCType.SQLXML),
    OBJECT(Object.class, JDBCType.UNKNOWN);

    private final Class<?> javaClass;
    private final JDBCType jdbcTypeFromJavaType;
    private static double jvmVersion = 0.0;
    private static final JavaType[] VALUES = values();

    private JavaType(Class<?> javaClass, JDBCType jdbcTypeFromJavaType) {
        this.javaClass = javaClass;
        this.jdbcTypeFromJavaType = jdbcTypeFromJavaType;
    }

    static Class<?> getJavaClass(String className) {
        if (0.0 == jvmVersion) {
            try {
                /*
                 * Note: getProperty could throw a SecurityException if there is a security manager that doesn't allow
                 * checkPropertyAccess. Unlikely to happen & doesn't appear to be a graceful way to handle so will let
                 * that exception through.
                 */
                String jvmSpecVersion = System.getProperty("java.specification.version");
                if (jvmSpecVersion != null) {
                    jvmVersion = Double.parseDouble(jvmSpecVersion);
                }
            } catch (NumberFormatException e) {
                // Setting the version to be less that 1.8 so we don't try to set every time.
                jvmVersion = 0.1;
            }
        }

        if (jvmVersion < 1.8) {
            return null;
        }

        switch (className) {
            case "LocalDate":
                return LocalDate.class;
            case "LocalTime":
                return LocalTime.class;
            case "LocalDateTime":
                return LocalDateTime.class;
            case "OffsetTime":
                return OffsetTime.class;
            case "OffsetDateTime":
                return OffsetDateTime.class;
            default:
                return null;
        }
    }

    static JavaType of(Object obj) {
        if (obj instanceof SQLServerDataTable || obj instanceof ResultSet || obj instanceof ISQLServerDataRecord)
            return JavaType.TVP;
        if (null != obj) {
            for (JavaType javaType : VALUES)
                // if JVM version is prior to Java 8, the javaClass variable can be
                // null if the java type is introduced in Java 8
                if (null != javaType.javaClass) {
                    if (javaType.javaClass.isInstance(obj))
                        return javaType;
                }
        }

        return JavaType.OBJECT;
    }

    /**
     * Returns the JDBC type to use with this Java type. We use the static JDBC type associated with the Java type by
     * default, ignoring the JDBC type specified by the application. This behavior is overridden for certain Java types,
     * such as InputStream, which requires the JDBC type to be specified externally in order to distinguish between.
     */
    JDBCType getJDBCType(SSType ssType, JDBCType jdbcTypeFromApp) {
        return jdbcTypeFromJavaType;
    }

    enum SetterConversionAE {
        BIT(JavaType.BOOLEAN, EnumSet.of(JDBCType.BIT, JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER,
                JDBCType.BIGINT)),

        SHORT(JavaType.SHORT, EnumSet.of(JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER, JDBCType.BIGINT)),

        INTEGER(JavaType.INTEGER, EnumSet.of(JDBCType.INTEGER, JDBCType.BIGINT)),
        LONG(JavaType.LONG, EnumSet.of(JDBCType.BIGINT)),

        BIGDECIMAL(JavaType.BIGDECIMAL, EnumSet.of(JDBCType.MONEY, JDBCType.SMALLMONEY, JDBCType.DECIMAL,
                JDBCType.NUMERIC)),

        BYTE(JavaType.BYTE, EnumSet.of(JDBCType.BINARY, JDBCType.VARBINARY, JDBCType.LONGVARBINARY, JDBCType.TINYINT)),

        BYTEARRAY(JavaType.BYTEARRAY, EnumSet.of(JDBCType.BINARY, JDBCType.VARBINARY, JDBCType.LONGVARBINARY)),

        DATE(JavaType.DATE, EnumSet.of(JDBCType.DATE)),

        DATETIMEOFFSET(JavaType.DATETIMEOFFSET, EnumSet.of(JDBCType.DATETIMEOFFSET)),

        DOUBLE(JavaType.DOUBLE, EnumSet.of(JDBCType.DOUBLE)),

        FLOAT(JavaType.FLOAT, EnumSet.of(JDBCType.REAL, JDBCType.DOUBLE)),

        STRING(JavaType.STRING, EnumSet.of(JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.LONGVARCHAR, JDBCType.NCHAR,
                JDBCType.NVARCHAR, JDBCType.LONGNVARCHAR, JDBCType.GUID)),

        TIME(JavaType.TIME, EnumSet.of(JDBCType.TIME)),

        TIMESTAMP(JavaType.TIMESTAMP, EnumSet.of(JDBCType.TIME, // This is needed to send nanoseconds to the driver as
                                                                // setTime() is only milliseconds
                JDBCType.TIMESTAMP, // This is datetime2
                JDBCType.DATETIME, JDBCType.SMALLDATETIME));

        private final EnumSet<JDBCType> to;
        private final JavaType from;
        private static final SetterConversionAE[] VALUES = values();

        private SetterConversionAE(JavaType from, EnumSet<JDBCType> to) {
            this.from = from;
            this.to = to;
        }

        private static final EnumMap<JavaType, EnumSet<JDBCType>> setterConversionAEMap = new EnumMap<>(JavaType.class);

        static {
            for (JavaType javaType : JavaType.VALUES)
                setterConversionAEMap.put(javaType, EnumSet.noneOf(JDBCType.class));

            for (SetterConversionAE conversion : VALUES)
                setterConversionAEMap.get(conversion.from).addAll(conversion.to);
        }

        static boolean converts(JavaType fromJavaType, JDBCType toJDBCType, boolean sendStringParametersAsUnicode) {
            if ((null == fromJavaType) || (JavaType.OBJECT == fromJavaType))
                return true;
            else if (!sendStringParametersAsUnicode && fromJavaType == JavaType.BYTEARRAY
                    && (toJDBCType == JDBCType.VARCHAR || toJDBCType == JDBCType.CHAR
                            || toJDBCType == JDBCType.LONGVARCHAR)) {
                // when column is encrypted and sendStringParametersAsUnicode is false,
                // does not throw exception if the column is char/varchar/varcharmax,
                // in order to allow send char/varchar/varcharmax as MBCS (BYTEARRAY type)
                return true;
            } else if (!setterConversionAEMap.containsKey(fromJavaType))
                return false;
            return setterConversionAEMap.get(fromJavaType).contains(toJDBCType);
        }
    }

}


enum JDBCType {
    UNKNOWN(Category.UNKNOWN, 999, Object.class.getName()),
    ARRAY(Category.UNKNOWN, java.sql.Types.ARRAY, Object.class.getName()),
    BIGINT(Category.NUMERIC, java.sql.Types.BIGINT, Long.class.getName()),
    BINARY(Category.BINARY, java.sql.Types.BINARY, "[B"),
    BIT(Category.NUMERIC, java.sql.Types.BIT, Boolean.class.getName()),
    BLOB(Category.BLOB, java.sql.Types.BLOB, java.sql.Blob.class.getName()),
    BOOLEAN(Category.NUMERIC, java.sql.Types.BOOLEAN, Boolean.class.getName()),
    CHAR(Category.CHARACTER, java.sql.Types.CHAR, String.class.getName()),
    CLOB(Category.CLOB, java.sql.Types.CLOB, java.sql.Clob.class.getName()),
    DATALINK(Category.UNKNOWN, java.sql.Types.DATALINK, Object.class.getName()),
    DATE(Category.DATE, java.sql.Types.DATE, java.sql.Date.class.getName()),
    DATETIMEOFFSET(Category.DATETIMEOFFSET, microsoft.sql.Types.DATETIMEOFFSET, microsoft.sql.DateTimeOffset.class
            .getName()),
    DECIMAL(Category.NUMERIC, java.sql.Types.DECIMAL, BigDecimal.class.getName()),
    DISTINCT(Category.UNKNOWN, java.sql.Types.DISTINCT, Object.class.getName()),
    DOUBLE(Category.NUMERIC, java.sql.Types.DOUBLE, Double.class.getName()),
    FLOAT(Category.NUMERIC, java.sql.Types.FLOAT, Double.class.getName()),
    INTEGER(Category.NUMERIC, java.sql.Types.INTEGER, Integer.class.getName()),
    JAVA_OBJECT(Category.UNKNOWN, java.sql.Types.JAVA_OBJECT, Object.class.getName()),
    LONGNVARCHAR(Category.LONG_NCHARACTER, -16, String.class.getName()),
    LONGVARBINARY(Category.LONG_BINARY, java.sql.Types.LONGVARBINARY, "[B"),
    LONGVARCHAR(Category.LONG_CHARACTER, java.sql.Types.LONGVARCHAR, String.class.getName()),
    NCHAR(Category.NCHARACTER, -15, String.class.getName()),
    NCLOB(Category.NCLOB, 2011, java.sql.NClob.class.getName()),
    NULL(Category.UNKNOWN, java.sql.Types.NULL, Object.class.getName()),
    NUMERIC(Category.NUMERIC, java.sql.Types.NUMERIC, BigDecimal.class.getName()),
    NVARCHAR(Category.NCHARACTER, -9, String.class.getName()),
    OTHER(Category.UNKNOWN, java.sql.Types.OTHER, Object.class.getName()),
    REAL(Category.NUMERIC, java.sql.Types.REAL, Float.class.getName()),
    REF(Category.UNKNOWN, java.sql.Types.REF, Object.class.getName()),
    ROWID(Category.UNKNOWN, -8, Object.class.getName()),
    SMALLINT(Category.NUMERIC, java.sql.Types.SMALLINT, Short.class.getName()),
    SQLXML(Category.SQLXML, 2009, Object.class.getName()),
    STRUCT(Category.UNKNOWN, java.sql.Types.STRUCT, Object.class.getName()),
    TIME(Category.TIME, java.sql.Types.TIME, java.sql.Time.class.getName()),
    TIME_WITH_TIMEZONE(Category.TIME_WITH_TIMEZONE, 2013, java.time.OffsetTime.class.getName()),
    TIMESTAMP(Category.TIMESTAMP, java.sql.Types.TIMESTAMP, java.sql.Timestamp.class.getName()),
    TIMESTAMP_WITH_TIMEZONE(Category.TIMESTAMP_WITH_TIMEZONE, 2014, java.time.OffsetDateTime.class.getName()),
    TINYINT(Category.NUMERIC, java.sql.Types.TINYINT, Short.class.getName()),
    VARBINARY(Category.BINARY, java.sql.Types.VARBINARY, "[B"),
    VARCHAR(Category.CHARACTER, java.sql.Types.VARCHAR, String.class.getName()),
    MONEY(Category.NUMERIC, microsoft.sql.Types.MONEY, BigDecimal.class.getName()),
    SMALLMONEY(Category.NUMERIC, microsoft.sql.Types.SMALLMONEY, BigDecimal.class.getName()),
    TVP(Category.TVP, microsoft.sql.Types.STRUCTURED, Object.class.getName()),
    DATETIME(Category.TIMESTAMP, microsoft.sql.Types.DATETIME, java.sql.Timestamp.class.getName()),
    SMALLDATETIME(Category.TIMESTAMP, microsoft.sql.Types.SMALLDATETIME, java.sql.Timestamp.class.getName()),
    GUID(Category.CHARACTER, microsoft.sql.Types.GUID, String.class.getName()),
    SQL_VARIANT(Category.SQL_VARIANT, microsoft.sql.Types.SQL_VARIANT, Object.class.getName()),
    GEOMETRY(Category.GEOMETRY, microsoft.sql.Types.GEOMETRY, Object.class.getName()),
    GEOGRAPHY(Category.GEOGRAPHY, microsoft.sql.Types.GEOGRAPHY, Object.class.getName()),
    LOCALDATETIME(Category.TIMESTAMP, java.sql.Types.TIMESTAMP, LocalDateTime.class.getName());

    final Category category;
    private final int intValue;
    private final String className;
    private static final JDBCType[] VALUES = values();

    final String className() {
        return className;
    }

    private JDBCType(Category category, int intValue, String className) {
        this.category = category;
        this.intValue = intValue;
        this.className = className;
    }

    /**
     * Returns the integer value of JDBCType.
     * 
     * @return integer representation of JDBCType
     */
    public int getIntValue() {
        return this.intValue;
    }

    enum Category {
        CHARACTER,
        LONG_CHARACTER,
        CLOB,
        NCHARACTER,
        LONG_NCHARACTER,
        NCLOB,
        BINARY,
        LONG_BINARY,
        BLOB,
        NUMERIC,
        DATE,
        TIME,
        TIMESTAMP,
        TIME_WITH_TIMEZONE,
        TIMESTAMP_WITH_TIMEZONE,
        DATETIMEOFFSET,
        SQLXML,
        UNKNOWN,
        TVP,
        GUID,
        SQL_VARIANT,
        GEOMETRY,
        GEOGRAPHY;

        private static final Category[] VALUES = values();
    }

    // This SetterConversion enum is based on the Category enum
    enum SetterConversion {
        CHARACTER(JDBCType.Category.CHARACTER, EnumSet.of(JDBCType.Category.NUMERIC, JDBCType.Category.DATE,
                JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP, JDBCType.Category.DATETIMEOFFSET,
                JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER,
                JDBCType.Category.LONG_NCHARACTER, JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY,
                JDBCType.Category.GUID, JDBCType.Category.SQL_VARIANT)),

        LONG_CHARACTER(JDBCType.Category.LONG_CHARACTER, EnumSet.of(JDBCType.Category.CHARACTER,
                JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER,
                JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY)),

        CLOB(JDBCType.Category.CLOB, EnumSet.of(JDBCType.Category.CLOB, JDBCType.Category.LONG_CHARACTER,
                JDBCType.Category.LONG_NCHARACTER)),

        NCHARACTER(JDBCType.Category.NCHARACTER, EnumSet.of(JDBCType.Category.NCHARACTER,
                JDBCType.Category.LONG_NCHARACTER, JDBCType.Category.NCLOB, JDBCType.Category.SQL_VARIANT)),

        LONG_NCHARACTER(JDBCType.Category.LONG_NCHARACTER, EnumSet.of(JDBCType.Category.NCHARACTER,
                JDBCType.Category.LONG_NCHARACTER)),

        NCLOB(JDBCType.Category.NCLOB, EnumSet.of(JDBCType.Category.LONG_NCHARACTER, JDBCType.Category.NCLOB)),

        BINARY(JDBCType.Category.BINARY, EnumSet.of(JDBCType.Category.NUMERIC, JDBCType.Category.DATE,
                JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP, JDBCType.Category.CHARACTER,
                JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER,
                JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY, JDBCType.Category.BLOB, JDBCType.Category.GUID,
                JDBCType.Category.SQL_VARIANT)),

        LONG_BINARY(JDBCType.Category.LONG_BINARY, EnumSet.of(JDBCType.Category.BINARY, JDBCType.Category.LONG_BINARY)),

        BLOB(JDBCType.Category.BLOB, EnumSet.of(JDBCType.Category.LONG_BINARY, JDBCType.Category.BLOB)),

        NUMERIC(JDBCType.Category.NUMERIC, EnumSet.of(JDBCType.Category.NUMERIC, JDBCType.Category.CHARACTER,
                JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER,
                JDBCType.Category.SQL_VARIANT)),

        DATE(JDBCType.Category.DATE, EnumSet.of(JDBCType.Category.DATE, JDBCType.Category.TIMESTAMP,
                JDBCType.Category.DATETIMEOFFSET, JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER,
                JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER, JDBCType.Category.SQL_VARIANT)),

        TIME(JDBCType.Category.TIME, EnumSet.of(JDBCType.Category.TIME, JDBCType.Category.TIMESTAMP,
                JDBCType.Category.DATETIMEOFFSET, JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER,
                JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER, JDBCType.Category.SQL_VARIANT)),

        TIMESTAMP(JDBCType.Category.TIMESTAMP, EnumSet.of(JDBCType.Category.DATE, JDBCType.Category.TIME,
                JDBCType.Category.TIMESTAMP, JDBCType.Category.DATETIMEOFFSET, JDBCType.Category.CHARACTER,
                JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER, JDBCType.Category.LONG_NCHARACTER,
                JDBCType.Category.SQL_VARIANT)),

        TIME_WITH_TIMEZONE(JDBCType.Category.TIME_WITH_TIMEZONE, EnumSet.of(JDBCType.Category.TIME_WITH_TIMEZONE,
                JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER,
                JDBCType.Category.LONG_NCHARACTER)),

        TIMESTAMP_WITH_TIMEZONE(JDBCType.Category.TIMESTAMP_WITH_TIMEZONE, EnumSet.of(
                JDBCType.Category.TIMESTAMP_WITH_TIMEZONE, JDBCType.Category.TIME_WITH_TIMEZONE,
                JDBCType.Category.CHARACTER, JDBCType.Category.LONG_CHARACTER, JDBCType.Category.NCHARACTER,
                JDBCType.Category.LONG_NCHARACTER)),

        DATETIMEOFFSET(JDBCType.Category.DATETIMEOFFSET, EnumSet.of(JDBCType.Category.DATE, JDBCType.Category.TIME,
                JDBCType.Category.TIMESTAMP, JDBCType.Category.DATETIMEOFFSET)),

        SQLXML(JDBCType.Category.SQLXML, EnumSet.of(JDBCType.Category.SQLXML)),

        TVP(JDBCType.Category.TVP, EnumSet.of(JDBCType.Category.TVP)),

        GEOMETRY(JDBCType.Category.GEOMETRY, EnumSet.of(JDBCType.Category.GEOMETRY)),

        GEOGRAPHY(JDBCType.Category.GEOGRAPHY, EnumSet.of(JDBCType.Category.GEOGRAPHY));

        private final JDBCType.Category from;
        private final EnumSet<JDBCType.Category> to;
        private static final SetterConversion[] VALUES = values();

        private SetterConversion(JDBCType.Category from, EnumSet<JDBCType.Category> to) {
            this.from = from;
            this.to = to;
        }

        private static final EnumMap<JDBCType.Category, EnumSet<JDBCType.Category>> conversionMap = new EnumMap<>(
                JDBCType.Category.class);

        static {
            for (JDBCType.Category category : JDBCType.Category.VALUES)
                conversionMap.put(category, EnumSet.noneOf(JDBCType.Category.class));

            for (SetterConversion conversion : VALUES)
                conversionMap.get(conversion.from).addAll(conversion.to);
        }

        static boolean converts(JDBCType fromJDBCType, JDBCType toJDBCType) {
            return conversionMap.get(fromJDBCType.category).contains(toJDBCType.category);
        }
    }

    boolean convertsTo(JDBCType jdbcType) {
        return SetterConversion.converts(this, jdbcType);
    }

    enum UpdaterConversion {
        CHARACTER(JDBCType.Category.CHARACTER, EnumSet.of(SSType.Category.NUMERIC, SSType.Category.DATE,
                SSType.Category.TIME, SSType.Category.DATETIME, SSType.Category.DATETIME2,
                SSType.Category.DATETIMEOFFSET, SSType.Category.CHARACTER, SSType.Category.LONG_CHARACTER,
                SSType.Category.NCHARACTER, SSType.Category.LONG_NCHARACTER, SSType.Category.XML,
                SSType.Category.BINARY, SSType.Category.LONG_BINARY, SSType.Category.UDT, SSType.Category.GUID,
                SSType.Category.TIMESTAMP, SSType.Category.SQL_VARIANT)),

        LONG_CHARACTER(JDBCType.Category.LONG_CHARACTER, EnumSet.of(SSType.Category.CHARACTER,
                SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER, SSType.Category.LONG_NCHARACTER,
                SSType.Category.XML, SSType.Category.BINARY, SSType.Category.LONG_BINARY)),

        CLOB(JDBCType.Category.CLOB, EnumSet.of(SSType.Category.LONG_CHARACTER, SSType.Category.LONG_NCHARACTER,
                SSType.Category.XML)),

        NCHARACTER(JDBCType.Category.NCHARACTER, EnumSet.of(SSType.Category.NCHARACTER, SSType.Category.LONG_NCHARACTER,
                SSType.Category.XML, SSType.Category.SQL_VARIANT)),

        LONG_NCHARACTER(JDBCType.Category.LONG_NCHARACTER, EnumSet.of(SSType.Category.NCHARACTER,
                SSType.Category.LONG_NCHARACTER, SSType.Category.XML)),

        NCLOB(JDBCType.Category.NCLOB, EnumSet.of(SSType.Category.LONG_NCHARACTER, SSType.Category.XML)),

        BINARY(JDBCType.Category.BINARY, EnumSet.of(SSType.Category.NUMERIC, SSType.Category.DATETIME,
                SSType.Category.CHARACTER, SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER,
                SSType.Category.LONG_NCHARACTER, SSType.Category.XML, SSType.Category.BINARY,
                SSType.Category.LONG_BINARY, SSType.Category.UDT, SSType.Category.TIMESTAMP, SSType.Category.GUID,
                SSType.Category.SQL_VARIANT)),

        LONG_BINARY(JDBCType.Category.LONG_BINARY, EnumSet.of(SSType.Category.XML, SSType.Category.BINARY,
                SSType.Category.LONG_BINARY, SSType.Category.UDT)),

        BLOB(JDBCType.Category.BLOB, EnumSet.of(SSType.Category.LONG_BINARY, SSType.Category.XML)),
        SQLXML(JDBCType.Category.SQLXML, EnumSet.of(SSType.Category.XML)),

        NUMERIC(JDBCType.Category.NUMERIC, EnumSet.of(SSType.Category.NUMERIC, SSType.Category.CHARACTER,
                SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER, SSType.Category.LONG_NCHARACTER,
                SSType.Category.SQL_VARIANT)),

        DATE(JDBCType.Category.DATE, EnumSet.of(SSType.Category.DATE, SSType.Category.DATETIME,
                SSType.Category.DATETIME2, SSType.Category.DATETIMEOFFSET, SSType.Category.CHARACTER,
                SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER, SSType.Category.LONG_NCHARACTER,
                SSType.Category.SQL_VARIANT)),

        TIME(JDBCType.Category.TIME, EnumSet.of(SSType.Category.TIME, SSType.Category.DATETIME,
                SSType.Category.DATETIME2, SSType.Category.DATETIMEOFFSET, SSType.Category.CHARACTER,
                SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER, SSType.Category.LONG_NCHARACTER,
                SSType.Category.SQL_VARIANT)),

        TIMESTAMP(JDBCType.Category.TIMESTAMP, EnumSet.of(SSType.Category.DATE, SSType.Category.TIME,
                SSType.Category.DATETIME, SSType.Category.DATETIME2, SSType.Category.DATETIMEOFFSET,
                SSType.Category.CHARACTER, SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER,
                SSType.Category.LONG_NCHARACTER, SSType.Category.SQL_VARIANT)),

        DATETIMEOFFSET(JDBCType.Category.DATETIMEOFFSET, EnumSet.of(SSType.Category.DATE, SSType.Category.TIME,
                SSType.Category.DATETIME, SSType.Category.DATETIME2, SSType.Category.DATETIMEOFFSET,
                SSType.Category.CHARACTER, SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER,
                SSType.Category.LONG_NCHARACTER)),

        TIME_WITH_TIMEZONE(JDBCType.Category.TIME_WITH_TIMEZONE, EnumSet.of(SSType.Category.TIME,
                SSType.Category.DATETIME, SSType.Category.DATETIME2, SSType.Category.DATETIMEOFFSET,
                SSType.Category.CHARACTER, SSType.Category.LONG_CHARACTER, SSType.Category.NCHARACTER,
                SSType.Category.LONG_NCHARACTER)),

        TIMESTAMP_WITH_TIMEZONE(JDBCType.Category.TIMESTAMP_WITH_TIMEZONE, EnumSet.of(SSType.Category.DATE,
                SSType.Category.TIME, SSType.Category.DATETIME, SSType.Category.DATETIME2,
                SSType.Category.DATETIMEOFFSET, SSType.Category.CHARACTER, SSType.Category.LONG_CHARACTER,
                SSType.Category.NCHARACTER, SSType.Category.LONG_NCHARACTER)),

        SQL_VARIANT(JDBCType.Category.SQL_VARIANT, EnumSet.of(SSType.Category.SQL_VARIANT));

        private final JDBCType.Category from;
        private final EnumSet<SSType.Category> to;
        private static final UpdaterConversion[] VALUES = values();

        private UpdaterConversion(JDBCType.Category from, EnumSet<SSType.Category> to) {
            this.from = from;
            this.to = to;
        }

        private static final EnumMap<JDBCType.Category, EnumSet<SSType.Category>> conversionMap = new EnumMap<>(
                JDBCType.Category.class);

        static {
            for (JDBCType.Category category : JDBCType.Category.VALUES)
                conversionMap.put(category, EnumSet.noneOf(SSType.Category.class));

            for (UpdaterConversion conversion : VALUES)
                conversionMap.get(conversion.from).addAll(conversion.to);
        }

        static boolean converts(JDBCType fromJDBCType, SSType toSSType) {
            return conversionMap.get(fromJDBCType.category).contains(toSSType.category);
        }
    }

    boolean convertsTo(SSType ssType) {
        return UpdaterConversion.converts(this, ssType);
    }

    static JDBCType of(int intValue) throws SQLServerException {
        for (JDBCType jdbcType : VALUES)
            if (jdbcType.intValue == intValue)
                return jdbcType;

        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownJDBCType"));
        Object[] msgArgs = {intValue};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
        return UNKNOWN;
    }

    /**
     * Identifies numerically signed data types.
     * 
     * @return true if the type can be signed
     */
    private final static EnumSet<JDBCType> signedTypes = EnumSet.of(SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE,
            DECIMAL, NUMERIC, MONEY, SMALLMONEY);

    boolean isSigned() {
        return signedTypes.contains(this);
    }

    /**
     * Identifies binary JDBC data types.
     * 
     * @return true if the JDBC type is binary
     */
    private final static EnumSet<JDBCType> binaryTypes = EnumSet.of(BINARY, VARBINARY, LONGVARBINARY, BLOB);

    boolean isBinary() {
        return binaryTypes.contains(this);
    }

    /**
     * Identifies textual JDBC data types -- those types for which conversion from another type is simply a matter of
     * representing that type as a string.
     *
     * Note: SQLXML does not qualify as a "textual" type in this context. That is, calling, for example,
     * timestamp.toString() does not result in an XML representation of a timestamp.
     *
     * @return true if the JDBC type is textual
     */
    private final static EnumSet<Category> textualCategories = EnumSet.of(Category.CHARACTER, Category.LONG_CHARACTER,
            Category.CLOB, Category.NCHARACTER, Category.LONG_NCHARACTER, Category.NCLOB);

    boolean isTextual() {
        return textualCategories.contains(category);
    }

    /**
     * Returns if data types are supported by JDBC.
     * 
     * @return true if the type is unsupported
     */
    boolean isUnsupported() {
        return Category.UNKNOWN == category;
    }

    /**
     * Returns this type's java.sql.Types value according to the JDBC version expected by the JRE.
     *
     * JDBC3 types are expected for SE 5. JDBC4 types are expected for SE 6 and later.
     */
    int asJavaSqlType() {
        if ("1.5".equals(Util.SYSTEM_SPEC_VERSION)) {
            switch (this) {
                case NCHAR:
                    return java.sql.Types.CHAR;
                case NVARCHAR:
                case SQLXML:
                    return java.sql.Types.VARCHAR;
                case LONGNVARCHAR:
                    return java.sql.Types.LONGVARCHAR;
                case NCLOB:
                    return java.sql.Types.CLOB;
                case ROWID:
                    return java.sql.Types.OTHER;
                default:
                    return intValue;
            }
        } else {
            return intValue;
        }
    }

    /*
     * Used for verifying if a data type can be normalized for AE.
     */
    enum NormalizationAE {
        CHARACTER_NORMALIZED_TO(JDBCType.CHAR, EnumSet.of(SSType.CHAR, SSType.VARCHAR, SSType.VARCHARMAX)),

        VARCHARACTER_NORMALIZED_TO(JDBCType.VARCHAR, EnumSet.of(SSType.CHAR, SSType.VARCHAR, SSType.VARCHARMAX)),

        LONGVARCHARACTER_NORMALIZED_TO(JDBCType.LONGVARCHAR, EnumSet.of(SSType.CHAR, SSType.VARCHAR,
                SSType.VARCHARMAX)),

        NCHAR_NORMALIZED_TO(JDBCType.NCHAR, EnumSet.of(SSType.NCHAR, SSType.NVARCHAR, SSType.NVARCHARMAX)),

        NVARCHAR_NORMALIZED_TO(JDBCType.NVARCHAR, EnumSet.of(SSType.NCHAR, SSType.NVARCHAR, SSType.NVARCHARMAX)),

        LONGNVARCHAR_NORMALIZED_TO(JDBCType.LONGNVARCHAR, EnumSet.of(SSType.NCHAR, SSType.NVARCHAR,
                SSType.NVARCHARMAX)),

        BIT_NORMALIZED_TO(JDBCType.BIT, EnumSet.of(SSType.BIT, SSType.TINYINT, SSType.SMALLINT, SSType.INTEGER,
                SSType.BIGINT)),

        TINYINT_NORMALIZED_TO(JDBCType.TINYINT, EnumSet.of(SSType.TINYINT, SSType.SMALLINT, SSType.INTEGER,
                SSType.BIGINT)),

        SMALLINT_NORMALIZED_TO(JDBCType.SMALLINT, EnumSet.of(SSType.SMALLINT, SSType.INTEGER, SSType.BIGINT)),

        INTEGER_NORMALIZED_TO(JDBCType.INTEGER, EnumSet.of(SSType.INTEGER, SSType.BIGINT)),

        BIGINT_NORMALIZED_TO(JDBCType.BIGINT, EnumSet.of(SSType.BIGINT)),

        BINARY_NORMALIZED_TO(JDBCType.BINARY, EnumSet.of(SSType.BINARY, SSType.VARBINARY, SSType.VARBINARYMAX)),

        VARBINARY_NORMALIZED_TO(JDBCType.VARBINARY, EnumSet.of(SSType.BINARY, SSType.VARBINARY, SSType.VARBINARYMAX)),

        LONGVARBINARY_NORMALIZED_TO(JDBCType.LONGVARBINARY, EnumSet.of(SSType.BINARY, SSType.VARBINARY,
                SSType.VARBINARYMAX)),

        FLOAT_NORMALIZED_TO(JDBCType.DOUBLE, EnumSet.of(SSType.FLOAT)),

        REAL_NORMALIZED_TO(JDBCType.REAL, EnumSet.of(SSType.REAL)),

        DECIMAL_NORMALIZED_TO(JDBCType.DECIMAL, EnumSet.of(SSType.DECIMAL, SSType.NUMERIC)),

        SMALLMONEY_NORMALIZED_TO(JDBCType.SMALLMONEY, EnumSet.of(SSType.SMALLMONEY, SSType.MONEY)),

        MONEY_NORMALIZED_TO(JDBCType.MONEY, EnumSet.of(SSType.MONEY)),

        NUMERIC_NORMALIZED_TO(JDBCType.NUMERIC, EnumSet.of(SSType.DECIMAL, SSType.NUMERIC)),

        DATE_NORMALIZED_TO(JDBCType.DATE, EnumSet.of(SSType.DATE)),

        TIME_NORMALIZED_TO(JDBCType.TIME, EnumSet.of(SSType.TIME)),

        DATETIME2_NORMALIZED_TO(JDBCType.TIMESTAMP, EnumSet.of(SSType.DATETIME2)),

        DATETIMEOFFSET_NORMALIZED_TO(JDBCType.DATETIMEOFFSET, EnumSet.of(SSType.DATETIMEOFFSET)),

        DATETIME_NORMALIZED_TO(JDBCType.DATETIME, EnumSet.of(SSType.DATETIME)),

        SMALLDATETIME_NORMALIZED_TO(JDBCType.SMALLDATETIME, EnumSet.of(SSType.SMALLDATETIME)),

        GUID_NORMALIZED_TO(JDBCType.GUID, EnumSet.of(SSType.GUID)),;

        private final JDBCType from;
        private final EnumSet<SSType> to;
        private static final NormalizationAE[] VALUES = values();

        private NormalizationAE(JDBCType from, EnumSet<SSType> to) {
            this.from = from;
            this.to = to;
        }

        private static final EnumMap<JDBCType, EnumSet<SSType>> normalizationMapAE = new EnumMap<>(JDBCType.class);

        static {
            for (JDBCType jdbcType : JDBCType.VALUES)
                normalizationMapAE.put(jdbcType, EnumSet.noneOf(SSType.class));

            for (NormalizationAE conversion : VALUES)
                normalizationMapAE.get(conversion.from).addAll(conversion.to);
        }

        static boolean converts(JDBCType fromJDBCType, SSType toSSType) {
            return normalizationMapAE.get(fromJDBCType).contains(toSSType);
        }
    }

    boolean normalizationCheck(SSType ssType) {
        return NormalizationAE.converts(this, ssType);
    }

}


final class DataTypes {
    // ResultSet & CallableStatement getXXX conversions (SSType --> JDBCType)
    static final void throwConversionError(String fromType, String toType) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedConversionFromTo"));
        Object[] msgArgs = {fromType, toType};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
    }

    /**
     * Max length in Unicode characters allowed by the "short" NVARCHAR type. Values longer than this must use
     * NVARCHAR(max) (Yukon or later) or NTEXT (Shiloh)
     */
    final static int SHORT_VARTYPE_MAX_CHARS = 4000;

    /**
     * Max length in bytes allowed by the "short" VARBINARY/VARCHAR types. Values longer than this must use
     * VARBINARY(max)/VARCHAR(max) (Yukon or later) or IMAGE/TEXT (Shiloh)
     */
    final static int SHORT_VARTYPE_MAX_BYTES = 8000;

    /**
     * A type with unlimited max size, known as varchar(max), varbinary(max) and nvarchar(max), which has a max size of
     * 0xFFFF, defined by PARTLENTYPE.
     */
    final static int SQL_USHORTVARMAXLEN = 65535; // 0xFFFF

    /**
     * From SQL Server 2005 Books Online : ntext, text, and image (Transact-SQL)
     * http://msdn.microsoft.com/en-us/library/ms187993.aspx
     * 
     * image "... through 2^31 - 1 (2,147,483,687) bytes."
     * 
     * text "... maximum length of 2^31 - 1 (2,147,483,687) characters."
     * 
     * ntext "... maximum length of 2^30 - 1 (1,073,741,823) characters."
     */
    final static int NTEXT_MAX_CHARS = 0x3FFFFFFF;
    final static int IMAGE_TEXT_MAX_BYTES = 0x7FFFFFFF;

    /**
     * Transact-SQL Data Types: http://msdn.microsoft.com/en-us/library/ms179910.aspx
     * 
     * varbinary(max) "max indicates that the maximum storage size is 2^31 - 1 bytes. The storage size is the actual
     * length of the data entered + 2 bytes."
     * 
     * varchar(max) "max indicates that the maximum storage size is 2^31 - 1 bytes. The storage size is the actual
     * length of the data entered + 2 bytes."
     * 
     * nvarchar(max) "max indicates that the maximum storage size is 2^31 - 1 bytes. The storage size, in bytes, is two
     * times the number of characters entered + 2 bytes."
     * 
     * Normally, that would mean that the maximum length of nvarchar(max) data is 0x3FFFFFFE characters and that the
     * maximum length of varchar(max) or varbinary(max) data is 0x3FFFFFFD bytes. However... Despite the documentation,
     * SQL Server returns 2^30 - 1 and 2^31 - 1 respectively as the PRECISION of these types, so use that instead.
     */
    final static int MAX_VARTYPE_MAX_CHARS = 0x3FFFFFFF;
    final static int MAX_VARTYPE_MAX_BYTES = 0x7FFFFFFF;

    // Special length indicator for varchar(max), nvarchar(max) and varbinary(max).
    static final int MAXTYPE_LENGTH = 0xFFFF;

    static final int UNKNOWN_STREAM_LENGTH = -1;

    // Utility methods to check a reported length against the maximums allowed
    static final long getCheckedLength(SQLServerConnection con, JDBCType jdbcType, long length,
            boolean allowUnknown) throws SQLServerException {
        long maxLength;

        switch (jdbcType) {
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
                // assert MAX_VARTYPE_MAX_CHARS == NTEXT_MAX_CHARS;
                maxLength = DataTypes.MAX_VARTYPE_MAX_CHARS;
                break;

            default:
                // assert MAX_VARTYPE_MAX_BYTES == IMAGE_TEXT_MAX_BYTES;
                maxLength = DataTypes.MAX_VARTYPE_MAX_BYTES;
                break;
        }

        if (length < (allowUnknown ? -1 : 0) || length > maxLength) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
            Object[] msgArgs = {length};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, false);
        }

        return length;
    }
}
