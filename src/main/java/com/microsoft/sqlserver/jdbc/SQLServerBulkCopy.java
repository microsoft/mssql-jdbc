/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;

import javax.sql.RowSet;

/**
 * Lets you efficiently bulk load a SQL Server table with data from another source. <br>
 * <br>
 * Microsoft SQL Server includes a popular command-prompt utility named bcp for moving data from one table to another, whether on a single server or
 * between servers. The SQLServerBulkCopy class lets you write code solutions in Java that provide similar functionality. There are other ways to load
 * data into a SQL Server table (INSERT statements, for example), but SQLServerBulkCopy offers a significant performance advantage over them. <br>
 * The SQLServerBulkCopy class can be used to write data only to SQL Server tables. However, the data source is not limited to SQL Server; any data
 * source can be used, as long as the data can be read with a ResultSet or ISQLServerBulkRecord instance.
 */
public class SQLServerBulkCopy implements java.lang.AutoCloseable {
    /*
     * Class to represent the column mappings between the source and destination table
     */
    private class ColumnMapping {
        String sourceColumnName = null;
        int sourceColumnOrdinal = -1;
        String destinationColumnName = null;
        int destinationColumnOrdinal = -1;

        ColumnMapping(String source,
                String dest) {
            this.sourceColumnName = source;
            this.destinationColumnName = dest;
        }

        ColumnMapping(String source,
                int dest) {
            this.sourceColumnName = source;
            this.destinationColumnOrdinal = dest;
        }

        ColumnMapping(int source,
                String dest) {
            this.sourceColumnOrdinal = source;
            this.destinationColumnName = dest;
        }

        ColumnMapping(int source,
                int dest) {
            this.sourceColumnOrdinal = source;
            this.destinationColumnOrdinal = dest;
        }
    }

    /*
     * Class name for logging.
     */
    private static final String loggerClassName = "com.microsoft.sqlserver.jdbc.SQLServerBulkCopy";

    private static final int SQL_SERVER_2016_VERSION = 13;

    /*
     * Logger
     */
    private static final java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger(loggerClassName);

    /*
     * Destination server connection.
     */
    private SQLServerConnection connection;

    /*
     * Options to control how the WriteToServer methods behave.
     */
    private SQLServerBulkCopyOptions copyOptions;

    /*
     * Mappings between columns in the data source and columns in the destination
     */
    private List<ColumnMapping> columnMappings;

    /*
     * Flag if SQLServerBulkCopy owns the connection and should close it when Close is called
     */
    private boolean ownsConnection;

    /*
     * Name of destination table on server.
     * 
     * If destinationTable has not been set when WriteToServer is called, an Exception is thrown.
     * 
     * destinationTable is a three-part name (<database>.<owningschema>.<name>). You can qualify the table name with its database and owning schema if
     * you choose. However, if the table name uses an underscore ("_") or any other special characters, you must escape the name using surrounding
     * brackets. For more information, see "Identifiers" in SQL Server Books Online.
     * 
     * You can bulk-copy data to a temporary table by using a value such as tempdb..#table or tempdb.<owner>.#table for the destinationTable property.
     */
    private String destinationTableName;

    /*
     * Source data (from a Record). Is null unless the corresponding version of writeToServer is called.
     */
    private ISQLServerBulkRecord sourceBulkRecord;

    /*
     * Source data (from ResultSet). Is null unless the corresponding version of writeToServer is called.
     */
    private ResultSet sourceResultSet;

    /*
     * Metadata for the source table columns
     */
    private ResultSetMetaData sourceResultSetMetaData;

    /* The CekTable for the destination table. */
    private CekTable destCekTable = null;

    /*
     * Metadata for the destination table columns
     */
    class BulkColumnMetaData {
        String columnName;
        SSType ssType = null;
        int jdbcType;
        int precision, scale;
        SQLCollation collation;
        byte[] flags = new byte[2];
        boolean isIdentity = false;
        boolean isNullable;
        String collationName;
        CryptoMetadata cryptoMeta = null;
        DateTimeFormatter dateTimeFormatter = null;

        // used when allowEncryptedValueModifications is on and encryption is turned off in connection
        String encryptionType = null;

        BulkColumnMetaData(Column column) throws SQLServerException {
            this.cryptoMeta = column.getCryptoMetadata();
            TypeInfo typeInfo = column.getTypeInfo();
            this.columnName = column.getColumnName();
            this.ssType = typeInfo.getSSType();
            this.flags = typeInfo.getFlags();
            this.isIdentity = typeInfo.isIdentity();
            this.isNullable = typeInfo.isNullable();
            precision = typeInfo.getPrecision();
            this.scale = typeInfo.getScale();
            collation = typeInfo.getSQLCollation();
            this.jdbcType = ssType.getJDBCType().getIntValue();
        }

        // This constructor is needed for the source meta data.
        BulkColumnMetaData(String colName,
                boolean isNullable,
                int precision,
                int scale,
                int jdbcType,
                DateTimeFormatter dateTimeFormatter) throws SQLServerException {
            this.columnName = colName;
            this.isNullable = isNullable;
            this.precision = precision;
            this.scale = scale;
            this.jdbcType = jdbcType;
            this.dateTimeFormatter = dateTimeFormatter;
        }

        BulkColumnMetaData(Column column,
                String collationName,
                String encryptionType) throws SQLServerException {
            this(column);
            this.collationName = collationName;
            this.encryptionType = encryptionType;
        }

        // update the cryptoMeta of source when reading from forward only resultset
        BulkColumnMetaData(BulkColumnMetaData bulkColumnMetaData,
                CryptoMetadata cryptoMeta) {
            this.columnName = bulkColumnMetaData.columnName;
            this.isNullable = bulkColumnMetaData.isNullable;
            this.precision = bulkColumnMetaData.precision;
            this.scale = bulkColumnMetaData.scale;
            this.jdbcType = bulkColumnMetaData.jdbcType;
            this.cryptoMeta = cryptoMeta;
        }
    };

    /*
     * A map to store the metadata information for the destination table.
     */
    private Map<Integer, BulkColumnMetaData> destColumnMetadata;

    /*
     * A map to store the metadata information for the source table.
     */
    private Map<Integer, BulkColumnMetaData> srcColumnMetadata;

    /*
     * Variable to store destination column count.
     */
    private int destColumnCount;

    /*
     * Variable to store source column count.
     */
    private int srcColumnCount;

    /*
     * Timer for the bulk copy operation. The other timeout timers in the TDS layer only measure the response of the first packet from SQL Server.
     */
    private final class BulkTimeoutTimer implements Runnable {
        private final int timeoutSeconds;
        private int secondsRemaining;
        private final TDSCommand command;
        private Thread timerThread;
        private volatile boolean canceled = false;

        BulkTimeoutTimer(int timeoutSeconds,
                TDSCommand command) {
            assert timeoutSeconds > 0;
            assert null != command;

            this.timeoutSeconds = timeoutSeconds;
            this.secondsRemaining = timeoutSeconds;
            this.command = command;
        }

        final void start() {
            timerThread = new Thread(this);
            timerThread.setDaemon(true);
            timerThread.start();
        }

        final void stop() {
            canceled = true;
            timerThread.interrupt();
        }

        final boolean expired() {
            return (secondsRemaining <= 0);
        }

        public void run() {
            try {
                // Poll every second while time is left on the timer.
                // Return if/when the timer is canceled.
                do {
                    if (canceled)
                        return;

                    Thread.sleep(1000);
                }
                while (--secondsRemaining > 0);
            }
            catch (InterruptedException e) {
                // re-interrupt the current thread, in order to restore the thread's interrupt status.
                Thread.currentThread().interrupt();
                return;
            }

            // If the timer wasn't canceled before it ran out of
            // time then interrupt the registered command.
            try {
                command.interrupt(SQLServerException.getErrString("R_queryTimedOut"));
            }
            catch (SQLServerException e) {
                // Unfortunately, there's nothing we can do if we
                // fail to time out the request. There is no way
                // to report back what happened.
                command.log(Level.FINE, "Command could not be timed out. Reason: " + e.getMessage());
            }
        }
    }

    private BulkTimeoutTimer timeoutTimer = null;
    
    /**
     * The maximum temporal precision we can send when using varchar(precision) in bulkcommand, to send a smalldatetime/datetime 
     * value.
     */
    private static final int sourceBulkRecordTemporalMaxPrecision = 50;

    /**
     * Initializes a new instance of the SQLServerBulkCopy class using the specified open instance of SQLServerConnection.
     * 
     * @param connection
     *            Open instance of Connection to destination server. Must be from the Microsoft JDBC driver for SQL Server.
     * @throws SQLServerException
     *             If the supplied type is not a connection from the Microsoft JDBC driver for SQL Server.
     */
    public SQLServerBulkCopy(Connection connection) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "SQLServerBulkCopy", connection);

        if (null == connection || !(connection instanceof SQLServerConnection)) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDestConnection"), null, false);
        }

        if (connection instanceof SQLServerConnection) {
            this.connection = (SQLServerConnection) connection;
        }
        else {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDestConnection"), null, false);
        }
        ownsConnection = false;

        // useInternalTransaction will be false by default. When a connection object is passed in, bulk copy
        // will use that connection object's transaction, i.e. no transaction management is done by bulk copy.
        copyOptions = new SQLServerBulkCopyOptions();

        initializeDefaults();

        loggerExternal.exiting(loggerClassName, "SQLServerBulkCopy");
    }

    /**
     * Initializes and opens a new instance of SQLServerConnection based on the supplied connectionString.
     * 
     * @param connectionUrl
     *            Connection string for the destination server.
     * @throws SQLServerException
     *             If a connection cannot be established.
     */
    public SQLServerBulkCopy(String connectionUrl) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "SQLServerBulkCopy", "connectionUrl not traced.");
        if ((connectionUrl == null) || "".equals(connectionUrl.trim())) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_nullConnection"), null, 0, false);
        }

        ownsConnection = true;
        SQLServerDriver driver = new SQLServerDriver();
        connection = (SQLServerConnection) driver.connect(connectionUrl, null);
        if (null == connection) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_invalidConnection"), null, 0, false);
        }

        copyOptions = new SQLServerBulkCopyOptions();

        initializeDefaults();

        loggerExternal.exiting(loggerClassName, "SQLServerBulkCopy");
    }

    /**
     * Adds a new column mapping, using ordinals to specify both the source and destination columns.
     * 
     * @param sourceColumn
     *            Source column ordinal.
     * @param destinationColumn
     *            Destination column ordinal.
     * @throws SQLServerException
     *             If the column mapping is invalid
     */
    public void addColumnMapping(int sourceColumn,
            int destinationColumn) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "addColumnMapping", new Object[] {sourceColumn, destinationColumn});

        if (0 >= sourceColumn) {
            throwInvalidArgument("sourceColumn");
        }
        else if (0 >= destinationColumn) {
            throwInvalidArgument("destinationColumn");
        }
        columnMappings.add(new ColumnMapping(sourceColumn, destinationColumn));

        loggerExternal.exiting(loggerClassName, "addColumnMapping");
    }

    /**
     * Adds a new column mapping, using an ordinal for the source column and a string for the destination column.
     * 
     * @param sourceColumn
     *            Source column ordinal.
     * @param destinationColumn
     *            Destination column name.
     * @throws SQLServerException
     *             If the column mapping is invalid
     */
    public void addColumnMapping(int sourceColumn,
            String destinationColumn) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "addColumnMapping", new Object[] {sourceColumn, destinationColumn});

        if (0 >= sourceColumn) {
            throwInvalidArgument("sourceColumn");
        }
        else if (null == destinationColumn || destinationColumn.isEmpty()) {
            throwInvalidArgument("destinationColumn");
        }
        columnMappings.add(new ColumnMapping(sourceColumn, destinationColumn.trim()));

        loggerExternal.exiting(loggerClassName, "addColumnMapping");
    }

    /**
     * Adds a new column mapping, using a column name to describe the source column and an ordinal to specify the destination column.
     * 
     * @param sourceColumn
     *            Source column name.
     * @param destinationColumn
     *            Destination column ordinal.
     * @throws SQLServerException
     *             If the column mapping is invalid
     */
    public void addColumnMapping(String sourceColumn,
            int destinationColumn) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "addColumnMapping", new Object[] {sourceColumn, destinationColumn});

        if (0 >= destinationColumn) {
            throwInvalidArgument("destinationColumn");
        }
        else if (null == sourceColumn || sourceColumn.isEmpty()) {
            throwInvalidArgument("sourceColumn");
        }
        columnMappings.add(new ColumnMapping(sourceColumn.trim(), destinationColumn));

        loggerExternal.exiting(loggerClassName, "addColumnMapping");
    }

    /**
     * Adds a new column mapping, using column names to specify both source and destination columns.
     * 
     * @param sourceColumn
     *            Source column name.
     * @param destinationColumn
     *            Destination column name.
     * @throws SQLServerException
     *             If the column mapping is invalid
     */
    public void addColumnMapping(String sourceColumn,
            String destinationColumn) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "addColumnMapping", new Object[] {sourceColumn, destinationColumn});

        if (null == sourceColumn || sourceColumn.isEmpty()) {
            throwInvalidArgument("sourceColumn");
        }
        else if (null == destinationColumn || destinationColumn.isEmpty()) {
            throwInvalidArgument("destinationColumn");
        }
        columnMappings.add(new ColumnMapping(sourceColumn.trim(), destinationColumn.trim()));

        loggerExternal.exiting(loggerClassName, "addColumnMapping");
    }

    /**
     * Clears the contents of the column mappings
     */
    public void clearColumnMappings() {
        loggerExternal.entering(loggerClassName, "clearColumnMappings");

        columnMappings.clear();

        loggerExternal.exiting(loggerClassName, "clearColumnMappings");
    }

    /**
     * Closes the SQLServerBulkCopy instance
     */
    public void close() {
        loggerExternal.entering(loggerClassName, "close");

        if (ownsConnection) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                // Ignore this exception
            }
        }

        loggerExternal.exiting(loggerClassName, "close");
    }

    /**
     * Gets the name of the destination table on the server.
     * 
     * @return Destination table name.
     */
    public String getDestinationTableName() {
        return destinationTableName;
    }

    /**
     * Sets the name of the destination table on the server.
     * 
     * @param tableName
     *            Destination table name.
     * @throws SQLServerException
     *             If the table name is null
     */
    public void setDestinationTableName(String tableName) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "setDestinationTableName", tableName);

        if (null == tableName || 0 == tableName.trim().length()) {
            throwInvalidArgument("tableName");
        }

        destinationTableName = tableName.trim();

        loggerExternal.exiting(loggerClassName, "setDestinationTableName");
    }

    /**
     * Gets the current SQLServerBulkCopyOptions.
     * 
     * @return Current SQLServerBulkCopyOptions settings.
     */
    public SQLServerBulkCopyOptions getBulkCopyOptions() {
        return copyOptions;
    }

    /**
     * Update the behavior of the SQLServerBulkCopy instance according to the options supplied, if supplied SQLServerBulkCopyOption is not null.
     * 
     * @param copyOptions
     *            Settings to change how the WriteToServer methods behave.
     * @throws SQLServerException
     *             If the SQLServerBulkCopyOption class was constructed using an existing Connection and the UseInternalTransaction option is
     *             specified.
     */
    public void setBulkCopyOptions(SQLServerBulkCopyOptions copyOptions) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "updateBulkCopyOptions", copyOptions);

        if (null != copyOptions) {
            // Verify that copyOptions does not have useInternalTransaction set.
            // UseInternalTrasnaction can only be used with a connection string.
            // Setting it with an external connection object should throw
            // exception.
            if (!ownsConnection && copyOptions.isUseInternalTransaction()) {
                SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidTransactionOption"), null, false);
            }

            this.copyOptions = copyOptions;
        }
        loggerExternal.exiting(loggerClassName, "updateBulkCopyOptions");
    }

    /**
     * Copies all rows in the supplied ResultSet to a destination table specified by the destinationTableName property of the SQLServerBulkCopy
     * object.
     * 
     * @param sourceData
     *            ResultSet to read data rows from.
     * @throws SQLServerException
     *             If there are any issues encountered when performing the bulk copy operation
     */
    public void writeToServer(ResultSet sourceData) throws SQLServerException {
        writeResultSet(sourceData, false);
    }

    /**
     * Copies all rows in the supplied RowSet to a destination table specified by the destinationTableName property of the SQLServerBulkCopy object.
     * 
     * @param sourceData
     *            RowSet to read data rows from.
     * @throws SQLServerException
     *             If there are any issues encountered when performing the bulk copy operation
     */
    public void writeToServer(RowSet sourceData) throws SQLServerException {
        writeResultSet(sourceData, true);
    }

    /**
     * Copies all rows in the supplied ResultSet to a destination table specified by the destinationTableName property of the SQLServerBulkCopy
     * object.
     * 
     * @param sourceData
     *            ResultSet to read data rows from.
     * @param isRowSet
     * @throws SQLServerException
     *             If there are any issues encountered when performing the bulk copy operation
     */
    private void writeResultSet(ResultSet sourceData,
            boolean isRowSet) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "writeToServer");

        if (null == sourceData) {
            throwInvalidArgument("sourceData");
        }

        try {
            if (isRowSet) // Default RowSet implementation in Java doesn't have isClosed() implemented so need to do an alternate check instead.
            {
                if (!sourceData.isBeforeFirst()) {
                    sourceData.beforeFirst();
                }
            }
            else {
                if (sourceData.isClosed()) {
                    SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_resultsetClosed"), null, false);
                }
            }
        }
        catch (SQLException e) {

            throw new SQLServerException(null, e.getMessage(), null, 0, false);
        }

        sourceResultSet = sourceData;

        sourceBulkRecord = null;

        // Save the resultset metadata as it is used in many places.
        try {
            sourceResultSetMetaData = sourceResultSet.getMetaData();
        }
        catch (SQLException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveColMeta"), e);
        }

        writeToServer();

        loggerExternal.exiting(loggerClassName, "writeToServer");
    }

    /**
     * Copies all rows from the supplied ISQLServerBulkRecord to a destination table specified by the destinationTableName property of the
     * SQLServerBulkCopy object.
     * 
     * @param sourceData
     *            SQLServerBulkReader to read data rows from.
     * @throws SQLServerException
     *             If there are any issues encountered when performing the bulk copy operation
     */
    public void writeToServer(ISQLServerBulkRecord sourceData) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "writeToServer");

        if (null == sourceData) {
            throwInvalidArgument("sourceData");
        }

        sourceBulkRecord = sourceData;
        sourceResultSet = null;

        writeToServer();

        loggerExternal.exiting(loggerClassName, "writeToServer");
    }

    /*
     * Initializes the defaults for member variables that require it.
     */
    private void initializeDefaults() {
        columnMappings = new LinkedList<>();
        destinationTableName = null;
        sourceBulkRecord = null;
        sourceResultSet = null;
        sourceResultSetMetaData = null;
        srcColumnCount = 0;
        srcColumnMetadata = null;
        destColumnMetadata = null;
        destColumnCount = 0;
    }

    private void sendBulkLoadBCP() throws SQLServerException {
        final class InsertBulk extends TDSCommand {
            InsertBulk() {
                super("InsertBulk", 0);
                int timeoutSeconds = copyOptions.getBulkCopyTimeout();
                timeoutTimer = (timeoutSeconds > 0) ? (new BulkTimeoutTimer(timeoutSeconds, this)) : null;
            }

            final boolean doExecute() throws SQLServerException {
                if (null != timeoutTimer) {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(this.toString() + ": Starting bulk timer...");

                    timeoutTimer.start();
                }

                // doInsertBulk inserts the rows in one batch. It returns true if there are more rows in
                // the resultset, false otherwise. We keep sending rows, one batch at a time until the
                // resultset is done.
                try {
                    while (doInsertBulk(this))
                        ;
                }
                catch (SQLServerException topLevelException) {
                    // Get to the root of this exception.
                    Throwable rootCause = topLevelException;
                    while (null != rootCause.getCause()) {
                        rootCause = rootCause.getCause();
                    }

                    // Check whether it is a timeout exception.
                    if (rootCause instanceof SQLException) {
                        checkForTimeoutException((SQLException) rootCause, timeoutTimer);
                    }

                    // It is not a timeout exception. Re-throw.
                    throw topLevelException;
                }

                if (null != timeoutTimer) {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(this.toString() + ": Stopping bulk timer...");

                    timeoutTimer.stop();
                }

                return true;
            }
        }

        connection.executeCommand(new InsertBulk());
    }

    /*
     * write ColumnData token in COLMETADATA header
     */
    private void writeColumnMetaDataColumnData(TDSWriter tdsWriter,
            int idx) throws SQLServerException {
        int srcColumnIndex, destPrecision;
        int bulkJdbcType, bulkPrecision, bulkScale;
        SQLCollation collation;
        SSType destSSType;
        boolean isStreaming, srcNullable;
        // For varchar, precision is the size of the varchar type.
        /*
         * UserType USHORT/ULONG; (Changed to ULONG in TDS 7.2) The user type ID of the data type of the column. The value will be 0x0000 with the
         * exceptions of TIMESTAMP (0x0050) and alias types (greater than 0x00FF)
         */
        byte[] userType = new byte[4];
        userType[0] = (byte) 0x00;
        userType[1] = (byte) 0x00;
        userType[2] = (byte) 0x00;
        userType[3] = (byte) 0x00;
        tdsWriter.writeBytes(userType);

        /*
         * Flags token - Bit flags in least significant bit order https://msdn.microsoft.com/en-us/library/dd357363.aspx flags[0] = (byte) 0x05;
         * flags[1] = (byte) 0x00;
         */
        int destColumnIndex = columnMappings.get(idx).destinationColumnOrdinal;

        /*
         * TYPE_INFO FIXEDLENTYPE Example INT: tdsWriter.writeByte((byte) 0x38);
         */
        srcColumnIndex = columnMappings.get(idx).sourceColumnOrdinal;

        byte flags[] = destColumnMetadata.get(destColumnIndex).flags;
        // If AllowEncryptedValueModification is set to true (and of course AE is off),
        // the driver will not sent AE information, so, we need to set Encryption bit flag to 0.
        if (null == srcColumnMetadata.get(srcColumnIndex).cryptoMeta && null == destColumnMetadata.get(destColumnIndex).cryptoMeta
                && true == copyOptions.isAllowEncryptedValueModifications()) {

            // flags[1]>>3 & 0x01 is the encryption bit flag.
            // it is the 4th least significant bit in this byte, so minus 8 to set it to 0.
            if (1 == (flags[1] >> 3 & 0x01)) {
                flags[1] = (byte) (flags[1] - 8);
            }
        }
        tdsWriter.writeBytes(flags);

        bulkJdbcType = srcColumnMetadata.get(srcColumnIndex).jdbcType;
        bulkPrecision = srcColumnMetadata.get(srcColumnIndex).precision;
        bulkScale = srcColumnMetadata.get(srcColumnIndex).scale;
        srcNullable = srcColumnMetadata.get(srcColumnIndex).isNullable;

        destSSType = destColumnMetadata.get(destColumnIndex).ssType;
        destPrecision = destColumnMetadata.get(destColumnIndex).precision;

        bulkPrecision = validateSourcePrecision(bulkPrecision, bulkJdbcType, destPrecision);

        collation = destColumnMetadata.get(destColumnIndex).collation;
        if (null == collation)
            collation = connection.getDatabaseCollation();

        if ((java.sql.Types.NCHAR == bulkJdbcType) || (java.sql.Types.NVARCHAR == bulkJdbcType) || (java.sql.Types.LONGNVARCHAR == bulkJdbcType)) {
            isStreaming = (DataTypes.SHORT_VARTYPE_MAX_CHARS < bulkPrecision) || (DataTypes.SHORT_VARTYPE_MAX_CHARS < destPrecision);
        }
        else {
            isStreaming = (DataTypes.SHORT_VARTYPE_MAX_BYTES < bulkPrecision) || (DataTypes.SHORT_VARTYPE_MAX_BYTES < destPrecision);
        }

        CryptoMetadata destCryptoMeta = destColumnMetadata.get(destColumnIndex).cryptoMeta;
        
        /*
         * if source is encrypted and destination is unenecrypted, use destination's sql type to send since there is no way of finding if source is
         * encrypted without accessing the resultset.
         * 
         * Send destination type if source resultset set is of type SQLServer, encryption is enabled and destination column is not encrypted
         */
        if ((sourceResultSet instanceof SQLServerResultSet) && (connection.isColumnEncryptionSettingEnabled()) && (null != destCryptoMeta)) {
            bulkJdbcType = destColumnMetadata.get(destColumnIndex).jdbcType;
            bulkPrecision = destPrecision;
            bulkScale = destColumnMetadata.get(destColumnIndex).scale;
        }

        // use varbinary to send if destination is encrypted
        if (((null != destColumnMetadata.get(destColumnIndex).encryptionType) && copyOptions.isAllowEncryptedValueModifications())
                || (null != destColumnMetadata.get(destColumnIndex).cryptoMeta)) {
            tdsWriter.writeByte((byte) 0xA5);

            if (isStreaming) {
                tdsWriter.writeShort((short) 0xFFFF);
            }
            else {
                tdsWriter.writeShort((short) (bulkPrecision));
            }

        }
        // In this case we will explicitly send binary value.
        else if (((java.sql.Types.CHAR == bulkJdbcType) || (java.sql.Types.VARCHAR == bulkJdbcType) || (java.sql.Types.LONGVARCHAR == bulkJdbcType))
                && (SSType.BINARY == destSSType || SSType.VARBINARY == destSSType || SSType.VARBINARYMAX == destSSType
                        || SSType.IMAGE == destSSType)) {
            if (isStreaming) {
                // Send as VARBINARY if streaming is enabled
                tdsWriter.writeByte((byte) 0xA5);
            }
            else {
                tdsWriter.writeByte((byte) ((SSType.BINARY == destSSType) ? 0xAD : 0xA5));
            }
            tdsWriter.writeShort((short) (bulkPrecision));
        }
        else {
            writeTypeInfo(tdsWriter, bulkJdbcType, bulkScale, bulkPrecision, destSSType, collation, isStreaming, srcNullable, false);
        }

        if (null != destCryptoMeta) {
            int baseDestJDBCType = destCryptoMeta.baseTypeInfo.getSSType().getJDBCType().asJavaSqlType();
            int baseDestPrecision = destCryptoMeta.baseTypeInfo.getPrecision();

            if ((java.sql.Types.NCHAR == baseDestJDBCType) || (java.sql.Types.NVARCHAR == baseDestJDBCType)
                    || (java.sql.Types.LONGNVARCHAR == baseDestJDBCType))
                isStreaming = (DataTypes.SHORT_VARTYPE_MAX_CHARS < baseDestPrecision);
            else
                isStreaming = (DataTypes.SHORT_VARTYPE_MAX_BYTES < baseDestPrecision);

            // Send CryptoMetaData
            tdsWriter.writeShort(destCryptoMeta.getOrdinal());	// Ordinal
            tdsWriter.writeBytes(userType);		// usertype
            // BaseTypeInfo
            writeTypeInfo(tdsWriter, baseDestJDBCType, destCryptoMeta.baseTypeInfo.getScale(), baseDestPrecision,
                    destCryptoMeta.baseTypeInfo.getSSType(), collation, isStreaming, srcNullable, true);
            tdsWriter.writeByte(destCryptoMeta.cipherAlgorithmId);
            tdsWriter.writeByte(destCryptoMeta.encryptionType.getValue());
            tdsWriter.writeByte(destCryptoMeta.normalizationRuleVersion);
        }

        /*
         * ColName token The column name. Contains the column name length and column name. see: SQLServerConnection.java toUCS16(String s)
         */
        int destColNameLen = columnMappings.get(idx).destinationColumnName.length();
        String destColName = columnMappings.get(idx).destinationColumnName;
        byte colName[] = new byte[2 * destColNameLen];

        for (int i = 0; i < destColNameLen; ++i) {
            int c = destColName.charAt(i);
            colName[2 * i] = (byte) (c & 0xFF);
            colName[2 * i + 1] = (byte) ((c >> 8) & 0xFF);
        }

        tdsWriter.writeByte((byte) destColNameLen);
        tdsWriter.writeBytes(colName);
    }

    private void writeTypeInfo(TDSWriter tdsWriter,
            int srcJdbcType,
            int srcScale,
            int srcPrecision,
            SSType destSSType,
            SQLCollation collation,
            boolean isStreaming,
            boolean srcNullable,
            boolean isBaseType) throws SQLServerException {
        switch (srcJdbcType) {
            case java.sql.Types.INTEGER: // 0x38
                if (!srcNullable) {
                    tdsWriter.writeByte(TDSType.INT4.byteValue());
                }
                else {
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x04);
                }
                break;

            case java.sql.Types.BIGINT: // 0x7f
                if (!srcNullable) {
                    tdsWriter.writeByte(TDSType.INT8.byteValue());
                }
                else {
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x08);
                }
                break;

            case java.sql.Types.BIT: // 0x32
                if (!srcNullable) {
                    tdsWriter.writeByte(TDSType.BIT1.byteValue());
                }
                else {
                    tdsWriter.writeByte(TDSType.BITN.byteValue());
                    tdsWriter.writeByte((byte) 0x01);
                }
                break;

            case java.sql.Types.SMALLINT: // 0x34
                if (!srcNullable) {
                    tdsWriter.writeByte(TDSType.INT2.byteValue());
                }
                else {
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x02);
                }
                break;

            case java.sql.Types.TINYINT: // 0x30
                if (!srcNullable) {
                    tdsWriter.writeByte(TDSType.INT1.byteValue());
                }
                else {
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x01);
                }
                break;

            case java.sql.Types.DOUBLE: // (FLT8TYPE) 0x3E
                if (!srcNullable) {
                    tdsWriter.writeByte(TDSType.FLOAT8.byteValue());
                }
                else {
                    tdsWriter.writeByte(TDSType.FLOATN.byteValue());
                    tdsWriter.writeByte((byte) 0x08);
                }
                break;

            case java.sql.Types.REAL: // (FLT4TYPE) 0x3B
                if (!srcNullable) {
                    tdsWriter.writeByte(TDSType.FLOAT4.byteValue());
                }
                else {
                    tdsWriter.writeByte(TDSType.FLOATN.byteValue());
                    tdsWriter.writeByte((byte) 0x04);
                }
                break;

            case microsoft.sql.Types.MONEY:
            case microsoft.sql.Types.SMALLMONEY:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                if (isBaseType && ((SSType.MONEY == destSSType) || (SSType.SMALLMONEY == destSSType))) {
                    tdsWriter.writeByte(TDSType.MONEYN.byteValue()); // 0x6E
                    if (SSType.MONEY == destSSType)
                        tdsWriter.writeByte((byte) 8);
                    else
                        tdsWriter.writeByte((byte) 4);
                }
                else {
                    if (java.sql.Types.DECIMAL == srcJdbcType)
                        tdsWriter.writeByte(TDSType.DECIMALN.byteValue()); // 0x6A
                    else
                        tdsWriter.writeByte(TDSType.NUMERICN.byteValue()); // 0x6C
                    tdsWriter.writeByte((byte) TDSWriter.BIGDECIMAL_MAX_LENGTH); // maximum length
                    tdsWriter.writeByte((byte) srcPrecision); // unsigned byte
                    tdsWriter.writeByte((byte) srcScale); // unsigned byte
                }
                break;

            case microsoft.sql.Types.GUID:
            case java.sql.Types.CHAR: // 0xAF
                if (isBaseType && (SSType.GUID == destSSType)) {
                    tdsWriter.writeByte(TDSType.GUID.byteValue());
                    tdsWriter.writeByte((byte) 0x10);
                }
                else {
                    // BIGCHARTYPE
                    tdsWriter.writeByte(TDSType.BIGCHAR.byteValue());

                    tdsWriter.writeShort((short) (srcPrecision));

                    collation.writeCollation(tdsWriter);
                }
                break;

            case java.sql.Types.NCHAR: // 0xEF
                tdsWriter.writeByte(TDSType.NCHAR.byteValue());
                tdsWriter.writeShort(isBaseType ? (short) (srcPrecision) : (short) (2 * srcPrecision));
                collation.writeCollation(tdsWriter);
                break;

            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.VARCHAR: // 0xA7
                // BIGVARCHARTYPE
                tdsWriter.writeByte(TDSType.BIGVARCHAR.byteValue());
                if (isStreaming) {
                    tdsWriter.writeShort((short) 0xFFFF);
                }
                else {
                    tdsWriter.writeShort((short) (srcPrecision));
                }
                collation.writeCollation(tdsWriter);
                break;

            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NVARCHAR: // 0xE7
                tdsWriter.writeByte(TDSType.NVARCHAR.byteValue());
                if (isStreaming) {
                    tdsWriter.writeShort((short) 0xFFFF);
                }
                else {
                    tdsWriter.writeShort(isBaseType ? (short) (srcPrecision) : (short) (2 * srcPrecision));
                }
                collation.writeCollation(tdsWriter);
                break;

            case java.sql.Types.BINARY: // 0xAD
                tdsWriter.writeByte(TDSType.BIGBINARY.byteValue());
                tdsWriter.writeShort((short) (srcPrecision));
                break;

            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.VARBINARY: // 0xA5
                // BIGVARBINARY
                tdsWriter.writeByte(TDSType.BIGVARBINARY.byteValue());
                if (isStreaming) {
                    tdsWriter.writeShort((short) 0xFFFF);
                }
                else {
                    tdsWriter.writeShort((short) (srcPrecision));
                }
                break;

            case microsoft.sql.Types.DATETIME:
            case microsoft.sql.Types.SMALLDATETIME:
            case java.sql.Types.TIMESTAMP:
                if ((!isBaseType) && (null != sourceBulkRecord)) {
                    tdsWriter.writeByte(TDSType.BIGVARCHAR.byteValue());
                    tdsWriter.writeShort((short) (srcPrecision));
                    collation.writeCollation(tdsWriter);
                }
                else {
                    switch (destSSType) {
                        case SMALLDATETIME:
                            if (!srcNullable)
                                tdsWriter.writeByte(TDSType.DATETIME4.byteValue());
                            else {
                                tdsWriter.writeByte(TDSType.DATETIMEN.byteValue());
                                tdsWriter.writeByte((byte) 4);
                            }
                            break;
                        case DATETIME:
                            if (!srcNullable)
                                tdsWriter.writeByte(TDSType.DATETIME8.byteValue());
                            else {
                                tdsWriter.writeByte(TDSType.DATETIMEN.byteValue());
                                tdsWriter.writeByte((byte) 8);
                            }
                            break;
                        default:
                            // DATETIME2 0x2A
                            tdsWriter.writeByte(TDSType.DATETIME2N.byteValue());
                            tdsWriter.writeByte((byte) srcScale);
                            break;
                    }
                }
                break;

            case java.sql.Types.DATE: // 0x28
                /*
                 * SQL Server supports numerous string literal formats for temporal types, hence sending them as varchar with approximate
                 * precision(length) needed to send supported string literals if destination is unencrypted. string literal formats supported by
                 * temporal types are available in MSDN page on data types.
                 */
                if ((!isBaseType) && (null != sourceBulkRecord)) {
                    tdsWriter.writeByte(TDSType.BIGVARCHAR.byteValue());
                    tdsWriter.writeShort((short) (srcPrecision));
                    collation.writeCollation(tdsWriter);
                }
                else {
                    tdsWriter.writeByte(TDSType.DATEN.byteValue());
                }
                break;

            case java.sql.Types.TIME: // 0x29
                if ((!isBaseType) && (null != sourceBulkRecord)) {
                    tdsWriter.writeByte(TDSType.BIGVARCHAR.byteValue());
                    tdsWriter.writeShort((short) (srcPrecision));
                    collation.writeCollation(tdsWriter);
                }
                else {
                    tdsWriter.writeByte(TDSType.TIMEN.byteValue());
                    tdsWriter.writeByte((byte) srcScale);
                }
                break;

            // Send as DATETIMEOFFSET for TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE
            case 2013:	// java.sql.Types.TIME_WITH_TIMEZONE
            case 2014:	// java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                tdsWriter.writeByte(TDSType.DATETIMEOFFSETN.byteValue());
                tdsWriter.writeByte((byte) srcScale);
                break;

            case microsoft.sql.Types.DATETIMEOFFSET: // 0x2B
                if ((!isBaseType) && (null != sourceBulkRecord)) {
                    tdsWriter.writeByte(TDSType.BIGVARCHAR.byteValue());
                    tdsWriter.writeShort((short) (srcPrecision));
                    collation.writeCollation(tdsWriter);
                }
                else {
                    tdsWriter.writeByte(TDSType.DATETIMEOFFSETN.byteValue());
                    tdsWriter.writeByte((byte) srcScale);
                }
                break;
            case microsoft.sql.Types.SQL_VARIANT:  //0x62
                tdsWriter.writeByte(TDSType.SQL_VARIANT.byteValue());
                tdsWriter.writeInt(TDS.SQL_VARIANT_LENGTH); 
                break;
            default:
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                String unsupportedDataType = JDBCType.of(srcJdbcType).toString().toLowerCase(Locale.ENGLISH);
                throw new SQLServerException(form.format(new Object[] {unsupportedDataType}), null, 0, null);
        }
    }

    /*
     * Writes the CEK table needed for AE. Cek table (with 0 entries) will be present if AE was enabled and server supports it! OR if encryption was
     * disabled in connection options
     */
    private void writeCekTable(TDSWriter tdsWriter) throws SQLServerException {
        if (connection.getServerSupportsColumnEncryption()) {
            if ((null != destCekTable) && (0 < destCekTable.getSize())) {
                tdsWriter.writeShort((short) destCekTable.getSize());
                for (int cekIndx = 0; cekIndx < destCekTable.getSize(); cekIndx++) {
                    tdsWriter.writeInt(destCekTable.getCekTableEntry(cekIndx).getColumnEncryptionKeyValues().get(0).databaseId);
                    tdsWriter.writeInt(destCekTable.getCekTableEntry(cekIndx).getColumnEncryptionKeyValues().get(0).cekId);
                    tdsWriter.writeInt(destCekTable.getCekTableEntry(cekIndx).getColumnEncryptionKeyValues().get(0).cekVersion);
                    tdsWriter.writeBytes(destCekTable.getCekTableEntry(cekIndx).getColumnEncryptionKeyValues().get(0).cekMdVersion);

                    // We don't need to send the keys. So count for EncryptionKeyValue is 0 in EK_INFO TDS rule.
                    tdsWriter.writeByte((byte) 0x00);
                }
            }
            else {
                // If no encrypted columns, but the connection setting is true write 0 as the size of the CekTable.
                tdsWriter.writeShort((short) 0);
            }
        }
    }

    /*
     * <COLMETADATA> ... </COLMETADATA>
     */
    private void writeColumnMetaData(TDSWriter tdsWriter) throws SQLServerException {

        /*
         * TDS rules for Always Encrypted metadata COLMETADATA = TokenType Count CekTable NoMetaData / (1 *ColumnData) CekTable = EkValueCount EK_INFO
         * EK_INFO = DatabaseId CekId CekVersion CekMDVersion Count EncryptionKeyValue
         */

        /*
         * TokenType: The token value is 0x81
         */
        tdsWriter.writeByte((byte) TDS.TDS_COLMETADATA);

        /*
         * Count token: The count of columns. Should be USHORT. Not Supported in Java.
         * 
         * Remedy: tdsWriter.writeShort((short)columnMappings.size());
         */
        byte[] count = new byte[2];
        count[0] = (byte) (columnMappings.size() & 0xFF);
        count[1] = (byte) ((columnMappings.size() >> 8) & 0xFF);
        tdsWriter.writeBytes(count);

        writeCekTable(tdsWriter);

        /*
         * Writing ColumnData section Columndata tokens is written for each destination column in columnMappings
         */
        for (int i = 0; i < columnMappings.size(); i++) {
            writeColumnMetaDataColumnData(tdsWriter, i);
        }
    }

    /*
     * Helper method that throws a timeout exception if the cause of the exception was that the query was cancelled
     */
    private void checkForTimeoutException(SQLException e,
            BulkTimeoutTimer timeoutTimer) throws SQLServerException {
        if ((null != e.getSQLState()) && (e.getSQLState().equals(SQLState.STATEMENT_CANCELED.getSQLStateCode())) && timeoutTimer.expired()) {
            // If SQLServerBulkCopy is managing the transaction, a rollback is needed.
            if (copyOptions.isUseInternalTransaction()) {
                connection.rollback();
            }

            throw new SQLServerException(SQLServerException.getErrString("R_queryTimedOut"), SQLState.STATEMENT_CANCELED, DriverError.NOT_SET, e);
        }
    }

    /*
     * Validates whether the source JDBC types are compatible with the destination table data types. We need to do this only once for the whole bulk
     * copy session.
     */
    private void validateDataTypeConversions(int srcColOrdinal,
            int destColOrdinal) throws SQLServerException {
        // Reducing unnecessary assignment to optimize for performance. This reduces code readability, but
        // may be worth it for the bulk copy.

        CryptoMetadata sourceCryptoMeta = srcColumnMetadata.get(srcColOrdinal).cryptoMeta;
        CryptoMetadata destCryptoMeta = destColumnMetadata.get(destColOrdinal).cryptoMeta;

        JDBCType srcJdbcType = (null != sourceCryptoMeta) ? sourceCryptoMeta.baseTypeInfo.getSSType().getJDBCType()
                : JDBCType.of(srcColumnMetadata.get(srcColOrdinal).jdbcType);

        SSType destSSType = (null != destCryptoMeta) ? destCryptoMeta.baseTypeInfo.getSSType() : destColumnMetadata.get(destColOrdinal).ssType;

        // Throw if the source type cannot be converted to the destination type.
        if (!srcJdbcType.convertsTo(destSSType)) {
            DataTypes.throwConversionError(srcJdbcType.toString(), destSSType.toString());
        }
    }

    private String getDestTypeFromSrcType(int srcColIndx,
            int destColIndx,
            TDSWriter tdsWriter) throws SQLServerException {
        boolean isStreaming;

        SSType destSSType = (null != destColumnMetadata.get(destColIndx).cryptoMeta)
                ? destColumnMetadata.get(destColIndx).cryptoMeta.baseTypeInfo.getSSType() : destColumnMetadata.get(destColIndx).ssType;

        int bulkJdbcType, bulkPrecision, bulkScale;
        int srcPrecision;

        bulkJdbcType = srcColumnMetadata.get(srcColIndx).jdbcType;
        // For char/varchar precision is the size.
        bulkPrecision = srcPrecision = srcColumnMetadata.get(srcColIndx).precision;
        int destPrecision = destColumnMetadata.get(destColIndx).precision;
        bulkScale = srcColumnMetadata.get(srcColIndx).scale;

        CryptoMetadata destCryptoMeta = destColumnMetadata.get(destColIndx).cryptoMeta;
        if (null != destCryptoMeta || (null == destCryptoMeta && copyOptions.isAllowEncryptedValueModifications())) {
            // Encrypted columns are sent as binary data.
            tdsWriter.setCryptoMetaData(destColumnMetadata.get(destColIndx).cryptoMeta);

            // if destination is encrypted send metadata from destination and not from source
            if (DataTypes.SHORT_VARTYPE_MAX_BYTES < destPrecision) {
                return "varbinary(max)";
            }
            else {
                return "varbinary(" + destColumnMetadata.get(destColIndx).precision + ")";
            }
        }

        // isAllowEncryptedValueModifications is used only with source result set.
        if ((null != sourceResultSet) && (null != destColumnMetadata.get(destColIndx).encryptionType)
                && copyOptions.isAllowEncryptedValueModifications()) {
            return "varbinary(" + bulkPrecision + ")";
        }
        bulkPrecision = validateSourcePrecision(srcPrecision, bulkJdbcType, destPrecision);

        /*
         * if source is encrypted and destination is unenecrypted, use destination's sql type to send since there is no way of finding if source is
         * encrypted without accessing the resultset.
         * 
         * Send destination type if source resultset set is of type SQLServer, encryption is enabled and destination column is not encrypted
         */
        if ((sourceResultSet instanceof SQLServerResultSet) && (connection.isColumnEncryptionSettingEnabled()) && (null != destCryptoMeta)) {
            bulkJdbcType = destColumnMetadata.get(destColIndx).jdbcType;
            bulkPrecision = destPrecision;
            bulkScale = destColumnMetadata.get(destColIndx).scale;
        }

        if ((java.sql.Types.NCHAR == bulkJdbcType) || (java.sql.Types.NVARCHAR == bulkJdbcType) || (java.sql.Types.LONGNVARCHAR == bulkJdbcType)) {
            isStreaming = (DataTypes.SHORT_VARTYPE_MAX_CHARS < srcPrecision) || (DataTypes.SHORT_VARTYPE_MAX_CHARS < destPrecision);
        }
        else {
            isStreaming = (DataTypes.SHORT_VARTYPE_MAX_BYTES < srcPrecision) || (DataTypes.SHORT_VARTYPE_MAX_BYTES < destPrecision);
        }

        // SQL Server does not convert string to binary, we will have to explicitly convert before sending.
        if (Util.isCharType(bulkJdbcType) && Util.isBinaryType(destSSType)) {
            if (isStreaming)
                return "varbinary(max)";
            else
                // Return binary(n) or varbinary(n) or varbinary(max) depending on destination type/precision.
                return destSSType.toString() + "(" + ((DataTypes.SHORT_VARTYPE_MAX_BYTES < destPrecision) ? "max" : destPrecision) + ")";
        }

        switch (bulkJdbcType) {
            case java.sql.Types.INTEGER:
                return "int";

            case java.sql.Types.SMALLINT:
                return "smallint";

            case java.sql.Types.BIGINT:
                return "bigint";

            case java.sql.Types.BIT:
                return "bit";

            case java.sql.Types.TINYINT:
                return "tinyint";

            case java.sql.Types.DOUBLE:
                return "float";

            case java.sql.Types.REAL:
                return "real";

            case microsoft.sql.Types.MONEY:
            case microsoft.sql.Types.SMALLMONEY:
            case java.sql.Types.DECIMAL:
                return "decimal(" + bulkPrecision + ", " + bulkScale + ")";

            case java.sql.Types.NUMERIC:
                return "numeric(" + bulkPrecision + ", " + bulkScale + ")";

            case microsoft.sql.Types.GUID:
            case java.sql.Types.CHAR:
                // For char the value has to be between 0 to 8000.
                return "char(" + bulkPrecision + ")";

            case java.sql.Types.NCHAR:
                return "NCHAR(" + bulkPrecision + ")";

            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.VARCHAR:
                // Here the actual size of the varchar is used from the source table.
                // Doesn't need to match with the exact size of data or with the destination column size.
                if (isStreaming) {
                    return "varchar(max)";
                }
                else {
                    return "varchar(" + bulkPrecision + ")";
                }

                // For INSERT BULK operations, XMLTYPE is to be sent as NVARCHAR(N) or NVARCHAR(MAX) data type.
                // An error is produced if XMLTYPE is specified.
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NVARCHAR:
                if (isStreaming) {
                    return "NVARCHAR(MAX)";
                }
                else {
                    return "NVARCHAR(" + bulkPrecision + ")";
                }

            case java.sql.Types.BINARY:
                // For binary the value has to be between 0 to 8000.
                return "binary(" + bulkPrecision + ")";

            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.VARBINARY:
                if (isStreaming)
                    return "varbinary(max)";
                else
                    return "varbinary(" + bulkPrecision + ")";

            case microsoft.sql.Types.DATETIME:
            case microsoft.sql.Types.SMALLDATETIME:
            case java.sql.Types.TIMESTAMP:
                switch (destSSType) {
                    case SMALLDATETIME:
                        if (null != sourceBulkRecord) {
                            return "varchar(" + ((0 == bulkPrecision) ? sourceBulkRecordTemporalMaxPrecision  : bulkPrecision) + ")";
                        }
                        else {
                            return "smalldatetime";
                        }
                    case DATETIME:
                        if (null != sourceBulkRecord) {
                            return "varchar(" + ((0 == bulkPrecision) ? sourceBulkRecordTemporalMaxPrecision  : bulkPrecision) + ")";
                        }
                        else {
                            return "datetime";
                        }
                    default:
                        // datetime2
                        /*
                         * If encrypted, varbinary will be returned before. The code will come here only if unencrypted. For unencrypted bulk copy if
                         * the source is CSV, we send the data as varchar and SQL Server will do the conversion. if the source is ResultSet, we send
                         * the data as the corresponding temporal type.
                         */
                        if (null != sourceBulkRecord) {
                            return "varchar(" + ((0 == bulkPrecision) ? destPrecision : bulkPrecision) + ")";
                        }
                        else {
                            return "datetime2(" + bulkScale + ")";
                        }
                }

            case java.sql.Types.DATE:
                /*
                 * If encrypted, varbinary will be returned before. The code will come here only if unencrypted. For unencrypted bulk copy if the
                 * source is CSV, we send the data as varchar and SQL Server will do the conversion. if the source is ResultSet, we send the data as
                 * the corresponding temporal type.
                 */
                if (null != sourceBulkRecord) {
                    return "varchar(" + ((0 == bulkPrecision) ? destPrecision : bulkPrecision) + ")";
                }
                else {
                    return "date";
                }

            case java.sql.Types.TIME:
                /*
                 * If encrypted, varbinary will be returned before. The code will come here only if unencrypted. For unencrypted bulk copy if the
                 * source is CSV, we send the data as varchar and SQL Server will do the conversion. if the source is ResultSet, we send the data as
                 * the corresponding temporal type.
                 */
                if (null != sourceBulkRecord) {
                    return "varchar(" + ((0 == bulkPrecision) ? destPrecision : bulkPrecision) + ")";
                }
                else {
                    return "time(" + bulkScale + ")";
                }

                // Return DATETIMEOFFSET for TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE
            case 2013:	// java.sql.Types.TIME_WITH_TIMEZONE
            case 2014:	// java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                return "datetimeoffset(" + bulkScale + ")";

            case microsoft.sql.Types.DATETIMEOFFSET:
                /*
                 * If encrypted, varbinary will be returned before. The code will come here only if unencrypted. For unencrypted bulk copy if the
                 * source is CSV, we send the data as varchar and SQL Server will do the conversion. if the source is ResultSet, we send the data as
                 * the corresponding temporal type.
                 */
                if (null != sourceBulkRecord) {
                    return "varchar(" + ((0 == bulkPrecision) ? destPrecision : bulkPrecision) + ")";
                }
                else {
                    return "datetimeoffset(" + bulkScale + ")";
                }
            case microsoft.sql.Types.SQL_VARIANT:
                return "sql_variant";
            default: {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                Object[] msgArgs = {JDBCType.of(bulkJdbcType).toString().toLowerCase(Locale.ENGLISH)};
                SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
            }
        }
        return null;
    }

    private String createInsertBulkCommand(TDSWriter tdsWriter) throws SQLServerException {
        StringBuilder bulkCmd = new StringBuilder();
        List<String> bulkOptions = new ArrayList<>();
        String endColumn = " , ";
        bulkCmd.append("INSERT BULK " + destinationTableName + " (");

        for (int i = 0; i < (columnMappings.size()); ++i) {
            if (i == columnMappings.size() - 1) {
                endColumn = " ) ";
            }
            ColumnMapping colMapping = columnMappings.get(i);
            String columnCollation = destColumnMetadata.get(columnMappings.get(i).destinationColumnOrdinal).collationName;
            String addCollate = "";

            String destType = getDestTypeFromSrcType(colMapping.sourceColumnOrdinal, colMapping.destinationColumnOrdinal, tdsWriter)
                    .toUpperCase(Locale.ENGLISH);
            if (null != columnCollation && columnCollation.trim().length() > 0) {
                // we are adding collate in command only for char and varchar
                if (null != destType && (destType.toLowerCase(Locale.ENGLISH).trim().startsWith("char") || destType.toLowerCase(Locale.ENGLISH).trim().startsWith("varchar")))
                    addCollate = " COLLATE " + columnCollation;
            }
            bulkCmd.append("[" + colMapping.destinationColumnName + "] " + destType + addCollate + endColumn);
        }

        if (true == copyOptions.isCheckConstraints()) {
            bulkOptions.add("CHECK_CONSTRAINTS");
        }

        if (true == copyOptions.isFireTriggers()) {
            bulkOptions.add("FIRE_TRIGGERS");
        }

        if (true == copyOptions.isKeepNulls()) {
            bulkOptions.add("KEEP_NULLS");
        }

        if (copyOptions.getBatchSize() > 0) {
            bulkOptions.add("ROWS_PER_BATCH = " + copyOptions.getBatchSize());
        }

        if (true == copyOptions.isTableLock()) {
            bulkOptions.add("TABLOCK");
        }

        if (true == copyOptions.isAllowEncryptedValueModifications()) {
            bulkOptions.add("ALLOW_ENCRYPTED_VALUE_MODIFICATIONS");
        }

        Iterator<String> it = bulkOptions.iterator();
        if (it.hasNext()) {
            bulkCmd.append(" with (");
            while (it.hasNext()) {
                bulkCmd.append(it.next());
                if (it.hasNext()) {
                    bulkCmd.append(", ");
                }
            }
            bulkCmd.append(")");
        }

        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.finer(this.toString() + " TDSCommand: " + bulkCmd);

        return bulkCmd.toString();
    }

    private boolean doInsertBulk(TDSCommand command) throws SQLServerException {
        if (copyOptions.isUseInternalTransaction()) {
            // Begin a manual transaction for this batch.
            connection.setAutoCommit(false);
        }
        
        boolean insertRowByRow = false;

        if (null != sourceResultSet && sourceResultSet instanceof SQLServerResultSet) {
            SQLServerStatement src_stmt = (SQLServerStatement) ((SQLServerResultSet) sourceResultSet).getStatement();
            int resultSetServerCursorId = ((SQLServerResultSet) sourceResultSet).getServerCursorId();

            if (connection.equals(src_stmt.getConnection()) && 0 != resultSetServerCursorId) {
                insertRowByRow = true;
            }
            
            if (((SQLServerResultSet) sourceResultSet).isForwardOnly()) {
                try {
                    sourceResultSet.setFetchSize(1);
                }
                catch (SQLException e) {
                   SQLServerException.makeFromDriverError(connection, sourceResultSet, e.getMessage(), e.getSQLState(), true);
                }
            }
        }

        TDSWriter tdsWriter = null;
        boolean moreDataAvailable = false;

        try {
            if (!insertRowByRow) {
                tdsWriter = sendBulkCopyCommand(command);
            }

            try {
                // Write all ROW tokens in the stream.
                moreDataAvailable = writeBatchData(tdsWriter, command, insertRowByRow);
            }
            finally {
                tdsWriter = command.getTDSWriter();
            }
        }
        catch (SQLServerException ex) {
            if (null == tdsWriter) {
                tdsWriter = command.getTDSWriter();
            }

            // Close the TDS packet before handling the exception
            writePacketDataDone(tdsWriter);

            // Send Attention packet to interrupt a complete request that was already sent to the server
            command.startRequest(TDS.PKT_CANCEL_REQ);

            TDSParser.parse(command.startResponse(), command.getLogContext());
            command.interrupt(ex.getMessage());
            command.onRequestComplete();

            throw ex;
        }
        finally {
            if (null == tdsWriter) {
                tdsWriter = command.getTDSWriter();
            }
            
            // reset the cryptoMeta in IOBuffer
            tdsWriter.setCryptoMetaData(null);
        }
                
        if (!insertRowByRow) {
            // Write the DONE token in the stream. We may have to append the DONE token with every packet that is sent.
            // For the current packets the driver does not generate a DONE token, but the BulkLoadBCP stream needs a DONE token
            // after every packet. For now add it manually here for one packet.
            // Note: This may break if more than one packet is sent.
            // This is an example from https://msdn.microsoft.com/en-us/library/dd340549.aspx
            writePacketDataDone(tdsWriter);

            // Send to the server and read response.
            TDSParser.parse(command.startResponse(), command.getLogContext());
        }

        if (copyOptions.isUseInternalTransaction()) {
            // Commit the transaction for this batch.
            connection.commit();
        }

        return moreDataAvailable;
    }

    private TDSWriter sendBulkCopyCommand(TDSCommand command) throws SQLServerException {
        // Create and send the initial command for bulk copy ("INSERT BULK ...").
        TDSWriter tdsWriter = command.startRequest(TDS.PKT_QUERY);
        String bulkCmd = createInsertBulkCommand(tdsWriter);
        tdsWriter.writeString(bulkCmd);
        TDSParser.parse(command.startResponse(), command.getLogContext());

        // Send the bulk data. This is the BulkLoadBCP TDS stream.
        tdsWriter = command.startRequest(TDS.PKT_BULK);

        // Write the COLUMNMETADATA token in the stream.
        writeColumnMetaData(tdsWriter);

        return tdsWriter;
    }

    private void writePacketDataDone(TDSWriter tdsWriter) throws SQLServerException {
        // This is an example from https://msdn.microsoft.com/en-us/library/dd340549.aspx
        tdsWriter.writeByte((byte) 0xFD);
        tdsWriter.writeLong(0);
        tdsWriter.writeInt(0);
    }

    /*
     * Helper method to throw a SQLServerExeption with the invalidArgument message and given argument.
     */
    private void throwInvalidArgument(String argument) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
        Object[] msgArgs = {argument};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
    }

    /*
     * Helper method to throw a SQLServerExeption with the errorConvertingValue message and given arguments.
     */
    private void throwInvalidJavaToJDBC(String javaClassName,
            int jdbcType) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
        throw new SQLServerException(form.format(new Object[] {javaClassName, jdbcType}), null, 0, null);
    }

    /*
     * The bulk copy operation
     */
    private void writeToServer() throws SQLServerException {
        if (connection.isClosed()) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_connectionIsClosed"),
                    SQLServerException.EXCEPTION_XOPEN_CONNECTION_DOES_NOT_EXIST, false);
        }

        long start = System.currentTimeMillis();
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.finer(this.toString() + " Start writeToServer: " + start);

        getDestinationMetadata();

        // Get source metadata in the BulkColumnMetaData object so that we can access metadata
        // from the same object for both ResultSet and File.
        getSourceMetadata();

        validateColumnMappings();

        sendBulkLoadBCP();

        long end = System.currentTimeMillis();
        if (loggerExternal.isLoggable(Level.FINER)) {
            loggerExternal.finer(this.toString() + " End writeToServer: " + end);
            int seconds = (int) ((end - start) / 1000L);
            loggerExternal.finer(this.toString() + "Time elapsed: " + seconds + " seconds");
        }
    }

    private void validateStringBinaryLengths(Object colValue,
            int srcCol,
            int destCol) throws SQLServerException {
        int sourcePrecision;
        int destPrecision = destColumnMetadata.get(destCol).precision;
        int srcJdbcType = srcColumnMetadata.get(srcCol).jdbcType;
        SSType destSSType = destColumnMetadata.get(destCol).ssType;

        if ((Util.isCharType(srcJdbcType) && Util.isCharType(destSSType)) || (Util.isBinaryType(srcJdbcType) && Util.isBinaryType(destSSType))) {
            if (colValue instanceof String) {
                if (Util.isBinaryType(destSSType)) {  
                    // if the dest value is binary and the value is of type string. 
                    //Repro in test case: ImpISQLServerBulkRecord_IssuesTest#testSendValidValueforBinaryColumnAsString
                    sourcePrecision = (((String) colValue).getBytes().length) / 2;
                }
                else
                    sourcePrecision = ((String) colValue).length();
            }
            else if (colValue instanceof byte[]) {
                sourcePrecision = ((byte[]) colValue).length;
            }
            else {
                return;
            }

            if (sourcePrecision > destPrecision) {
                String srcType = JDBCType.of(srcJdbcType) + "(" + sourcePrecision + ")";
                String destType = destSSType.toString() + "(" + destPrecision + ")";
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                Object[] msgArgs = {srcType, destType};
                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
            }
        }
    }

    /*
     * Retrieves the column metadata for the destination table (and saves it for later)
     */
    private void getDestinationMetadata() throws SQLServerException {
        if (null == destinationTableName) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDestinationTable"), null, false);
        }

        SQLServerResultSet rs = null;
        SQLServerResultSet rsMoreMetaData = null;

        try {
            // Get destination metadata
            rs = ((SQLServerStatement) connection.createStatement())
                    .executeQueryInternal("SET FMTONLY ON SELECT * FROM " + destinationTableName + " SET FMTONLY OFF ");

            destColumnCount = rs.getMetaData().getColumnCount();
            destColumnMetadata = new HashMap<>();
            destCekTable = rs.getCekTable();

            if (!connection.getServerSupportsColumnEncryption()) {
                // SQL server prior to 2016 does not support encryption_type
                rsMoreMetaData = ((SQLServerStatement) connection.createStatement())
                        .executeQueryInternal("select collation_name from sys.columns where " + "object_id=OBJECT_ID('" + destinationTableName + "') "
                                + "order by column_id ASC");
            }
            else {
                rsMoreMetaData = ((SQLServerStatement) connection.createStatement())
                        .executeQueryInternal("select collation_name, encryption_type from sys.columns where " + "object_id=OBJECT_ID('"
                                + destinationTableName + "') " + "order by column_id ASC");
            }
            for (int i = 1; i <= destColumnCount; ++i) {
                if (rsMoreMetaData.next()) {
                    if (!connection.getServerSupportsColumnEncryption()) {
                        destColumnMetadata.put(i, new BulkColumnMetaData(rs.getColumn(i), rsMoreMetaData.getString("collation_name"), null));
                    }
                    else {
                        destColumnMetadata.put(i, new BulkColumnMetaData(rs.getColumn(i), rsMoreMetaData.getString("collation_name"),
                                rsMoreMetaData.getString("encryption_type")));
                    }
                }
                else {
                    destColumnMetadata.put(i, new BulkColumnMetaData(rs.getColumn(i)));
                }
            }
        }
        catch (SQLException e) {
            // Unable to retrieve metadata for destination
            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveColMeta"), e);
        }
        finally {
            if (null != rs)
                rs.close();
            if (null != rsMoreMetaData)
                rsMoreMetaData.close();
        }
    }

    /*
     * Retrieves the column metadata for the source (and saves it for later). Retrieving source metadata in BulkColumnMetaData object helps to access
     * source metadata from the same place for both ResultSet and File.
     */
    private void getSourceMetadata() throws SQLServerException {
        srcColumnMetadata = new HashMap<>();
        int currentColumn;
        if (null != sourceResultSet) {
            try {
                srcColumnCount = sourceResultSetMetaData.getColumnCount();
                for (int i = 1; i <= srcColumnCount; ++i) {
                    srcColumnMetadata.put(i,
                            new BulkColumnMetaData(sourceResultSetMetaData.getColumnName(i),
                                    (ResultSetMetaData.columnNoNulls != sourceResultSetMetaData.isNullable(i)),
                                    sourceResultSetMetaData.getPrecision(i), sourceResultSetMetaData.getScale(i),
                                    sourceResultSetMetaData.getColumnType(i), null));
                }
            }
            catch (SQLException e) {
                // Unable to retrieve meta data for destination
                throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveColMeta"), e);
            }
        }
        else if (null != sourceBulkRecord) {
            Set<Integer> columnOrdinals = sourceBulkRecord.getColumnOrdinals();
            if (null == columnOrdinals || 0 == columnOrdinals.size()) {
                throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveColMeta"), null);
            }
            else {
                srcColumnCount = columnOrdinals.size();
                for (Integer columnOrdinal : columnOrdinals) {
                    currentColumn = columnOrdinal;
                    srcColumnMetadata.put(currentColumn,
                            new BulkColumnMetaData(sourceBulkRecord.getColumnName(currentColumn), true, sourceBulkRecord.getPrecision(currentColumn),
                                    sourceBulkRecord.getScale(currentColumn), sourceBulkRecord.getColumnType(currentColumn),
                                    ((sourceBulkRecord instanceof SQLServerBulkCSVFileRecord)
                                            ? ((SQLServerBulkCSVFileRecord) sourceBulkRecord).getColumnDateTimeFormatter(currentColumn) : null)));
                }
            }
        }
        else {
            // Unable to retrieve meta data for source
            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveColMeta"), null);
        }
    }

    /*
     * Oracle 12c database returns precision = 0 for char/varchar data types.
     */
    private int validateSourcePrecision(int srcPrecision,
            int srcJdbcType,
            int destPrecision) {
        if ((1 > srcPrecision) && Util.isCharType(srcJdbcType)) {
            srcPrecision = destPrecision;
        }
        return srcPrecision;
    }

    /*
     * Validates the column mappings
     */
    private void validateColumnMappings() throws SQLServerException {
        try {
            if (columnMappings.isEmpty()) {
                // Check that the source schema matches the destination schema
                // If the number of columns are different, there is an obvious mismatch.
                if (destColumnCount != srcColumnCount) {
                    throw new SQLServerException(SQLServerException.getErrString("R_schemaMismatch"), SQLState.COL_NOT_FOUND, DriverError.NOT_SET,
                            null);
                }

                // Could validate that the data types can be converted, but easier to leave that check for later

                // Generate default column mappings
                ColumnMapping cm;
                for (int i = 1; i <= srcColumnCount; ++i) {
                    // Only skip identity column mapping if KEEP IDENTITY OPTION is FALSE
                    if (!(destColumnMetadata.get(i).isIdentity && !copyOptions.isKeepIdentity())) {
                        cm = new ColumnMapping(i, i);
                        // The vector destColumnMetadata is indexed from 1 to be consistent with column indexing.
                        cm.destinationColumnName = destColumnMetadata.get(i).columnName;
                        columnMappings.add(cm);
                    }
                }
                // if no mapping is provided for csv file and metadata is missing for some columns throw error
                if (null != sourceBulkRecord) {
                    Set<Integer> columnOrdinals = sourceBulkRecord.getColumnOrdinals();
                    Iterator<Integer> columnsIterator = columnOrdinals.iterator();
                    int j = 1;
                    while (columnsIterator.hasNext()) {
                        int currentOrdinal = columnsIterator.next();
                        if (j != currentOrdinal) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
                            Object[] msgArgs = {currentOrdinal};
                            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
                        }
                        j++;
                    }
                }
            }
            else {
                int numMappings = columnMappings.size();
                ColumnMapping cm;

                // Check that the destination names or ordinals exist
                for (int i = 0; i < numMappings; ++i) {
                    cm = columnMappings.get(i);

                    // Validate that the destination column name exists if the ordinal is not provided.
                    if (-1 == cm.destinationColumnOrdinal) {
                        boolean foundColumn = false;

                        for (int j = 1; j <= destColumnCount; ++j) {
                            if (destColumnMetadata.get(j).columnName.equals(cm.destinationColumnName)) {
                                foundColumn = true;
                                cm.destinationColumnOrdinal = j;
                                break;
                            }
                        }

                        if (!foundColumn) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
                            Object[] msgArgs = {cm.destinationColumnName};
                            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
                        }
                    }
                    else if (0 > cm.destinationColumnOrdinal || destColumnCount < cm.destinationColumnOrdinal) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
                        Object[] msgArgs = {cm.destinationColumnOrdinal};
                        throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
                    }
                    else {
                        cm.destinationColumnName = destColumnMetadata.get(cm.destinationColumnOrdinal).columnName;
                    }
                }

                // Check that the required source ordinals are present and within range
                for (int i = 0; i < numMappings; ++i) {
                    cm = columnMappings.get(i);

                    // Validate that the source column name exists if the ordinal is not provided.
                    if (-1 == cm.sourceColumnOrdinal) {
                        boolean foundColumn = false;
                        if (null != sourceResultSet) {
                            int columns = sourceResultSetMetaData.getColumnCount();
                            for (int j = 1; j <= columns; ++j) {
                                if (sourceResultSetMetaData.getColumnName(j).equals(cm.sourceColumnName)) {
                                    foundColumn = true;
                                    cm.sourceColumnOrdinal = j;
                                    break;
                                }
                            }
                        }
                        else {
                            Set<Integer> columnOrdinals = sourceBulkRecord.getColumnOrdinals();
                            for (Integer currentColumn : columnOrdinals) {
                                if (sourceBulkRecord.getColumnName(currentColumn).equals(cm.sourceColumnName)) {
                                    foundColumn = true;
                                    cm.sourceColumnOrdinal = currentColumn;
                                    break;
                                }
                            }
                        }

                        if (!foundColumn) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
                            Object[] msgArgs = {cm.sourceColumnName};
                            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
                        }
                    }
                    else // Validate that the source column is in range
                    {
                        boolean columnOutOfRange = true;
                        if (null != sourceResultSet) {
                            int columns = sourceResultSetMetaData.getColumnCount();
                            if (0 < cm.sourceColumnOrdinal && columns >= cm.sourceColumnOrdinal) {
                                columnOutOfRange = false;
                            }
                        }
                        else {
                            // check if the ordinal is in SQLServerBulkCSVFileRecord.addColumnMetadata()
                            if (srcColumnMetadata.containsKey(cm.sourceColumnOrdinal)) {
                                columnOutOfRange = false;
                            }
                        }

                        if (columnOutOfRange) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
                            Object[] msgArgs = {cm.sourceColumnOrdinal};
                            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
                        }
                    }

                    // Remove mappings for identity column if KEEP IDENTITY OPTION is FALSE
                    if (destColumnMetadata.get(cm.destinationColumnOrdinal).isIdentity && !copyOptions.isKeepIdentity()) {
                        columnMappings.remove(i);
                        numMappings--;
                        i--;
                    }
                }
            }
        }
        catch (SQLException e) {
            // Let the column mapping validations go straight through as a single exception
            if ((e instanceof SQLServerException) && (null != e.getSQLState()) && e.getSQLState().equals(SQLState.COL_NOT_FOUND.getSQLStateCode())) {
                throw (SQLServerException) e;
            }

            // Difficulty retrieving column names from source or destination
            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveColMeta"), e);
        }

        // Throw an exception is Column mapping is empty.
        // If keep identity option is set to be false and not other mapping is explicitly provided.
        if (columnMappings.isEmpty()) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_BulkColumnMappingsIsEmpty"), null, 0, false);
        }
    }

    private void writeNullToTdsWriter(TDSWriter tdsWriter,
            int srcJdbcType,
            boolean isStreaming) throws SQLServerException {

        switch (srcJdbcType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.LONGVARBINARY:
                if (isStreaming) {
                    tdsWriter.writeLong(PLPInputStream.PLP_NULL);
                }
                else {
                    tdsWriter.writeByte((byte) 0xFF);
                    tdsWriter.writeByte((byte) 0xFF);
                }
                return;
            case java.sql.Types.BIT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
            case java.sql.Types.REAL:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case 2013:	// java.sql.Types.TIME_WITH_TIMEZONE
            case 2014:	// java.sql.Types.TIMESTAMP_WITH_TIMEZONE
            case microsoft.sql.Types.DATETIMEOFFSET:
                tdsWriter.writeByte((byte) 0x00);
                return;
            case microsoft.sql.Types.SQL_VARIANT:
                tdsWriter.writeInt((byte) 0x00);
                return;
            default:
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                Object[] msgArgs = {JDBCType.of(srcJdbcType).toString().toLowerCase(Locale.ENGLISH)};
                SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
        }
    }

    private void writeColumnToTdsWriter(TDSWriter tdsWriter,
            int bulkPrecision,
            int bulkScale,
            int bulkJdbcType,
            boolean bulkNullable, // should it be destNullable instead?
            int srcColOrdinal,
            int destColOrdinal,
            boolean isStreaming,
            Object colValue) throws SQLServerException {
        SSType destSSType = destColumnMetadata.get(destColOrdinal).ssType;

        bulkPrecision = validateSourcePrecision(bulkPrecision, bulkJdbcType, destColumnMetadata.get(destColOrdinal).precision);

        CryptoMetadata sourceCryptoMeta = srcColumnMetadata.get(srcColOrdinal).cryptoMeta;
        if (((null != destColumnMetadata.get(destColOrdinal).encryptionType) && copyOptions.isAllowEncryptedValueModifications())
                // if destination is encrypted send varbinary explicitly(needed for unencrypted source)
                || (null != destColumnMetadata.get(destColOrdinal).cryptoMeta)) {
            bulkJdbcType = java.sql.Types.VARBINARY;
        }
        /*
         * if source is encrypted and destination is unenecrypted, use destination sql type to send since there is no way of finding if source is
         * encrypted without accessing the resultset, send destination type if source resultset set is of type SQLServer and encryption is enabled
         */
        else if (null != sourceCryptoMeta) {
            bulkJdbcType = destColumnMetadata.get(destColOrdinal).jdbcType;
            bulkScale = destColumnMetadata.get(destColOrdinal).scale;
        }
        else if (null != sourceBulkRecord) {
            // Bulk copy from CSV and destination is not encrypted. In this case, we send the temporal types as varchar and
            // SQL Server does the conversion. If destination is encrypted, then temporal types can not be sent as varchar.
            switch (bulkJdbcType) {
                case java.sql.Types.DATE:
                case java.sql.Types.TIME:
                case java.sql.Types.TIMESTAMP:
                case microsoft.sql.Types.DATETIMEOFFSET:
                    bulkJdbcType = java.sql.Types.VARCHAR;
                    break;
                default:
                    break;
            }
        }

        try {
            // We are sending the data using JDBCType and not using SSType as SQL Server will automatically do the conversion.
            switch (bulkJdbcType) {
                case java.sql.Types.INTEGER:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (bulkNullable) {
                            tdsWriter.writeByte((byte) 0x04);
                        }
                        tdsWriter.writeInt((int) colValue);
                    }
                    break;

                case java.sql.Types.SMALLINT:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (bulkNullable) {
                            tdsWriter.writeByte((byte) 0x02);
                        }
                        tdsWriter.writeShort(((Number) colValue).shortValue());
                    }
                    break;

                case java.sql.Types.BIGINT:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (bulkNullable) {
                            tdsWriter.writeByte((byte) 0x08);
                        }
                        tdsWriter.writeLong((long) colValue);
                    }
                    break;

                case java.sql.Types.BIT:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (bulkNullable) {
                            tdsWriter.writeByte((byte) 0x01);
                        }
                        tdsWriter.writeByte((byte) ((Boolean) colValue ? 1 : 0));
                    }
                    break;

                case java.sql.Types.TINYINT:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (bulkNullable) {
                            tdsWriter.writeByte((byte) 0x01);
                        }
                        // TINYINT JDBC type is returned as a short in getObject.
                        // MYSQL returns TINYINT as an Integer. Convert it to a Number to get the short value.
                        tdsWriter.writeByte((byte) ((((Number) colValue).shortValue()) & 0xFF));

                    }
                    break;

                case java.sql.Types.DOUBLE:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (bulkNullable) {
                            tdsWriter.writeByte((byte) 0x08);
                        }
                        tdsWriter.writeDouble((double) colValue);
                    }
                    break;

                case java.sql.Types.REAL:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (bulkNullable) {
                            tdsWriter.writeByte((byte) 0x04);
                        }
                        tdsWriter.writeReal((float) colValue);
                    }
                    break;

                case microsoft.sql.Types.MONEY:
                case microsoft.sql.Types.SMALLMONEY:
                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        /*
                         * if the precision that user provides is smaller than the precision of the actual value, the driver assumes the precision
                         * that user provides is the correct precision, and throws exception
                         */
                        if (bulkPrecision < Util.getValueLengthBaseOnJavaType(colValue, JavaType.of(colValue), null, null,
                                JDBCType.of(bulkJdbcType))) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                            Object[] msgArgs = {SSType.DECIMAL};
                            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET, null);
                        }
                        tdsWriter.writeBigDecimal((BigDecimal) colValue, bulkJdbcType, bulkPrecision, bulkScale);
                    }
                    break;

                case microsoft.sql.Types.GUID:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.CHAR:         	// Fixed-length, non-Unicode string data.
                case java.sql.Types.VARCHAR:        // Variable-length, non-Unicode string data.
                    if (isStreaming) // PLP
                    {
                        // PLP_BODY rule in TDS
                        // Use ResultSet.getString for non-streaming data and ResultSet.getCharacterStream() for streaming data,
                        // so that if the source data source does not have streaming enabled, the smaller size data will still work.
                        if (null == colValue) {
                            writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                        }
                        else {
                            // Send length as unknown.
                            tdsWriter.writeLong(PLPInputStream.UNKNOWN_PLP_LEN);
                            try {
                                // Read and Send the data as chunks
                                // VARBINARYMAX --- only when streaming.
                                Reader reader;
                                if (colValue instanceof Reader) {
                                    reader = (Reader) colValue;
                                }
                                else {
                                    reader = new StringReader(colValue.toString());
                                }

                                if ((SSType.BINARY == destSSType) || (SSType.VARBINARY == destSSType) || (SSType.VARBINARYMAX == destSSType)
                                        || (SSType.IMAGE == destSSType)) {
                                    tdsWriter.writeNonUnicodeReader(reader, DataTypes.UNKNOWN_STREAM_LENGTH, true, null);
                                }
                                else {
                                    SQLCollation destCollation = destColumnMetadata.get(destColOrdinal).collation;
                                    if (null != destCollation) {
                                        tdsWriter.writeNonUnicodeReader(reader, DataTypes.UNKNOWN_STREAM_LENGTH, false, destCollation.getCharset());
                                    }
                                    else {
                                        tdsWriter.writeNonUnicodeReader(reader, DataTypes.UNKNOWN_STREAM_LENGTH, false, null);
                                    }
                                }
                                reader.close();
                            }
                            catch (IOException e) {
                                throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                            }
                        }
                    }
                    else // Non-PLP
                    {
                        if (null == colValue) {
                            writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                        }
                        else {
                            String colValueStr = colValue.toString();
                            if ((SSType.BINARY == destSSType) || (SSType.VARBINARY == destSSType)) {
                                byte[] bytes = null;
                                try {
                                    bytes = ParameterUtils.HexToBin(colValueStr);
                                }
                                catch (SQLServerException e) {
                                    throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                                }
                                tdsWriter.writeShort((short) bytes.length);
                                tdsWriter.writeBytes(bytes);
                            }
                            else {
                                tdsWriter.writeShort((short) (colValueStr.length()));
                                // converting string into destination collation using Charset

                                SQLCollation destCollation = destColumnMetadata.get(destColOrdinal).collation;
                                if (null != destCollation) {
                                    tdsWriter.writeBytes(colValueStr.getBytes(destColumnMetadata.get(destColOrdinal).collation.getCharset()));

                                }
                                else {
                                    tdsWriter.writeBytes(colValueStr.getBytes());
                                }
                            }
                        }
                    }
                    break;

                /*
                 * The length value associated with these data types is specified within a USHORT. see MS-TDS.pdf page 38. However, nchar(n)
                 * nvarchar(n) supports n = 1 .. 4000 (see MSDN SQL 2014, SQL 2016 Transact-SQL) NVARCHAR/NCHAR/LONGNVARCHAR is not compatible with
                 * BINARY/VARBINARY as specified in enum UpdaterConversion of DataTypes.java
                 */
                case java.sql.Types.LONGNVARCHAR:
                case java.sql.Types.NCHAR:
                case java.sql.Types.NVARCHAR:
                    if (isStreaming) {
                        // PLP_BODY rule in TDS
                        // Use ResultSet.getString for non-streaming data and ResultSet.getNCharacterStream() for streaming data,
                        // so that if the source data source does not have streaming enabled, the smaller size data will still work.
                        if (null == colValue) {
                            writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                        }
                        else {
                            // Send length as unknown.
                            tdsWriter.writeLong(PLPInputStream.UNKNOWN_PLP_LEN);
                            try {
                                // Read and Send the data as chunks.
                                Reader reader;
                                if (colValue instanceof Reader) {
                                    reader = (Reader) colValue;
                                }
                                else {
                                    reader = new StringReader(colValue.toString());
                                }

                                // writeReader is unicode.
                                tdsWriter.writeReader(reader, DataTypes.UNKNOWN_STREAM_LENGTH, true);
                                reader.close();
                            }
                            catch (IOException e) {
                                throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                            }
                        }
                    }
                    else {
                        if (null == colValue) {
                            writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                        }
                        else {
                            int stringLength = colValue.toString().length();
                            byte[] typevarlen = new byte[2];
                            typevarlen[0] = (byte) (2 * stringLength & 0xFF);
                            typevarlen[1] = (byte) ((2 * stringLength >> 8) & 0xFF);
                            tdsWriter.writeBytes(typevarlen);
                            tdsWriter.writeString(colValue.toString());
                        }
                    }
                    break;

                case java.sql.Types.LONGVARBINARY:
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                    if (isStreaming) // PLP
                    {
                        // Check for null separately for streaming and non-streaming data types, there could be source data sources who
                        // does not support streaming data.
                        if (null == colValue) {
                            writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                        }
                        else {
                            // Send length as unknown.
                            tdsWriter.writeLong(PLPInputStream.UNKNOWN_PLP_LEN);
                            try {
                                // Read and Send the data as chunks
                                InputStream iStream;
                                if (colValue instanceof InputStream) {
                                    iStream = (InputStream) colValue;
                                }
                                else {
                                    if (colValue instanceof byte[]) {
                                        iStream = new ByteArrayInputStream((byte[]) colValue);
                                    }
                                    else
                                        iStream = new ByteArrayInputStream(ParameterUtils.HexToBin(colValue.toString()));
                                }
                                // We do not need to check for null values here as it is already checked above.
                                tdsWriter.writeStream(iStream, DataTypes.UNKNOWN_STREAM_LENGTH, true);
                                iStream.close();
                            }
                            catch (IOException e) {
                                throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                            }
                        }
                    }
                    else // Non-PLP
                    {
                        if (null == colValue) {
                            writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                        }
                        else {
                            byte[] srcBytes;
                            if (colValue instanceof byte[]) {
                                srcBytes = (byte[]) colValue;
                            }
                            else {
                                try {
                                    srcBytes = ParameterUtils.HexToBin(colValue.toString());
                                }
                                catch (SQLServerException e) {
                                    throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                                }
                            }
                            tdsWriter.writeShort((short) srcBytes.length);
                            tdsWriter.writeBytes(srcBytes);
                        }
                    }
                    break;

                case microsoft.sql.Types.DATETIME:
                case microsoft.sql.Types.SMALLDATETIME:
                case java.sql.Types.TIMESTAMP:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        switch (destSSType) {
                            case SMALLDATETIME:
                                if (bulkNullable)
                                    tdsWriter.writeByte((byte) 0x04);
                                tdsWriter.writeSmalldatetime(colValue.toString());
                                break;
                            case DATETIME:
                                if (bulkNullable)
                                    tdsWriter.writeByte((byte) 0x08);
                                tdsWriter.writeDatetime(colValue.toString());
                                break;
                            default:	// DATETIME2
                                if (bulkNullable) {
                                    if (2 >= bulkScale)
                                        tdsWriter.writeByte((byte) 0x06);
                                    else if (4 >= bulkScale)
                                        tdsWriter.writeByte((byte) 0x07);
                                    else
                                        tdsWriter.writeByte((byte) 0x08);
                                }
                                String timeStampValue = colValue.toString();
                                tdsWriter.writeTime(java.sql.Timestamp.valueOf(timeStampValue), bulkScale);
                                // Send only the date part
                                tdsWriter.writeDate(timeStampValue.substring(0, timeStampValue.lastIndexOf(' ')));
                        }
                    }
                    break;

                case java.sql.Types.DATE:
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        tdsWriter.writeByte((byte) 0x03);
                        tdsWriter.writeDate(colValue.toString());
                    }
                    break;

                case java.sql.Types.TIME:
                    // java.sql.Types.TIME allows maximum of 3 fractional second precision
                    // SQL Server time(n) allows maximum of 7 fractional second precision, to avoid truncation
                    // values are read as java.sql.Types.TIMESTAMP if srcJdbcType is java.sql.Types.TIME
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (2 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x03);
                        else if (4 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x04);
                        else
                            tdsWriter.writeByte((byte) 0x05);

                        tdsWriter.writeTime((java.sql.Timestamp) colValue, bulkScale);
                    }
                    break;

                case 2013:	// java.sql.Types.TIME_WITH_TIMEZONE
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (2 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x08);
                        else if (4 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x09);
                        else
                            tdsWriter.writeByte((byte) 0x0A);

                        tdsWriter.writeOffsetTimeWithTimezone((OffsetTime) colValue, bulkScale);
                    }
                    break;

                case 2014:	// java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (2 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x08);
                        else if (4 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x09);
                        else
                            tdsWriter.writeByte((byte) 0x0A);

                        tdsWriter.writeOffsetDateTimeWithTimezone((OffsetDateTime) colValue, bulkScale);
                    }
                    break;

                case microsoft.sql.Types.DATETIMEOFFSET:
                    // We can safely cast the result set to a SQLServerResultSet as the DatetimeOffset type is only available in the JDBC driver.
                    if (null == colValue) {
                        writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                    }
                    else {
                        if (2 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x08);
                        else if (4 >= bulkScale)
                            tdsWriter.writeByte((byte) 0x09);
                        else
                            tdsWriter.writeByte((byte) 0x0A);

                        tdsWriter.writeDateTimeOffset(colValue, bulkScale, destSSType);
                    }
                    break;
                case microsoft.sql.Types.SQL_VARIANT:
                    boolean isShiloh = (8 >= connection.getServerMajorVersion());
                    if (isShiloh) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_SQLVariantSupport"));
                        throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
                    }
                    writeSqlVariant(tdsWriter, colValue, sourceResultSet, srcColOrdinal, destColOrdinal, bulkJdbcType, bulkScale, isStreaming);
                    break;
                default:
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                    Object[] msgArgs = {JDBCType.of(bulkJdbcType).toString().toLowerCase(Locale.ENGLISH)};
                    SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
                    break;
            } // End of switch
        }
        catch (ClassCastException ex) {
            if (null == colValue) {
                // this should not really happen, since ClassCastException should only happen when colValue is not null.
                // just do one more checking here to make sure
                throwInvalidArgument("colValue");
            }
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
            Object[] msgArgs = {colValue.getClass().getSimpleName(), JDBCType.of(bulkJdbcType)};
            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET, ex);
        }
    }
    
    /**
     * Writes sql_variant data based on the baseType for bulkcopy
     * 
     * @throws SQLServerException
     */
    private void writeSqlVariant(TDSWriter tdsWriter,
            Object colValue,
            ResultSet sourceResultSet,
            int srcColOrdinal,
            int destColOrdinal,
            int bulkJdbcType,
            int bulkScale,
            boolean isStreaming) throws SQLServerException {
        if (null == colValue) {
            writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
            return;
        }
        SqlVariant variantType = ((SQLServerResultSet) sourceResultSet).getVariantInternalType(srcColOrdinal);
        int baseType = variantType.getBaseType();
        // for sql variant we normally should return the colvalue for time as time string. but for
        // bulkcopy we need it to be timestamp. so we have to retrieve it again once we are in bulkcopy
        // and make sure that the base type is time.
        if (TDSType.TIMEN == TDSType.valueOf(baseType)) {
            variantType.setIsBaseTypeTimeValue(true);
            ((SQLServerResultSet) sourceResultSet).setInternalVariantType(srcColOrdinal, variantType);
            colValue = ((SQLServerResultSet) sourceResultSet).getObject(srcColOrdinal);
        }
        switch (TDSType.valueOf(baseType)) {
            case INT8:
                writeBulkCopySqlVariantHeader(10, TDSType.INT8.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeLong(Long.valueOf(colValue.toString()));
                break;
                
            case INT4:
                writeBulkCopySqlVariantHeader(6, TDSType.INT4.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeInt(Integer.valueOf(colValue.toString()));
                break;
                
            case INT2:
                writeBulkCopySqlVariantHeader(4, TDSType.INT2.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeShort(Short.valueOf(colValue.toString()));
                break;
                
            case INT1:
                writeBulkCopySqlVariantHeader(3, TDSType.INT1.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeByte(Byte.valueOf(colValue.toString()));
                break;
                
            case FLOAT8:
                writeBulkCopySqlVariantHeader(10, TDSType.FLOAT8.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeDouble(Double.valueOf(colValue.toString()));
                break;
                
            case FLOAT4:
                writeBulkCopySqlVariantHeader(6, TDSType.FLOAT4.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeReal(Float.valueOf(colValue.toString()));
                break;
                
            case MONEY8:
                // For decimalN we right TDSWriter.BIGDECIMAL_MAX_LENGTH as maximum length = 17
                // 17 + 2 for basetype and probBytes + 2 for precision and length = 21 the length of data in header
                writeBulkCopySqlVariantHeader(21, TDSType.DECIMALN.byteValue(), (byte) 2, tdsWriter);
                tdsWriter.writeByte((byte) 38);
                tdsWriter.writeByte((byte) 4);
                tdsWriter.writeSqlVariantInternalBigDecimal((BigDecimal) colValue, bulkJdbcType);
                break;
                
            case MONEY4:
                writeBulkCopySqlVariantHeader(21, TDSType.DECIMALN.byteValue(), (byte) 2, tdsWriter);
                tdsWriter.writeByte((byte) 38);
                tdsWriter.writeByte((byte) 4);
                tdsWriter.writeSqlVariantInternalBigDecimal((BigDecimal) colValue, bulkJdbcType);
                break;
                
            case BIT1:
                writeBulkCopySqlVariantHeader(3, TDSType.BIT1.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeByte((byte) (((Boolean) colValue).booleanValue() ? 1 : 0));
                break;
                
            case DATEN:
                writeBulkCopySqlVariantHeader(5, TDSType.DATEN.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeDate(colValue.toString());
                break;
                
            case TIMEN:
                bulkScale = variantType.getScale();
                int timeHeaderLength = 0x08; // default
                if (2 >= bulkScale) {
                    timeHeaderLength = 0x06;
                }
                else if (4 >= bulkScale) {
                    timeHeaderLength = 0x07;
                }
                else {
                    timeHeaderLength = 0x08;
                }
                writeBulkCopySqlVariantHeader(timeHeaderLength, TDSType.TIMEN.byteValue(), (byte) 1, tdsWriter); // depending on scale, the header
                                                                                                                 // length
                // defers
                tdsWriter.writeByte((byte) bulkScale);
                tdsWriter.writeTime((java.sql.Timestamp) colValue, bulkScale);
                break;
                
            case DATETIME8:
                writeBulkCopySqlVariantHeader(10, TDSType.DATETIME8.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeDatetime(colValue.toString());
                break;
                
            case DATETIME4:
                // when the type is ambiguous, we write to bigger type
                writeBulkCopySqlVariantHeader(10, TDSType.DATETIME8.byteValue(), (byte) 0, tdsWriter);
                tdsWriter.writeDatetime(colValue.toString());
                break;
                
            case DATETIME2N:
                writeBulkCopySqlVariantHeader(10, TDSType.DATETIME2N.byteValue(), (byte) 1, tdsWriter); // 1 is probbytes for time
                tdsWriter.writeByte((byte) 0x03);
                String timeStampValue = colValue.toString();
                tdsWriter.writeTime(java.sql.Timestamp.valueOf(timeStampValue), 0x03); // datetime2 in sql_variant has up to scale 3 support
                // Send only the date part
                tdsWriter.writeDate(timeStampValue.substring(0, timeStampValue.lastIndexOf(' ')));
                break;
                
            case BIGCHAR:
                int length = colValue.toString().length();
                writeBulkCopySqlVariantHeader(9 + length, TDSType.BIGCHAR.byteValue(), (byte) 7, tdsWriter);
                tdsWriter.writeCollationForSqlVariant(variantType);   // writes collation info and sortID
                tdsWriter.writeShort((short) (length));
                SQLCollation destCollation = destColumnMetadata.get(destColOrdinal).collation;
                if (null != destCollation) {
                    tdsWriter.writeBytes(colValue.toString().getBytes(destColumnMetadata.get(destColOrdinal).collation.getCharset()));
                }
                else {
                    tdsWriter.writeBytes(colValue.toString().getBytes());
                }
                break;
                
            case BIGVARCHAR:
                length = colValue.toString().length();
                writeBulkCopySqlVariantHeader(9 + length, TDSType.BIGVARCHAR.byteValue(), (byte) 7, tdsWriter);
                tdsWriter.writeCollationForSqlVariant(variantType);   // writes collation info and sortID
                tdsWriter.writeShort((short) (length));

                destCollation = destColumnMetadata.get(destColOrdinal).collation;
                if (null != destCollation) {
                    tdsWriter.writeBytes(colValue.toString().getBytes(destColumnMetadata.get(destColOrdinal).collation.getCharset()));
                }
                else {
                    tdsWriter.writeBytes(colValue.toString().getBytes());
                }
                break;
                
            case NCHAR:
                length = colValue.toString().length() * 2;
                writeBulkCopySqlVariantHeader(9 + length, TDSType.NCHAR.byteValue(), (byte) 7, tdsWriter);
                tdsWriter.writeCollationForSqlVariant(variantType);   // writes collation info and sortID
                int stringLength = colValue.toString().length();
                byte[] typevarlen = new byte[2];
                typevarlen[0] = (byte) (2 * stringLength & 0xFF);
                typevarlen[1] = (byte) ((2 * stringLength >> 8) & 0xFF);
                tdsWriter.writeBytes(typevarlen);
                tdsWriter.writeString(colValue.toString());
                break;
                
            case NVARCHAR:
                length = colValue.toString().length() * 2;
                writeBulkCopySqlVariantHeader(9 + length, TDSType.NVARCHAR.byteValue(), (byte) 7, tdsWriter);
                tdsWriter.writeCollationForSqlVariant(variantType);   // writes collation info and sortID
                stringLength = colValue.toString().length();
                typevarlen = new byte[2];
                typevarlen[0] = (byte) (2 * stringLength & 0xFF);
                typevarlen[1] = (byte) ((2 * stringLength >> 8) & 0xFF);
                tdsWriter.writeBytes(typevarlen);
                tdsWriter.writeString(colValue.toString());
                break;
                
            case GUID:
                length = colValue.toString().length();
                writeBulkCopySqlVariantHeader(9 + length, TDSType.BIGCHAR.byteValue(), (byte) 7, tdsWriter);
                // since while reading collation from sourceMetaData in guid we don't read collation, cause we are reading binary
                // but in writing it we are using char, we need to get the collation.
                SQLCollation collation = (null != destColumnMetadata.get(srcColOrdinal).collation) ? destColumnMetadata.get(srcColOrdinal).collation
                        : connection.getDatabaseCollation();
                variantType.setCollation(collation);
                tdsWriter.writeCollationForSqlVariant(variantType);   // writes collation info and sortID
                tdsWriter.writeShort((short) (length));
                // converting string into destination collation using Charset
                destCollation = destColumnMetadata.get(destColOrdinal).collation;
                if (null != destCollation) {
                    tdsWriter.writeBytes(colValue.toString().getBytes(destColumnMetadata.get(destColOrdinal).collation.getCharset()));
                }
                else {
                    tdsWriter.writeBytes(colValue.toString().getBytes());
                }
                break;
                
            case BIGBINARY:
                byte[] b = (byte[]) colValue;
                length = b.length;
                writeBulkCopySqlVariantHeader(4 + length, TDSType.BIGVARBINARY.byteValue(), (byte) 2, tdsWriter);
                tdsWriter.writeShort((short) (variantType.getMaxLength())); // length
                if (null == colValue) {
                    writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                }
                else {
                    byte[] srcBytes;
                    if (colValue instanceof byte[]) {
                        srcBytes = (byte[]) colValue;
                    }
                    else {
                        try {
                            srcBytes = ParameterUtils.HexToBin(colValue.toString());
                        }
                        catch (SQLServerException e) {
                            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                        }
                    }
                    tdsWriter.writeBytes(srcBytes);
                }
                break;
                
            case BIGVARBINARY:
                b = (byte[]) colValue;
                length = b.length;
                writeBulkCopySqlVariantHeader(4 + length, TDSType.BIGVARBINARY.byteValue(), (byte) 2, tdsWriter);
                tdsWriter.writeShort((short) (variantType.getMaxLength())); // length
                if (null == colValue) {
                    writeNullToTdsWriter(tdsWriter, bulkJdbcType, isStreaming);
                }
                else {
                    byte[] srcBytes;
                    if (colValue instanceof byte[]) {
                        srcBytes = (byte[]) colValue;
                    }
                    else {
                        try {
                            srcBytes = ParameterUtils.HexToBin(colValue.toString());
                        }
                        catch (SQLServerException e) {
                            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                        }
                    }
                    tdsWriter.writeBytes(srcBytes);
                }
                break;
                
            default:
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                Object[] msgArgs = {JDBCType.of(bulkJdbcType).toString().toLowerCase(Locale.ENGLISH)};
                SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
                break;
        }
    }
    
    /**
     * Write header for sql_variant
     * 
     * @param length:
     *            length of base type + Basetype + probBytes
     * @param tdsType
     * @param probBytes
     * @param tdsWriter
     * @throws SQLServerException
     */
    private void writeBulkCopySqlVariantHeader(int length,
            byte tdsType,
            byte probBytes,
            TDSWriter tdsWriter) throws SQLServerException {
        tdsWriter.writeInt(length);
        tdsWriter.writeByte(tdsType);
        tdsWriter.writeByte(probBytes);
    }

    private Object readColumnFromResultSet(int srcColOrdinal,
            int srcJdbcType,
            boolean isStreaming,
            boolean isDestEncrypted) throws SQLServerException {
        CryptoMetadata srcCryptoMeta = null;

        // if source is encrypted read its baseTypeInfo
        if ((sourceResultSet instanceof SQLServerResultSet)
                && (null != (srcCryptoMeta = ((SQLServerResultSet) sourceResultSet).getterGetColumn(srcColOrdinal).getCryptoMetadata()))) {
            srcJdbcType = srcCryptoMeta.baseTypeInfo.getSSType().getJDBCType().asJavaSqlType();
            BulkColumnMetaData temp = srcColumnMetadata.get(srcColOrdinal);
            srcColumnMetadata.put(srcColOrdinal, new BulkColumnMetaData(temp, srcCryptoMeta));
        }

        try {
            // We are sending the data using JDBCType and not using SSType as SQL Server will automatically do the conversion.
            switch (srcJdbcType) {
                // For numeric types (like, int, smallint, bit, ...) use getObject() instead of get* methods as get* methods
                // return 0 if the value is null.
                // Change getObject to get* as other data sources may not have similar implementation.
                case java.sql.Types.INTEGER:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.BIGINT:
                case java.sql.Types.BIT:
                case java.sql.Types.TINYINT:
                case java.sql.Types.DOUBLE:
                case java.sql.Types.REAL:
                    return sourceResultSet.getObject(srcColOrdinal);

                case microsoft.sql.Types.MONEY:
                case microsoft.sql.Types.SMALLMONEY:
                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    return sourceResultSet.getBigDecimal(srcColOrdinal);

                case microsoft.sql.Types.GUID:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.CHAR:         	// Fixed-length, non-Unicode string data.
                case java.sql.Types.VARCHAR:        // Variable-length, non-Unicode string data.

                    // PLP if stream type and both the source and destination are not encrypted
                    // This is because AE does not support streaming types.
                    // Therefore an encrypted source or destination means the data must not actually be streaming data
                    if (isStreaming && !isDestEncrypted && (null == srcCryptoMeta)) // PLP
                    {
                        // Use ResultSet.getString for non-streaming data and ResultSet.getCharacterStream() for streaming data,
                        // so that if the source data source does not have streaming enabled, the smaller size data will still work.
                        return sourceResultSet.getCharacterStream(srcColOrdinal);
                    }
                    else // Non-PLP
                    {
                        return sourceResultSet.getString(srcColOrdinal);
                    }

                case java.sql.Types.LONGNVARCHAR:
                case java.sql.Types.NCHAR:
                case java.sql.Types.NVARCHAR:
                    // PLP if stream type and both the source and destination are not encrypted
                    // This is because AE does not support streaming types.
                    // Therefore an encrypted source or destination means the data must not actually be streaming data
                    if (isStreaming && !isDestEncrypted && (null == srcCryptoMeta)) // PLP
                    {
                        // Use ResultSet.getString for non-streaming data and ResultSet.getNCharacterStream() for streaming data,
                        // so that if the source data source does not have streaming enabled, the smaller size data will still work.
                        return sourceResultSet.getNCharacterStream(srcColOrdinal);
                    }
                    else {
                        return sourceResultSet.getObject(srcColOrdinal);
                    }

                case java.sql.Types.LONGVARBINARY:
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                    // PLP if stream type and both the source and destination are not encrypted
                    // This is because AE does not support streaming types.
                    // Therefore an encrypted source or destination means the data must not actually be streaming data
                    if (isStreaming && !isDestEncrypted && (null == srcCryptoMeta)) // PLP
                    {
                        return sourceResultSet.getBinaryStream(srcColOrdinal);
                    }
                    else // Non-PLP
                    {
                        return sourceResultSet.getBytes(srcColOrdinal);
                    }

                case microsoft.sql.Types.DATETIME:
                case microsoft.sql.Types.SMALLDATETIME:
                case java.sql.Types.TIMESTAMP:
                case java.sql.Types.TIME:
                    // java.sql.Types.TIME allows maximum of 3 fractional second precision
                    // SQL Server time(n) allows maximum of 7 fractional second precision, to avoid truncation
                    // values are read as java.sql.Types.TIMESTAMP if srcJdbcType is java.sql.Types.TIME
                    return sourceResultSet.getTimestamp(srcColOrdinal);

                case java.sql.Types.DATE:
                    return sourceResultSet.getDate(srcColOrdinal);

                case microsoft.sql.Types.DATETIMEOFFSET:
                    // We can safely cast the result set to a SQLServerResultSet as the DatetimeOffset type is only available in the JDBC driver.
                    return ((SQLServerResultSet) sourceResultSet).getDateTimeOffset(srcColOrdinal);

                case microsoft.sql.Types.SQL_VARIANT:               
                    return sourceResultSet.getObject(srcColOrdinal);
                default:
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                    Object[] msgArgs = {JDBCType.of(srcJdbcType).toString().toLowerCase(Locale.ENGLISH)};
                    SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
                    // This return will never be executed, but it is needed as Eclipse complains otherwise.
                    return null;
            } 
        }
        catch (SQLException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
        }
    }

    /*
     * Reads the given column from the result set current row and writes the data to tdsWriter.
     */
    private void writeColumn(TDSWriter tdsWriter,
            int srcColOrdinal,
            int destColOrdinal,
            Object colValue) throws SQLServerException {
        int srcPrecision, srcScale, destPrecision, srcJdbcType;
        SSType destSSType = null;
        boolean isStreaming, srcNullable;
        srcPrecision = srcColumnMetadata.get(srcColOrdinal).precision;
        srcScale = srcColumnMetadata.get(srcColOrdinal).scale;
        srcJdbcType = srcColumnMetadata.get(srcColOrdinal).jdbcType;
        srcNullable = srcColumnMetadata.get(srcColOrdinal).isNullable;

        destPrecision = destColumnMetadata.get(destColOrdinal).precision;

        if ((java.sql.Types.NCHAR == srcJdbcType) || (java.sql.Types.NVARCHAR == srcJdbcType) || (java.sql.Types.LONGNVARCHAR == srcJdbcType)) {
            isStreaming = (DataTypes.SHORT_VARTYPE_MAX_CHARS < srcPrecision) || (DataTypes.SHORT_VARTYPE_MAX_CHARS < destPrecision);
        }
        else {
            isStreaming = (DataTypes.SHORT_VARTYPE_MAX_BYTES < srcPrecision) || (DataTypes.SHORT_VARTYPE_MAX_BYTES < destPrecision);
        }

        CryptoMetadata destCryptoMeta = destColumnMetadata.get(destColOrdinal).cryptoMeta;
        if (null != destCryptoMeta) {
            destSSType = destCryptoMeta.baseTypeInfo.getSSType();
        }

        // Get the cell from the source result set if we are copying from result set.
        // If we are copying from a bulk reader colValue will be passed as the argument.
        if (null != sourceResultSet) {
            colValue = readColumnFromResultSet(srcColOrdinal, srcJdbcType, isStreaming, (null != destCryptoMeta));
            validateStringBinaryLengths(colValue, srcColOrdinal, destColOrdinal);

            // if AllowEncryptedValueModifications is set send varbinary read from source without checking type conversion
            if (!((copyOptions.isAllowEncryptedValueModifications())
                    // normalizationCheck() will be called for encrypted columns so skip this validation
                    || ((null != destCryptoMeta) && (null != colValue)))) {
                validateDataTypeConversions(srcColOrdinal, destColOrdinal);
            }
        }
        //If we are using ISQLBulkRecord and the data we are passing is char type, we need to check the source and dest precision
        else if (null != sourceBulkRecord && (null == destCryptoMeta)) {
            validateStringBinaryLengths(colValue, srcColOrdinal, destColOrdinal);
        }
        else if ((null != sourceBulkRecord) && (null != destCryptoMeta)) {
            // From CSV to encrypted column. Convert to respective object.
            if ((java.sql.Types.DATE == srcJdbcType) || (java.sql.Types.TIME == srcJdbcType) || (java.sql.Types.TIMESTAMP == srcJdbcType)
                    || (microsoft.sql.Types.DATETIMEOFFSET == srcJdbcType) || (2013 == srcJdbcType) || (2014 == srcJdbcType)) {
                colValue = getTemporalObjectFromCSV(colValue, srcJdbcType, srcColOrdinal);
            }
            else if ((java.sql.Types.NUMERIC == srcJdbcType) || (java.sql.Types.DECIMAL == srcJdbcType)) {
                int baseDestPrecision = destCryptoMeta.baseTypeInfo.getPrecision();
                int baseDestScale = destCryptoMeta.baseTypeInfo.getScale();
                if ((srcScale != baseDestScale) || (srcPrecision != baseDestPrecision)) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                    String src = JDBCType.of(srcJdbcType) + "(" + srcPrecision + "," + srcScale + ")";
                    String dest = destSSType + "(" + baseDestPrecision + "," + baseDestScale + ")";
                    Object[] msgArgs = {src, dest};
                    throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                }
            }
        }

        CryptoMetadata srcCryptoMeta = srcColumnMetadata.get(srcColOrdinal).cryptoMeta;
        // If destination is encrypted column, transparently encrypt the data
        if ((null != destCryptoMeta) && (null != colValue)) {
            JDBCType baseSrcJdbcType = (null != srcCryptoMeta)
                    ? srcColumnMetadata.get(srcColOrdinal).cryptoMeta.baseTypeInfo.getSSType().getJDBCType() : JDBCType.of(srcJdbcType);

            if (JDBCType.TIMESTAMP == baseSrcJdbcType) {
                if (SSType.DATETIME == destSSType) {
                    baseSrcJdbcType = JDBCType.DATETIME;
                }
                else if (SSType.SMALLDATETIME == destSSType) {
                    baseSrcJdbcType = JDBCType.SMALLDATETIME;
                }
            }

            if (!((SSType.MONEY == destSSType && JDBCType.DECIMAL == baseSrcJdbcType)
                    || (SSType.SMALLMONEY == destSSType && JDBCType.DECIMAL == baseSrcJdbcType)
                    || (SSType.GUID == destSSType && JDBCType.CHAR == baseSrcJdbcType))) {
                // check for bulkcopy from other than SQLServer, for instance for MYSQL, if anykind of chartype pass
                if (!(Util.isCharType(destSSType) && Util.isCharType(srcJdbcType)) && !(sourceResultSet instanceof SQLServerResultSet))
                    // check for normalization of AE data types
                    if (!baseSrcJdbcType.normalizationCheck(destSSType)) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedConversionAE"));
                        Object[] msgArgs = {baseSrcJdbcType, destSSType};
                        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                    }
            }
            // if source is encrypted and temporal, call IOBuffer functions to encrypt
            if ((baseSrcJdbcType == JDBCType.DATE) || (baseSrcJdbcType == JDBCType.TIMESTAMP) || (baseSrcJdbcType == JDBCType.TIME)
                    || (baseSrcJdbcType == JDBCType.DATETIMEOFFSET) || (baseSrcJdbcType == JDBCType.DATETIME)
                    || (baseSrcJdbcType == JDBCType.SMALLDATETIME)) {
                colValue = getEncryptedTemporalBytes(tdsWriter, baseSrcJdbcType, colValue, srcColOrdinal, destCryptoMeta.baseTypeInfo.getScale());
            }
            else {
                TypeInfo destTypeInfo = destCryptoMeta.getBaseTypeInfo();
                JDBCType destJdbcType = destTypeInfo.getSSType().getJDBCType();

                /*
                 * the following if checks that no casting exception would be thrown in the normalizedValue() method below a SQLServerException is
                 * then thrown before the ClassCastException could occur an example of how this situation could arise would be if the application
                 * creates encrypted source and destination tables the result set used to read the source would have AE disabled (causing colValue to
                 * be varbinary) AE would be enabled on the connection used to complete the bulkCopy operation
                 */
                if ((!Util.isBinaryType(destJdbcType.getIntValue())) && (colValue instanceof byte[])) {

                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                    Object[] msgArgs = {baseSrcJdbcType, destJdbcType};
                    throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                }
                // normalize the values before encrypting them
                colValue = SQLServerSecurityUtility.encryptWithKey(
                        normalizedValue(destJdbcType, colValue, baseSrcJdbcType, destTypeInfo.getPrecision(), destTypeInfo.getScale()),
                        destCryptoMeta, connection);
            }
        }
        writeColumnToTdsWriter(tdsWriter, srcPrecision, srcScale, srcJdbcType, srcNullable, srcColOrdinal, destColOrdinal, isStreaming, colValue);
    }

    // this method is called against jdbc41, but it require jdbc42 to work
    // therefore, we will throw exception.
    protected Object getTemporalObjectFromCSVWithFormatter(String valueStrUntrimmed,
            int srcJdbcType,
            int srcColOrdinal,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        SQLServerBulkCopy42Helper.getTemporalObjectFromCSVWithFormatter(valueStrUntrimmed, srcJdbcType, srcColOrdinal, dateTimeFormatter, connection,
                this);

        return null;
    }

    private Object getTemporalObjectFromCSV(Object value,
            int srcJdbcType,
            int srcColOrdinal) throws SQLServerException {
        // TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE are not supported with encrypted columns.
        if (2013 == srcJdbcType) {
            MessageFormat form1 = new MessageFormat(SQLServerException.getErrString("R_UnsupportedDataTypeAE"));
            Object[] msgArgs1 = {"TIME_WITH_TIMEZONE"};
            throw new SQLServerException(this, form1.format(msgArgs1), null, 0, false);
        }
        else if (2014 == srcJdbcType) {
            MessageFormat form2 = new MessageFormat(SQLServerException.getErrString("R_UnsupportedDataTypeAE"));
            Object[] msgArgs2 = {"TIMESTAMP_WITH_TIMEZONE"};
            throw new SQLServerException(this, form2.format(msgArgs2), null, 0, false);
        }

        String valueStr = null;
        String valueStrUntrimmed = null;

        if (null != value && value instanceof String) {
            valueStrUntrimmed = (String) value;
            valueStr = valueStrUntrimmed.trim();
        }

        // Handle null case
        if (null == valueStr) {
            switch (srcJdbcType) {
                case java.sql.Types.TIMESTAMP:
                case java.sql.Types.TIME:
                case java.sql.Types.DATE:
                case microsoft.sql.Types.DATETIMEOFFSET:
                    return null;
            }
        }

        // If we are here value is non-null.
        Calendar cal;

        // Get the temporal values from the formatter
        DateTimeFormatter dateTimeFormatter = srcColumnMetadata.get(srcColOrdinal).dateTimeFormatter;
        if (null != dateTimeFormatter) {
            return getTemporalObjectFromCSVWithFormatter(valueStrUntrimmed, srcJdbcType, srcColOrdinal, dateTimeFormatter);
        }

        // If we are here that means datetimeformatter is not present. Only default format is supported in this case.
        try {
            switch (srcJdbcType) {
                case java.sql.Types.TIMESTAMP:
                    // For CSV, value will be of String type.
                    return Timestamp.valueOf(valueStr);

                case java.sql.Types.TIME: {
                    String time = connection.baseYear() + "-01-01 " + valueStr;
                    Timestamp ts = java.sql.Timestamp.valueOf(time);
                    return ts;
                }
                case java.sql.Types.DATE:
                    return java.sql.Date.valueOf(valueStr);

                case microsoft.sql.Types.DATETIMEOFFSET:
                    int endIndx = valueStr.indexOf('-', 0);
                    int year = Integer.parseInt(valueStr.substring(0, endIndx));

                    int startIndx = ++endIndx; // skip the -
                    endIndx = valueStr.indexOf('-', startIndx);
                    int month = Integer.parseInt(valueStr.substring(startIndx, endIndx));

                    startIndx = ++endIndx; // skip the -
                    endIndx = valueStr.indexOf(' ', startIndx);
                    int day = Integer.parseInt(valueStr.substring(startIndx, endIndx));

                    startIndx = ++endIndx; // skip the space
                    endIndx = valueStr.indexOf(':', startIndx);
                    int hour = Integer.parseInt(valueStr.substring(startIndx, endIndx));

                    startIndx = ++endIndx; // skip the :
                    endIndx = valueStr.indexOf(':', startIndx);
                    int minute = Integer.parseInt(valueStr.substring(startIndx, endIndx));

                    startIndx = ++endIndx; // skip the :
                    endIndx = valueStr.indexOf('.', startIndx);
                    int seconds, offsethour, offsetMinute, totalOffset = 0, fractionalSeconds = 0;
                    boolean isNegativeOffset = false;
                    boolean hasTimeZone = false;
                    int fractionalSecondsLength = 0;
                    if (-1 != endIndx) // has fractional seconds, has a '.'
                    {
                        seconds = Integer.parseInt(valueStr.substring(startIndx, endIndx));

                        startIndx = ++endIndx; // skip the .
                        endIndx = valueStr.indexOf(' ', startIndx);
                        if (-1 != endIndx) // has time zone
                        {
                            fractionalSeconds = Integer.parseInt(valueStr.substring(startIndx, endIndx));
                            fractionalSecondsLength = endIndx - startIndx;
                            hasTimeZone = true;
                        }
                        else // no timezone
                        {
                            fractionalSeconds = Integer.parseInt(valueStr.substring(startIndx));
                            fractionalSecondsLength = valueStr.length() - startIndx;
                        }
                    }
                    else {
                        endIndx = valueStr.indexOf(' ', startIndx);
                        if (-1 != endIndx) {
                            hasTimeZone = true;
                            seconds = Integer.parseInt(valueStr.substring(startIndx, endIndx));
                        }
                        else {
                            seconds = Integer.parseInt(valueStr.substring(startIndx));
                            ++endIndx; // skip the space
                        }
                    }
                    if (hasTimeZone) {
                        startIndx = ++endIndx; // skip the space
                        if ('+' == valueStr.charAt(startIndx))
                            startIndx++; // skip +
                        else if ('-' == valueStr.charAt(startIndx)) {
                            isNegativeOffset = true;
                            startIndx++; // skip -
                        }
                        endIndx = valueStr.indexOf(':', startIndx);

                        offsethour = Integer.parseInt(valueStr.substring(startIndx, endIndx));
                        startIndx = ++endIndx; // skip :
                        offsetMinute = Integer.parseInt(valueStr.substring(startIndx));
                        totalOffset = offsethour * 60 + offsetMinute;
                        if (isNegativeOffset)
                            totalOffset = -totalOffset;
                    }
                    cal = new GregorianCalendar(new SimpleTimeZone(totalOffset * 60 * 1000, ""), Locale.US);
                    cal.clear();
                    cal.set(Calendar.HOUR_OF_DAY, hour);
                    cal.set(Calendar.MINUTE, minute);
                    cal.set(Calendar.SECOND, seconds);
                    cal.set(Calendar.DATE, day);
                    cal.set(Calendar.MONTH, month - 1);
                    cal.set(Calendar.YEAR, year);
                    for (int i = 0; i < (9 - fractionalSecondsLength); i++)
                        fractionalSeconds *= 10;

                    Timestamp ts = new Timestamp(cal.getTimeInMillis());
                    ts.setNanos(fractionalSeconds);
                    return microsoft.sql.DateTimeOffset.valueOf(ts, totalOffset);
            }
        }
        catch (IndexOutOfBoundsException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ParsingError"));
            Object[] msgArgs = {JDBCType.of(srcJdbcType)};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        catch (NumberFormatException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ParsingError"));
            Object[] msgArgs = {JDBCType.of(srcJdbcType)};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        catch (IllegalArgumentException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ParsingError"));
            Object[] msgArgs = {JDBCType.of(srcJdbcType)};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        // unreachable code. Need to do to compile from Eclipse.
        return value;
    }

    private byte[] getEncryptedTemporalBytes(TDSWriter tdsWriter,
            JDBCType srcTemporalJdbcType,
            Object colValue,
            int srcColOrdinal,
            int scale) throws SQLServerException {
        long utcMillis;
        GregorianCalendar calendar;

        switch (srcTemporalJdbcType) {
            case DATE:
                calendar = new GregorianCalendar(java.util.TimeZone.getDefault(), java.util.Locale.US);
                calendar.setLenient(true);
                calendar.clear();
                calendar.setTimeInMillis(((Date) colValue).getTime());
                return tdsWriter.writeEncryptedScaledTemporal(calendar, 0, // subsecond nanos (none for a date value)
                        0, // scale (dates are not scaled)
                        SSType.DATE, (short) 0);

            case TIME:
                calendar = new GregorianCalendar(java.util.TimeZone.getDefault(), java.util.Locale.US);
                calendar.setLenient(true);
                calendar.clear();
                utcMillis = ((java.sql.Timestamp) colValue).getTime();
                calendar.setTimeInMillis(utcMillis);
                int subSecondNanos;
                if (colValue instanceof java.sql.Timestamp) {
                    subSecondNanos = ((java.sql.Timestamp) colValue).getNanos();
                }
                else {
                    subSecondNanos = Nanos.PER_MILLISECOND * (int) (utcMillis % 1000);
                    if (subSecondNanos < 0)
                        subSecondNanos += Nanos.PER_SECOND;
                }
                return tdsWriter.writeEncryptedScaledTemporal(calendar, subSecondNanos, scale, SSType.TIME, (short) 0);

            case TIMESTAMP:
                calendar = new GregorianCalendar(java.util.TimeZone.getDefault(), java.util.Locale.US);
                calendar.setLenient(true);
                calendar.clear();
                utcMillis = ((java.sql.Timestamp) colValue).getTime();
                calendar.setTimeInMillis(utcMillis);
                subSecondNanos = ((java.sql.Timestamp) colValue).getNanos();
                return tdsWriter.writeEncryptedScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIME2, (short) 0);

            case DATETIME:
            case SMALLDATETIME:
                calendar = new GregorianCalendar(java.util.TimeZone.getDefault(), java.util.Locale.US);
                calendar.setLenient(true);
                calendar.clear();
                utcMillis = ((java.sql.Timestamp) colValue).getTime();
                calendar.setTimeInMillis(utcMillis);
                subSecondNanos = ((java.sql.Timestamp) colValue).getNanos();
                return tdsWriter.getEncryptedDateTimeAsBytes(calendar, subSecondNanos, srcTemporalJdbcType);

            case DATETIMEOFFSET:
                microsoft.sql.DateTimeOffset dtoValue = (microsoft.sql.DateTimeOffset) colValue;
                utcMillis = dtoValue.getTimestamp().getTime();
                subSecondNanos = dtoValue.getTimestamp().getNanos();
                int minutesOffset = dtoValue.getMinutesOffset();
                calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                calendar.setLenient(true);
                calendar.clear();
                calendar.setTimeInMillis(utcMillis);
                return tdsWriter.writeEncryptedScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIMEOFFSET, (short) minutesOffset);

            default:

                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnsupportedDataTypeAE"));
                Object[] msgArgs = {srcTemporalJdbcType};
                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
    }

    private byte[] normalizedValue(JDBCType destJdbcType,
            Object value,
            JDBCType srcJdbcType,
            int destPrecision,
            int destScale) throws SQLServerException {
        Long longValue = null;
        byte[] byteValue = null;
        int srcDataPrecision, srcDataScale;

        try {
            switch (destJdbcType) {
                case BIT:
                    longValue = (long) ((Boolean) value ? 1 : 0);
                    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong(longValue).array();

                case TINYINT:
                case SMALLINT:
                    switch (srcJdbcType) {
                        case BIT:
                            longValue = (long) ((Boolean) value ? 1 : 0);
                            break;
                        default:
                            if (value instanceof Integer) {
                                int intValue = (int) value;
                                short shortValue = (short) intValue;
                                longValue = (long) shortValue;
                            }
                            else
                                longValue = (long) (short) value;

                    }
                    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong(longValue).array();

                case INTEGER:
                    switch (srcJdbcType) {
                        case BIT:
                            longValue = (long) ((Boolean) value ? 1 : 0);
                            break;
                        case TINYINT:
                        case SMALLINT:
                            longValue = (long) (short) value;
                            break;
                        default:
                            longValue = new Long((Integer) value);
                    }
                    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong(longValue).array();

                case BIGINT:
                    switch (srcJdbcType) {
                        case BIT:
                            longValue = (long) ((Boolean) value ? 1 : 0);
                            break;
                        case TINYINT:
                        case SMALLINT:
                            longValue = (long) (short) value;
                            break;
                        case INTEGER:
                            longValue = new Long((Integer) value);
                            break;
                        default:
                            longValue = (long) value;
                    }
                    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong(longValue).array();

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                    byte[] byteArrayValue;
                    if (value instanceof String) {
                        byteArrayValue = ParameterUtils.HexToBin((String) value);
                    }
                    else {
                        byteArrayValue = (byte[]) value;
                    }
                    if (byteArrayValue.length > destPrecision) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                        Object[] msgArgs = {srcJdbcType, destJdbcType};
                        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                    }
                    return byteArrayValue;
                case GUID:
                    return Util.asGuidByteArray(UUID.fromString((String) value));

                case CHAR:
                case VARCHAR:
                case LONGVARCHAR:

                    // Throw exception if length sent in column metadata is smaller than actual data
                    if (((String) value).length() > destPrecision) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                        Object[] msgArgs = {srcJdbcType, destJdbcType};
                        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                    }
                    return ((String) value).getBytes(UTF_8);

                case NCHAR:
                case NVARCHAR:
                case LONGNVARCHAR:
                    // Throw exception if length sent in column metadata is smaller than actual data
                    if (((String) value).length() > destPrecision) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                        Object[] msgArgs = {srcJdbcType, destJdbcType};
                        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                    }
                    return ((String) value).getBytes(UTF_16LE);

                case REAL:
                case FLOAT:

                    Float floatValue = (value instanceof String) ? Float.parseFloat((String) value) : (Float) value;
                    return ByteBuffer.allocate((Float.SIZE / Byte.SIZE)).order(ByteOrder.LITTLE_ENDIAN).putFloat(floatValue).array();

                case DOUBLE:
                    Double doubleValue = (value instanceof String) ? Double.parseDouble((String) value) : (Double) value;
                    return ByteBuffer.allocate((Double.SIZE / Byte.SIZE)).order(ByteOrder.LITTLE_ENDIAN).putDouble(doubleValue).array();

                case NUMERIC:
                case DECIMAL:
                    srcDataScale = ((BigDecimal) value).scale();
                    srcDataPrecision = ((BigDecimal) value).precision();
                    BigDecimal bigDataValue = (BigDecimal) value;
                    if ((srcDataPrecision > destPrecision) || (srcDataScale > destScale)) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
                        Object[] msgArgs = {srcJdbcType, destJdbcType};
                        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                    }
                    else if (srcDataScale < destScale)
                        // update the scale of source data based on the metadata for scale sent early
                        bigDataValue = bigDataValue.setScale(destScale);

                    byteValue = DDC.convertBigDecimalToBytes(bigDataValue, bigDataValue.scale());
                    byte[] decimalbyteValue = new byte[16];
                    // removing the precision and scale information from the decimalToByte array
                    System.arraycopy(byteValue, 2, decimalbyteValue, 0, byteValue.length - 2);
                    return decimalbyteValue;

                case SMALLMONEY:
                case MONEY:
                    // For TDS we need to send the money value multiplied by 10^4 - this gives us the
                    // money value as integer. 4 is the default and only scale available with money.
                    // smallmoney is noralized to money.
                    BigDecimal bdValue = (BigDecimal) value;
                    // Need to validate range in the client side as we are converting BigDecimal to integers.
                    Util.validateMoneyRange(bdValue, destJdbcType);

                    // Get the total number of digits after the multiplication. Scale is hardcoded to 4. This is needed to get the proper rounding.
                    int digitCount = (bdValue.precision() - bdValue.scale()) + 4;

                    long moneyVal = ((BigDecimal) value)
                            .multiply(new BigDecimal(10000), new java.math.MathContext(digitCount, java.math.RoundingMode.HALF_UP)).longValue();
                    ByteBuffer bbuf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                    bbuf.putInt((int) (moneyVal >> 32)).array();
                    bbuf.putInt((int) moneyVal).array();
                    return bbuf.array();

                default:
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnsupportedDataTypeAE"));
                    Object[] msgArgs = {destJdbcType};
                    throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
            }
        }
        // we don't want to throw R_errorConvertingValue error as it might expose decrypted data if source was encrypted
        catch (NumberFormatException ex) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
            Object[] msgArgs = {srcJdbcType, destJdbcType};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        catch (IllegalArgumentException ex) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
            Object[] msgArgs = {srcJdbcType, destJdbcType};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        catch (ClassCastException ex) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidDataForAE"));
            Object[] msgArgs = {srcJdbcType, destJdbcType};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
    }

    private boolean goToNextRow() throws SQLServerException {
        try {
            if (null != sourceResultSet) {
                return sourceResultSet.next();
            }
            else {
                return sourceBulkRecord.next();
            }
        }
        catch (SQLException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
        }
    }

    /*
     * Writes data for a batch of rows to the TDSWriter object. Writes the following part in the BulkLoadBCP stream
     * (https://msdn.microsoft.com/en-us/library/dd340549.aspx) <ROW> ... </ROW>
     */
    private boolean writeBatchData(TDSWriter tdsWriter,
            TDSCommand command,
            boolean insertRowByRow) throws SQLServerException {
        int batchsize = copyOptions.getBatchSize();
        int row = 0;
        while (true) {
            // Default batchsize is 0 - means all rows are sent in one batch. In this case we will return
            // when all rows in the resultset are processed. If batchsize is not zero, we will return when one batch of rows are processed.
            if (0 != batchsize && row >= batchsize)
                return true;

            // No more data available, return false so we do not execute any more batches.
            if (!goToNextRow())
                return false;
            
            if (insertRowByRow) {
                // read response gotten from goToNextRow()
                ((SQLServerResultSet) sourceResultSet).getTDSReader().readPacket();

                tdsWriter = sendBulkCopyCommand(command);
            }

            // Write row header for each row.
            tdsWriter.writeByte((byte) TDS.TDS_ROW);
            int mappingColumnCount = columnMappings.size();

            // Copying from a resultset.
            if (null != sourceResultSet) {
                // Loop for each destination column. The mappings is a many to one mapping
                // where multiple source columns can be mapped to one destination column.
                for (ColumnMapping columnMapping : columnMappings) {
                    writeColumn(tdsWriter, columnMapping.sourceColumnOrdinal, columnMapping.destinationColumnOrdinal, null // cell
                            // value is
                            // retrieved
                            // inside
                            // writeRowData()
                            // method.
                    );
                }
            }
            // Copy from a file.
            else {
                // Get all the column values of the current row.
                Object[] rowObjects;

                try {
                    rowObjects = sourceBulkRecord.getRowData();
                }
                catch (Exception ex) {
                    // if no more data available to retrive
                    throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), ex);
                }

                for (ColumnMapping columnMapping : columnMappings) {
                    // If the SQLServerBulkCSVRecord does not have metadata for columns, it returns strings in the object array.
                    // COnvert the strings using destination table types.
                    writeColumn(tdsWriter, columnMapping.sourceColumnOrdinal, columnMapping.destinationColumnOrdinal,
                            rowObjects[columnMapping.sourceColumnOrdinal - 1]);
                }
            }
            row++;

            if (insertRowByRow) {
                writePacketDataDone(tdsWriter);
                tdsWriter.setCryptoMetaData(null);

                // Send to the server and read response.
                TDSParser.parse(command.startResponse(), command.getLogContext());
            }
        }
    }
}
