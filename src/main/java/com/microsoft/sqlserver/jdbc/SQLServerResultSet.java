/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Indicates the type of the row received from the server
 */
enum RowType {
    ROW,
    NBCROW,
    UNKNOWN,
}

/**
 * Top-level JDBC ResultSet implementation
 */
public class SQLServerResultSet implements ISQLServerResultSet {

    /** Generate the statement's logging ID */
    private static final AtomicInteger lastResultSetID = new AtomicInteger(0);
    private final String traceID;

    private static int nextResultSetID() {
        return lastResultSetID.incrementAndGet();
    }

    final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerResultSet");

    public String toString() {
        return traceID;
    }

    String logCursorState() {
        return " currentRow:" + currentRow + " numFetchedRows:" + numFetchedRows + " rowCount:" + rowCount;
    }

    protected static final java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.ResultSet");

    final private String loggingClassName;

    String getClassNameLogging() {
        return loggingClassName;
    }

    /** the statement that generated this result set */
    private final SQLServerStatement stmt;

    /** max rows to return from this result set */
    private final int maxRows;

    /** the meta data for this result set */
    private ResultSetMetaData metaData;

    /** is the result set close */
    private boolean isClosed = false;

    private final int serverCursorId;
    
    protected int getServerCursorId() {
        return serverCursorId;
    }

    /** the intended fetch direction to optimize cursor performance */
    private int fetchDirection;

    /** the desired fetch size to optimize cursor performance */
    private int fetchSize;

    /** true if the cursor is positioned on the insert row */
    private boolean isOnInsertRow = false;

    /** true if the last value read was SQL NULL */
    private boolean lastValueWasNull = false;

    /** The index (1-based) of the last column in the current row that has been marked for reading */
    private int lastColumnIndex;

    // Indicates if the null bit map is loaded for the current row
    // in the resultset
    private boolean areNullCompressedColumnsInitialized = false;

    // Indicates the type of the current row in the result set
    private RowType resultSetCurrentRowType = RowType.UNKNOWN;

    // getter for resultSetCurrentRowType
    final RowType getCurrentRowType() {
        return resultSetCurrentRowType;
    }

    // setter for resultSetCurrentRowType
    final void setCurrentRowType(RowType rowType) {
        resultSetCurrentRowType = rowType;
    }

    /**
     * Currently active Stream Note only one stream can be active at a time, JDBC spec calls for the streams to be closed when a column or row move
     * occurs
     */
    private Closeable activeStream;
    private Blob activeBlob;

    /**
     * A window of fetchSize quickly accessible rows for scrollable result sets
     */
    private final ScrollWindow scrollWindow;

    /**
     * Current row, which is either the actual (1-based) value or one of the special values defined below.
     */
    private static final int BEFORE_FIRST_ROW = 0;
    private static final int AFTER_LAST_ROW = -1;
    private static final int UNKNOWN_ROW = -2;
    private int currentRow = BEFORE_FIRST_ROW;

    /** Flag set to true if the current row was updated through this ResultSet object */
    private boolean updatedCurrentRow = false;

    final boolean getUpdatedCurrentRow() {
        return updatedCurrentRow;
    }

    final void setUpdatedCurrentRow(boolean rowUpdated) {
        updatedCurrentRow = rowUpdated;
    }

    /** Flag set to true if the current row was deleted through this ResultSet object */
    private boolean deletedCurrentRow = false;

    final boolean getDeletedCurrentRow() {
        return deletedCurrentRow;
    }

    final void setDeletedCurrentRow(boolean rowDeleted) {
        deletedCurrentRow = rowDeleted;
    }

    /**
     * Count of rows in this result set.
     *
     * The number of rows in the result set may be known when this ResultSet object is created, after the first full traversal of the result set, or
     * possibly never (as is the case with DYNAMIC cursors).
     */
    static final int UNKNOWN_ROW_COUNT = -3;
    private int rowCount;

    /** The current row's column values */
    private final Column[] columns;

    // The CekTable retrieved from the COLMETADATA token for this resultset.
    private CekTable cekTable = null;

    /* Gets the CekTable */
    CekTable getCekTable() {
        return cekTable;
    }

    final void setColumnName(int index,
            String name) {
        columns[index - 1].setColumnName(name);
    }

    /**
     * Skips columns between the last marked column and the target column, inclusive, optionally discarding their values as they are skipped.
     */
    private void skipColumns(int columnsToSkip,
            boolean discardValues) throws SQLServerException {
        assert lastColumnIndex >= 1;
        assert 0 <= columnsToSkip && columnsToSkip <= columns.length;

        for (int columnsSkipped = 0; columnsSkipped < columnsToSkip; ++columnsSkipped) {
            Column column = getColumn(lastColumnIndex++);
            column.skipValue(tdsReader, discardValues && isForwardOnly());
            if (discardValues)
                column.clear();
        }
    }

    /** TDS reader from which row values are read */
    private TDSReader tdsReader;
    
    protected TDSReader getTDSReader() {
        return tdsReader;
    }

    private final FetchBuffer fetchBuffer;

    /**
     * Make a new result set
     * 
     * @param stmtIn
     *            the generating statement
     */
    SQLServerResultSet(SQLServerStatement stmtIn) throws SQLServerException {
        int resultSetID = nextResultSetID();
        loggingClassName = "com.microsoft.sqlserver.jdbc.SQLServerResultSet" + ":" + resultSetID;
        traceID = "SQLServerResultSet:" + resultSetID;

        // Common initializer class for server-cursored and client-cursored ResultSets.
        // The common initializer builds columns from the column metadata and other table
        // info when present. Specialized subclasses take care of behavior that is specific
        // to either server-cursored or client-cursored ResultSets.
        abstract class CursorInitializer extends TDSTokenHandler {
            abstract int getRowCount();

            abstract int getServerCursorId();

            private StreamColumns columnMetaData = null;
            private StreamColInfo colInfo = null;
            private StreamTabName tabName = null;

            final Column[] buildColumns() throws SQLServerException {
                return columnMetaData.buildColumns(colInfo, tabName);
            }

            CursorInitializer(String name) {
                super(name);
            }

            boolean onColInfo(TDSReader tdsReader) throws SQLServerException {
                colInfo = new StreamColInfo();
                colInfo.setFromTDS(tdsReader);
                return true;
            }

            boolean onTabName(TDSReader tdsReader) throws SQLServerException {
                tabName = new StreamTabName();
                tabName.setFromTDS(tdsReader);
                return true;
            }

            boolean onColMetaData(TDSReader tdsReader) throws SQLServerException {
                columnMetaData = new StreamColumns(Util.shouldHonorAEForRead(stmt.stmtColumnEncriptionSetting, stmt.connection));
                columnMetaData.setFromTDS(tdsReader);
                cekTable = columnMetaData.getCekTable();
                return true;
            }
        }

        // Server-cursor initializer expects a cursorID and row count to be
        // returned in OUT parameters from the sp_cursor[prep]exec call.
        // There should not be any rows present when initializing a server-cursored
        // ResultSet (until/unless support for cursor auto-fetch is implemented).
        final class ServerCursorInitializer extends CursorInitializer {
            private final SQLServerStatement stmt;

            final int getRowCount() {
                return stmt.getServerCursorRowCount();
            }

            final int getServerCursorId() {
                return stmt.getServerCursorId();
            }

            ServerCursorInitializer(SQLServerStatement stmt) {
                super("ServerCursorInitializer");
                this.stmt = stmt;
            }

            boolean onRetStatus(TDSReader tdsReader) throws SQLServerException {
                // With server-cursored result sets, the column metadata is
                // followed by a return status and cursor-related OUT parameters
                // for the sp_cursor[prep]exec call. Two of those OUT parameters
                // are the cursor ID and row count needed to construct this
                // ResultSet.
                stmt.consumeExecOutParam(tdsReader);
                return true;
            }

            boolean onRetValue(TDSReader tdsReader) throws SQLServerException {
                // The first OUT parameter after the sp_cursor[prep]exec OUT parameters
                // is the start of the application OUT parameters. Leave parsing
                // of them up to CallableStatement OUT param handlers.
                return false;
            }
        }

        // Client-cursor initializer expects 0 or more rows or a row-level error
        // to follow the column metadata.
        final class ClientCursorInitializer extends CursorInitializer {
            private int rowCount = UNKNOWN_ROW_COUNT;

            final int getRowCount() {
                return rowCount;
            }

            final int getServerCursorId() {
                return 0;
            }

            ClientCursorInitializer() {
                super("ClientCursorInitializer");
            }

            boolean onRow(TDSReader tdsReader) throws SQLServerException {
                // A ROW token indicates the start of the fetch buffer
                return false;
            }

            boolean onNBCRow(TDSReader tdsReader) throws SQLServerException {
                // A NBCROW token indicates the start of the fetch buffer
                return false;
            }

            boolean onError(TDSReader tdsReader) throws SQLServerException {
                // An ERROR token indicates a row error in lieu of a row.
                // In this case, the row error is in lieu of the first row.
                // Stop parsing and let the fetch buffer handle the error.
                rowCount = 0;
                return false;
            }

            boolean onDone(TDSReader tdsReader) throws SQLServerException {
                // When initializing client-cursored ResultSets, a DONE token
                // following the column metadata indicates an empty result set.
                rowCount = 0;

                // Continue to read the error message if DONE packet has error flag
                int packetType = tdsReader.peekTokenType();
                if (TDS.TDS_DONE == packetType) {
                    short status = tdsReader.peekStatusFlag();
                    // check if status flag has DONE_ERROR set i.e., 0x2
                    if ((status & 0x0002) != 0) {
                        // Consume the DONE packet if there is error
                        StreamDone doneToken = new StreamDone();
                        doneToken.setFromTDS(tdsReader);
                        return true;
                    }
                }

                return false;
            }
        }

        this.stmt = stmtIn;
        this.maxRows = stmtIn.maxRows;
        this.fetchSize = stmtIn.nFetchSize;
        this.fetchDirection = stmtIn.nFetchDirection;

        CursorInitializer initializer = stmtIn.executedSqlDirectly ? (new ClientCursorInitializer()) : (new ServerCursorInitializer(stmtIn));

        TDSParser.parse(stmtIn.resultsReader(), initializer);
        this.columns = initializer.buildColumns();
        this.rowCount = initializer.getRowCount();
        this.serverCursorId = initializer.getServerCursorId();

        // If this result set does not use a server cursor, then the result set rows
        // (if any) are already present in the fetch buffer at which the statement's
        // TDSReader now points.
        //
        // If this result set uses a server cursor, then without support for server
        // cursor autofetch, there are initially no rows with which to populate the
        // fetch buffer. The app will have to do a server cursor fetch first.
        this.tdsReader = (0 == serverCursorId) ? stmtIn.resultsReader() : null;

        this.fetchBuffer = new FetchBuffer();

        this.scrollWindow = isForwardOnly() ? null : new ScrollWindow(fetchSize);
        this.numFetchedRows = 0;

        // increment opened resultset counter
        stmtIn.incrResultSetCount();

        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine(toString() + " created by (" + stmt.toString() + ")");
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isWrapperFor");
        boolean f = iface.isInstance(this);
        loggerExternal.exiting(getClassNameLogging(), "isWrapperFor", f);
        return f;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "unwrap");
        T t;
        try {
            t = iface.cast(this);
        }
        catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        loggerExternal.exiting(getClassNameLogging(), "unwrap", t);
        return t;
    }

    private SQLServerException rowErrorException = null;

    /**
     * Check if the result set is closed
     * 
     * @throws SQLServerException
     */
    /* L0 */ void checkClosed() throws SQLServerException {

        if (isClosed) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_resultsetClosed"), null, false);
        }

        stmt.checkClosed();

        // This ResultSet isn't closed, but also check whether it's effectively dead
        // due to a row error. Once a ResultSet encounters a row error, nothing more
        // can be done with it other than closing it.
        if (null != rowErrorException)
            throw rowErrorException;
    }

    public boolean isClosed() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isClosed");
        boolean result = isClosed || stmt.isClosed();
        loggerExternal.exiting(getClassNameLogging(), "isClosed", result);
        return result;
    }

    /**
     * Called by ResultSet API methods to disallow method use on forward only result sets.
     *
     * @throws SQLServerException
     *             if the result set is forward only.
     */
    private void throwNotScrollable() throws SQLServerException {
        SQLServerException.makeFromDriverError(stmt.connection, this, SQLServerException.getErrString("R_requestedOpNotSupportedOnForward"), null,
                true);
    }

    protected boolean isForwardOnly() {
        return TYPE_SS_DIRECT_FORWARD_ONLY == stmt.getSQLResultSetType() || TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == stmt.getSQLResultSetType();
    }

    private boolean isDynamic() {
        return 0 != serverCursorId && TDS.SCROLLOPT_DYNAMIC == stmt.getCursorType();
    }

    private void verifyResultSetIsScrollable() throws SQLServerException {
        if (isForwardOnly())
            throwNotScrollable();
    }

    /**
     * Called by ResultSet API methods to disallow method use on read only result sets.
     *
     * @throws SQLServerException
     *             if the result set is read only.
     */
    private void throwNotUpdatable() throws SQLServerException {
        SQLServerException.makeFromDriverError(stmt.connection, this, SQLServerException.getErrString("R_resultsetNotUpdatable"), null, true);
    }

    private void verifyResultSetIsUpdatable() throws SQLServerException {
        if (CONCUR_READ_ONLY == stmt.resultSetConcurrency || 0 == serverCursorId)
            throwNotUpdatable();
    }

    /**
     * Checks whether the result set has a current row.
     *
     * @return true if there is a current row
     * @return false if the result set is positioned before the first row or after the last row.
     */
    private boolean hasCurrentRow() {
        return BEFORE_FIRST_ROW != currentRow && AFTER_LAST_ROW != currentRow;
    }

    /**
     * Verifies whether this result set has a current row.
     *
     * This check DOES NOT consider whether the cursor is on the insert row. The result set may or may not have a current row regardless whether the
     * cursor is on the insert row. Consider the following scenarios:
     *
     * beforeFirst(); moveToInsertRow(); relative(1); No current row to move relative to. Throw "no current row" exception.
     *
     * first(); moveToInsertRow(); relative(1); Call to relative moves off of the insert row one row past the current row. That is, the cursor ends up
     * on the second row of the result set.
     *
     * @throws SQLServerException
     *             if the result set has no current row
     */
    private void verifyResultSetHasCurrentRow() throws SQLServerException {
        if (!hasCurrentRow()) {
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_resultsetNoCurrentRow"), null, true);
        }
    }

    /**
     * Called by ResultSet API methods to disallow method use when cursor is on a deleted row.
     *
     * @throws SQLServerException
     *             if the cursor is not on an updatable row.
     */
    private void verifyCurrentRowIsNotDeleted(String errResource) throws SQLServerException {
        if (currentRowDeleted()) {
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString(errResource), null, true);
        }
    }

    /**
     * Called by ResultSet API methods to disallow method use when the column index is not in the range of columns returned in the results.
     *
     * @throws SQLServerException
     *             if the column index is out of bounds
     */
    private void verifyValidColumnIndex(int index) throws SQLServerException {
        int nCols = columns.length;

        // Rows that come back from server side cursors tack on a "hidden" ROWSTAT column
        // (used to detect deletes from a keyset) at the end of each row. Don't include
        // that column in the list of valid columns.
        if (0 != serverCursorId)
            --nCols;

        if (index < 1 || index > nCols) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_indexOutOfRange"));
            Object[] msgArgs = {index};
            SQLServerException.makeFromDriverError(stmt.connection, stmt, form.format(msgArgs), "07009", false);
        }
    }

    /**
     * Called by ResultSet API methods to disallow method use when cursor is on the insert row.
     *
     * @throws SQLServerException
     *             if the cursor is on the insert row.
     */
    private void verifyResultSetIsNotOnInsertRow() throws SQLServerException {
        if (isOnInsertRow) {
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_mustNotBeOnInsertRow"), null, true);
        }
    }

    private void throwUnsupportedCursorOp() throws SQLServerException {
        // Absolute positioning of dynamic cursors is unsupported.
        SQLServerException.makeFromDriverError(stmt.connection, this, SQLServerException.getErrString("R_unsupportedCursorOperation"), null, true);
    }

    /**
     * Close the result set.
     *
     * Note that the public close() method performs all of the cleanup work through this internal method which cannot throw any exceptions. This is
     * done deliberately to ensure that ALL of the object's client-side and server-side state is cleaned up as best as possible, even under conditions
     * which would normally result in exceptions being thrown.
     */
    private void closeInternal() {
        // Calling close on a closed ResultSet is a no-op per JDBC spec
        if (isClosed)
            return;

        // Mark this ResultSet as closed, then clean up.
        isClosed = true;

        // Discard the current fetch buffer contents.
        discardFetchBuffer();

        // Close the server cursor if there is one.
        closeServerCursor();

        // Clean up client-side state
        metaData = null;

        // decrement opened resultset counter
        stmt.decrResultSetCount();
    }

    public void close() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "close");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        closeInternal();
        loggerExternal.exiting(getClassNameLogging(), "close");
    }

    /**
     * Find a column index given a column name
     * 
     * @param columnName
     *            the name of the column
     * @throws SQLServerException
     *             If any errors occur.
     * @return the column index
     */
    public int findColumn(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "findColumn", columnName);
        checkClosed();

        // In order to be as accurate as possible when locating column name
        // indexes, as well as be deterministic when running on various client
        // locales, we search for column names using the following scheme:

        // Per JDBC spec 27.1.5 "if there are multiple columns with the same name
        // [findColumn] will return the value of the first matching name".

        // 1. Search using case-sensitive non-locale specific (binary) compare first.
        // 2. Search using case-insensitive, non-locale specific (binary) compare last.

        // NOTE: Any attempt to use a locale aware comparison will fail because:
        //
        // 1. SQL allows any valid UNICODE characters in the column name.
        // 2. SQL does not store any locale info associated with the column name.
        // 3. We cannot second guess the developer and decide to use VM locale or
        // database default locale when making comparisons, this would produce
        // inconsistent results on different clients or different servers.

        // Search using case-sensitive, non-locale specific (binary) compare.
        // If the user supplies a true match for the column name, we will find it here.
        int i;
        for (i = 0; i < columns.length; i++) {
            if (columns[i].getColumnName().equals(columnName)) {
                loggerExternal.exiting(getClassNameLogging(), "findColumn", i + 1);
                return i + 1;
            }
        }

        // Check for case-insensitive match using a non-locale aware method.
        // Per JDBC spec, 27.3 "The driver will do a case-insensitive search for
        // columnName in it's attempt to map it to the column's index".
        // Use VM supplied String.equalsIgnoreCase to do the "case-insensitive search".
        for (i = 0; i < columns.length; i++) {
            if (columns[i].getColumnName().equalsIgnoreCase(columnName)) {
                loggerExternal.exiting(getClassNameLogging(), "findColumn", i + 1);
                return i + 1;
            }
        }
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumnName"));
        Object[] msgArgs = {columnName};
        SQLServerException.makeFromDriverError(stmt.connection, stmt, form.format(msgArgs), "07009", false);

        return 0;
    }

    final int getColumnCount() {
        int nCols = columns.length;
        if (0 != serverCursorId)
            nCols--; // Do not include SQL Server's automatic rowstat column
        return nCols;
    }

    final Column getColumn(int columnIndex) throws SQLServerException {
        // Close any stream that might be open on the current column
        // before moving to another one.
        if (null != activeStream) {
            try {
            	fillBlobs();
                activeStream.close();
            }
            catch (IOException e) {
                SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
            }
            finally {
                activeStream = null;
            }
        }

        return columns[columnIndex - 1];
    }

    /**
     * This function initializes null compressed columns only when the row type is NBCROW and if the areNullCompressedColumnsInitialized is false. In
     * all other cases this will be a no-op.
     * 
     * @throws SQLServerException
     */
    private void initializeNullCompressedColumns() throws SQLServerException {
        if (resultSetCurrentRowType.equals(RowType.NBCROW) && (!areNullCompressedColumnsInitialized)) {
            int columnNo = 0;
            // no of bytes to be read from the stream
            int noOfBytes = ((this.columns.length - 1) >> 3) + 1;// equivalent of (int)Math.ceil(this.columns.length/8.0) and gives better perf
            for (int byteNo = 0; byteNo < noOfBytes; byteNo++) {

                int byteValue = tdsReader.readUnsignedByte();

                // if this byte is 0, skip to the next byte
                // and increment the column number by 8(no of bits)
                if (byteValue == 0) {
                    columnNo = columnNo + 8;
                    continue;
                }

                for (int bitNo = 0; bitNo < 8 && columnNo < this.columns.length; bitNo++) {
                    if ((byteValue & (1 << bitNo)) != 0) {
                        this.columns[columnNo].initFromCompressedNull();
                    }
                    columnNo++;
                }
            }
            areNullCompressedColumnsInitialized = true;
        }
    }

    private Column loadColumn(int index) throws SQLServerException {
        assert 1 <= index && index <= columns.length;

        initializeNullCompressedColumns();

        // Skip any columns between the last indexed column and the target column,
        // retaining their values so they can be retrieved later.
        if (index > lastColumnIndex && (!this.columns[index - 1].isInitialized()))
            skipColumns(index - lastColumnIndex, false);

        // Then return the target column
        return getColumn(index);
    }

    /* L0 */ private void NotImplemented() throws SQLServerException {
        SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_notSupported"), null, false);
    }

    /**
     * Clear result set warnings
     * 
     * @throws SQLServerException
     *             when an error occurs
     */
    /* L0 */ public void clearWarnings() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "clearWarnings");
        loggerExternal.exiting(getClassNameLogging(), "clearWarnings");
    }

    /* ----------------- JDBC API methods ------------------ */

    private void moverInit() throws SQLServerException {
    	fillBlobs();
        cancelInsert();
        cancelUpdates();
    }

    public boolean relative(int rows) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "relative", rows);

        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + " rows:" + rows + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if (1) there is no curent row or (2)
        // the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();
        verifyResultSetHasCurrentRow();

        moverInit();
        moveRelative(rows);
        boolean value = hasCurrentRow();
        loggerExternal.exiting(getClassNameLogging(), "relative", value);
        return value;
    }

    private void moveRelative(int rowsToMove) throws SQLServerException {
        // Relative moves must be from somewhere within the result set
        assert hasCurrentRow();

        // If rows is 0, the cursor's position does not change.
        if (0 == rowsToMove)
            return;

        if (rowsToMove > 0)
            moveForward(rowsToMove);
        else
            moveBackward(rowsToMove);
    }

    private void moveForward(int rowsToMove) throws SQLServerException {
        assert hasCurrentRow();
        assert rowsToMove > 0;

        // If there's a chance that the move can happen just in the scroll window then try that first
        if (scrollWindow.getRow() + rowsToMove <= scrollWindow.getMaxRows()) {
            int rowsMoved = 0;
            while (rowsToMove > 0 && scrollWindow.next(this)) {
                ++rowsMoved;
                --rowsToMove;
            }

            // Update the current row
            updateCurrentRow(rowsMoved);

            // If the move happened entirely in the scroll window, then we're done.
            if (0 == rowsToMove)
                return;
        }

        // All or part of the move lies outside the scroll window.
        assert rowsToMove > 0;

        // For client-cursored result sets, where the fetch buffer contains all of the rows, moves outside of
        // the scroll window are done via an absolute in the fetch buffer.
        if (0 == serverCursorId) {
            assert UNKNOWN_ROW != currentRow;
            currentRow = clientMoveAbsolute(currentRow + rowsToMove);
            return;
        }

        // For server-cursored result sets (where the fetch buffer and scroll window are the same size),
        // moves outside the scroll window require fetching more rows from the server.
        //
        // A few words on fetching strategy with server cursors
        // There is an assumption here that moving past the current position is an indication
        // that the result set is being traversed in forward order, so it makes sense to grab a
        // block of fetchSize rows from the server, starting at the desired location, to maximize
        // the number of rows that can be consumed before the next server fetch. That assumption
        // isn't necessarily true.
        if (1 == rowsToMove)
            doServerFetch(TDS.FETCH_NEXT, 0, fetchSize);
        else
            doServerFetch(TDS.FETCH_RELATIVE, rowsToMove + scrollWindow.getRow() - 1, fetchSize);

        // If the new fetch buffer returned no rows, then the cursor has reached the end of the result set.
        if (!scrollWindow.next(this)) {
            currentRow = AFTER_LAST_ROW;
            return;
        }

        // The move succeeded, so update the current row.
        updateCurrentRow(rowsToMove);
    }

    private void moveBackward(int rowsToMove) throws SQLServerException {
        assert hasCurrentRow();
        assert rowsToMove < 0;

        // If the move is contained in scroll window then handle it there.
        if (scrollWindow.getRow() + rowsToMove >= 1) {
            for (int rowsMoved = 0; rowsMoved > rowsToMove; --rowsMoved)
                scrollWindow.previous(this);

            updateCurrentRow(rowsToMove);
            return;
        }

        // The move lies outside the scroll window.

        // For client-cursored result sets, where the fetch buffer contains all of the rows, moves outside of
        // the scroll window are done via an absolute move in the fetch buffer.
        if (0 == serverCursorId) {
            assert UNKNOWN_ROW != currentRow;

            // Relative moves to before the first row must be handled here; a negative argument
            // to clientMoveAbsolute is interpreted as relative to the last row, not the first.
            if (currentRow + rowsToMove < 1) {
                moveBeforeFirst();
            }
            else {
                currentRow = clientMoveAbsolute(currentRow + rowsToMove);
            }

            return;
        }

        // For server-cursored result sets (where the fetch buffer and scroll window are the same size),
        // moves outside the scroll window require fetching more rows from the server.
        //
        // A few words on fetching strategy with server cursors
        // There is an assumption here that moving to the previous row is an indication
        // that the result set is being traversed in reverse order, so it makes sense to grab a
        // block of fetchSize rows from the server, ending with the desired location, to maximize
        // the number of rows that can be consumed before the next server fetch. That assumption
        // isn't necessarily true.
        //
        // Also, when moving further back than the previous row, it is not generally feasible to
        // try to fetch a block of rows ending with the target row, since a move far enough back
        // that the start of the fetch buffer would be before the first row of the result set would
        // position the cursor before the first row and return no rows, even though the target row
        // may not be before the first row. Instead, such moves are done so that the target row
        // is the first row in the returned block of rows rather than the last row.
        if (-1 == rowsToMove) {
            doServerFetch(TDS.FETCH_PREV_NOADJUST, 0, fetchSize);

            // If the new fetch buffer returned no rows, then the cursor has reached the start of the result set.
            if (!scrollWindow.next(this)) {
                currentRow = BEFORE_FIRST_ROW;
                return;
            }

            // Scroll past the last of the returned rows, and ...
            while (scrollWindow.next(this))
                ;

            // back up one row.
            scrollWindow.previous(this);
        }
        else {
            doServerFetch(TDS.FETCH_RELATIVE, rowsToMove + scrollWindow.getRow() - 1, fetchSize);

            // If the new fetch buffer returned no rows, then the cursor has reached the start of the result set.
            if (!scrollWindow.next(this)) {
                currentRow = BEFORE_FIRST_ROW;
                return;
            }
        }

        // The move succeeded, so update the current row.
        updateCurrentRow(rowsToMove);
    }

    /**
     * Update the current row's position if known.
     *
     * If known, the current row is assumed to be at a valid position somewhere in the ResultSet. That is, the current row is not before the first row
     * or after the last row.
     */
    private void updateCurrentRow(int rowsToMove) {
        if (UNKNOWN_ROW != currentRow) {
            assert currentRow >= 1;
            currentRow += rowsToMove;
            assert currentRow >= 1;
        }
    }

    /**
     * Initially moves the cursor to the first row of this ResultSet object, with subsequent calls moving the cursor to the second row, the third row,
     * and so on.
     *
     * @return false when there are no more rows to read
     */
    public boolean next() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "next");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        moverInit();

        // If the cursor is already positioned after the last row in this result set
        // then it can't move any farther forward.
        if (AFTER_LAST_ROW == currentRow) {
            loggerExternal.exiting(getClassNameLogging(), "next", false);
            return false;
        }

        // For scrollable cursors, next() is just a special case of relative()
        if (!isForwardOnly()) {
            if (BEFORE_FIRST_ROW == currentRow)
                moveFirst();
            else
                moveForward(1);
            
            boolean value = hasCurrentRow();
            loggerExternal.exiting(getClassNameLogging(), "next", value);
            return value;
        }

        // Fast path for forward only cursors...

        // Server forward only cursors do not honor SET ROWCOUNT,
        // so enforce any maxRows limit here.
        if (0 != serverCursorId && maxRows > 0) {
            if (currentRow == maxRows) {
                currentRow = AFTER_LAST_ROW;
                loggerExternal.exiting(getClassNameLogging(), "next", false);
                return false;
            }
        }

        // There is no scroll window for forward only cursors,
        // so try to get the next row directly from the fetch buffer.
        if (fetchBufferNext()) {
            // Update the current row.
            // Note that if the position was before the first row, the current
            // row should be updated to row 1.
            if (BEFORE_FIRST_ROW == currentRow)
                currentRow = 1;
            else
                updateCurrentRow(1);

            // We should never be asked to read more rows than maxRows.
            // Server forward only is handled above, and maxRows should
            // be enforced by the server for DIRECT forward only cursors.
            assert 0 == maxRows || currentRow <= maxRows;
            loggerExternal.exiting(getClassNameLogging(), "next", true);
            // Return that a row was read.
            return true;
        }

        // We're out of rows in the fetch buffer. If this is a server
        // cursor, then try to load up the fetch buffer with the next
        // set of fetchSize rows.
        if (0 != serverCursorId) {
            doServerFetch(TDS.FETCH_NEXT, 0, fetchSize);

            // If there are rows in the freshly-loaded fetch buffer
            // then return the first of them.
            if (fetchBufferNext()) {
                if (BEFORE_FIRST_ROW == currentRow)
                    currentRow = 1;
                else
                    updateCurrentRow(1);

                assert 0 == maxRows || currentRow <= maxRows;
                loggerExternal.exiting(getClassNameLogging(), "next", true);
                return true;
            }
        }

        // Otherwise, we have reached the end of the result set
        if (UNKNOWN_ROW_COUNT == rowCount)
            rowCount = currentRow;

        currentRow = AFTER_LAST_ROW;
        loggerExternal.exiting(getClassNameLogging(), "next", false);
        return false;
    }

    public boolean wasNull() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "wasNull");
        checkClosed();
        fillBlobs();
        loggerExternal.exiting(getClassNameLogging(), "wasNull", lastValueWasNull);
        return lastValueWasNull;
    }

    /**
     * @return true if the cursor is before the first row in this result set, returns false otherwise or if the result set contains no rows.
     */
    public boolean isBeforeFirst() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "isBeforeFirst");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        //
        // We deviate from JDBC spec here and allow this call on scrollable result sets.
        // Other drivers do the same. Hibernate requires this behavior.
        // verifyResultSetIsScrollable();

        if (0 != serverCursorId) {
            switch (stmt.getCursorType()) {
                case TDS.SCROLLOPT_FORWARD_ONLY:
                    throwNotScrollable();
                    break;

                case TDS.SCROLLOPT_DYNAMIC:
                    throwUnsupportedCursorOp();
                    break;

                case TDS.SCROLLOPT_FAST_FORWARD:
                    throwNotScrollable();
                    break;

                // All other types (KEYSET, STATIC) return a row count up front
                default:
                    break;
            }
        }

        if (isOnInsertRow)
            return false;

        // If the cursor is not positioned before the first row in this result set
        // then the answer is obvious:
        if (BEFORE_FIRST_ROW != currentRow)
            return false;

        // If the cursor is positioned before the first row in this result set then
        // isBeforeFirst returns true only if the result set is also not empty.

        // For client-cursored result sets, determining whether the result set is empty
        // is just a matter of checking whether there are rows in the fetch buffer.
        if (0 == serverCursorId)
            return fetchBufferHasRows();

        // For server-cursored result sets, the row count tells whether the result set
        // is empty. Assumption: server cursors that do not provide a row count (e.g. DYNAMIC)
        // are handled above.
        assert rowCount >= 0;
        boolean value = rowCount > 0;
        loggerExternal.exiting(getClassNameLogging(), "isBeforeFirst", value);
        return value;
    }

    public boolean isAfterLast() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "isAfterLast");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        //
        // We deviate from JDBC spec here and allow this call on forward only client-cursored
        // result sets. Other drivers do the same. Hibernate requires this behavior.
        if (0 != serverCursorId) {
            verifyResultSetIsScrollable();

            // Scrollable DYNAMIC cursors do not support isAfterLast() since they
            // don't provide a row count and cannot distinguish an empty fetch
            // buffer from an empty result set.
            if (TDS.SCROLLOPT_DYNAMIC == stmt.getCursorType() && !isForwardOnly())
                throwUnsupportedCursorOp();
        }

        if (isOnInsertRow)
            return false;

        // By the time the cursor is positioned after the last row of the result set,
        // the count of rows must be known.
        assert !(AFTER_LAST_ROW == currentRow && UNKNOWN_ROW_COUNT == rowCount);

        boolean value = AFTER_LAST_ROW == currentRow && rowCount > 0;
        loggerExternal.exiting(getClassNameLogging(), "isAfterLast", value);
        return value;
    }

    /**
     * Determines whether the cursor is on the first row in this ResultSet object.
     *
     * This method should be called only on ResultSet objects that are scrollable: TYPE_SCROLL_SENSITIVE, TYPE_SCROLL_INSENSITIVE,
     * TYPE_SS_SCROLL_STATIC, TYPE_SS_SCROLL_KEYSET, TYPE_SS_SCROLL_DYNAMIC.
     *
     * @return true if the cursor is on the first row in this result set
     */
    public boolean isFirst() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "isFirst");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        // DYNAMIC cursors do not support isFirst(). There is no way to determine absolute
        // position within the result set with a DYNAMIC cursor.
        if (isDynamic())
            throwUnsupportedCursorOp();

        if (isOnInsertRow)
            return false;

        // At this point we must have a cursor that is scrollable and non-DYNAMIC.
        // That is, we have a cursor that has a notion of absolute position.
        assert UNKNOWN_ROW != currentRow;

        // Just return whether that absolution position is the first row.
        boolean value = 1 == currentRow;
        loggerExternal.exiting(getClassNameLogging(), "isFirst", value);
        return value;
    }

    /**
     * Determines whether the cursor is on the last row in this ResultSet object.
     *
     * This method should be called only on ResultSet objects that are scrollable: TYPE_SCROLL_SENSITIVE, TYPE_SCROLL_INSENSITIVE,
     * TYPE_SS_SCROLL_STATIC, TYPE_SS_SCROLL_KEYSET, TYPE_SS_SCROLL_DYNAMIC.
     *
     * @return true if the cursor is on the last row in this result set
     */
    public boolean isLast() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "isLast");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        // DYNAMIC cursors do not support isLast(). There is no way to determine absolute
        // position within the result set with a DYNAMIC cursor.
        if (isDynamic())
            throwUnsupportedCursorOp();

        if (isOnInsertRow)
            return false;

        // If the cursor is before the first row or after the last row then
        // it is by definition not on the last row.
        if (!hasCurrentRow())
            return false;

        // At this point circumstances are such that we must know the current row
        assert currentRow >= 1;

        // Determining whether the current row is the last row is easy if we know the row count
        if (UNKNOWN_ROW_COUNT != rowCount) {
            assert currentRow <= rowCount;
            return currentRow == rowCount;
        }

        // If we don't know the row count, determining whether the current row is
        // the last row is not quite as straightforward, but still reasonably efficient.
        // Presumably since we've ruled out a DYNAMIC cursor, the only way we would
        // not know the row count at this point is if we have a client cursor.
        assert 0 == serverCursorId;

        // All server cursors other than DYNAMIC give us a row count. If we have
        // a client cursor, then we can tell whether the current row is the last
        // row just by checking whether there are any more rows in the fetch buffer.
        // A call to isLast() logically should not modify the current position in
        // the response, so save the current position and restore it on exit.
        boolean isLast = !next();
        previous();
        loggerExternal.exiting(getClassNameLogging(), "isLast", isLast);
        return isLast;
    }

    public void beforeFirst() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "beforeFirst");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        moverInit();
        moveBeforeFirst();
        loggerExternal.exiting(getClassNameLogging(), "beforeFirst");
    }

    private void moveBeforeFirst() throws SQLServerException {
        if (0 == serverCursorId) {
            fetchBufferBeforeFirst();
            scrollWindow.clear();
        }
        else {
            doServerFetch(TDS.FETCH_FIRST, 0, 0);
        }

        currentRow = BEFORE_FIRST_ROW;
    }

    public void afterLast() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "afterLast");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        moverInit();
        moveAfterLast();
        loggerExternal.exiting(getClassNameLogging(), "afterLast");
    }

    private void moveAfterLast() throws SQLServerException {
        assert !isForwardOnly();

        if (0 == serverCursorId)
            clientMoveAfterLast();
        else
            doServerFetch(TDS.FETCH_LAST, 0, 0);

        currentRow = AFTER_LAST_ROW;
    }

    /**
     * Moves the cursor to the first row in this ResultSet object.
     *
     * This method should be called only on ResultSet objects that are scrollable: TYPE_SCROLL_SENSITIVE, TYPE_SCROLL_INSENSITIVE,
     * TYPE_SS_SCROLL_STATIC, TYPE_SS_SCROLL_KEYSET, TYPE_SS_SCROLL_DYNAMIC.
     *
     * @return true if the cursor is on a valid row, otherwise returns false if there are no rows in this ResultSet object
     */
    public boolean first() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "first");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        moverInit();
        moveFirst();
        boolean value = hasCurrentRow();
        loggerExternal.exiting(getClassNameLogging(), "first", value);
        return value;
    }

    private void moveFirst() throws SQLServerException {
        if (0 == serverCursorId) {
            moveBeforeFirst();
        }
        else {
            // Fetch the first block of up to fetchSize rows
            doServerFetch(TDS.FETCH_FIRST, 0, fetchSize);
        }

        // Start the scroll window at the first row in the fetch buffer
        if (!scrollWindow.next(this)) {
            // If there are no rows in the result set then just ensure the current row
            // is positioned at a consistent location so that subsequent ResultSet
            // operations behave consistently.
            //
            // The actual position of the server cursor does not matter in this case.
            currentRow = AFTER_LAST_ROW;
            return;
        }

        // Adjust the current row appropriately
        currentRow = isDynamic() ? UNKNOWN_ROW : 1;
    }

    /**
     * Moves the cursor to the last row in this ResultSet object.
     *
     * This method should be called only on ResultSet objects that are scrollable: TYPE_SCROLL_SENSITIVE, TYPE_SCROLL_INSENSITIVE,
     * TYPE_SS_SCROLL_STATIC, TYPE_SS_SCROLL_KEYSET, TYPE_SS_SCROLL_DYNAMIC.
     *
     * @return true if the cursor is on a valid row, otherwise returns false if there are no rows in this ResultSet object
     */
    public boolean last() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "last");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        moverInit();
        moveLast();
        boolean value = hasCurrentRow();
        loggerExternal.exiting(getClassNameLogging(), "last", value);
        return value;
    }

    private void moveLast() throws SQLServerException {
        if (0 == serverCursorId) {
            currentRow = clientMoveAbsolute(-1);
            return;
        }

        // Fetch the last block of up to fetchSize rows from the result set
        doServerFetch(TDS.FETCH_LAST, 0, fetchSize);

        // Start the scroll window at the first row in the fetch buffer
        if (!scrollWindow.next(this)) {
            // If there are no rows in the result set then just ensure the current row
            // is positioned at a consistent location so that subsequent ResultSet
            // operations behave consistently.
            //
            // The actual position of the server cursor does not matter in this case.
            currentRow = AFTER_LAST_ROW;
            return;
        }

        // Scroll to the last of the returned rows
        while (scrollWindow.next(this))
            ;
        scrollWindow.previous(this);

        // Adjust the current row appropriately
        currentRow = isDynamic() ? UNKNOWN_ROW : rowCount;
    }

    /**
     * Retrieves the number of the current row in this ResultSet object. The first row is number 1, the second is 2, and so on.
     *
     * @return the number of the current row; 0 if there is no current row
     */
    public int getRow() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getRow");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // DYNAMIC (scrollable) cursors do not support getRow() since they do not have any
        // concept of absolute position.
        if (isDynamic() && !isForwardOnly())
            throwUnsupportedCursorOp();

        // From JDBC spec:
        // [returns] 0 if there is no current row.
        //
        // From our ResultSet Cursors feature spec:
        // getRow returns 0 when in insert mode.
        if (!hasCurrentRow() || isOnInsertRow)
            return 0;

        // We should be dealing with a cursor type that has a notion of absolute position
        assert currentRow >= 1;

        // Return that absolute position
        loggerExternal.exiting(getClassNameLogging(), "getRow", currentRow);
        return currentRow;
    }

    /**
     * Moves the cursor to the specified row in this ResultSet object. The specified row may be positive, negative or zero.
     *
     * This method should be called only on ResultSet objects that are scrollable: TYPE_SCROLL_SENSITIVE, TYPE_SCROLL_INSENSITIVE,
     * TYPE_SS_SCROLL_STATIC, TYPE_SS_SCROLL_KEYSET, TYPE_SS_SCROLL_DYNAMIC.
     *
     * @return true if the cursor is on a valid row in this result set, otherwise returns false if the cursor is before the first row or after the
     *         last row
     */
    public boolean absolute(int row) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "absolute");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + " row:" + row + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        // DYNAMIC cursors do not support absolute(). There is no way to determine absolute
        // position within the result set with a DYNAMIC cursor.
        if (isDynamic())
            throwUnsupportedCursorOp();

        moverInit();
        moveAbsolute(row);
        boolean value = hasCurrentRow();
        loggerExternal.exiting(getClassNameLogging(), "absolute", value);
        return value;
    }

    private void moveAbsolute(int row) throws SQLServerException {
        // Absolute positioning is not allowed for cursor types that don't support
        // knowing the absolute position.
        assert UNKNOWN_ROW != currentRow;
        assert !isDynamic();

        switch (row) {
            // If row is 0, the cursor is positioned before the first row.
            case 0:
                moveBeforeFirst();
                return;

            // Calling absolute(1) is the same as calling the method first().
            case 1:
                moveFirst();
                return;

            // Calling absolute(-1) is the same as calling the method last().
            case -1:
                moveLast();
                return;
        }

        // Depending on how much we know about the result set, an absolute move
        // can be translated into a relative move. The advantage to doing this
        // is that we gain the benefit of using the scroll window, which reduces
        // calls to the server when absolute moves can translate to small moves
        // relative to the current row.
        if (hasCurrentRow()) {
            assert currentRow >= 1;

            // If the absolute move is from the start of the result set (+ve rows)
            // then we can easily express it as a relative (to the current row) move:
            // the amount to move is just the difference between the current row and
            // the target absolute row.
            if (row > 0) {
                moveRelative(row - currentRow);
                return;
            }

            // If the absolute move is from the end of the result set (-ve rows)
            // then we also need to know how many rows are in the result set.
            // If we do then we can convert to an absolute move from the start
            // of the result set, and apply the logic above.
            if (UNKNOWN_ROW_COUNT != rowCount) {
                assert row < 0;
                moveRelative((rowCount + row + 1) - currentRow);
                return;
            }
        }

        // Ok, so there's no chance of a relative move. In other words, the current
        // position may be before the first row or after the last row. Or perhaps
        // it's an absolute move from the end of the result set and we don't know
        // how many rows there are yet (can happen with a scrollable client cursor).
        // In that case, we need to move absolutely.

        // Try to fetch a block of up to fetchSize rows starting at row row.
        if (0 == serverCursorId) {
            currentRow = clientMoveAbsolute(row);
            return;
        }

        doServerFetch(TDS.FETCH_ABSOLUTE, row, fetchSize);

        // If the absolute server fetch didn't land somewhere on the result set
        // then it's either before the first row or after the last row.
        if (!scrollWindow.next(this)) {
            currentRow = (row < 0) ? BEFORE_FIRST_ROW : AFTER_LAST_ROW;
            return;
        }

        // The absolute server fetch landed somewhere on the result set,
        // so update the current row to reflect the new position.
        if (row > 0) {
            // The current row is just the row to which we moved.
            currentRow = row;
        }
        else {
            // Absolute fetch with -ve row is relative to the end of the result set.
            assert row < 0;
            assert rowCount + row + 1 >= 1;
            currentRow = rowCount + row + 1;
        }
    }

    private boolean fetchBufferHasRows() throws SQLServerException {
        // Never call this with server cursors without first determining whether the
        // fetch buffer exists yet.
        assert 0 == serverCursorId;
        assert null != tdsReader;

        assert lastColumnIndex >= 0;

        // If we're somewhere in the middle of a row, then obviously the fetch buffer has rows!
        if (lastColumnIndex >= 1)
            return true;

        // We're not somewhere in the middle of a row, so we're either at the start of a row
        // or looking at an empty fetch buffer. Peeking at the next TDS token tells us which.
        int tdsTokenType = tdsReader.peekTokenType();

        // Return whether the next item in the response appears to be a row or something
        // that should have been a row.
        return (TDS.TDS_ROW == tdsTokenType || TDS.TDS_NBCROW == tdsTokenType || TDS.TDS_MSG == tdsTokenType || TDS.TDS_ERR == tdsTokenType);
    }

    final void discardCurrentRow() throws SQLServerException {
        assert lastColumnIndex >= 0;

        updatedCurrentRow = false;
        deletedCurrentRow = false;
        if (lastColumnIndex >= 1) {
            initializeNullCompressedColumns();
            // Discard columns up to, but not including, the last indexed column
            for (int columnIndex = 1; columnIndex < lastColumnIndex; ++columnIndex)
                getColumn(columnIndex).clear();

            // Skip and discard the remainder of the last indexed column
            // and all subsequent columns
            skipColumns(columns.length + 1 - lastColumnIndex, true);
        }

        // reset areNullCompressedColumnsInitialized to false and row type to unknown
        resultSetCurrentRowType = RowType.UNKNOWN;
        areNullCompressedColumnsInitialized = false;
    }

    final int fetchBufferGetRow() {
        if (isForwardOnly())
            return numFetchedRows;

        return scrollWindow.getRow();
    }

    final void fetchBufferBeforeFirst() throws SQLServerException {
        // Never call this with server cursors without first determining whether the
        // fetch buffer exists yet.
        assert 0 == serverCursorId;
        assert null != tdsReader;

        discardCurrentRow();

        fetchBuffer.reset();
        lastColumnIndex = 0;
    }

    final TDSReaderMark fetchBufferMark() {
        // Never call this if we don't have a fetch buffer...
        assert null != tdsReader;

        return tdsReader.mark();
    }

    final void fetchBufferReset(TDSReaderMark mark) throws SQLServerException {
        // Never call this if we don't have a fetch buffer...
        assert null != tdsReader;

        assert null != mark;

        discardCurrentRow();

        tdsReader.reset(mark);

        lastColumnIndex = 1;
    }

    final boolean fetchBufferNext() throws SQLServerException {
        // If we don't have a fetch buffer yet then there are no rows to fetch
        if (null == tdsReader)
            return false;

        // We do have a fetch buffer. So discard the current row in the fetch buffer and ...
        discardCurrentRow();

        // ... scan for the next row.
        // If we didn't find one, then we're done.
        RowType fetchBufferCurrentRowType = RowType.UNKNOWN;
        try {
            fetchBufferCurrentRowType = fetchBuffer.nextRow();
            if (fetchBufferCurrentRowType.equals(RowType.UNKNOWN))
                return false;
        }
        catch (SQLServerException e) {
            currentRow = AFTER_LAST_ROW;
            rowErrorException = e;
            throw e;
        }
        finally {
            lastColumnIndex = 0;
            resultSetCurrentRowType = fetchBufferCurrentRowType;
        }

        // Otherwise, we found a row.
        ++numFetchedRows;
        lastColumnIndex = 1;
        return true;
    }

    private void clientMoveAfterLast() throws SQLServerException {
        assert UNKNOWN_ROW != currentRow;

        int rowsSkipped = 0;
        while (fetchBufferNext())
            ++rowsSkipped;

        if (UNKNOWN_ROW_COUNT == rowCount) {
            assert AFTER_LAST_ROW != currentRow;
            rowCount = ((BEFORE_FIRST_ROW == currentRow) ? 0 : currentRow) + rowsSkipped;
        }
    }

    private int clientMoveAbsolute(int row) throws SQLServerException {
        assert 0 == serverCursorId;

        scrollWindow.clear();

        // Because there is no way to know how far from the end of the result
        // set we are, if we are asked to move some number of rows from the end
        // of the result set then we must translate that into some number of rows
        // to move from the start (i.e. convert a negative row move to a positive
        // row move).
        if (row < 0) {
            // In order to convert a negative move to an absolute positive move,
            // we need to know the number of rows in the result set. If we don't
            // already know the row count, then moving after the last row is
            // the only way to compute it (as a side effect).
            if (UNKNOWN_ROW_COUNT == rowCount) {
                clientMoveAfterLast();
                currentRow = AFTER_LAST_ROW;
            }

            // Now that we know the row count, we can translate the negative
            // move into a positive one.
            assert rowCount >= 0;

            // If we are moving backward from the end more rows than are in the
            // result set, then the ultimate position ends up before the first row.
            if (rowCount + row < 0) {
                moveBeforeFirst();
                return BEFORE_FIRST_ROW;
            }

            row = rowCount + row + 1;
        }

        // At this point we were either given a positive row movement from
        // the start of the result set or we have converted the negative row
        // movement that we were given from the end of the result set into a
        // positive row movememnt.
        assert row > 0;

        // If the target row lies somewhere before the current row (including the
        // current row itself, because moving to the current row moves back to
        // the _beginning_ of the current row), then we have to move all the way
        // back to the beginning of the result set, and then move from there.
        if (AFTER_LAST_ROW == currentRow || row <= currentRow)
            moveBeforeFirst();

        // Now move from the current row (which may be before the first row)
        // to the target row.
        assert BEFORE_FIRST_ROW == currentRow || currentRow < row;
        while (currentRow != row) {
            if (!fetchBufferNext()) {
                if (UNKNOWN_ROW_COUNT == rowCount)
                    rowCount = currentRow;
                return AFTER_LAST_ROW;
            }

            // Update the current row.
            // Note that if the position was before the first row, the current
            // row should be updated to row 1.
            if (BEFORE_FIRST_ROW == currentRow)
                currentRow = 1;
            else
                updateCurrentRow(1);
        }

        return row;
    }

    /**
     * Moves the cursor to the previous row in this ResultSet object.
     *
     * This method should be called only on ResultSet objects that are scrollable: TYPE_SCROLL_SENSITIVE, TYPE_SCROLL_INSENSITIVE,
     * TYPE_SS_SCROLL_STATIC, TYPE_SS_SCROLL_KEYSET, TYPE_SS_SCROLL_DYNAMIC.
     *
     * @return true if the cursor is on a valid row in this result set
     */
    public boolean previous() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "previous");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        moverInit();

        if (BEFORE_FIRST_ROW == currentRow)
            return false;

        if (AFTER_LAST_ROW == currentRow)
            moveLast();
        else
            moveBackward(-1);

        boolean value = hasCurrentRow();
        loggerExternal.exiting(getClassNameLogging(), "previous", value);
        return value;
    }

    private void cancelInsert() {
        if (isOnInsertRow) {
            isOnInsertRow = false;
            clearColumnsValues();
        }
    }

    /** Clear any updated column values for the current row in the result set. */
    final void clearColumnsValues() {
        int l = columns.length;
        for (Column column : columns) column.cancelUpdates();
    }

    /* L0 */ public SQLWarning getWarnings() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getWarnings");
        loggerExternal.exiting(getClassNameLogging(), "getWarnings", null);
        return null;
    }

    public void setFetchDirection(int direction) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setFetchDirection", direction);
        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the type of this ResultSet object is TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        if ((ResultSet.FETCH_FORWARD != direction && ResultSet.FETCH_REVERSE != direction && ResultSet.FETCH_UNKNOWN != direction) ||

                (ResultSet.FETCH_FORWARD != direction && (SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY == stmt.resultSetType
                        || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == stmt.resultSetType))) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidFetchDirection"));
            Object[] msgArgs = {direction};
            SQLServerException.makeFromDriverError(stmt.connection, stmt, form.format(msgArgs), null, false);
        }

        fetchDirection = direction;
        loggerExternal.exiting(getClassNameLogging(), "setFetchDirection");
    }

    public int getFetchDirection() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFetchDirection");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getFetchDirection", fetchDirection);
        return fetchDirection;
    }

    public void setFetchSize(int rows) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setFetchSize", rows);
        checkClosed();
        if (rows < 0)
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_invalidFetchSize"), null, false);

        fetchSize = (0 == rows) ? stmt.defaultFetchSize : rows;
        loggerExternal.exiting(getClassNameLogging(), "setFetchSize");
    }

    /* L0 */ public int getFetchSize() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFetchSize");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getFloat", fetchSize);
        return fetchSize;
    }

    /* L0 */ public int getType() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getType");
        checkClosed();

        int value = stmt.getResultSetType();
        loggerExternal.exiting(getClassNameLogging(), "getType", value);
        return value;
    }

    /* L0 */ public int getConcurrency() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getConcurrency");
        checkClosed();
        int value = stmt.getResultSetConcurrency();
        loggerExternal.exiting(getClassNameLogging(), "getConcurrency", value);
        return value;

    }

    /* ---------------------- Column gets --------------------------------- */

    /**
     * Does all the common stuff necessary when calling a getter for the column at index.
     *
     * @param index
     *            the index of the column to get
     */
    Column getterGetColumn(int index) throws SQLServerException {
        // Note that we don't verify here that we're not on the insert row. According to
        // our RS cursors spec:
        // Columns on the insert row are initially in an uninitialized state. Calls to
        // update<type> set the column state to initialized. A call to get<type> for an
        // uninitialized column throws an exception.
        // It doesn't say anything about calls to initialized columns (i.e. what happens
        // if update<type> has been called). Shipped behavior is that the value returned
        // is the value of the current row, not the updated value in the insert row!

        verifyResultSetHasCurrentRow();
        verifyCurrentRowIsNotDeleted("R_cantGetColumnValueFromDeletedRow");
        verifyValidColumnIndex(index);

        if (updatedCurrentRow) {
            doRefreshRow();
            verifyResultSetHasCurrentRow();
        }

        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + " Getting Column:" + index);

        fillBlobs();
        return loadColumn(index);
    }

    private Object getValue(int columnIndex,
            JDBCType jdbcType) throws SQLServerException {
        return getValue(columnIndex, jdbcType, null, null);
    }

    private Object getValue(int columnIndex,
            JDBCType jdbcType,
            Calendar cal) throws SQLServerException {
        return getValue(columnIndex, jdbcType, null, cal);
    }

    private Object getValue(int columnIndex,
            JDBCType jdbcType,
            InputStreamGetterArgs getterArgs) throws SQLServerException {
        return getValue(columnIndex, jdbcType, getterArgs, null);
    }

    private Object getValue(int columnIndex,
            JDBCType jdbcType,
            InputStreamGetterArgs getterArgs,
            Calendar cal) throws SQLServerException {
        Object o = getterGetColumn(columnIndex).getValue(jdbcType, getterArgs, cal, tdsReader);
        lastValueWasNull = (null == o);
        return o;
    }
    
    void setInternalVariantType(int columnIndex, SqlVariant type) throws SQLServerException{
        getterGetColumn(columnIndex).setInternalVariant(type);
    }
    
    SqlVariant getVariantInternalType(int columnIndex) throws SQLServerException {
        return getterGetColumn(columnIndex).getInternalVariant();
    }    
    
    private Object getStream(int columnIndex,
            StreamType streamType) throws SQLServerException {
        Object value = getValue(columnIndex, streamType.getJDBCType(),
                new InputStreamGetterArgs(streamType, stmt.getExecProps().isResponseBufferingAdaptive(), isForwardOnly(), toString()));

        activeStream = (Closeable) value;
        return value;
    }

    private SQLXML getSQLXMLInternal(int columnIndex) throws SQLServerException {
        SQLServerSQLXML value = (SQLServerSQLXML) getValue(columnIndex, JDBCType.SQLXML,
                new InputStreamGetterArgs(StreamType.SQLXML, stmt.getExecProps().isResponseBufferingAdaptive(), isForwardOnly(), toString()));

        if (null != value)
            activeStream = value.getStream();
        return value;
    }

    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getAsciiStream", columnIndex);
        checkClosed();
        InputStream value = (InputStream) getStream(columnIndex, StreamType.ASCII);
        loggerExternal.exiting(getClassNameLogging(), "getAsciiStream", value);
        return value;
    }

    public java.io.InputStream getAsciiStream(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getAsciiStream", columnName);
        checkClosed();
        InputStream value = (InputStream) getStream(findColumn(columnName), StreamType.ASCII);
        loggerExternal.exiting(getClassNameLogging(), "getAsciiStream", value);
        return value;
    }

    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getBigDecimal", new Object[] {columnIndex, scale});
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(columnIndex, JDBCType.DECIMAL);
        if (null != value)
            value = value.setScale(scale, BigDecimal.ROUND_DOWN);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    @Deprecated
    public BigDecimal getBigDecimal(String columnName,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "columnName", new Object[] {columnName, scale});
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(columnName), JDBCType.DECIMAL);
        if (null != value)
            value = value.setScale(scale, BigDecimal.ROUND_DOWN);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBinaryStream", columnIndex);
        checkClosed();
        InputStream value = (InputStream) getStream(columnIndex, StreamType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBinaryStream", value);
        return value;
    }

    public java.io.InputStream getBinaryStream(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBinaryStream", columnName);
        checkClosed();
        InputStream value = (InputStream) getStream(findColumn(columnName), StreamType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBinaryStream", value);
        return value;
    }

    public boolean getBoolean(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBoolean", columnIndex);
        checkClosed();
        Boolean value = (Boolean) getValue(columnIndex, JDBCType.BIT);
        loggerExternal.exiting(getClassNameLogging(), "getBoolean", value);
        return null != value ? value : false;
    }

    public boolean getBoolean(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBoolean", columnName);
        checkClosed();
        Boolean value = (Boolean) getValue(findColumn(columnName), JDBCType.BIT);
        loggerExternal.exiting(getClassNameLogging(), "getBoolean", value);
        return null != value ? value : false;
    }

    public byte getByte(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getByte", columnIndex);
        checkClosed();
        Short value = (Short) getValue(columnIndex, JDBCType.TINYINT);
        loggerExternal.exiting(getClassNameLogging(), "getByte", value);
        return null != value ? value.byteValue() : 0;
    }

    public byte getByte(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getByte", columnName);
        checkClosed();
        Short value = (Short) getValue(findColumn(columnName), JDBCType.TINYINT);
        loggerExternal.exiting(getClassNameLogging(), "getByte", value);
        return null != value ? value.byteValue() : 0;
    }

    public byte[] getBytes(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBytes", columnIndex);
        checkClosed();
        byte[] value = (byte[]) getValue(columnIndex, JDBCType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBytes", value);
        return value;
    }

    public byte[] getBytes(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBytes", columnName);
        checkClosed();
        byte[] value = (byte[]) getValue(findColumn(columnName), JDBCType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBytes", value);
        return value;
    }

    public java.sql.Date getDate(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDate", columnIndex);
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(columnIndex, JDBCType.DATE);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    public java.sql.Date getDate(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDate", columnName);
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(findColumn(columnName), JDBCType.DATE);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    public java.sql.Date getDate(int columnIndex,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDate", new Object[] {columnIndex, cal});
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(columnIndex, JDBCType.DATE, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    public java.sql.Date getDate(String colName,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDate", new Object[] {colName, cal});
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(findColumn(colName), JDBCType.DATE, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    public double getDouble(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDouble", columnIndex);
        checkClosed();
        Double value = (Double) getValue(columnIndex, JDBCType.DOUBLE);
        loggerExternal.exiting(getClassNameLogging(), "getDouble", value);
        return null != value ? value : 0;
    }

    public double getDouble(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDouble", columnName);
        checkClosed();
        Double value = (Double) getValue(findColumn(columnName), JDBCType.DOUBLE);
        loggerExternal.exiting(getClassNameLogging(), "getDouble", value);
        return null != value ? value : 0;
    }

    public float getFloat(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFloat", columnIndex);
        checkClosed();
        Float value = (Float) getValue(columnIndex, JDBCType.REAL);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return null != value ? value : 0;
    }

    public float getFloat(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFloat", columnName);
        checkClosed();
        Float value = (Float) getValue(findColumn(columnName), JDBCType.REAL);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return null != value ? value : 0;
    }
    
    public Geometry getGeometry(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFloat", columnIndex);
        checkClosed();
        Geometry value = (Geometry) getValue(columnIndex, JDBCType.GEOMETRY);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return value;
    }

    public Geometry getGeometry(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFloat", columnName);
        checkClosed();
        Geometry value = (Geometry) getValue(findColumn(columnName), JDBCType.GEOMETRY);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return value;
    }
    
    public Geography getGeography(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFloat", columnIndex);
        checkClosed();
        Geography value = (Geography) getValue(columnIndex, JDBCType.GEOGRAPHY);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return value;
    }

    public Geography getGeography(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFloat", columnName);
        checkClosed();
        Geography value = (Geography) getValue(findColumn(columnName), JDBCType.GEOGRAPHY);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return value;
    }

    public int getInt(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getInt", columnIndex);
        checkClosed();
        Integer value = (Integer) getValue(columnIndex, JDBCType.INTEGER);
        loggerExternal.exiting(getClassNameLogging(), "getInt", value);
        return null != value ? value : 0;
    }

    public int getInt(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getInt", columnName);
        checkClosed();
        Integer value = (Integer) getValue(findColumn(columnName), JDBCType.INTEGER);
        loggerExternal.exiting(getClassNameLogging(), "getInt", value);
        return null != value ? value : 0;
    }

    public long getLong(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getLong", columnIndex);
        checkClosed();
        Long value = (Long) getValue(columnIndex, JDBCType.BIGINT);
        loggerExternal.exiting(getClassNameLogging(), "getLong", value);
        return null != value ? value : 0;
    }

    public long getLong(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getLong", columnName);
        checkClosed();
        Long value = (Long) getValue(findColumn(columnName), JDBCType.BIGINT);
        loggerExternal.exiting(getClassNameLogging(), "getLong", value);
        return null != value ? value : 0;
    }

    public java.sql.ResultSetMetaData getMetaData() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMetaData");
        checkClosed();
        if (metaData == null)
            metaData = new SQLServerResultSetMetaData(stmt.connection, this);
        loggerExternal.exiting(getClassNameLogging(), "getMetaData", metaData);
        return metaData;
    }

    public Object getObject(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getObject", columnIndex);
        checkClosed();
        Object value = getValue(columnIndex, getterGetColumn(columnIndex).getTypeInfo().getSSType().getJDBCType());
        loggerExternal.exiting(getClassNameLogging(), "getObject", value);
        return value;
    }

    public <T> T getObject(int columnIndex,
            Class<T> type) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getObject", columnIndex);
        checkClosed();
        Object returnValue;
        if (type == String.class) {
            returnValue = getString(columnIndex);
        }
        else if (type == Byte.class) {
            byte byteValue = getByte(columnIndex);
            returnValue = wasNull() ? null : byteValue;
        }
        else if (type == Short.class) {
            short shortValue = getShort(columnIndex);
            returnValue = wasNull() ? null : shortValue;
        }
        else if (type == Integer.class) {
            int intValue = getInt(columnIndex);
            returnValue = wasNull() ? null : intValue;
        }
        else if (type == Long.class) {
            long longValue = getLong(columnIndex);
            returnValue = wasNull() ? null : longValue;
        }
        else if (type == BigDecimal.class) {
            returnValue = getBigDecimal(columnIndex);
        }
        else if (type == Boolean.class) {
            boolean booleanValue = getBoolean(columnIndex);
            returnValue = wasNull() ? null : booleanValue;
        }
        else if (type == java.sql.Date.class) {
            returnValue = getDate(columnIndex);
        }
        else if (type == java.sql.Time.class) {
            returnValue = getTime(columnIndex);
        }
        else if (type == java.sql.Timestamp.class) {
            returnValue = getTimestamp(columnIndex);
        }
        else if (type == microsoft.sql.DateTimeOffset.class) {
            returnValue = getDateTimeOffset(columnIndex);
        }
        else if (type == UUID.class) {
            // read binary, avoid string allocation and parsing
            byte[] guid = getBytes(columnIndex);
            returnValue = guid != null ? Util.readGUIDtoUUID(guid) : null;
        }
        else if (type == SQLXML.class) {
            returnValue = getSQLXML(columnIndex);
        }
        else if (type == Blob.class) {
            returnValue = getBlob(columnIndex);
        }
        else if (type == Clob.class) {
            returnValue = getClob(columnIndex);
        }
        else if (type == NClob.class) {
            returnValue = getNClob(columnIndex);
        }
        else if (type == byte[].class) {
            returnValue = getBytes(columnIndex);
        }
        else if (type == Float.class) {
            float floatValue = getFloat(columnIndex);
            returnValue = wasNull() ? null : floatValue;
        }
        else if (type == Double.class) {
            double doubleValue = getDouble(columnIndex);
            returnValue = wasNull() ? null : doubleValue;
        }
        else {
            // if the type is not supported the specification says the should
            // a SQLException instead of SQLFeatureNotSupportedException
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedConversionTo"));
            Object[] msgArgs = {type};
            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET, null);
        }
        loggerExternal.exiting(getClassNameLogging(), "getObject", columnIndex);
        return type.cast(returnValue);
    }

    public Object getObject(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getObject", columnName);
        checkClosed();
        Object value = getObject(findColumn(columnName));
        loggerExternal.exiting(getClassNameLogging(), "getObject", value);
        return value;
    }

    public <T> T getObject(String columnName,
            Class<T> type) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getObject", columnName);
        checkClosed();
        T value = getObject(findColumn(columnName), type);
        loggerExternal.exiting(getClassNameLogging(), "getObject", value);
        return value;
    }

    public short getShort(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getShort", columnIndex);
        checkClosed();
        Short value = (Short) getValue(columnIndex, JDBCType.SMALLINT);
        loggerExternal.exiting(getClassNameLogging(), "getShort", value);
        return null != value ? value : 0;
    }

    public short getShort(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getShort", columnName);
        checkClosed();
        Short value = (Short) getValue(findColumn(columnName), JDBCType.SMALLINT);
        loggerExternal.exiting(getClassNameLogging(), "getShort", value);
        return null != value ? value : 0;
    }

    public String getString(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getString", columnIndex);
        checkClosed();

        String value = null;
        Object objectValue = getValue(columnIndex, JDBCType.CHAR);
        if (null != objectValue) {
            value = objectValue.toString();
        }
        loggerExternal.exiting(getClassNameLogging(), "getString", value);
        return value;
    }

    public String getString(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getString", columnName);
        checkClosed();

        String value = null;
        Object objectValue = getValue(findColumn(columnName), JDBCType.CHAR);
        if (null != objectValue) {
            value = objectValue.toString();
        }
        loggerExternal.exiting(getClassNameLogging(), "getString", value);
        return value;
    }

    public String getNString(int columnIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNString", columnIndex);
        checkClosed();
        String value = (String) getValue(columnIndex, JDBCType.NCHAR);
        loggerExternal.exiting(getClassNameLogging(), "getNString", value);
        return value;
    }

    public String getNString(String columnLabel) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNString", columnLabel);
        checkClosed();
        String value = (String) getValue(findColumn(columnLabel), JDBCType.NCHAR);
        loggerExternal.exiting(getClassNameLogging(), "getNString", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a microsoft.sql.datetimeoffset object in the Java
     * programming language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLException
     *             when an error occurs
     */
    public String getUniqueIdentifier(int columnIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getUniqueIdentifier", columnIndex);
        checkClosed();
        String value = (String) getValue(columnIndex, JDBCType.GUID);
        loggerExternal.exiting(getClassNameLogging(), "getUniqueIdentifier", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a microsoft.sql.datetimeoffset object in the Java
     * programming language.
     * 
     * @param columnLabel
     *            the name of the column
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLException
     *             when an error occurs
     */
    public String getUniqueIdentifier(String columnLabel) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getUniqueIdentifier", columnLabel);
        checkClosed();
        String value = (String) getValue(findColumn(columnLabel), JDBCType.GUID);
        loggerExternal.exiting(getClassNameLogging(), "getUniqueIdentifier", value);
        return value;
    }

    public java.sql.Time getTime(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTime", columnIndex);
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(columnIndex, JDBCType.TIME);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    public java.sql.Time getTime(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTime", columnName);
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(findColumn(columnName), JDBCType.TIME);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    public java.sql.Time getTime(int columnIndex,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTime", new Object[] {columnIndex, cal});
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(columnIndex, JDBCType.TIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    public java.sql.Time getTime(String colName,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTime", new Object[] {colName, cal});
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(findColumn(colName), JDBCType.TIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTimestamp", columnIndex);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(columnIndex, JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getTimestamp", value);
        return value;
    }

    public java.sql.Timestamp getTimestamp(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTimestamp", columnName);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(columnName), JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getTimestamp", value);
        return value;
    }

    public java.sql.Timestamp getTimestamp(int columnIndex,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTimestamp", new Object[] {columnIndex, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(columnIndex, JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTimeStamp", value);
        return value;
    }

    public java.sql.Timestamp getTimestamp(String colName,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTimestamp", new Object[] {colName, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(colName), JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTimestamp", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             when an error occurs
     */
    public java.sql.Timestamp getDateTime(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDateTime", columnIndex);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(columnIndex, JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     * 
     * @param columnName
     *            is the name of the column
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             If any errors occur.
     */
    public java.sql.Timestamp getDateTime(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDateTime", columnName);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(columnName), JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp object in the Java programming
     * language. This method uses the given calendar to construct an appropriate millisecond value for the timestamp if the underlying database does
     * not store timezone information.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param cal
     *            the java.util.Calendar object to use in constructing the dateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             If any errors occur.
     */
    public java.sql.Timestamp getDateTime(int columnIndex,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDateTime", new Object[] {columnIndex, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(columnIndex, JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp object in the Java programming
     * language. This method uses the given calendar to construct an appropriate millisecond value for the timestamp if the underlying database does
     * not store timezone information.
     * 
     * @param colName
     *            the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then the label is the name of the
     *            column
     * @param cal
     *            the java.util.Calendar object to use in constructing the dateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             If any errors occur.
     */
    public java.sql.Timestamp getDateTime(String colName,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDateTime", new Object[] {colName, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(colName), JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             when an error occurs
     */
    public java.sql.Timestamp getSmallDateTime(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", columnIndex);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(columnIndex, JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     * 
     * @param columnName
     *            is the name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             If any errors occur.
     */
    public java.sql.Timestamp getSmallDateTime(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", columnName);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(columnName), JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    /**
     * Retrieves the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp object in the Java programming
     * language.
     * 
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param cal
     *            the java.util.Calendar object to use in constructing the smalldateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             If any errors occur.
     */
    public java.sql.Timestamp getSmallDateTime(int columnIndex,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", new Object[] {columnIndex, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(columnIndex, JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    /**
     * 
     * @param colName
     *            The name of a column
     * @param cal
     *            the java.util.Calendar object to use in constructing the smalldateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             If any errors occur.
     */
    public java.sql.Timestamp getSmallDateTime(String colName,
            Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", new Object[] {colName, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(colName), JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    public microsoft.sql.DateTimeOffset getDateTimeOffset(int columnIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getDateTimeOffset", columnIndex);
        checkClosed();

        // DateTimeOffset is not supported with SQL Server versions earlier than Katmai
        if (!stmt.connection.isKatmaiOrLater())
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET,
                    null);

        microsoft.sql.DateTimeOffset value = (microsoft.sql.DateTimeOffset) getValue(columnIndex, JDBCType.DATETIMEOFFSET);
        loggerExternal.exiting(getClassNameLogging(), "getDateTimeOffset", value);
        return value;
    }

    public microsoft.sql.DateTimeOffset getDateTimeOffset(String columnName) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getDateTimeOffset", columnName);
        checkClosed();

        // DateTimeOffset is not supported with SQL Server versions earlier than Katmai
        if (!stmt.connection.isKatmaiOrLater())
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET,
                    null);

        microsoft.sql.DateTimeOffset value = (microsoft.sql.DateTimeOffset) getValue(findColumn(columnName), JDBCType.DATETIMEOFFSET);
        loggerExternal.exiting(getClassNameLogging(), "getDateTimeOffset", value);
        return value;
    }

    @Deprecated
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getUnicodeStream", columnIndex);
        NotImplemented();
        return null;
    }

    @Deprecated
    public java.io.InputStream getUnicodeStream(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getUnicodeStream", columnName);
        NotImplemented();
        return null;
    }

    public Object getObject(int i,
            java.util.Map<String, Class<?>> map) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getObject", new Object[] {i, map});
        NotImplemented();
        return null;
    }

    public Ref getRef(int i) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getRef");
        NotImplemented();
        return null;
    }

    public Blob getBlob(int i) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBlob", i);
        checkClosed();
        Blob value = (Blob) getValue(i, JDBCType.BLOB);
        loggerExternal.exiting(getClassNameLogging(), "getBlob", value);
        activeBlob = value;
        return value;
    }

    public Blob getBlob(String colName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBlob", colName);
        checkClosed();
        Blob value = (Blob) getValue(findColumn(colName), JDBCType.BLOB);
        loggerExternal.exiting(getClassNameLogging(), "getBlob", value);
        activeBlob = value;
        return value;
    }

    public Clob getClob(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getClob", columnIndex);
        checkClosed();
        Clob value = (Clob) getValue(columnIndex, JDBCType.CLOB);
        loggerExternal.exiting(getClassNameLogging(), "getClob", value);
        return value;
    }

    public Clob getClob(String colName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getClob", colName);
        checkClosed();
        Clob value = (Clob) getValue(findColumn(colName), JDBCType.CLOB);
        loggerExternal.exiting(getClassNameLogging(), "getClob", value);
        return value;
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNClob", columnIndex);
        checkClosed();
        NClob value = (NClob) getValue(columnIndex, JDBCType.NCLOB);
        loggerExternal.exiting(getClassNameLogging(), "getNClob", value);
        return value;
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNClob", columnLabel);
        checkClosed();
        NClob value = (NClob) getValue(findColumn(columnLabel), JDBCType.NCLOB);
        loggerExternal.exiting(getClassNameLogging(), "getNClob", value);
        return value;
    }

    public Array getArray(int i) throws SQLServerException {
        NotImplemented();
        return null;
    }

    public Object getObject(String colName,
            java.util.Map<String, Class<?>> map) throws SQLServerException {
        NotImplemented();
        return null;
    }

    public Ref getRef(String colName) throws SQLServerException {
        NotImplemented();
        return null;
    }

    public Array getArray(String colName) throws SQLServerException {
        NotImplemented();
        return null;
    }

    public String getCursorName() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getCursorName");
        SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_positionedUpdatesNotSupported"), null, false);
        loggerExternal.exiting(getClassNameLogging(), "getCursorName", null);
        return null;
    }

    public java.io.Reader getCharacterStream(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getCharacterStream", columnIndex);
        checkClosed();
        Reader value = (Reader) getStream(columnIndex, StreamType.CHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getCharacterStream", value);
        return value;
    }

    public java.io.Reader getCharacterStream(String columnName) throws SQLServerException {
        checkClosed();
        loggerExternal.entering(getClassNameLogging(), "getCharacterStream", columnName);
        Reader value = (Reader) getStream(findColumn(columnName), StreamType.CHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getCharacterStream", value);
        return value;
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNCharacterStream", columnIndex);
        checkClosed();
        Reader value = (Reader) getStream(columnIndex, StreamType.NCHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getNCharacterStream", value);
        return value;
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNCharacterStream", columnLabel);
        checkClosed();
        Reader value = (Reader) getStream(findColumn(columnLabel), StreamType.NCHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getNCharacterStream", value);
        return value;
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBigDecimal", columnIndex);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(columnIndex, JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBigDecimal", columnName);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(columnName), JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    /**
     * Retrieves the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnIndex
     *            The zero-based ordinal of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             when an error occurs
     */
    public BigDecimal getMoney(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMoney", columnIndex);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(columnIndex, JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getMoney", value);
        return value;
    }

    /**
     * Retrieves the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnName
     *            is the name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public BigDecimal getMoney(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMoney", columnName);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(columnName), JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getMoney", value);
        return value;
    }

    /**
     * Retrieves the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnIndex
     *            The zero-based ordinal of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *             If any errors occur.
     */
    public BigDecimal getSmallMoney(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getSmallMoney", columnIndex);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(columnIndex, JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getSmallMoney", value);
        return value;
    }

    /**
     * Retrieves the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnName
     *            is the name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public BigDecimal getSmallMoney(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getSmallMoney", columnName);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(columnName), JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getSmallMoney", value);
        return value;
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    public RowId getRowId(String columnLabel) throws SQLException {

        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getSQLXML", columnIndex);
        SQLXML xml = getSQLXMLInternal(columnIndex);
        loggerExternal.exiting(getClassNameLogging(), "getSQLXML", xml);
        return xml;
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getSQLXML", columnLabel);
        SQLXML xml = getSQLXMLInternal(findColumn(columnLabel));
        loggerExternal.exiting(getClassNameLogging(), "getSQLXML", xml);
        return xml;
    }

    public boolean rowUpdated() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "rowUpdated");
        checkClosed();
        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        // From ResultSet cursor feature spec:
        // SQL Server does not detect updated rows for any cursor type
        loggerExternal.exiting(getClassNameLogging(), "rowUpdated", false);
        return false;
    }

    public boolean rowInserted() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "rowInserted");
        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        // From ResultSet cursor feature spec:
        // SQL Server does not detect inserted rows for any cursor type
        loggerExternal.exiting(getClassNameLogging(), "rowInserted", false);
        return false;
    }

    public boolean rowDeleted() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "rowDeleted");
        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        if (isOnInsertRow || !hasCurrentRow())
            return false;

        boolean deleted = currentRowDeleted();
        loggerExternal.exiting(getClassNameLogging(), "rowDeleted", deleted);
        return deleted;
    }

    /**
     * Determines whether the current row of this result set is deleted.
     *
     * A row may be deleted via the result set cursor (via ResultSet.deleteRow) or it may have been deleted outside the cursor. This function checks
     * for both possibilities.
     */
    private boolean currentRowDeleted() throws SQLServerException {
        // Never call this function without a current row
        assert hasCurrentRow();

        // Having a current row implies we have a fetch buffer in which that row exists.
        assert null != tdsReader;

        return deletedCurrentRow || (0 != serverCursorId && TDS.ROWSTAT_FETCH_MISSING == loadColumn(columns.length).getInt(tdsReader));
    }

    /* ---------------- Column updates ---------------------- */

    /**
     * Does all the common stuff necessary when calling a getter for the column at index.
     *
     * @param index
     *            the index of the column to get
     */
    private Column updaterGetColumn(int index) throws SQLServerException {
        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        verifyValidColumnIndex(index);

        // Verify that the column is updatable (i.e. that it is not a computed column).
        if (!columns[index - 1].isUpdatable()) {
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_cantUpdateColumn"), "07009", false);
        }

        // Column values on the insert row are always updatable,
        // regardless whether this ResultSet has any current row.
        if (!isOnInsertRow) {
            // Column values can only be updated on the insert row and the current row.
            // We just determined that we're not on the insert row, so make sure
            // that this ResultSet has a current row (i.e. that the ResultSet's position
            // is not before the first row or after the last row).
            if (!hasCurrentRow()) {
                SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_resultsetNoCurrentRow"), null, true);
            }

            // A current row exists. Its column values are updatable only if the row
            // is not a deleted row ("hole").
            verifyCurrentRowIsNotDeleted("R_cantUpdateDeletedRow");
        }

        return getColumn(index);
    }

    private void updateValue(int columnIndex,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            boolean forceEncrypt) throws SQLServerException {
        updaterGetColumn(columnIndex).updateValue(jdbcType, value, javaType, null, null, null, stmt.connection, stmt.stmtColumnEncriptionSetting,
                null, forceEncrypt, columnIndex);
    }

    private void updateValue(int columnIndex,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        updaterGetColumn(columnIndex).updateValue(jdbcType, value, javaType, null, cal, null, stmt.connection, stmt.stmtColumnEncriptionSetting, null,
                forceEncrypt, columnIndex);
    }

    private void updateValue(int columnIndex,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        updaterGetColumn(columnIndex).updateValue(jdbcType, value, javaType, null, null, scale, stmt.connection, stmt.stmtColumnEncriptionSetting,
                precision, forceEncrypt, columnIndex);
    }

    private void updateStream(int columnIndex,
            StreamType streamType,
            Object value,
            JavaType javaType,
            long length) throws SQLServerException {
        updaterGetColumn(columnIndex).updateValue(streamType.getJDBCType(), value, javaType, new StreamSetterArgs(streamType, length), null, null,
                stmt.connection, stmt.stmtColumnEncriptionSetting, null, false, columnIndex);
    }

    private void updateSQLXMLInternal(int columnIndex,
            SQLXML value) throws SQLServerException {
        updaterGetColumn(columnIndex).updateValue(JDBCType.SQLXML, value, JavaType.SQLXML,
                new StreamSetterArgs(StreamType.SQLXML, DataTypes.UNKNOWN_STREAM_LENGTH), null, null, stmt.connection,
                stmt.stmtColumnEncriptionSetting, null, false, columnIndex);
    }

    public void updateNull(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "updateNull", index);

        checkClosed();
        updateValue(index, updaterGetColumn(index).getTypeInfo().getSSType().getJDBCType(), null, JavaType.OBJECT, false);

        loggerExternal.exiting(getClassNameLogging(), "updateNull");
    }

    public void updateBoolean(int index,
            boolean x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBoolean", new Object[] {index, x});
        checkClosed();
        updateValue(index, JDBCType.BIT, x, JavaType.BOOLEAN, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBoolean");
    }

    /**
     * Updates the designated column with a <code>boolean</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateBoolean(int index,
            boolean x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBoolean", new Object[] {index, x, forceEncrypt});
        checkClosed();
        updateValue(index, JDBCType.BIT, x, JavaType.BOOLEAN, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateBoolean");
    }

    public void updateByte(int index,
            byte x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateByte", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.TINYINT, x, JavaType.BYTE, false);

        loggerExternal.exiting(getClassNameLogging(), "updateByte");
    }

    /**
     * Updates the designated column with a <code>byte</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateByte(int index,
            byte x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateByte", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.TINYINT, x, JavaType.BYTE, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateByte");
    }

    public void updateShort(int index,
            short x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateShort", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.SMALLINT, x, JavaType.SHORT, false);

        loggerExternal.exiting(getClassNameLogging(), "updateShort");
    }

    /**
     * Updates the designated column with a <code>short</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateShort(int index,
            short x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateShort", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.SMALLINT, x, JavaType.SHORT, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateShort");
    }

    public void updateInt(int index,
            int x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateInt", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.INTEGER, x, JavaType.INTEGER, false);

        loggerExternal.exiting(getClassNameLogging(), "updateInt");
    }

    /**
     * Updates the designated column with an <code>int</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateInt(int index,
            int x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateInt", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.INTEGER, x, JavaType.INTEGER, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateInt");
    }

    public void updateLong(int index,
            long x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateLong", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.BIGINT, x, JavaType.LONG, false);

        loggerExternal.exiting(getClassNameLogging(), "updateLong");
    }

    /**
     * Updates the designated column with a <code>long</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateLong(int index,
            long x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateLong", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.BIGINT, x, JavaType.LONG, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateLong");
    }

    public void updateFloat(int index,
            float x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateFloat", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.REAL, x, JavaType.FLOAT, false);

        loggerExternal.exiting(getClassNameLogging(), "updateFloat");
    }

    /**
     * Updates the designated column with a <code>float</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateFloat(int index,
            float x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateFloat", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.REAL, x, JavaType.FLOAT, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateFloat");
    }

    public void updateDouble(int index,
            double x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDouble", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.DOUBLE, x, JavaType.DOUBLE, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDouble");
    }

    /**
     * Updates the designated column with a <code>double</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateDouble(int index,
            double x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDouble", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.DOUBLE, x, JavaType.DOUBLE, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDouble");
    }

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateMoney(int index,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateMoney", new Object[] {index, x});
        checkClosed();
        updateValue(index, JDBCType.MONEY, x, JavaType.BIGDECIMAL, false);

        loggerExternal.exiting(getClassNameLogging(), "updateMoney");
    }

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateMoney(int index,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateMoney", new Object[] {index, x, forceEncrypt});
        checkClosed();
        updateValue(index, JDBCType.MONEY, x, JavaType.BIGDECIMAL, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateMoney");
    }

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            is the column name
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateMoney(String columnName,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateMoney", new Object[] {columnName, x});
        checkClosed();
        updateValue(findColumn(columnName), JDBCType.MONEY, x, JavaType.BIGDECIMAL, false);

        loggerExternal.exiting(getClassNameLogging(), "updateMoney");
    }

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            the column name
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateMoney(String columnName,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateMoney", new Object[] {columnName, x, forceEncrypt});
        checkClosed();
        updateValue(findColumn(columnName), JDBCType.MONEY, x, JavaType.BIGDECIMAL, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateMoney");
    }

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods
     * are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateSmallMoney(int index,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallMoney", new Object[] {index, x});
        checkClosed();
        updateValue(index, JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, false);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallMoney");
    }

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods
     * are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateSmallMoney(int index,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallMoney", new Object[] {index, x, forceEncrypt});
        checkClosed();
        updateValue(index, JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallMoney");
    }

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods
     * are called to update the database.
     *
     * @param columnName
     *            the column name
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateSmallMoney(String columnName,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallMoney", new Object[] {columnName, x});
        checkClosed();
        updateValue(findColumn(columnName), JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, false);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallMoney");
    }

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods
     * are called to update the database.
     *
     * @param columnName
     *            the column name
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateSmallMoney(String columnName,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallMoney", new Object[] {columnName, x, forceEncrypt});
        checkClosed();
        updateValue(findColumn(columnName), JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallMoney");
    }

    public void updateBigDecimal(int index,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBigDecimal", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBigDecimal");
    }

    /**
     * Updates the designated column with a <code>java.math.BigDecimal</code> value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateBigDecimal(int index,
            BigDecimal x,
            Integer precision,
            Integer scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBigDecimal", new Object[] {index, x, scale});

        checkClosed();
        updateValue(index, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBigDecimal");
    }

    /**
     * Updates the designated column with a <code>java.math.BigDecimal</code> value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateBigDecimal(int index,
            BigDecimal x,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBigDecimal", new Object[] {index, x, scale, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateBigDecimal");
    }

    public void updateString(int columnIndex,
            String stringValue) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateString", new Object[] {columnIndex, stringValue});

        checkClosed();
        updateValue(columnIndex, JDBCType.VARCHAR, stringValue, JavaType.STRING, false);

        loggerExternal.exiting(getClassNameLogging(), "updateString");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex
     *            the first column is 1, the second is 2, ...
     * @param stringValue
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateString(int columnIndex,
            String stringValue,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateString", new Object[] {columnIndex, stringValue, forceEncrypt});

        checkClosed();
        updateValue(columnIndex, JDBCType.VARCHAR, stringValue, JavaType.STRING, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateString");
    }

    public void updateNString(int columnIndex,
            String nString) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNString", new Object[] {columnIndex, nString});

        checkClosed();
        updateValue(columnIndex, JDBCType.NVARCHAR, nString, JavaType.STRING, false);

        loggerExternal.exiting(getClassNameLogging(), "updateNString");
    }

    /**
     * Updates the designated column with a <code>String</code> value. It is intended for use when updating <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns. The updater methods are used to update column values in the current row or the insert row. The updater
     * methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnIndex
     *            the first column is 1, the second 2, ...
     * @param nString
     *            the value for the column to be updated
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateNString(int columnIndex,
            String nString,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNString", new Object[] {columnIndex, nString, forceEncrypt});

        checkClosed();
        updateValue(columnIndex, JDBCType.NVARCHAR, nString, JavaType.STRING, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateNString");
    }

    public void updateNString(String columnLabel,
            String nString) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNString", new Object[] {columnLabel, nString});

        checkClosed();
        updateValue(findColumn(columnLabel), JDBCType.NVARCHAR, nString, JavaType.STRING, false);

        loggerExternal.exiting(getClassNameLogging(), "updateNString");
    }

    /**
     * Updates the designated column with a <code>String</code> value. It is intended for use when updating <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns. The updater methods are used to update column values in the current row or the insert row. The updater
     * methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnLabel
     *            the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then the label is the name of the
     *            column
     * @param nString
     *            the value for the column to be updated
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateNString(String columnLabel,
            String nString,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNString", new Object[] {columnLabel, nString, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnLabel), JDBCType.NVARCHAR, nString, JavaType.STRING, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateNString");
    }

    public void updateBytes(int index,
            byte x[]) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBytes", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.BINARY, x, JavaType.BYTEARRAY, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBytes");
    }

    /**
     * Updates the designated column with a <code>byte</code> array value. The updater methods are used to update column values in the current row or
     * the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods
     * are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateBytes(int index,
            byte x[],
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBytes", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.BINARY, x, JavaType.BYTEARRAY, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateBytes");
    }

    public void updateDate(int index,
            java.sql.Date x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDate", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.DATE, x, JavaType.DATE, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDate");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value. The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateDate(int index,
            java.sql.Date x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDate", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.DATE, x, JavaType.DATE, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDate");
    }

    public void updateTime(int index,
            java.sql.Time x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTime", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.TIME, x, JavaType.TIME, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateTime(int index,
            java.sql.Time x,
            Integer scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTime", new Object[] {index, x, scale});

        checkClosed();
        updateValue(index, JDBCType.TIME, x, JavaType.TIME, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateTime(int index,
            java.sql.Time x,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTime", new Object[] {index, x, scale, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.TIME, x, JavaType.TIME, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateTime");
    }

    public void updateTimestamp(int index,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTimestamp", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTimestamp");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateTimestamp(int index,
            java.sql.Timestamp x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTimestamp", new Object[] {index, x, scale});

        checkClosed();
        updateValue(index, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTimestamp");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateTimestamp(int index,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTimestamp", new Object[] {index, x, scale, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateTimestamp");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateDateTime(int index,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTime", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.DATETIME, x, JavaType.TIMESTAMP, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateDateTime(int index,
            java.sql.Timestamp x,
            Integer scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTime", new Object[] {index, x, scale});

        checkClosed();
        updateValue(index, JDBCType.DATETIME, x, JavaType.TIMESTAMP, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateDateTime(int index,
            java.sql.Timestamp x,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTime", new Object[] {index, x, scale, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.DATETIME, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateSmallDateTime(int index,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallDateTime", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, false);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateSmallDateTime(int index,
            java.sql.Timestamp x,
            Integer scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallDateTime", new Object[] {index, x, scale});

        checkClosed();
        updateValue(index, JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateSmallDateTime(int index,
            java.sql.Timestamp x,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallDateTime", new Object[] {index, x, scale, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallDateTime");
    }

    public void updateDateTimeOffset(int index,
            microsoft.sql.DateTimeOffset x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTimeOffset", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTimeOffset");
    }

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a zero-based column ordinal.
     * 
     * @param index
     *            The zero-based ordinal of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @param scale
     *            scale of the column
     * @throws SQLException
     *             when an error occurs
     */
    public void updateDateTimeOffset(int index,
            microsoft.sql.DateTimeOffset x,
            Integer scale) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTimeOffset", new Object[] {index, x, scale});

        checkClosed();
        updateValue(index, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTimeOffset");
    }

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a zero-based column ordinal.
     * 
     * @param index
     *            The zero-based ordinal of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @param scale
     *            scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateDateTimeOffset(int index,
            microsoft.sql.DateTimeOffset x,
            Integer scale,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTimeOffset", new Object[] {index, x, scale, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTimeOffset");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param index
     *            The zero-based ordinal of a column.
     * @param x
     *            the new column value
     * @throws SQLException
     *             when an error occurs
     */
    public void updateUniqueIdentifier(int index,
            String x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateUniqueIdentifier", new Object[] {index, x});

        checkClosed();
        updateValue(index, JDBCType.GUID, x, JavaType.STRING, null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateUniqueIdentifier");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param index
     *            The zero-based ordinal of a column.
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateUniqueIdentifier(int index,
            String x,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateUniqueIdentifier", new Object[] {index, x, forceEncrypt});

        checkClosed();
        updateValue(index, JDBCType.GUID, x, JavaType.STRING, null, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateUniqueIdentifier");
    }

    public void updateAsciiStream(int columnIndex,
            InputStream x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateAsciiStream", new Object[] {columnIndex, x});

        checkClosed();
        updateStream(columnIndex, StreamType.ASCII, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateAsciiStream");
    }

    public void updateAsciiStream(int index,
            java.io.InputStream x,
            int length) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateAsciiStream", new Object[] {index, x, length});

        checkClosed();
        updateStream(index, StreamType.ASCII, x, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateAsciiStream");
    }

    public void updateAsciiStream(int columnIndex,
            InputStream x,
            long length) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "updateAsciiStream", new Object[] {columnIndex, x, length});

        checkClosed();
        updateStream(columnIndex, StreamType.ASCII, x, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateAsciiStream");
    }

    public void updateAsciiStream(String columnLabel,
            InputStream x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateAsciiStream", new Object[] {columnLabel, x});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.ASCII, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateAsciiStream");
    }

    public void updateAsciiStream(java.lang.String columnName,
            java.io.InputStream x,
            int length) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateAsciiStream", new Object[] {columnName, x, length});

        checkClosed();
        updateStream(findColumn(columnName), StreamType.ASCII, x, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateAsciiStream");
    }

    public void updateAsciiStream(String columnName,
            InputStream streamValue,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateAsciiStream", new Object[] {columnName, streamValue, length});

        checkClosed();
        updateStream(findColumn(columnName), StreamType.ASCII, streamValue, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateAsciiStream");
    }

    public void updateBinaryStream(int columnIndex,
            InputStream x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBinaryStream", new Object[] {columnIndex, x});

        checkClosed();
        updateStream(columnIndex, StreamType.BINARY, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateBinaryStream");
    }

    public void updateBinaryStream(int columnIndex,
            InputStream streamValue,
            int length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBinaryStream", new Object[] {columnIndex, streamValue, length});

        checkClosed();
        updateStream(columnIndex, StreamType.BINARY, streamValue, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateBinaryStream");
    }

    public void updateBinaryStream(int columnIndex,
            InputStream x,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBinaryStream", new Object[] {columnIndex, x, length});

        checkClosed();
        updateStream(columnIndex, StreamType.BINARY, x, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateBinaryStream");
    }

    public void updateBinaryStream(String columnLabel,
            InputStream x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBinaryStream", new Object[] {columnLabel, x});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.BINARY, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateBinaryStream");
    }

    public void updateBinaryStream(String columnName,
            InputStream streamValue,
            int length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBinaryStream", new Object[] {columnName, streamValue, length});

        checkClosed();
        updateStream(findColumn(columnName), StreamType.BINARY, streamValue, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateBinaryStream");
    }

    public void updateBinaryStream(String columnLabel,
            InputStream x,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBinaryStream", new Object[] {columnLabel, x, length});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.BINARY, x, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateBinaryStream");
    }

    public void updateCharacterStream(int columnIndex,
            Reader x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateCharacterStream", new Object[] {columnIndex, x});

        checkClosed();
        updateStream(columnIndex, StreamType.CHARACTER, x, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateCharacterStream");
    }

    public void updateCharacterStream(int columnIndex,
            Reader readerValue,
            int length) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateCharacterStream", new Object[] {columnIndex, readerValue, length});

        checkClosed();
        updateStream(columnIndex, StreamType.CHARACTER, readerValue, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateCharacterStream");
    }

    public void updateCharacterStream(int columnIndex,
            Reader x,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateCharacterStream", new Object[] {columnIndex, x, length});

        checkClosed();
        updateStream(columnIndex, StreamType.CHARACTER, x, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateCharacterStream");
    }

    public void updateCharacterStream(String columnLabel,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateCharacterStream", new Object[] {columnLabel, reader});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.CHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateCharacterStream");
    }

    public void updateCharacterStream(String columnName,
            Reader readerValue,
            int length) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateCharacterStream", new Object[] {columnName, readerValue, length});

        checkClosed();
        updateStream(findColumn(columnName), StreamType.CHARACTER, readerValue, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateCharacterStream");
    }

    public void updateCharacterStream(String columnLabel,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateCharacterStream", new Object[] {columnLabel, reader, length});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.CHARACTER, reader, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateNCharacterStream");
    }

    public void updateNCharacterStream(int columnIndex,
            Reader x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNCharacterStream", new Object[] {columnIndex, x});

        checkClosed();
        updateStream(columnIndex, StreamType.NCHARACTER, x, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateNCharacterStream");
    }

    public void updateNCharacterStream(int columnIndex,
            Reader x,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNCharacterStream", new Object[] {columnIndex, x, length});

        checkClosed();
        updateStream(columnIndex, StreamType.NCHARACTER, x, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateNCharacterStream");
    }

    public void updateNCharacterStream(String columnLabel,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNCharacterStream", new Object[] {columnLabel, reader});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.NCHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateNCharacterStream");
    }

    public void updateNCharacterStream(String columnLabel,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNCharacterStream", new Object[] {columnLabel, reader, length});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.NCHARACTER, reader, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateNCharacterStream");
    }

    public void updateObject(int index,
            Object obj) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {index, obj});

        checkClosed();
        updateObject(index, obj, null, null, null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateObject(int index,
            Object x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {index, x, scale});

        checkClosed();
        updateObject(index, x, scale, null, null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the {@code updateRow} or {@code insertRow} methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateObject(int index,
            Object x,
            int precision,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {index, x, scale});

        checkClosed();
        updateObject(index, x, scale, null, precision, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the {@code updateRow} or {@code insertRow} methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateObject(int index,
            Object x,
            int precision,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {index, x, scale, forceEncrypt});

        checkClosed();
        updateObject(index, x, scale, null, precision, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    protected final void updateObject(int index,
            Object x,
            Integer scale,
            JDBCType jdbcType,
            Integer precision,
            boolean forceEncrypt) throws SQLServerException {
        Column column = updaterGetColumn(index);
        SSType ssType = column.getTypeInfo().getSSType();

        if (null == x) {
            if (null == jdbcType || jdbcType.isUnsupported()) {
                // JDBCType is not specified by user or is unsupported, derive from SSType
                jdbcType = ssType.getJDBCType();
            }

            column.updateValue(jdbcType, x, JavaType.OBJECT, null, // streamSetterArgs
                    null, scale, stmt.connection, stmt.stmtColumnEncriptionSetting, precision, forceEncrypt, index);
        }
        else {
            JavaType javaType = JavaType.of(x);
            JDBCType objectJdbcType = javaType.getJDBCType(ssType, ssType.getJDBCType());

            if (null == jdbcType) {
                // JDBCType is not specified by user, derive from the object's JavaType
                jdbcType = objectJdbcType;
            }
            else {
                // Check convertibility of the value to the desired JDBC type.
                if (!objectJdbcType.convertsTo(jdbcType))
                    DataTypes.throwConversionError(objectJdbcType.toString(), jdbcType.toString());
            }

            StreamSetterArgs streamSetterArgs = null;
            switch (javaType) {
                case READER:
                    streamSetterArgs = new StreamSetterArgs(StreamType.CHARACTER, DataTypes.UNKNOWN_STREAM_LENGTH);
                    break;

                case INPUTSTREAM:
                    streamSetterArgs = new StreamSetterArgs(jdbcType.isTextual() ? StreamType.CHARACTER : StreamType.BINARY,
                            DataTypes.UNKNOWN_STREAM_LENGTH);
                    break;

                case SQLXML:
                    streamSetterArgs = new StreamSetterArgs(StreamType.SQLXML, DataTypes.UNKNOWN_STREAM_LENGTH);
                    break;

                default:
                    // Do nothing
                    break;
            }

            column.updateValue(jdbcType, x, javaType, streamSetterArgs, null, scale, stmt.connection, stmt.stmtColumnEncriptionSetting, precision,
                    forceEncrypt, index);
        }
    }

    public void updateNull(String columnName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "updateNull", columnName);

        checkClosed();
        int columnIndex = findColumn(columnName);
        updateValue(columnIndex, updaterGetColumn(columnIndex).getTypeInfo().getSSType().getJDBCType(), null, JavaType.OBJECT, false);

        loggerExternal.exiting(getClassNameLogging(), "updateNull");
    }

    public void updateBoolean(String columnName,
            boolean x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBoolean", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BIT, x, JavaType.BOOLEAN, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBoolean");
    }

    /**
     * Updates the designated column with a <code>boolean</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateBoolean(String columnName,
            boolean x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBoolean", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BIT, x, JavaType.BOOLEAN, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateBoolean");
    }

    public void updateByte(String columnName,
            byte x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateByte", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BINARY, x, JavaType.BYTE, false);

        loggerExternal.exiting(getClassNameLogging(), "updateByte");
    }

    /**
     * Updates the designated column with a <code>byte</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     *
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateByte(String columnName,
            byte x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateByte", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BINARY, x, JavaType.BYTE, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateByte");
    }

    public void updateShort(String columnName,
            short x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateShort", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.SMALLINT, x, JavaType.SHORT, false);

        loggerExternal.exiting(getClassNameLogging(), "updateShort");
    }

    /**
     * Updates the designated column with a <code>short</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateShort(String columnName,
            short x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateShort", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.SMALLINT, x, JavaType.SHORT, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateShort");
    }

    public void updateInt(String columnName,
            int x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateInt", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.INTEGER, x, JavaType.INTEGER, false);

        loggerExternal.exiting(getClassNameLogging(), "updateInt");
    }

    /**
     * Updates the designated column with an <code>int</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateInt(String columnName,
            int x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateInt", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.INTEGER, x, JavaType.INTEGER, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateInt");
    }

    public void updateLong(String columnName,
            long x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateLong", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BIGINT, x, JavaType.LONG, false);

        loggerExternal.exiting(getClassNameLogging(), "updateLong");
    }

    /**
     * Updates the designated column with a <code>long</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateLong(String columnName,
            long x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateLong", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BIGINT, x, JavaType.LONG, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateLong");
    }

    public void updateFloat(String columnName,
            float x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateFloat", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.REAL, x, JavaType.FLOAT, false);

        loggerExternal.exiting(getClassNameLogging(), "updateFloat");
    }

    /**
     * Updates the designated column with a <code>float </code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateFloat(String columnName,
            float x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateFloat", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.REAL, x, JavaType.FLOAT, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateFloat");
    }

    public void updateDouble(String columnName,
            double x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDouble", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DOUBLE, x, JavaType.DOUBLE, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDouble");
    }

    /**
     * Updates the designated column with a <code>double</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateDouble(String columnName,
            double x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDouble", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DOUBLE, x, JavaType.DOUBLE, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDouble");
    }

    public void updateBigDecimal(String columnName,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBigDecimal", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBigDecimal");
    }

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code> value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateBigDecimal(String columnName,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBigDecimal", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateBigDecimal");
    }

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code> value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column and Always Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set
     *            to false, the driver will not force encryption on parameters.
     * @param x
     *            BigDecimal value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateBigDecimal(String columnName,
            BigDecimal x,
            Integer precision,
            Integer scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBigDecimal", new Object[] {columnName, x, precision, scale});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBigDecimal");
    }

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code> value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column and Always Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set
     *            to false, the driver will not force encryption on parameters.
     * @param x
     *            BigDecimal value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateBigDecimal(String columnName,
            BigDecimal x,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBigDecimal", new Object[] {columnName, x, precision, scale, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateBigDecimal");
    }

    public void updateString(String columnName,
            String x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateString", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.VARCHAR, x, JavaType.STRING, false);

        loggerExternal.exiting(getClassNameLogging(), "updateString");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateString(String columnName,
            String x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateString", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.VARCHAR, x, JavaType.STRING, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateString");
    }

    public void updateBytes(String columnName,
            byte x[]) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBytes", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BINARY, x, JavaType.BYTEARRAY, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBytes");
    }

    /**
     * Updates the designated column with a byte array value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateBytes(String columnName,
            byte x[],
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBytes", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BINARY, x, JavaType.BYTEARRAY, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateBytes");
    }

    public void updateDate(String columnName,
            java.sql.Date x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDate", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATE, x, JavaType.DATE, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDate");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value. The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateDate(String columnName,
            java.sql.Date x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDate", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATE, x, JavaType.DATE, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDate");
    }

    public void updateTime(String columnName,
            java.sql.Time x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTime", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.TIME, x, JavaType.TIME, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateTime(String columnName,
            java.sql.Time x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTime", new Object[] {columnName, x, scale});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.TIME, x, JavaType.TIME, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateTime(String columnName,
            java.sql.Time x,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTime", new Object[] {columnName, x, scale, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.TIME, x, JavaType.TIME, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateTime");
    }

    public void updateTimestamp(String columnName,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTimestamp", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTimestamp");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateTimestamp(String columnName,
            java.sql.Timestamp x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTimestamp", new Object[] {columnName, x, scale});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateTimestamp");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateTimestamp(String columnName,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateTimestamp", new Object[] {columnName, x, scale, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateTimestamp");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateDateTime(String columnName,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTime", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATETIME, x, JavaType.TIMESTAMP, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateDateTime(String columnName,
            java.sql.Timestamp x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTime", new Object[] {columnName, x, scale});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATETIME, x, JavaType.TIMESTAMP, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateDateTime(String columnName,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTime", new Object[] {columnName, x, scale, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATETIME, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateSmallDateTime(String columnName,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallDateTime", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, false);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateSmallDateTime(String columnName,
            java.sql.Timestamp x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallDateTime", new Object[] {columnName, x, scale});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallDateTime");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to update column values in the current
     * row or the insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code>
     * methods are called to update the database.
     *
     * @param columnName
     *            is the name of the column
     * @param x
     *            the new column value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateSmallDateTime(String columnName,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSmallDateTime", new Object[] {columnName, x, scale, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateSmallDateTime");
    }

    public void updateDateTimeOffset(String columnName,
            microsoft.sql.DateTimeOffset x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTimeOffset", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTimeOffset");
    }

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a column name.
     * 
     * @param columnName
     *            The name of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateDateTimeOffset(String columnName,
            microsoft.sql.DateTimeOffset x,
            int scale) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTimeOffset", new Object[] {columnName, x, scale});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, false);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTimeOffset");
    }

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a column name.
     * 
     * @param columnName
     *            The name of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             If any errors occur.
     */
    public void updateDateTimeOffset(String columnName,
            microsoft.sql.DateTimeOffset x,
            int scale,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateDateTimeOffset", new Object[] {columnName, x, scale, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateDateTimeOffset");
    }

    /**
     * Updates the designated column with a <code>String</code>value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnName
     *            The name of a column.
     * @param x
     *            the new column value
     * @throws SQLException
     *             If any errors occur.
     */
    public void updateUniqueIdentifier(String columnName,
            String x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateUniqueIdentifier", new Object[] {columnName, x});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.GUID, x, JavaType.STRING, null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateUniqueIdentifier");
    }

    /**
     * Updates the designated column with a <code>String</code>value. The updater methods are used to update column values in the current row or the
     * insert row. The updater methods do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * 
     * @param columnName
     *            The name of a column.
     * @param x
     *            the new column value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             If any errors occur.
     */
    public void updateUniqueIdentifier(String columnName,
            String x,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateUniqueIdentifier", new Object[] {columnName, x, forceEncrypt});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.GUID, x, JavaType.STRING, null, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateUniqueIdentifier");
    }

    public void updateObject(String columnName,
            Object x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {columnName, x, scale});

        checkClosed();
        updateObject(findColumn(columnName), x, scale, null, null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the {@code updateRow} or {@code insertRow} methods are called to update the database.
     *
     * @param columnName
     *            The name of a column.
     * @param x
     *            the new column value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateObject(String columnName,
            Object x,
            int precision,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {columnName, x, precision, scale});

        checkClosed();
        updateObject(findColumn(columnName), x, scale, null, precision, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the {@code updateRow} or {@code insertRow} methods are called to update the database.
     *
     * @param columnName
     *            The name of a column.
     * @param x
     *            the new column value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateObject(String columnName,
            Object x,
            int precision,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {columnName, x, precision, scale, forceEncrypt});

        checkClosed();
        updateObject(findColumn(columnName), x, scale, null, precision, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateObject(String columnName,
            Object x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {columnName, x});

        checkClosed();
        updateObject(findColumn(columnName), x, null, null, null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateRowId(int columnIndex,
            RowId x) throws SQLException {
        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    public void updateRowId(String columnLabel,
            RowId x) throws SQLException {
        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    public void updateSQLXML(int columnIndex,
            SQLXML xmlObject) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSQLXML", new Object[] {columnIndex, xmlObject});
        updateSQLXMLInternal(columnIndex, xmlObject);
        loggerExternal.exiting(getClassNameLogging(), "updateSQLXML");
    }

    public void updateSQLXML(String columnLabel,
            SQLXML x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateSQLXML", new Object[] {columnLabel, x});
        updateSQLXMLInternal(findColumn(columnLabel), x);
        loggerExternal.exiting(getClassNameLogging(), "updateSQLXML");
    }

    public int getHoldability() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getHoldability");

        checkClosed();

        int holdability =

                // Client-cursored result sets are always holdable because there is
                // no server cursor for the server to close, and no way for the driver
                // to detect the commit anyway...
                (0 == stmt.getServerCursorId()) ? ResultSet.HOLD_CURSORS_OVER_COMMIT :

                // For Yukon and later server-cursored result sets, holdability
                // was determined at statement execution time and does not change.
                        stmt.getExecProps().getHoldability();

        loggerExternal.exiting(getClassNameLogging(), "getHoldability", holdability);

        return holdability;
    }

    /* ----------------------- Update result set ------------------------- */

    public void insertRow() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "insertRow");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        final class InsertRowRPC extends TDSCommand {
            final String tableName;

            InsertRowRPC(String tableName) {
                super("InsertRowRPC", 0);
                this.tableName = tableName;
            }

            final boolean doExecute() throws SQLServerException {
                doInsertRowRPC(this, tableName);
                return true;
            }
        }

        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        if (!isOnInsertRow) {
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_mustBeOnInsertRow"), null, true);
        }

        // Determine the table/view into which the row is to be inserted.
        //
        // The logic for doing this is as follows:
        //
        // If values were set for any of the columns in this ResultSet
        // then use the table associated with the first column in the
        // SELECT list whose value was set.
        //
        // If no values were set for any of the columns (insert row with
        // default values) then choose the table name associated with the
        // first updatable column. An updatable column is one that is not
        // a computed expression, not hidden, and has a non-empty table name.
        //
        // If no values were set for any columns and no columns are updatable,
        // then the table name cannot be determined, so error.
        Column tableColumn = null;
        for (Column column : columns) {
            if (column.hasUpdates()) {
                tableColumn = column;
                break;
            }

            if (null == tableColumn && column.isUpdatable())
                tableColumn = column;
        }

        if (null == tableColumn) {
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_noColumnParameterValue"), null, true);
        }

        assert tableColumn.isUpdatable();
        assert null != tableColumn.getTableName();

        stmt.executeCommand(new InsertRowRPC(tableColumn.getTableName().asEscapedString()));

        if (UNKNOWN_ROW_COUNT != rowCount)
            ++rowCount;
        loggerExternal.exiting(getClassNameLogging(), "insertRow");
    }

    private void doInsertRowRPC(TDSCommand command,
            String tableName) throws SQLServerException {
        assert 0 != serverCursorId;
        assert null != tableName;
        assert tableName.length() > 0;

        TDSWriter tdsWriter = command.startRequest(TDS.PKT_RPC);
        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSOR);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2
        tdsWriter.writeRPCInt(null, serverCursorId, false);
        tdsWriter.writeRPCInt(null, (int) TDS.SP_CURSOR_OP_INSERT, false);
        tdsWriter.writeRPCInt(null, fetchBufferGetRow(), false);

        if (hasUpdatedColumns()) {
            tdsWriter.writeRPCStringUnicode(tableName);

            for (Column column : columns) column.sendByRPC(tdsWriter, stmt.connection);
        }
        else {
            tdsWriter.writeRPCStringUnicode("");
            tdsWriter.writeRPCStringUnicode("INSERT INTO " + tableName + " DEFAULT VALUES");
        }

        TDSParser.parse(command.startResponse(), command.getLogContext());
    }

    public void updateRow() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "updateRow");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        final class UpdateRowRPC extends TDSCommand {
            UpdateRowRPC() {
                super("UpdateRowRPC", 0);
            }

            final boolean doExecute() throws SQLServerException {
                doUpdateRowRPC(this);
                return true;
            }
        }

        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        // From JDBC spec:
        // [updateRow] must be called when the cursor is on the current row;
        // an exception will be thrown if it is called with the cursor is on the insert row.
        verifyResultSetIsNotOnInsertRow();
        verifyResultSetHasCurrentRow();

        // Deleted rows cannot be updated.
        verifyCurrentRowIsNotDeleted("R_cantUpdateDeletedRow");

        if (!hasUpdatedColumns()) {
            SQLServerException.makeFromDriverError(stmt.connection, stmt, SQLServerException.getErrString("R_noColumnParameterValue"), null, true);
        }

        try {
            stmt.executeCommand(new UpdateRowRPC());
        }
        finally {
            cancelUpdates();
        }

        updatedCurrentRow = true;
        loggerExternal.exiting(getClassNameLogging(), "updateRow");
    }

    private void doUpdateRowRPC(TDSCommand command) throws SQLServerException {
        assert 0 != serverCursorId;

        TDSWriter tdsWriter = command.startRequest(TDS.PKT_RPC);
        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSOR);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2
        tdsWriter.writeRPCInt(null, serverCursorId, false);
        tdsWriter.writeRPCInt(null, TDS.SP_CURSOR_OP_UPDATE | TDS.SP_CURSOR_OP_SETPOSITION, false);
        tdsWriter.writeRPCInt(null, fetchBufferGetRow(), false);
        tdsWriter.writeRPCStringUnicode("");

        assert hasUpdatedColumns();

        for (Column column : columns) column.sendByRPC(tdsWriter, stmt.connection);

        TDSParser.parse(command.startResponse(), command.getLogContext());
    }

    /** Determines whether there are updated columns in this result set. */
    final boolean hasUpdatedColumns() {
        for (Column column : columns)
            if (column.hasUpdates())
                return true;

        return false;
    }

    public void deleteRow() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "deleteRow");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        final class DeleteRowRPC extends TDSCommand {
            DeleteRowRPC() {
                super("DeleteRowRPC", 0);
            }

            final boolean doExecute() throws SQLServerException {
                doDeleteRowRPC(this);
                return true;
            }
        }

        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if this method is called on a ResultSet object that is not updatable ...
        verifyResultSetIsUpdatable();

        // ... or when the cursor is before the first row, after the last row, or on the insert row.
        verifyResultSetIsNotOnInsertRow();
        verifyResultSetHasCurrentRow();

        // Deleted rows cannot be deleted.
        verifyCurrentRowIsNotDeleted("R_cantUpdateDeletedRow");

        try {
            stmt.executeCommand(new DeleteRowRPC());
        }
        finally {
            cancelUpdates();
        }

        deletedCurrentRow = true;
        loggerExternal.exiting(getClassNameLogging(), "deleteRow");
    }

    private void doDeleteRowRPC(TDSCommand command) throws SQLServerException {
        assert 0 != serverCursorId;

        TDSWriter tdsWriter = command.startRequest(TDS.PKT_RPC);
        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSOR);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2
        tdsWriter.writeRPCInt(null, serverCursorId, false);
        tdsWriter.writeRPCInt(null, TDS.SP_CURSOR_OP_DELETE | TDS.SP_CURSOR_OP_SETPOSITION, false);
        tdsWriter.writeRPCInt(null, fetchBufferGetRow(), false);
        tdsWriter.writeRPCStringUnicode("");

        TDSParser.parse(command.startResponse(), command.getLogContext());
    }

    public void refreshRow() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "refreshRow");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // This method is not supported for ResultSet objects that are type TYPE_FORWARD_ONLY.
        verifyResultSetIsScrollable();

        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        // From JDBC spec:
        // Throws SQLException if this method is called when the cursor is on the insert row.
        verifyResultSetIsNotOnInsertRow();

        // Verify that the cursor is not before the first row, after the last row, or on a deleted row.
        verifyResultSetHasCurrentRow();
        verifyCurrentRowIsNotDeleted("R_cantUpdateDeletedRow");

        // From the JDBC spec:
        // [This method] does nothing for [ResultSet objects] that are type TYPE_SCROLL_INSENSITIVE.
        //
        // Included in that definition is result sets for which the server was asked to return a server cursor,
        // but ended up returning the results directly instead.
        if (ResultSet.TYPE_SCROLL_INSENSITIVE == stmt.getResultSetType() || 0 == serverCursorId)
            return;

        // From JDBC spec:
        // If refreshRow is called after calling updater [methods] but before calling updateRow,
        // then the updates made to the row are lost.
        cancelUpdates();

        doRefreshRow();
        loggerExternal.exiting(getClassNameLogging(), "refreshRow");
    }

    private void doRefreshRow() throws SQLServerException {
        assert hasCurrentRow();

        // Save off the current row offset into the fetch buffer so that we can attempt to
        // restore to that position after refetching.
        int fetchBufferSavedRow = fetchBufferGetRow();

        // Refresh all the rows in the fetch buffer. This is allowed by the JDBC spec:
        // If the fetch size is greater than one, the driver may refetch multiple rows at once.
        doServerFetch(TDS.FETCH_REFRESH, 0, 0);

        // Scroll back to the current row. Note that with DYNAMIC cursors, there is no guarantee
        // that the contents of the fetch buffer after a refresh look anything like they did before
        // the refresh -- rows may have been added, modified, or deleted -- so the current row
        // may not end up where it was before the refresh.
        int fetchBufferRestoredRow = 0;
        while (fetchBufferRestoredRow < fetchBufferSavedRow && (isForwardOnly() ? fetchBufferNext() : scrollWindow.next(this))) {
            ++fetchBufferRestoredRow;
        }

        if (fetchBufferRestoredRow < fetchBufferSavedRow) {
            currentRow = AFTER_LAST_ROW;
            return;
        }

        // Finally, ensure that we've cleared the flag that the current row was updated. This ensures
        // that a subsequent getter doesn't do another unnecessary refresh when called.
        updatedCurrentRow = false;
    }

    private void cancelUpdates() {
        if (!isOnInsertRow)
            clearColumnsValues();
    }

    public void cancelRowUpdates() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "cancelRowUpdates");
        checkClosed();

        // From JDBC spec:
        // Throws SQLException if this method is called when the cursor is on the insert row or if
        // this ResultSet object has a concurrency of CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();
        verifyResultSetIsNotOnInsertRow();

        cancelUpdates();
        loggerExternal.exiting(getClassNameLogging(), "cancelRowUpdates");
    }

    public void moveToInsertRow() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "moveToInsertRow");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        cancelUpdates();
        isOnInsertRow = true;
        loggerExternal.exiting(getClassNameLogging(), "moveToInsertRow");
    }

    public void moveToCurrentRow() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "moveToCurrentRow");
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + logCursorState());

        checkClosed();

        // From JDBC spec:
        // Throws SQLException if the concurrency of this ResultSet object is CONCUR_READ_ONLY.
        verifyResultSetIsUpdatable();

        // Note that we don't verify that the row is on the insert row. From the JDBC spec:
        // This method should be called only when the cursor is on the insert row
        // and has no effect if the cursor is not on the insert row.

        cancelInsert();
        loggerExternal.exiting(getClassNameLogging(), "moveToCurrentRow");

    }

    /* L0 */ public java.sql.Statement getStatement() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getStatement");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getStatement", stmt);
        return stmt;
    }

    /* JDBC 3.0 */

    public void updateClob(int columnIndex,
            Clob clobValue) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateClob", new Object[] {columnIndex, clobValue});

        checkClosed();
        updateValue(columnIndex, JDBCType.CLOB, clobValue, JavaType.CLOB, false);

        loggerExternal.exiting(getClassNameLogging(), "updateClob");
    }

    public void updateClob(int columnIndex,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateClob", new Object[] {columnIndex, reader});

        checkClosed();
        updateStream(columnIndex, StreamType.CHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateClob");
    }

    public void updateClob(int columnIndex,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateClob", new Object[] {columnIndex, reader, length});

        checkClosed();
        updateStream(columnIndex, StreamType.CHARACTER, reader, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateClob");
    }

    public void updateClob(String columnName,
            Clob clobValue) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateClob", new Object[] {columnName, clobValue});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.CLOB, clobValue, JavaType.CLOB, false);

        loggerExternal.exiting(getClassNameLogging(), "updateClob");
    }

    public void updateClob(String columnLabel,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateClob", new Object[] {columnLabel, reader});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.CHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateClob");
    }

    public void updateClob(String columnLabel,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateClob", new Object[] {columnLabel, reader, length});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.CHARACTER, reader, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateClob");
    }

    public void updateNClob(int columnIndex,
            NClob nClob) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateClob", new Object[] {columnIndex, nClob});

        checkClosed();
        updateValue(columnIndex, JDBCType.NCLOB, nClob, JavaType.NCLOB, false);

        loggerExternal.exiting(getClassNameLogging(), "updateNClob");
    }

    public void updateNClob(int columnIndex,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNClob", new Object[] {columnIndex, reader});

        checkClosed();
        updateStream(columnIndex, StreamType.NCHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateNClob");
    }

    public void updateNClob(int columnIndex,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNClob", new Object[] {columnIndex, reader, length});

        checkClosed();
        updateStream(columnIndex, StreamType.NCHARACTER, reader, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateNClob");
    }

    public void updateNClob(String columnLabel,
            NClob nClob) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNClob", new Object[] {columnLabel, nClob});

        checkClosed();
        updateValue(findColumn(columnLabel), JDBCType.NCLOB, nClob, JavaType.NCLOB, false);

        loggerExternal.exiting(getClassNameLogging(), "updateNClob");
    }

    public void updateNClob(String columnLabel,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNClob", new Object[] {columnLabel, reader});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.NCHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateNClob");
    }

    public void updateNClob(String columnLabel,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateNClob", new Object[] {columnLabel, reader, length});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.NCHARACTER, reader, JavaType.READER, length);

        loggerExternal.exiting(getClassNameLogging(), "updateNClob");
    }

    public void updateBlob(int columnIndex,
            Blob blobValue) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBlob", new Object[] {columnIndex, blobValue});

        checkClosed();
        updateValue(columnIndex, JDBCType.BLOB, blobValue, JavaType.BLOB, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBlob");
    }

    public void updateBlob(int columnIndex,
            InputStream inputStream) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBlob", new Object[] {columnIndex, inputStream});

        checkClosed();
        updateStream(columnIndex, StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateBlob");
    }

    public void updateBlob(int columnIndex,
            InputStream inputStream,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBlob", new Object[] {columnIndex, inputStream, length});

        checkClosed();
        updateStream(columnIndex, StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateBlob");
    }

    public void updateBlob(String columnName,
            Blob blobValue) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBlob", new Object[] {columnName, blobValue});

        checkClosed();
        updateValue(findColumn(columnName), JDBCType.BLOB, blobValue, JavaType.BLOB, false);

        loggerExternal.exiting(getClassNameLogging(), "updateBlob");
    }

    public void updateBlob(String columnLabel,
            InputStream inputStream) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBlob", new Object[] {columnLabel, inputStream});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);

        loggerExternal.exiting(getClassNameLogging(), "updateBlob");
    }

    public void updateBlob(String columnLabel,
            InputStream inputStream,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateBlob", new Object[] {columnLabel, inputStream, length});

        checkClosed();
        updateStream(findColumn(columnLabel), StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, length);

        loggerExternal.exiting(getClassNameLogging(), "updateBlob");
    }

    /* L3 */ public void updateArray(int columnIndex,
            Array x) throws SQLServerException {
        stmt.NotImplemented();
    }

    /* L3 */ public void updateArray(java.lang.String columnName,
            Array x) throws SQLServerException {
        stmt.NotImplemented();
    }

    /* L3 */ public void updateRef(int columnIndex,
            Ref x) throws SQLServerException {
        stmt.NotImplemented();
    }

    /* L3 */ public void updateRef(java.lang.String columnName,
            Ref x) throws SQLServerException {
        stmt.NotImplemented();
    }

    /* L3 */ public java.net.URL getURL(int columnIndex) throws SQLServerException {
        stmt.NotImplemented();
        return null;
    }

    /* L3 */ public java.net.URL getURL(String sColumn) throws SQLServerException {
        stmt.NotImplemented();
        return null;
    }

    /** Absolute position in this ResultSet where the fetch buffer starts */
    private int numFetchedRows;

    /**
     * Fetch buffer that provides a source of rows to this ResultSet.
     *
     * The FetchBuffer class is different conceptually from the ScrollWindow class. The latter provides indexing and arbitrary scrolling over rows
     * provided by the fetch buffer. The fetch buffer itself just provides the rows and supports rewinding back to the start of the buffer. When
     * server cursors are involved, the fetch buffer is typically the same size as the scroll window. However, with client side scrollable cursors,
     * the fetch buffer would contain all of the rows in the result set, but the scroll window would only contain some of them (determined by
     * ResultSet.setFetchSize).
     *
     * The fetch buffer contains 0 or more ROW tokens followed by a DONE (cmd=SELECT, 0xC1) token indicating the number of rows in the fetch buffer.
     *
     * For client-cursored result sets, the fetch buffer contains all of the rows in the result set, and the DONE token indicates the total number of
     * rows in the result set, though we don't use that information, as we always count rows as we encounter them.
     */
    private final class FetchBuffer {
        private final class FetchBufferTokenHandler extends TDSTokenHandler {
            FetchBufferTokenHandler() {
                super("FetchBufferTokenHandler");
            }

            // Even though the cursor fetch RPC call specified the "no metadata" option,
            // the server still returns a COLMETADATA_TOKEN containing the magic NoMetaData
            // value that we need to read through.
            boolean onColMetaData(TDSReader tdsReader) throws SQLServerException {
                (new StreamColumns(Util.shouldHonorAEForRead(stmt.stmtColumnEncriptionSetting, stmt.connection))).setFromTDS(tdsReader);
                return true;
            }

            boolean onRow(TDSReader tdsReader) throws SQLServerException {
                ensureStartMark();

                // Consume the ROW token, leaving tdsReader at the start of
                // this row's column values.
                if (TDS.TDS_ROW != tdsReader.readUnsignedByte())
                    assert false;
                fetchBufferCurrentRowType = RowType.ROW;
                return false;
            }

            boolean onNBCRow(TDSReader tdsReader) throws SQLServerException {
                ensureStartMark();

                // Consume the NBCROW token, leaving tdsReader at the start of
                // nullbitmap.
                if (TDS.TDS_NBCROW != tdsReader.readUnsignedByte())
                    assert false;

                fetchBufferCurrentRowType = RowType.NBCROW;
                return false;
            }

            boolean onDone(TDSReader tdsReader) throws SQLServerException {
                ensureStartMark();

                int token = tdsReader.peekTokenType();
                StreamDone doneToken = new StreamDone();
                doneToken.setFromTDS(tdsReader);
        		
                int packetType = tdsReader.peekTokenType();
                if (-1 != packetType && TDS.TDS_DONEINPROC == token) {
                	switch (packetType) {
	                	case TDS.TDS_ENV_CHG:
	                	case TDS.TDS_ERR:
							return true;
						default:
							break;
                	}
                }

                // Done with all the rows in this fetch buffer and done with parsing
                // unless it's a server cursor, in which case there is a RETSTAT and
                // another DONE token to follow.
                done = true;
                return 0 != serverCursorId;
            }

            boolean onRetStatus(TDSReader tdsReader) throws SQLServerException {
                // Check the return status for the bit indicating that
                // "counter-intuitive" cursor behavior has happened and
                // that a fixup is necessary.
                StreamRetStatus retStatusToken = new StreamRetStatus();
                retStatusToken.setFromTDS(tdsReader);
                needsServerCursorFixup = (2 == retStatusToken.getStatus());
                return true;
            }

            void onEOF(TDSReader tdsReader) throws SQLServerException {
                super.onEOF(tdsReader);
                done = true;
            }
        }

        private final FetchBufferTokenHandler fetchBufferTokenHandler = new FetchBufferTokenHandler();

        /** Location in the TDS response of the start of the fetch buffer */
        private TDSReaderMark startMark;

        final void clearStartMark() {
            startMark = null;
        }

        private RowType fetchBufferCurrentRowType = RowType.UNKNOWN;
        private boolean done;
        private boolean needsServerCursorFixup;

        final boolean needsServerCursorFixup() {
            return needsServerCursorFixup;
        }

        FetchBuffer() {
            init();
        }

        final void ensureStartMark() {
            if (null == startMark && !isForwardOnly()) {
                if (logger.isLoggable(java.util.logging.Level.FINEST))
                    logger.finest(toString() + " Setting fetch buffer start mark");

                startMark = tdsReader.mark();
            }
        }

        /**
         * Repositions the fetch buffer back to the beginning.
         */
        final void reset() {
            assert null != tdsReader;
            assert null != startMark;

            tdsReader.reset(startMark);
            fetchBufferCurrentRowType = RowType.UNKNOWN;
            done = false;
        }

        /**
         * Initializes the fetch buffer with new contents and optionally sets a TDSReaderMark at the start of the fetch buffer to allow the fetch
         * buffer to be scrolled back to the beginning.
         */
        final void init() {
            startMark = (0 == serverCursorId && !isForwardOnly()) ? tdsReader.mark() : null;
            fetchBufferCurrentRowType = RowType.UNKNOWN;
            done = false;
            needsServerCursorFixup = false;
        }

        /**
         * Moves to the next row in the fetch buffer.
         */
        final RowType nextRow() throws SQLServerException {
            fetchBufferCurrentRowType = RowType.UNKNOWN;

            while (null != tdsReader && !done && fetchBufferCurrentRowType.equals(RowType.UNKNOWN))
                TDSParser.parse(tdsReader, fetchBufferTokenHandler);

            if (fetchBufferCurrentRowType.equals(RowType.UNKNOWN) && null != fetchBufferTokenHandler.getDatabaseError()) {
                SQLServerException.makeFromDatabaseError(stmt.connection, null, fetchBufferTokenHandler.getDatabaseError().getMessage(),
                        fetchBufferTokenHandler.getDatabaseError(), false);
            }

            return fetchBufferCurrentRowType;
        }
    }

    private final class CursorFetchCommand extends TDSCommand {
        private final int serverCursorId;
        private int fetchType;
        private int startRow;
        private int numRows;

        CursorFetchCommand(int serverCursorId,
                int fetchType,
                int startRow,
                int numRows) {
            super("doServerFetch", stmt.queryTimeout);
            this.serverCursorId = serverCursorId;
            this.fetchType = fetchType;
            this.startRow = startRow;
            this.numRows = numRows;
        }

        final boolean doExecute() throws SQLServerException {
            TDSWriter tdsWriter = startRequest(TDS.PKT_RPC);
            tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
            tdsWriter.writeShort(TDS.PROCID_SP_CURSORFETCH);
            tdsWriter.writeByte(TDS.RPC_OPTION_NO_METADATA);
            tdsWriter.writeByte((byte) 0);  // RPC procedure option 2
            tdsWriter.writeRPCInt(null, serverCursorId, false);
            tdsWriter.writeRPCInt(null, fetchType, false);
            tdsWriter.writeRPCInt(null, startRow, false);
            tdsWriter.writeRPCInt(null, numRows, false);

            // To free up the thread on the server that is feeding us these results,
            // read the entire response off the wire UNLESS this is a forward only
            // updatable result set AND responseBuffering was explicitly set to adaptive
            // at the Statement level. This override is the only way that apps
            // can do a forward only updatable pass through a ResultSet with large
            // data values.
            tdsReader = startResponse(isForwardOnly() && CONCUR_READ_ONLY != stmt.resultSetConcurrency
                    && stmt.getExecProps().wasResponseBufferingSet() && stmt.getExecProps().isResponseBufferingAdaptive());

            return false;
        }

        final void processResponse(TDSReader responseTDSReader) throws SQLServerException {
            tdsReader = responseTDSReader;
            discardFetchBuffer();
        }
    }

    /**
     * Position a server side cursor.
     *
     * @param fetchType
     *            The type of fetch
     * @param startRow
     *            The starting row
     * @param numRows
     *            The number of rows to fetch
     * @exception SQLServerException
     *                The cursor was invalid.
     */
    final void doServerFetch(int fetchType,
            int startRow,
            int numRows) throws SQLServerException {
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + " fetchType:" + fetchType + " startRow:" + startRow + " numRows:" + numRows);

        // Discard the current fetch buffer contents
        discardFetchBuffer();

        // Reinitialize the fetch buffer
        fetchBuffer.init();

        // Fetch the requested block of rows from the server
        CursorFetchCommand cursorFetch = new CursorFetchCommand(serverCursorId, fetchType, startRow, numRows);
        stmt.executeCommand(cursorFetch);

        numFetchedRows = 0;
        resultSetCurrentRowType = RowType.UNKNOWN;
        areNullCompressedColumnsInitialized = false;
        lastColumnIndex = 0;

        // If necessary, resize the scroll window to the new fetch size
        if (null != scrollWindow && TDS.FETCH_REFRESH != fetchType)
            scrollWindow.resize(fetchSize);

        // Correct for SQL Server's "counter-intuitive" behavior which positions the cursor
        // on the first row of the result set when a negative move would have logically
        // positioned the cursor before the first row instead. When this happens, the server
        // returns the first block of rows, which is indistinguishable from a regular request
        // for the first block of rows except for a flag set in the return status to indicate
        // what happened. When this behavior does happen, force the cursor to move to before
        // the first row. See the Engine Cursors Functional Specification for all the details....
        if (numRows < 0 || startRow < 0) {
            // Scroll past all the returned rows, caching in the scroll window as we go.
            try {
                while (scrollWindow.next(this))
                    ;
            }
            catch (SQLException e) {
                // If there is a row error in the results, don't throw an exception from here.
                // Ignore it for now and defer the exception until the app encounters the
                // error through normal cursor movement.
                if (logger.isLoggable(java.util.logging.Level.FINER))
                    logger.finer(toString() + " Ignored exception from row error during server cursor fixup: " + e.getMessage());
            }

            // Force the cursor to move to before the first row if necessary.
            if (fetchBuffer.needsServerCursorFixup()) {
                doServerFetch(TDS.FETCH_FIRST, 0, 0);
                return;
            }

            // Put the scroll window back before the first row.
            scrollWindow.reset();
        }
    }
    
    /*
     * Iterates through the list of objects which rely on the stream that's about to be closed, filling them with their data
     * Will skip over closed blobs, implemented in SQLServerBlob
     */
    private void fillBlobs() {
    	if (null != activeBlob && activeBlob instanceof SQLServerBlob) {
    		try {
    			((SQLServerBlob)activeBlob).fillByteArray();
    		} catch (SQLException e) {
    			if (logger.isLoggable(java.util.logging.Level.FINER)) {
    				logger.finer(toString() + "Filling blobs before closing: " + e.getMessage());
    			}
    		} finally {
    			activeBlob = null;
    		}
    	}
	}

    /**
     * Discards the contents of the current fetch buffer.
     *
     * This method ensures that the contents of the current fetch buffer have been completely read from the TDS channel, processed, and discarded.
     *
     * Note that exceptions resulting from database errors, such as row errors or transaction rollbacks, and from I/O errors, such as a closed
     * connection, are caught, logged, and ignored. The expectation is that callers of this method just want the fetch buffer cleared out and do not
     * care about what errors may have occurred when it was last populated. If the connection is closed while discarding the fetch buffer, then the
     * fetch buffer is considered to be discarded.
     */
    private void discardFetchBuffer() {
    	//fills blobs before discarding anything
    	fillBlobs();

        // Clear the TDSReader mark at the start of the fetch buffer
        fetchBuffer.clearStartMark();

        // Clear all row TDSReader marks in the scroll window
        if (null != scrollWindow)
            scrollWindow.clear();

        // Once there are no TDSReader marks left referring to the fetch buffer
        // contents, process the remainder of the current row and all subsequent rows.
        try {
            while (fetchBufferNext())
                ;
        }
        catch (SQLServerException e) {
            if (logger.isLoggable(java.util.logging.Level.FINER))
                logger.finer(this + " Encountered exception discarding fetch buffer: " + e.getMessage());
        }
    }

    /**
     * Close a server side cursor and free up its resources in the database.
     *
     * If closing fails for any reason then the cursor is considered closed.
     */
    final void closeServerCursor() {
        if (0 == serverCursorId)
            return;

        // If the connection is already closed, don't bother trying to close the server cursor.
        // We won't be able to, and it's already closed on the server anyway.
        if (stmt.connection.isSessionUnAvailable()) {
            if (logger.isLoggable(java.util.logging.Level.FINER))
                logger.finer(this + ": Not closing cursor:" + serverCursorId + "; connection is already closed.");
        }
        else {
            if (logger.isLoggable(java.util.logging.Level.FINER))
                logger.finer(toString() + " Closing cursor:" + serverCursorId);

            final class CloseServerCursorCommand extends UninterruptableTDSCommand {
                CloseServerCursorCommand() {
                    super("closeServerCursor");
                }

                final boolean doExecute() throws SQLServerException {
                    TDSWriter tdsWriter = startRequest(TDS.PKT_RPC);
                    tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
                    tdsWriter.writeShort(TDS.PROCID_SP_CURSORCLOSE);
                    tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
                    tdsWriter.writeByte((byte) 0);  // RPC procedure option 2
                    tdsWriter.writeRPCInt(null, serverCursorId, false);
                    TDSParser.parse(startResponse(), getLogContext());
                    return true;
                }
            }

            // Try to close the server cursor. Any failure is caught, logged, and ignored.
            try {
                stmt.executeCommand(new CloseServerCursorCommand());
            }
            catch (SQLServerException e) {
                if (logger.isLoggable(java.util.logging.Level.FINER))
                    logger.finer(toString() + " Ignored error closing cursor:" + serverCursorId + " " + e.getMessage());
            }

            if (logger.isLoggable(java.util.logging.Level.FINER))
                logger.finer(toString() + " Closed cursor:" + serverCursorId);
        }
    }
}
