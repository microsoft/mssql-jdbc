/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * ScrollWindow provides an efficient way to scroll around within a limited number of rows, typically the ResultSet fetch size, by saving and
 * restoring row state, such as starting point in the response and updated/deleted status, on movement within the window.
 *
 * Without a scroll window, scrolling backward through a result set would be very costly, requiring reindexing the fetch buffer up to row N-1 for each
 * move to the previous row.
 */
final class ScrollWindow {
    /** Set of marks for the rows in the window */
    private TDSReaderMark[] rowMark;

    /** Set of flags indicating which rows have been updated through the ResultSet */
    private boolean[] updatedRow;

    /** Set of flags indicating which rows have been deleted through the ResultSet */
    private boolean[] deletedRow;

    /** Set of enums indicating the types of rows in a ResultSet */
    private RowType[] rowType;

    /** Size (in rows) of this scroll window */
    private int size = 0;

    /** Max number of rows in the window (less than or equal to size) */
    private int maxRows = 0;

    final int getMaxRows() {
        return maxRows;
    }

    /** Current row in the window (1-indexed) */
    private int currentRow;

    final int getRow() {
        return currentRow;
    }

    ScrollWindow(int size) {
        setSize(size);
        reset();
    }

    private void setSize(int size) {
        assert this.size != size;
        this.size = size;
        this.maxRows = size;
        this.rowMark = new TDSReaderMark[size];
        this.updatedRow = new boolean[size];
        this.deletedRow = new boolean[size];
        this.rowType = new RowType[size];
        for (int i = 0; i < size; i++) {
            rowType[i] = RowType.UNKNOWN;
        }
    }

    final void clear() {
        for (int i = 0; i < rowMark.length; ++i) {
            rowMark[i] = null;
            updatedRow[i] = false;
            deletedRow[i] = false;
            rowType[i] = RowType.UNKNOWN;
        }

        assert size > 0;
        maxRows = size;
        reset();
    }

    final void reset() {
        currentRow = 0;
    }

    final void resize(int newSize) {
        assert newSize > 0;
        if (newSize != size)
            setSize(newSize);
    }

    final String logCursorState() {
        return " currentRow:" + currentRow + " maxRows:" + maxRows;
    }

    final boolean next(SQLServerResultSet rs) throws SQLServerException {
        if (SQLServerResultSet.logger.isLoggable(java.util.logging.Level.FINER))
            SQLServerResultSet.logger.finer(rs.toString() + logCursorState());

        // Precondition:
        // Current position should always be on a row in the window or
        // just before the first row or just after the last row.
        assert 0 <= currentRow && currentRow <= maxRows + 1;

        // If the position is already beyond the end of the window,
        // then it can move no farther forward.
        if (maxRows + 1 == currentRow)
            return false;

        // Otherwise, we are going to attempt to move the current
        // position to the next row. First, save off the row
        // updated/deleted status for the current row so it
        // can be restored later if we ever move to this row again.
        if (currentRow >= 1) {
            updatedRow[currentRow - 1] = rs.getUpdatedCurrentRow();
            deletedRow[currentRow - 1] = rs.getDeletedCurrentRow();
            rowType[currentRow - 1] = rs.getCurrentRowType();
        }

        // Start on the next row
        ++currentRow;

        // If we were on the last row of the window then make sure
        // the move past the last row consumes the remainder of that
        // row from the fetch buffer. The fetch buffer should be
        // left pointing beyond the last row of the window, which
        // is most likely the end of the fetch buffer as well.
        if (maxRows + 1 == currentRow) {
            rs.fetchBufferNext();
            return false;
        }

        // We weren't on the last row of the window. If we already
        // know that there was another row in the fetch buffer,
        // then restore the response buffer position and updated/deleted
        // status for the new row.
        if (null != rowMark[currentRow - 1]) {
            rs.fetchBufferReset(rowMark[currentRow - 1]);
            rs.setCurrentRowType(rowType[currentRow - 1]);
            rs.setUpdatedCurrentRow(updatedRow[currentRow - 1]);
            rs.setDeletedCurrentRow(deletedRow[currentRow - 1]);
            return true;
        }

        // We weren't on the last row of the window and we don't
        // know whether there are additional rows in the fetch
        // buffer, so try to read another row now. If we find
        // one then keep track of its position in the response
        // buffer.
        if (rs.fetchBufferNext()) {
            rowMark[currentRow - 1] = rs.fetchBufferMark();
            rowType[currentRow - 1] = rs.getCurrentRowType();

            if (SQLServerResultSet.logger.isLoggable(java.util.logging.Level.FINEST))
                SQLServerResultSet.logger.finest(
                        rs.toString() + " Set mark " + rowMark[currentRow - 1] + " for row " + currentRow + " of type " + rowType[currentRow - 1]);

            return true;
        }

        // We weren't on the last row of the window and we now
        // know that there are no more rows in the fetch buffer,
        // so adjust maxRows down accordingly.
        maxRows = currentRow - 1;
        return false;
    }

    final void previous(SQLServerResultSet rs) throws SQLServerException {
        if (SQLServerResultSet.logger.isLoggable(java.util.logging.Level.FINER))
            SQLServerResultSet.logger.finer(rs.toString() + logCursorState());

        // Precondition:
        // Current position should always be on a row in the window or
        // just before the first row or just after the last row.
        assert 0 <= currentRow && currentRow <= maxRows + 1;

        // If the position is already before the start of the window,
        // then it can move no farther back.
        if (0 == currentRow)
            return;

        // Otherwise, we are going to attempt to move the current
        // position to the previous row. First, save off the row
        // updated/deleted status for the current row so it
        // can be restored later if we ever move to this row again.
        if (currentRow <= maxRows) {
            assert currentRow >= 1;
            updatedRow[currentRow - 1] = rs.getUpdatedCurrentRow();
            deletedRow[currentRow - 1] = rs.getDeletedCurrentRow();
            rowType[currentRow - 1] = rs.getCurrentRowType();
        }

        // Start on the previous row
        --currentRow;

        // If we were on the first row of the window before moving,
        // then we're now before the first row and we're done.
        if (0 == currentRow)
            return;

        // If we weren't on the first row before moving then we
        // are now on the previous row. Restore the saved
        // position in the response buffer and updated/deleted
        // state for the now current row.
        assert null != rowMark[currentRow - 1];
        rs.fetchBufferReset(rowMark[currentRow - 1]);
        rs.setCurrentRowType(rowType[currentRow - 1]);
        rs.setUpdatedCurrentRow(updatedRow[currentRow - 1]);
        rs.setDeletedCurrentRow(deletedRow[currentRow - 1]);
    }
}
