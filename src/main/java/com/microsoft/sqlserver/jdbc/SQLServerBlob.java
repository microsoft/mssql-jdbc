/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Represents a binary LOB object and implements a java.sql.Blob.
 */
public final class SQLServerBlob extends SQLServerLob implements java.sql.Blob, java.io.Serializable {
    /**
     * Always refresh SerialVersionUID when prompted
     */
    private static final long serialVersionUID = -3526170228097889085L;

    // Error messages
    private static final String R_CANT_SET_NULL = "R_cantSetNull";
    private static final String R_INVALID_POSITION_INDEX = "R_invalidPositionIndex";
    private static final String R_INVALID_LENGTH = "R_invalidLength";

    private static final Logger _LOGGER = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerBlob");

    // Unique id generator for each instance (use for logging).
    private static final AtomicInteger BASE_ID = new AtomicInteger(0);

    /**
     * The value of the BLOB that this Blob object represents. This value is never null unless/until the free() method
     * is called.
     */
    private byte[] value;

    private transient SQLServerConnection con;

    /** check if LOB has been freed */
    private boolean isClosed = false;

    /**
     * Active streams which must be closed when the Blob is closed
     * 
     * Initial size of the array is based on an assumption that a Blob object is typically used either for input or
     * output, and then only once. The array size grows automatically if multiple streams are used.
     */
    ArrayList<Closeable> activeStreams = new ArrayList<>(1);

    /** trace id */
    private final String traceID;

    /**
     * Returns string representation of object
     */
    public final String toString() {
        return traceID;
    }

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return BASE_ID.incrementAndGet();
    }

    /**
     * Create a new BLOB
     *
     * @param connection
     *        the database connection this blob is implemented on
     * @param data
     *        the BLOB's data
     * @deprecated Use {@link SQLServerConnection#createBlob()} instead.
     */
    @Deprecated
    public SQLServerBlob(SQLServerConnection connection, byte[] data) {
        traceID = this.getClass().getSimpleName() + nextInstanceID();
        con = connection;

        // Disallow Blobs with internal null values. We throw a
        // NullPointerException here
        // because the method signature of the public constructor does not
        // permit a SQLException
        // to be thrown.
        if (null == data)
            throw new NullPointerException(SQLServerException.getErrString(R_CANT_SET_NULL));

        value = data;

        if (_LOGGER.isLoggable(Level.FINE)) {
            String loggingInfo = (null != connection) ? connection.toString() : "null connection";
            _LOGGER.fine(this.toString() + " created by (" + loggingInfo + ")");
        }
    }

    SQLServerBlob(SQLServerConnection connection) {
        traceID = this.getClass().getSimpleName() + nextInstanceID();
        con = connection;
        value = new byte[0];
        if (_LOGGER.isLoggable(Level.FINE))
            _LOGGER.fine(this.toString() + " created by (" + connection.toString() + ")");
    }

    SQLServerBlob(BaseInputStream stream) {
        traceID = this.getClass().getSimpleName() + nextInstanceID();
        activeStreams.add(stream);
        if (_LOGGER.isLoggable(Level.FINE))
            _LOGGER.fine(this.toString() + " created by (null connection)");
    }

    @Override
    public void free() throws SQLException {
        if (!isClosed) {
            // Close active streams, ignoring any errors, since nothing can be
            // done with them after that point anyway.
            if (null != activeStreams) {
                for (Closeable stream : activeStreams) {
                    try {
                        stream.close();
                    } catch (IOException ioException) {
                        _LOGGER.fine(this.toString() + " ignored IOException closing stream " + stream + ": "
                                + ioException.getMessage());
                    }
                }
                activeStreams = null;
            }

            // Discard the value
            value = null;
            isClosed = true;
        }
    }

    /**
     * Throws a SQLException if the LOB has been freed.
     */
    private void checkClosed() throws SQLServerException {
        if (isClosed) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_isFreed"));
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[] {"Blob"}), null, true);
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        checkClosed();
        // If the LOB is currently streaming and the stream hasn't been read, read it.
        if (!delayLoadingLob && null == value && !activeStreams.isEmpty()) {
            getBytesFromStream();
        }

        if (null == value && !activeStreams.isEmpty()) {
            InputStream stream = (InputStream) activeStreams.get(0);
            try {
                stream.reset();
            } catch (IOException e) {
                throw new SQLServerException(e.getMessage(), null, 0, e);
            }
            return (InputStream) activeStreams.get(0);
        } else {
            if (value == null) {
                throw new SQLServerException("Unexpected Error: blob value is null while all streams are closed.",
                        null);
            }
            return getBinaryStreamInternal(0, value.length);
        }
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        SQLServerException.throwFeatureNotSupportedException();
        return null;
    }

    private InputStream getBinaryStreamInternal(int pos, int length) {
        assert null != value;
        assert pos >= 0;
        assert 0 <= length && length <= value.length - pos;
        assert null != activeStreams;

        InputStream getterStream = new ByteArrayInputStream(value, pos, length);
        activeStreams.add(getterStream);
        return getterStream;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        checkClosed();

        getBytesFromStream();
        if (pos < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_POSITION_INDEX));
            Object[] msgArgs = {pos};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (length < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_LENGTH));
            Object[] msgArgs = {length};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Adjust pos to zero based.
        pos--;

        // Bound the starting position if necessary
        if (pos > value.length)
            pos = value.length;

        // Bound the length if necessary
        if (length > value.length - pos)
            length = (int) (value.length - pos);

        byte[] bTemp = new byte[length];
        System.arraycopy(value, (int) pos, bTemp, 0, length);
        return bTemp;
    }

    @Override
    public long length() throws SQLException {
        checkClosed();
        if (value == null && activeStreams.get(0) instanceof BaseInputStream) {
            return (long) ((BaseInputStream) activeStreams.get(0)).payloadLength;
        }
        getBytesFromStream();
        return value.length;
    }

    @Override
    void fillFromStream() throws SQLException {
        if (!isClosed) {
            getBytesFromStream();
        }
    }

    /**
     * Converts stream to byte[]
     * 
     * @throws SQLServerException
     */
    private void getBytesFromStream() throws SQLServerException {
        if (null == value) {
            BaseInputStream stream = (BaseInputStream) activeStreams.get(0);
            try {
                stream.reset();
            } catch (IOException e) {
                throw new SQLServerException(e.getMessage(), null, 0, e);
            }
            value = stream.getBytes();
        }
    }

    @Override
    public long position(java.sql.Blob pattern, long start) throws SQLException {
        checkClosed();

        getBytesFromStream();
        if (start < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_POSITION_INDEX));
            Object[] msgArgs = {start};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (null == pattern)
            return -1;

        return position(pattern.getBytes((long) 1, (int) pattern.length()), start);
    }

    @Override
    public long position(byte[] bPattern, long start) throws SQLException {
        checkClosed();
        getBytesFromStream();
        if (start < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_POSITION_INDEX));
            Object[] msgArgs = {start};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Back compat: Handle null search string as not found rather than throw
        // an exception.
        // JDBC spec doesn't describe the behavior for a null pattern.
        if (null == bPattern)
            return -1;

        // Adjust start to zero based.
        start--;

        // Search for pattern in value.
        for (int pos = (int) start; pos <= value.length - bPattern.length; ++pos) {
            boolean match = true;
            for (int i = 0; i < bPattern.length; ++i) {
                if (value[pos + i] != bPattern[i]) {
                    match = false;
                    break;
                }
            }

            if (match) {
                return pos + 1L;
            }
        }

        return -1;
    }

    @Override
    public void truncate(long len) throws SQLException {
        checkClosed();
        getBytesFromStream();

        if (len < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_LENGTH));
            Object[] msgArgs = {len};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (value.length > len) {
            byte[] bNew = new byte[(int) len];
            System.arraycopy(value, 0, bNew, 0, (int) len);
            value = bNew;
        }
    }

    @Override
    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        checkClosed();

        if (pos < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_POSITION_INDEX));
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[] {pos}), null, true);
        }

        return new SQLServerBlobOutputStream(this, pos);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        checkClosed();

        getBytesFromStream();
        if (null == bytes)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString(R_CANT_SET_NULL), null,
                    true);

        return setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        checkClosed();
        getBytesFromStream();

        if (null == bytes)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString(R_CANT_SET_NULL), null,
                    true);

        // Offset must be within incoming bytes boundary.
        if (offset < 0 || offset > bytes.length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidOffset"));
            Object[] msgArgs = {offset};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // len must be within incoming bytes boundary.
        if (len < 0 || len > bytes.length - offset) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_LENGTH));
            Object[] msgArgs = {len};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Note position for Blob.setBytes is 1 based not zero based.
        // Position must be in range of existing Blob data or exactly 1 byte
        // past the end of data to request "append" mode.
        if (pos <= 0 || pos > value.length + 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString(R_INVALID_POSITION_INDEX));
            Object[] msgArgs = {pos};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Adjust pos to zero based.
        pos--;

        // Overwrite past end of value case.
        if (len >= value.length - pos) {
            // Make sure the new value length wouldn't exceed the maximum
            // allowed
            DataTypes.getCheckedLength(con, JDBCType.BLOB, pos + len, false);

            // Start with the original value, up to the starting position
            byte[] combinedValue = new byte[(int) pos + len];
            System.arraycopy(value, 0, combinedValue, 0, (int) pos);

            // Copy rest of data.
            System.arraycopy(bytes, offset, combinedValue, (int) pos, len);
            value = combinedValue;
        } else {
            // Overwrite internal to value case.
            System.arraycopy(bytes, offset, value, (int) pos, len);
        }

        return len;
    }
}


/**
 * SQLServerBlobOutputStream is a simple java.io.OutputStream interface implementing class that forwards all calls to
 * SQLServerBlob.setBytes. This class is returned to caller by SQLServerBlob class when setBinaryStream is called.
 * <p>
 * SQLServerBlobOutputStream starts writing at postion startPos and continues to write in a forward only manner.
 * Reset/mark are not supported.
 */
final class SQLServerBlobOutputStream extends java.io.OutputStream {
    private SQLServerBlob parentBlob = null;
    private long currentPos;

    SQLServerBlobOutputStream(SQLServerBlob parentBlob, long startPos) {
        this.parentBlob = parentBlob;
        this.currentPos = startPos;
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (null == b)
            return;
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            // Call parent's setBytes and update position.
            // setBytes can throw a SQLServerException, we translate
            // this to an IOException here.
            int bytesWritten = parentBlob.setBytes(currentPos, b, off, len);
            currentPos += bytesWritten;
        } catch (SQLException ex) {
            throw new IOException(ex.getMessage());
        }
    }

    @Override
    public void write(int b) throws java.io.IOException {
        byte[] bTemp = new byte[1];
        bTemp[0] = (byte) (b & 0xFF);
        write(bTemp, 0, bTemp.length);
    }
}
