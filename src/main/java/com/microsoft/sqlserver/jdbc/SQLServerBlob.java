/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQLServerBlob represents a binary LOB object and implements a java.sql.Blob.
 */

public final class SQLServerBlob implements java.sql.Blob, java.io.Serializable {
    private static final long serialVersionUID = -3526170228097889085L;

    // The value of the BLOB that this Blob object represents.
    // This value is never null unless/until the free() method is called.
    private byte[] value;

    private transient SQLServerConnection con;
    private boolean isClosed = false;

    // Active streams which must be closed when the Blob is closed
    //
    // Initial size of the array is based on an assumption that a Blob object is
    // typically used either for input or output, and then only once. The array size
    // grows automatically if multiple streams are used.
    ArrayList<Closeable> activeStreams = new ArrayList<>(1);

    static private final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerBlob");

    static private final AtomicInteger baseID = new AtomicInteger(0);   // Unique id generator for each instance (used for logging).
    final private String traceID;
    
    final public String toString() {
        return traceID;
    }

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    /**
     * Create a new BLOB
     *
     * @param connection
     *            the database connection this blob is implemented on
     * @param data
     *            the BLOB's data
     * @deprecated Use {@link SQLServerConnection#createBlob()} instead. 
     */
    @Deprecated
    public SQLServerBlob(SQLServerConnection connection,
            byte data[]) {
        traceID = " SQLServerBlob:" + nextInstanceID();
        con = connection;

        // Disallow Blobs with internal null values. We throw a NullPointerException here
        // because the method signature of the public constructor does not permit a SQLException
        // to be thrown.
        if (null == data)
            throw new NullPointerException(SQLServerException.getErrString("R_cantSetNull"));

        value = data;

        if (logger.isLoggable(Level.FINE)) {
            String loggingInfo = (null != connection) ? connection.toString() : "null connection";
            logger.fine(toString() + " created by (" + loggingInfo + ")");
        }
    }

    SQLServerBlob(SQLServerConnection connection) {
        traceID = " SQLServerBlob:" + nextInstanceID();
        con = connection;
        value = new byte[0];
        if (logger.isLoggable(Level.FINE))
            logger.fine(toString() + " created by (" + connection.toString() + ")");
    }

    SQLServerBlob(BaseInputStream stream) throws SQLServerException {
        traceID = " SQLServerBlob:" + nextInstanceID();
        activeStreams.add(stream);
        if (logger.isLoggable(Level.FINE))
            logger.fine(toString() + " created by (null connection)");
    }

    /**
     * Frees this Blob object and releases the resources that it holds.
     * <p>
     * After free() has been called, any attempt to invoke a method other than free() will result in a SQLException being thrown. If free() is called
     * multiple times, the subsequent calls to free are treated as a no-op.
     */
    public void free() throws SQLException {
        if (!isClosed) {
            // Close active streams, ignoring any errors, since nothing can be done with them after that point anyway.
            if (null != activeStreams) {
                for (Closeable stream : activeStreams) {
                    try {
                        stream.close();
                    }
                    catch (IOException ioException) {
                        logger.fine(toString() + " ignored IOException closing stream " + stream + ": " + ioException.getMessage());
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

    public InputStream getBinaryStream() throws SQLException {
        checkClosed();

        if (null == value && !activeStreams.isEmpty()) {
            InputStream stream = (InputStream) activeStreams.get(0);
            try {
                stream.reset();
            }
            catch (IOException e) {
                throw new SQLServerException(e.getMessage(), null, 0, e);
            }
            return (InputStream) activeStreams.get(0);
        }
        else {
            if (value == null) {
                throw new SQLServerException("Unexpected Error: blob value is null while all streams are closed.", null);
            }
            return getBinaryStreamInternal(0, value.length);
        }
    }

    public InputStream getBinaryStream(long pos,
            long length) throws SQLException {
        // Not implemented - partial materialization
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    private InputStream getBinaryStreamInternal(int pos,
            int length) {
        assert null != value;
        assert pos >= 0;
        assert 0 <= length && length <= value.length - pos;
        assert null != activeStreams;

        InputStream getterStream = new ByteArrayInputStream(value, pos, length);
        activeStreams.add(getterStream);
        return getterStream;
    }

    /**
     * Retrieves all or part of the BLOB value that this Blob object represents, as an array of bytes. This byte array contains up to length
     * consecutive bytes starting at position pos.
     *
     * @param pos
     *            - the ordinal position of the first byte in the BLOB value to be extracted; the first byte is at position 1
     * @param length
     *            - the number of consecutive bytes to be copied; the value for length must be 0 or greater
     * @return a byte array containing up to length consecutive bytes from the BLOB value designated by this Blob object, starting with the byte at
     *         position pos
     * @throws SQLException
     *             - if there is an error accessing the BLOB value; if pos is less than 1 or length is less than 0
     */
    public byte[] getBytes(long pos,
            int length) throws SQLException {
        checkClosed();

        getBytesFromStream();
        if (pos < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {pos};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (length < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
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

        byte bTemp[] = new byte[length];
        System.arraycopy(value, (int) pos, bTemp, 0, length);
        return bTemp;
    }

    /**
     * Return the length of the BLOB
     * 
     * @throws SQLException
     *             when an error occurs
     * @return the data length
     */
    public long length() throws SQLException {
        checkClosed();
        if (value == null && activeStreams.get(0) instanceof PLPInputStream) {
        	return (long)((PLPInputStream)activeStreams.get(0)).payloadLength;
        }
        getBytesFromStream();
        return value.length;
    }
    
    /**
     * Function for the result set to maintain blobs it has created
     * @throws SQLException
     */
    void fillByteArray() throws SQLException {
    	if(!isClosed) {
    		getBytesFromStream();
    	}
    }
    
    /**
     * Converts stream to byte[]
     * @throws SQLServerException
     */
    private void getBytesFromStream() throws SQLServerException {
        if (null == value) {
            BaseInputStream stream = (BaseInputStream) activeStreams.get(0);
            try {
                stream.reset();
            }
            catch (IOException e) {
                throw new SQLServerException(e.getMessage(), null, 0, e);
            }
            value = stream.getBytes();
        }
    }

    /**
     * Retrieves the byte position in the BLOB value designated by this Blob object at which pattern begins. The search begins at position start.
     *
     * @param pattern
     *            - the Blob object designating the BLOB value for which to search
     * @param start
     *            - the position in the BLOB value at which to begin searching; the first position is 1
     * @return the postion at which the pattern begins, else -1
     * @throws SQLException
     *             - if there is an error accessing the BLOB value or if start is less than 1
     */
    public long position(Blob pattern,
            long start) throws SQLException {
        checkClosed();
        
        getBytesFromStream();
        if (start < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {start};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (null == pattern)
            return -1;

        return position(pattern.getBytes((long) 1, (int) pattern.length()), start);
    }

    /**
     * Retrieves the byte position at which the specified byte array pattern begins within the BLOB value that this Blob object represents. The search
     * for pattern begins at position start.
     *
     * @param bPattern
     *            - the byte array for which to search
     * @param start
     *            - the position at which to begin searching; the first position is 1
     * @return the position at which the pattern appears, else -1
     * @throws SQLException
     *             - if there is an error accessing the BLOB or if start is less than 1
     */
    public long position(byte[] bPattern,
            long start) throws SQLException {
        checkClosed();
        getBytesFromStream();
        if (start < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {start};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Back compat: Handle null search string as not found rather than throw an exception.
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

    /* JDBC 3.0 methods */

    /**
     * Truncate a BLOB
     * 
     * @param len
     *            the new length for the BLOB
     * @throws SQLException
     *             when an error occurs
     */
    public void truncate(long len) throws SQLException {
        checkClosed();
        getBytesFromStream();
        
        if (len < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
            Object[] msgArgs = {len};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (value.length > len) {
            byte bNew[] = new byte[(int) len];
            System.arraycopy(value, 0, bNew, 0, (int) len);
            value = bNew;
        }
    }

    /**
     * Retrieves a stream that can be used to write to the BLOB value that this Blob object represents
     * 
     * @param pos
     *            - the position in the BLOB value at which to start writing; the first position is 1
     * @return a java.io.OutputStream object to which data can be written
     * @throws SQLException
     *             - if there is an error accessing the BLOB value or if pos is less than 1
     */
    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        checkClosed();

        if (pos < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[] {pos}), null, true);
        }

        return new SQLServerBlobOutputStream(this, pos);
    }

    /**
     * Writes the given array of bytes into the Blob starting at position pos, and returns the number of bytes written.
     * 
     * @param pos
     *            the position (1 based) in the Blob object at which to start writing the data.
     * @param bytes
     *            the array of bytes to be written into the Blob.
     * @throws SQLException
     *             if there is an error accessing the BLOB value.
     * @return the number of bytes written.
     */
    public int setBytes(long pos,
            byte[] bytes) throws SQLException {
        checkClosed();
        
        getBytesFromStream();
        if (null == bytes)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_cantSetNull"), null, true);

        return setBytes(pos, bytes, 0, bytes.length);
    }

    /**
     * Writes all or part of the given byte array to the BLOB value that this Blob object represents and returns the number of bytes written. Writing
     * starts at position pos in the BLOB value; len bytes from the given byte wrray are written. The array of bytes will overwrite the existing bytes
     * in the Blob object starting at the position pos. If the end of the Blob value is reached while writing the array bytes, then the length of the
     * Blob value will be increased to accomodate the extra bytes.
     *
     * SQL Server behavior: If the value specified for pos is greater than the length+1 of the BLOB value then a SQLException is thrown.
     *
     * @param pos
     *            - the position in the BLOB object at which to start writing; the first position is 1
     * @param bytes
     *            - the array of bytes to be written to this BLOB object.
     * @param offset
     *            - the offset (0-based) into the array bytes at which to start reading the bytes to set
     * @param len
     *            - the number of bytes to be written to the BLOB value from the array of bytes bytes
     * @return the number of bytes written.
     * @throws SQLException
     *             - if there is an error accessing the BLOB value or if pos is less than 1
     */
    public int setBytes(long pos,
            byte[] bytes,
            int offset,
            int len) throws SQLException {
        checkClosed();
        getBytesFromStream();

        if (null == bytes)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_cantSetNull"), null, true);

        // Offset must be within incoming bytes boundary.
        if (offset < 0 || offset > bytes.length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidOffset"));
            Object[] msgArgs = {offset};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // len must be within incoming bytes boundary.
        if (len < 0 || len > bytes.length - offset) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
            Object[] msgArgs = {len};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Note position for Blob.setBytes is 1 based not zero based.
        // Position must be in range of existing Blob data or exactly 1 byte
        // past the end of data to request "append" mode.
        if (pos <= 0 || pos > value.length + 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {pos};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Adjust pos to zero based.
        pos--;

        // Overwrite past end of value case.
        if (len >= value.length - pos) {
            // Make sure the new value length wouldn't exceed the maximum allowed
            DataTypes.getCheckedLength(con, JDBCType.BLOB, pos + len, false);
            assert pos + len <= Integer.MAX_VALUE;

            // Start with the original value, up to the starting position
            byte combinedValue[] = new byte[(int) pos + len];
            System.arraycopy(value, 0, combinedValue, 0, (int) pos);

            // Copy rest of data.
            System.arraycopy(bytes, offset, combinedValue, (int) pos, len);
            value = combinedValue;
        }
        else {
            // Overwrite internal to value case.
            System.arraycopy(bytes, offset, value, (int) pos, len);
        }

        return len;
    }
}

/**
 * SQLServerBlobOutputStream is a simple java.io.OutputStream interface implementing class that forwards all calls to SQLServerBlob.setBytes. This
 * class is returned to caller by SQLServerBlob class when setBinaryStream is called.
 * <p>
 * SQLServerBlobOutputStream starts writing at postion startPos and continues to write in a forward only manner. Reset/mark are not supported.
 */
final class SQLServerBlobOutputStream extends java.io.OutputStream {
    private SQLServerBlob parentBlob = null;
    private long currentPos;

    SQLServerBlobOutputStream(SQLServerBlob parentBlob,
            long startPos) {
        this.parentBlob = parentBlob;
        this.currentPos = startPos;
    }

    // java.io.OutputStream interface methods.

    public void write(byte[] b) throws IOException {
        if (null == b)
            return;
        write(b, 0, b.length);
    }

    public void write(byte[] b,
            int off,
            int len) throws IOException {
        try {
            // Call parent's setBytes and update position.
            // setBytes can throw a SQLServerException, we translate
            // this to an IOException here.
            int bytesWritten = parentBlob.setBytes(currentPos, b, off, len);
            currentPos += bytesWritten;
        }
        catch (SQLException ex) {
            throw new IOException(ex.getMessage());
        }
    }

    public void write(int b) throws java.io.IOException {
        byte[] bTemp = new byte[1];
        bTemp[0] = (byte) (b & 0xFF);
        write(bTemp, 0, bTemp.length);
    }
}
