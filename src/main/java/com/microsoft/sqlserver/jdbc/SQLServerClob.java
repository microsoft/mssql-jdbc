/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQLServerClob represents a character LOB object and implements java.sql.Clob.
 */

public class SQLServerClob extends SQLServerClobBase implements Clob {
    private static final long serialVersionUID = 2872035282200133865L;
    
    // Loggers should be class static to avoid lock contention with multiple threads
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerClob");

    /**
     * Create a new CLOB
     *
     * @param connection
     *            the database connection this blob is implemented on
     * @param data
     *            the CLOB's data
     * @deprecated Use {@link SQLServerConnection#createClob()} instead.
     */
    @Deprecated
    public SQLServerClob(SQLServerConnection connection,
            String data) {
        super(connection, data, (null == connection) ? null : connection.getDatabaseCollation(), logger, null);

        if (null == data)
            throw new NullPointerException(SQLServerException.getErrString("R_cantSetNull"));
    }

    SQLServerClob(SQLServerConnection connection) {
        super(connection, "", connection.getDatabaseCollation(), logger, null);
    }

    SQLServerClob(BaseInputStream stream,
            TypeInfo typeInfo) throws SQLServerException, UnsupportedEncodingException {
        super(null, stream, typeInfo.getSQLCollation(), logger, typeInfo);
    }

    final JDBCType getJdbcType() {
        return JDBCType.CLOB;
    }
}

abstract class SQLServerClobBase implements Serializable {
    private static final long serialVersionUID = 8691072211054430124L;

    // The value of the CLOB that this Clob object represents.
    // This value is never null unless/until the free() method is called.
    private String value;

    private final SQLCollation sqlCollation;

    private boolean isClosed = false;
    
    private final TypeInfo typeInfo;

    // Active streams which must be closed when the Clob/NClob is closed
    //
    // Initial size of the array is based on an assumption that a Clob/NClob object is
    // typically used either for input or output, and then only once. The array size
    // grows automatically if multiple streams are used.
    private ArrayList<Closeable> activeStreams = new ArrayList<>(1);

    transient SQLServerConnection con;
    
    private final Logger logger;
    
    final private String traceID = getClass().getName().substring(1 + getClass().getName().lastIndexOf('.')) + ":" + nextInstanceID();

    final public String toString() {
        return traceID;
    }

    static private final AtomicInteger baseID = new AtomicInteger(0);   // Unique id generator for each instance (used for logging).
    // Returns unique id for each instance.

    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    abstract JDBCType getJdbcType();

    private String getDisplayClassName() {
        String fullClassName = getJdbcType().className();
        return fullClassName.substring(1 + fullClassName.lastIndexOf('.'));
    }

    /**
     * Create a new CLOB from a String
     * 
     * @param connection
     *            SQLServerConnection
     * @param data
     *            the CLOB data
     * @param collation
     *            the data collation
     * @param logger
     *            logger information
     * @param typeInfo
     *            the column TYPE_INFO
     */
    SQLServerClobBase(SQLServerConnection connection,
            Object data,
            SQLCollation collation,
            Logger logger,
            TypeInfo typeInfo) {
        this.con = connection;
        if (data instanceof BaseInputStream) {
            activeStreams.add((Closeable) data);
        }
        else {
            this.value = (String) data;
        }
        this.sqlCollation = collation;
        this.logger = logger;
        this.typeInfo = typeInfo;
        if (logger.isLoggable(Level.FINE)) {
            String loggingInfo = (null != connection) ? connection.toString() : "null connection";
            logger.fine(toString() + " created by (" + loggingInfo + ")");
        }
    }

    /**
     * Frees this Clob/NClob object and releases the resources that it holds.
     *
     * After free() has been called, any attempt to invoke a method other than free() will result in a SQLException being thrown. If free() is called
     * multiple times, the subsequent calls to free are treated as a no-op.
     * 
     * @throws SQLException
     *             when an error occurs
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

            // Discard the value.
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
            SQLServerException.makeFromDriverError(con, null, form.format(new Object[] {getDisplayClassName()}), null, true);
        }
    }

    /**
     * Materialize the CLOB as an ASCII stream.
     * 
     * @throws SQLException
     *             when an error occurs
     * @return the data as an input stream
     */
    public InputStream getAsciiStream() throws SQLException {
        checkClosed();

        if (null != sqlCollation && !sqlCollation.supportsAsciiConversion())
            DataTypes.throwConversionError(getDisplayClassName(), "AsciiStream");

        // Need to use a BufferedInputStream since the stream returned by this method is assumed to support mark/reset
        InputStream getterStream;
        if (null == value && !activeStreams.isEmpty()) {
            InputStream inputStream = (InputStream) activeStreams.get(0);
            try {
                inputStream.reset();
                getterStream = new BufferedInputStream(
                        new ReaderInputStream(new InputStreamReader(inputStream), US_ASCII, inputStream.available()));
            }
            catch (IOException e) {
                throw new SQLServerException(e.getMessage(), null, 0, e);
            }
        }
        else {
            getStringFromStream();
            getterStream = new BufferedInputStream(new ReaderInputStream(new StringReader(value), US_ASCII, value.length()));

        }
        return getterStream;
    }

    /**
     * Retrieves the CLOB value designated by this Clob object as a java.io.Reader object (or as a stream of characters).
     * 
     * @throws SQLException
     *             if there is an error accessing the CLOB value
     * @return a java.io.Reader object containing the CLOB data
     */
    public Reader getCharacterStream() throws SQLException {
        checkClosed();

        getStringFromStream();
        Reader getterStream = new StringReader(value);
        activeStreams.add(getterStream);
        return getterStream;
    }

    /**
     * Returns the Clob data as a java.io.Reader object or as a stream of characters with the specified position and length.
     * 
     * @param pos
     *            A long that indicates the offset to the first character of the partial value to be retrieved.
     * @param length
     *            A long that indicates the length in characters of the partial value to be retrieved.
     * @return A Reader object that contains the Clob data.
     * @throws SQLException
     *             when an error occurs.
     */
    public Reader getCharacterStream(long pos,
            long length) throws SQLException {
        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    /**
     * Retrieves a copy of the specified substring in the CLOB value designated by this Clob object. The substring begins at position pos and has up
     * to length consecutive characters.
     *
     * @param pos
     *            - the first character of the substring to be extracted. The first character is at position 1.
     * @param length
     *            - the number of consecutive characters to be copied; the value for length must be 0 or greater
     * @return a String that is the specified substring in the CLOB value designated by this Clob object
     * @throws SQLException
     *             - if there is an error accessing the CLOB value; if pos is less than 1 or length is less than 0
     */
    public String getSubString(long pos,
            int length) throws SQLException {
        checkClosed();

        getStringFromStream();
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
        if (pos > value.length())
            pos = value.length();

        // Bound the requested length to no larger than the remainder of the value beyond pos so that the
        // endIndex computed for the substring call below is within bounds.
        if (length > value.length() - pos)
            length = (int) (value.length() - pos);

        // Note String.substring uses beginIndex and endIndex (not pos and length), so calculate endIndex.
        return value.substring((int) pos, (int) pos + length);
    }

    /**
     * Retrieves the number of characters in the CLOB value designated by this Clob object.
     * 
     * @throws SQLException
     *             when an error occurs
     * @return length of the CLOB in characters
     */
    public long length() throws SQLException {
        checkClosed();

        getStringFromStream();
        return value.length();
    }
    
    /**
     * Converts the stream to String
     * @throws SQLServerException
     */
    private void getStringFromStream() throws SQLServerException {
        if (null == value && !activeStreams.isEmpty()) {
            BaseInputStream stream = (BaseInputStream) activeStreams.get(0);
            try {
                stream.reset();
            }
            catch (IOException e) {
                throw new SQLServerException(e.getMessage(), null, 0, e);
            }
            value = new String((stream).getBytes(), typeInfo.getCharset());
        }
    }

    /**
     * Retrieves the character position at which the specified Clob object searchstr appears in this Clob object. The search begins at position start.
     *
     * @param searchstr
     *            - the Clob for which to search
     * @param start
     *            - the position at which to begin searching; the first position is 1
     * @return the position at which the Clob object appears or -1 if it is not present; the first position is 1
     * @throws SQLException
     *             - if there is an error accessing the CLOB value or if start is less than 1
     */
    public long position(Clob searchstr,
            long start) throws SQLException {
        checkClosed();

        getStringFromStream();
        if (start < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {start};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (null == searchstr)
            return -1;

        return position(searchstr.getSubString(1, (int) searchstr.length()), start);
    }

    /**
     * Retrieves the character position at which the specified substring searchstr appears in the SQL CLOB value represented by this Clob object. The
     * search begins at position start.
     *
     * @param searchstr
     *            - the substring for which to search
     * @param start
     *            - the position at which to begin searching; the first position is 1
     * @return the position at which the substring appears or -1 if it is not present; the first position is 1
     * @throws SQLException
     *             - if there is an error accessing the CLOB value or if start is less than 1
     */
    public long position(String searchstr,
            long start) throws SQLException {
        checkClosed();

        getStringFromStream();
        if (start < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {start};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Back compat: Handle null search string as not found rather than throw an exception.
        // JDBC spec doesn't describe the behavior for a null search string.
        if (null == searchstr)
            return -1;

        int pos = value.indexOf(searchstr, (int) (start - 1));
        if (-1 != pos)
            return pos + 1L;

        return -1;
    }

    /* JDBC 3.0 methods */

    /**
     * Truncates the CLOB value that this Clob designates to have a length of len characters.
     * 
     * @param len
     *            the length, in characters, to which the CLOB value should be truncated
     * @throws SQLException
     *             when an error occurs
     */
    public void truncate(long len) throws SQLException {
        checkClosed();

        getStringFromStream();
        if (len < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
            Object[] msgArgs = {len};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        if (len <= Integer.MAX_VALUE && value.length() > len)
            value = value.substring(0, (int) len);
    }

    /**
     * Retrieves a stream to be used to write Ascii characters to the CLOB value that this Clob object represents, starting at position pos.
     * 
     * @param pos
     *            the position at which to start writing to this CLOB object
     * @throws SQLException
     *             when an error occurs
     * @return the stream to which ASCII encoded characters can be written
     */
    public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
        checkClosed();

        if (pos < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {pos};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        return new SQLServerClobAsciiOutputStream(this, pos);
    }

    /**
     * Retrieves a stream to be used to write a stream of Unicode characters to the CLOB value that this Clob object represents, at position pos.
     * 
     * @param pos
     *            the position at which to start writing to the CLOB value
     * @throws SQLException
     *             when an error occurs
     * @return a stream to which Unicode encoded characters can be written
     */
    public java.io.Writer setCharacterStream(long pos) throws SQLException {
        checkClosed();

        if (pos < 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {pos};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        return new SQLServerClobWriter(this, pos);
    }

    /**
     * Writes the given Java String to the CLOB value that this Clob object designates at the position pos.
     * 
     * @param pos
     *            the position at which to start writing to the CLOB
     * @param s
     *            the string to be written to the CLOB value that this Clob designates
     * @throws SQLException
     *             when an error occurs
     * @return the number of characters written
     */
    public int setString(long pos,
            String s) throws SQLException {
        checkClosed();

        if (null == s)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_cantSetNull"), null, true);

        return setString(pos, s, 0, s.length());
    }

    /**
     * Writes len characters of str, starting at character offset, to the CLOB value that this Clob represents. The string will overwrite the existing
     * characters in the Clob object starting at the position pos. If the end of the Clob value is reached while writing the given string, then the
     * length of the Clob value will be increased to accomodate the extra characters.
     *
     * SQL Server behavior: If the value specified for pos is greater than then length+1 of the CLOB value then a SQLException is thrown.
     *
     * @param pos
     *            - the position at which to start writing to this CLOB object; The first position is 1
     * @param str
     *            - the string to be written to the CLOB value that this Clob object represents
     * @param offset
     *            - the offset (0-based) into str to start reading the characters to be written
     * @param len
     *            - the number of characters to be written
     * @return the number of characters written
     * @throws SQLException
     *             - if there is an error accessing the CLOB value or if pos is less than 1
     */
    public int setString(long pos,
            String str,
            int offset,
            int len) throws SQLException {
        checkClosed();

        getStringFromStream();
        if (null == str)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_cantSetNull"), null, true);

        // Offset must be within incoming string str boundary.
        if (offset < 0 || offset > str.length()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidOffset"));
            Object[] msgArgs = {offset};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // len must be within incoming string str boundary.
        if (len < 0 || len > str.length() - offset) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
            Object[] msgArgs = {len};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Note position for Clob.setString is 1 based not zero based.
        // Position must be in range of existing Clob data or exactly 1 character
        // past the end of data to request "append" mode.
        if (pos < 1 || pos > value.length() + 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPositionIndex"));
            Object[] msgArgs = {pos};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, true);
        }

        // Adjust position to zero based.
        pos--;

        // Overwrite past end of value case.
        if (len >= value.length() - pos) {
            // Make sure the new value length wouldn't exceed the maximum allowed
            DataTypes.getCheckedLength(con, getJdbcType(), pos + len, false);
            assert pos + len <= Integer.MAX_VALUE;

            // Start with the original value, up to the starting position
            StringBuilder sb = new StringBuilder((int) pos + len);
            sb.append(value.substring(0, (int) pos));

            // Append the new value
            sb.append(str.substring(offset, offset + len));

            // Use the combined string as the new value
            value = sb.toString();
        }

        // Overwrite internal to value case.
        else {
            // Start with the original value, up to the starting position
            StringBuilder sb = new StringBuilder(value.length());
            sb.append(value.substring(0, (int) pos));

            // Append the new value
            sb.append(str.substring(offset, offset + len));

            // Append the remainder of the original value
            // that was not replaced by the new value
            sb.append(value.substring((int) pos + len));

            // Use the combined string as the new value
            value = sb.toString();
        }

        return len;
    }
}

// SQLServerClobWriter is a simple java.io.Writer interface implementing class that
// forwards all calls to SQLServerClob.setString. This class is returned to caller by
// SQLServerClob class when setCharacterStream is called.
//
// SQLServerClobWriter starts writing at postion streamPos and continues to write
// in a forward only manner. There is no reset with java.io.Writer.
//
final class SQLServerClobWriter extends java.io.Writer {
    private SQLServerClobBase parentClob = null;
    private long streamPos;

    SQLServerClobWriter(SQLServerClobBase parentClob,
            long streamPos) {
        this.parentClob = parentClob;
        this.streamPos = streamPos;
    }

    public void write(char[] cbuf) throws IOException {
        if (null == cbuf)
            return;
        write(new String(cbuf));
    }

    public void write(char[] cbuf,
            int off,
            int len) throws IOException {
        if (null == cbuf)
            return;
        write(new String(cbuf, off, len));
    }

    public void write(int b) throws java.io.IOException {
        char[] c = new char[1];
        c[0] = (char) b;
        write(new String(c));
    }

    public void write(String str,
            int off,
            int len) throws IOException {
        checkClosed();
        try {
            // Call parent's setString and update position.
            // setString can throw a SQLServerException, we translate
            // this to an IOException here.
            int charsWritten = parentClob.setString(streamPos, str, off, len);
            streamPos += charsWritten;
        }
        catch (SQLException ex) {
            throw new IOException(ex.getMessage());
        }
    }

    public void write(String str) throws IOException {
        if (null == str)
            return;
        write(str, 0, str.length());
    }

    public void flush() throws IOException {
        checkClosed();
    }

    public void close() throws IOException {
        checkClosed();
        parentClob = null;
    }

    private void checkClosed() throws IOException {
        if (null == parentClob)
            throw new IOException(SQLServerException.getErrString("R_streamIsClosed"));
    }
}

// SQLServerClobAsciiOutputStream is a simple java.io.OutputStream interface implementing class that
// forwards all calls to SQLServerClob.setString. This class is returned to caller by
// SQLServerClob class when setAsciiStream is called.
//
// SQLServerClobAsciiOutputStream starts writing at character postion streamPos and continues to write
// in a forward only manner. Reset/mark are not supported.
//
final class SQLServerClobAsciiOutputStream extends java.io.OutputStream {
    private SQLServerClobBase parentClob = null;
    private long streamPos;

    SQLServerClobAsciiOutputStream(SQLServerClobBase parentClob,
            long streamPos) {
        this.parentClob = parentClob;
        this.streamPos = streamPos;
    }

    public void write(byte[] b) throws IOException {
        if (null == b)
            return;
        write(b, 0, b.length);
    }

    public void write(byte[] b,
            int off,
            int len) throws IOException {
        if (null == b)
            return;
        try {
            // Convert bytes to string using US-ASCII translation.
            String s = new String(b, off, len, "US-ASCII");

            // Call parent's setString and update position.
            // setString can throw a SQLServerException, we translate
            // this to an IOException here.
            int charsWritten = parentClob.setString(streamPos, s);
            streamPos += charsWritten;
        }
        catch (SQLException ex) {
            throw new IOException(ex.getMessage());
        }
    }

    private byte[] bSingleByte = new byte[1];

    public void write(int b) throws java.io.IOException {
        bSingleByte[0] = (byte) (b & 0xFF);
        write(bSingleByte, 0, bSingleByte.length);
    }
}
