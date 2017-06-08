/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SimpleInputStream is an InputStream implementation that reads from TDS.
 * 
 * This class is to support adaptive streaming of non plp aka simple byte types char, byte etc.
 * 
 */
abstract class BaseInputStream extends InputStream {
    abstract byte[] getBytes() throws SQLServerException;

    // Flag indicating whether the stream conforms to adaptive response buffering API restrictions
    final boolean isAdaptive;

    // Flag indicating whether the stream consumes and discards data as it reads it
    final boolean isStreaming;

    /** Generate the logging ID */
    private String parentLoggingInfo = "";
    private static final AtomicInteger lastLoggingID = new AtomicInteger(0);

    private static int nextLoggingID() {
        return lastLoggingID.incrementAndGet();
    }

    static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.InputStream");;
    private String traceID;

    final public String toString() {
        if (traceID == null)
            traceID = getClass().getName() + "ID:" + nextLoggingID();
        return traceID;
    }

    final void setLoggingInfo(String info) {
        parentLoggingInfo = info;
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString());
    }

    int streamPos = 0;
    int markedStreamPos = 0;
    TDSReaderMark currentMark;
    private ServerDTVImpl dtv;
    TDSReader tdsReader;
    int readLimit = 0;
    boolean isReadLimitSet = false;

    BaseInputStream(TDSReader tdsReader,
            boolean isAdaptive,
            boolean isStreaming,
            ServerDTVImpl dtv) {
        this.tdsReader = tdsReader;
        this.isAdaptive = isAdaptive;
        this.isStreaming = isStreaming;

        if (isAdaptive)
            clearCurrentMark();
        else
            currentMark = tdsReader.mark();
        this.dtv = dtv;
    }

    final void clearCurrentMark() {
        currentMark = null;
        isReadLimitSet = false;
        if (isAdaptive && isStreaming)
            tdsReader.stream();
    }

    void closeHelper() throws IOException {
        if (isAdaptive && null != dtv) {
            if (logger.isLoggable(java.util.logging.Level.FINER))
                logger.finer(toString() + " closing the adaptive stream.");
            dtv.setPositionAfterStreamed(tdsReader);
        }
        currentMark = null;
        tdsReader = null;
        dtv = null;
    }

    /**
     * Verifies stream is open and throws IOException if otherwise.
     */
    final void checkClosed() throws IOException {
        if (null == tdsReader)
            throw new IOException(SQLServerException.getErrString("R_streamIsClosed"));
    }

    /**
     * Tests if this input stream supports the mark and reset methods.
     * 
     * @return true if mark and reset are supported.
     */
    public boolean markSupported() {
        return true;
    }

    void setReadLimit(int readLimit) {
        // we buffer the whole stream in the full case so readlimit is meaningless.
        // spec does not say what to do with -ve values.
        if (isAdaptive && readLimit > 0) {
            this.readLimit = readLimit;
            isReadLimitSet = true;
        }
    }

    /**
     * Resets stream to saved mark position.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     */
    void resetHelper() throws IOException {
        checkClosed();
        // if no mark set already throw
        if (null == currentMark)
            throw new IOException(SQLServerException.getErrString("R_streamWasNotMarkedBefore"));
        tdsReader.reset(currentMark);
    }
}

final class SimpleInputStream extends BaseInputStream {

    // Stated length of the payload
    private final int payloadLength;

    /**
     * Initializes the input stream.
     */
    SimpleInputStream(TDSReader tdsReader,
            int payLoadLength,
            InputStreamGetterArgs getterArgs,
            ServerDTVImpl dtv) throws SQLServerException {
        super(tdsReader, getterArgs.isAdaptive, getterArgs.isStreaming, dtv);
        setLoggingInfo(getterArgs.logContext);
        this.payloadLength = payLoadLength;
    }

    /**
     * Closes the stream releasing all resources held.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     */
    public void close() throws IOException {
        if (null == tdsReader)
            return;
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + "Enter Closing SimpleInputStream.");

        // Discard the remainder of the stream, positioning the TDSReader
        // at the next item in the TDS response. Once the stream is closed,
        // it can no longer access the discarded response data.
        skip(payloadLength - streamPos);

        closeHelper();
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + "Exit Closing SimpleInputStream.");
    }

    /**
     * Checks if we have EOS state.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     */
    private boolean isEOS() throws IOException {
        assert streamPos <= payloadLength;
        return (streamPos == payloadLength);
    }

    // java.io.InputStream interface methods.

    /**
     * Skips over and discards n bytes of data from this input stream.
     * 
     * @param n
     *            the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public long skip(long n) throws IOException {
        checkClosed();
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + " Skipping :" + n);
        if (n < 0)
            return 0L;
        if (isEOS())
            return 0;

        int skipAmount;
        if (streamPos + n > payloadLength) {
            skipAmount = payloadLength - streamPos;
        }
        else {
            skipAmount = (int) n;
        }
        try {
            tdsReader.skip(skipAmount);
        }
        catch (SQLServerException e) {
            throw new IOException(e.getMessage());
        }
        streamPos += skipAmount;
        if (isReadLimitSet && ((streamPos - markedStreamPos) > readLimit))
            clearCurrentMark();

        return skipAmount;

    }

    /**
     * Returns the number of bytes that can be read (or skipped over) from this input stream without blocking by the next caller of a method for this
     * input stream.
     * 
     * @return the actual number of bytes available.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public int available() throws IOException {
        checkClosed();
        assert streamPos <= payloadLength;

        int available = payloadLength - streamPos;
        if (tdsReader.available() < available)
            available = tdsReader.available();
        return available;
    }

    private byte[] bSingleByte;

    /**
     * Reads the next byte of data from the input stream.
     * 
     * @return the byte read or -1 meaning no more bytes.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public int read() throws IOException {
        checkClosed();
        if (null == bSingleByte)
            bSingleByte = new byte[1];
        if (isEOS())
            return -1;
        int bytesRead = read(bSingleByte, 0, 1);
        return (0 == bytesRead) ? -1 : (bSingleByte[0] & 0xFF);
    }

    /**
     * Reads available data into supplied byte array.
     * 
     * @param b
     *            array of bytes to fill.
     * @return the number of bytes read or -1 meaning no bytes read.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public int read(byte[] b) throws IOException {
        checkClosed();
        return read(b, 0, b.length);
    }

    /**
     * Reads available data into supplied byte array.
     * 
     * @param b
     *            array of bytes to fill.
     * @param offset
     *            the offset into array b where to start writing.
     * @param maxBytes
     *            the max number of bytes to write into b.
     * @return the number of bytes read or -1 meaning no bytes read.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public int read(byte b[],
            int offset,
            int maxBytes) throws IOException {
        checkClosed();
        if (logger.isLoggable(java.util.logging.Level.FINER))
            logger.finer(toString() + " Reading " + maxBytes + " from stream offset " + streamPos + " payload length " + payloadLength);

        if (offset < 0 || maxBytes < 0 || offset + maxBytes > b.length)
            throw new IndexOutOfBoundsException();

        if (0 == maxBytes)
            return 0;
        if (isEOS())
            return -1;

        int readAmount;
        if (streamPos + maxBytes > payloadLength) {
            readAmount = payloadLength - streamPos;
        }
        else {
            readAmount = maxBytes;
        }

        try {
            tdsReader.readBytes(b, offset, readAmount);
        }
        catch (SQLServerException e) {
            throw new IOException(e.getMessage());
        }
        streamPos += readAmount;

        if (isReadLimitSet && ((streamPos - markedStreamPos) > readLimit))
            clearCurrentMark();

        return readAmount;
    }

    /**
     * Marks the current position in this input stream.
     * 
     * @param readLimit
     *            the number of bytes to hold
     */
    public void mark(int readLimit) {
        if (null != tdsReader && readLimit > 0) {
            currentMark = tdsReader.mark();
            markedStreamPos = streamPos;
            setReadLimit(readLimit);
        }
    }

    /**
     * Resets stream to saved mark position.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     */
    public void reset() throws IOException {
        resetHelper();
        streamPos = markedStreamPos;
    }

    /**
     * Helper function to convert the entire PLP stream into a contiguous byte array. This call is inefficient (in terms of memory usage and run time)
     * for very large PLPs. Use it only if a contiguous byte array is required.
     */
    final byte[] getBytes() throws SQLServerException {
        // We should always retrieve the entire stream, and only once.
        assert 0 == streamPos;

        byte[] value = new byte[payloadLength];
        try {
            read(value);
            close();
        }
        catch (IOException e) {
            SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
        }

        return value;
    }

}
