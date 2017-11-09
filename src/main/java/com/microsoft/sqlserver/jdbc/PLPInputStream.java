/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * PLPInputStream is an InputStream implementation that reads from a TDS PLP stream.
 * 
 * Note PLP stands for Partially Length-prefixed Bytes. TDS 7.2 introduced this new streaming format for streaming of large types such as
 * varchar(max), nvarchar(max), varbinary(max) and XML.
 * 
 * See TDS specification, 6.3.3 Datatype Dependant Data Streams: Partially Length-prefixed Bytes for more details on the PLP format.
 */

class PLPInputStream extends BaseInputStream {
    static final long PLP_NULL = 0xFFFFFFFFFFFFFFFFL;
    static final long UNKNOWN_PLP_LEN = 0xFFFFFFFFFFFFFFFEL;
    static final int PLP_TERMINATOR = 0x00000000;
    private final static byte[] EMPTY_PLP_BYTES = new byte[0];

    // Stated length of the PLP stream payload; -1 if unknown length.
    int payloadLength;

    private static final int PLP_EOS = -1;
    private int currentChunkRemain;

    private int markedChunkRemain;
    private int leftOverReadLimit = 0;

    private byte[] oneByteArray = new byte[1];

    /**
     * Non-destructive method for checking whether a PLP value at the current TDSReader location is null.
     */
    final static boolean isNull(TDSReader tdsReader) throws SQLServerException {
        TDSReaderMark mark = tdsReader.mark();
        //Temporary stream cannot get closes, since it closes the main stream. 
        try {
            return null == PLPInputStream.makeTempStream(tdsReader, false, null);
        }
        finally {
            tdsReader.reset(mark);
        }
    }

    /**
     * Create a new input stream.
     * 
     * @param tdsReader
     *            TDS reader pointing at the start of the PLP data
     * @param discardValue
     *            boolean to represent if base input stream is adaptive and is streaming
     * @param dtv
     *            DTV implementation for values set from the TDS response stream.
     * @return PLPInputStream that is created
     * @throws SQLServerException
     *             when an error occurs
     */
    final static PLPInputStream makeTempStream(TDSReader tdsReader,
            boolean discardValue,
            ServerDTVImpl dtv) throws SQLServerException {
        return makeStream(tdsReader, discardValue, discardValue, dtv);
    }

    final static PLPInputStream makeStream(TDSReader tdsReader,
            InputStreamGetterArgs getterArgs,
            ServerDTVImpl dtv) throws SQLServerException {
        PLPInputStream is = makeStream(tdsReader, getterArgs.isAdaptive, getterArgs.isStreaming, dtv);
        if (null != is)
            is.setLoggingInfo(getterArgs.logContext);
        return is;
    }

    private static PLPInputStream makeStream(TDSReader tdsReader,
            boolean isAdaptive,
            boolean isStreaming,
            ServerDTVImpl dtv) throws SQLServerException {
        // Read total length of PLP stream.
        long payloadLength = tdsReader.readLong();

        // If length is PLP_NULL, then return a null PLP value.
        if (PLP_NULL == payloadLength)
            return null;

        return new PLPInputStream(tdsReader, payloadLength, isAdaptive, isStreaming, dtv);
    }

    /**
     * Initializes the input stream.
     */
    PLPInputStream(TDSReader tdsReader,
            long statedPayloadLength,
            boolean isAdaptive,
            boolean isStreaming,
            ServerDTVImpl dtv) throws SQLServerException {
        super(tdsReader, isAdaptive, isStreaming, dtv);
        this.payloadLength = (UNKNOWN_PLP_LEN != statedPayloadLength) ? ((int) statedPayloadLength) : -1;
        this.currentChunkRemain = this.markedChunkRemain = 0;
    }

    /**
     * Helper function to convert the entire PLP stream into a contiguous byte array. This call is inefficient (in terms of memory usage and run time)
     * for very large PLPs. Use it only if a contiguous byte array is required.
     */
    byte[] getBytes() throws SQLServerException {
        byte[] value;

        // The following 0-byte read just ensures that the number of bytes
        // remaining in the current chunk is known.
        readBytesInternal(null, 0, 0);

        if (PLP_EOS == currentChunkRemain) {
            value = EMPTY_PLP_BYTES;
        }
        else {
            // If the PLP payload length is known, allocate the final byte array now.
            // Otherwise, start with the size of the first chunk. Additional chunks
            // will cause the array to be reallocated & copied.
            value = new byte[(-1 != payloadLength) ? payloadLength : currentChunkRemain];

            int bytesRead = 0;
            while (PLP_EOS != currentChunkRemain) {
                // If the current byte array isn't large enough to hold
                // the contents of the current chunk, then make it larger.
                if (value.length == bytesRead) {
                    byte[] newValue = new byte[bytesRead + currentChunkRemain];
                    System.arraycopy(value, 0, newValue, 0, bytesRead);
                    value = newValue;
                }

                bytesRead += readBytesInternal(value, bytesRead, currentChunkRemain);
            }
        }

        // Always close the stream after retrieving it
        try {
            close();
        }
        catch (IOException e) {
            SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
        }

        return value;
    }

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
        if (n < 0)
            return 0L;
        if (n > Integer.MAX_VALUE)
            n = Integer.MAX_VALUE;

        long bytesread = readBytes(null, 0, (int) n);

        if (-1 == bytesread)
            return 0;
        else
            return bytesread;
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
        try {

            // The following 0-byte read just ensures that the number of bytes
            // remaining in the current chunk is known.
            if (0 == currentChunkRemain)
                readBytesInternal(null, 0, 0);

            if (PLP_EOS == currentChunkRemain)
                return 0;

            // Return the lesser of the number of bytes available for reading
            // from the underlying TDSReader and the number of bytes left in
            // the current chunk.
            int available = tdsReader.available();
            if (available > currentChunkRemain)
                available = currentChunkRemain;

            return available;
        }
        catch (SQLServerException e) {
            throw new IOException(e.getMessage());
        }

    }

    /**
     * Reads the next byte of data from the input stream.
     * 
     * @return the byte read or -1 meaning no more bytes.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public int read() throws IOException {
        checkClosed();

        if (-1 != readBytes(oneByteArray, 0, 1))
            return oneByteArray[0] & 0xFF;
        return -1;
    }

    /**
     * Reads available data into supplied byte array.
     * 
     * @param b
     *            array of bytes to fill.
     * @return the number of bytes read or 0 meaning no bytes read.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public int read(byte[] b) throws IOException {
        // If b is null, a NullPointerException is thrown.
        if (null == b)
            throw new NullPointerException();

        checkClosed();

        return readBytes(b, 0, b.length);
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
     * @return the number of bytes read or 0 meaning no bytes read.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public int read(byte b[],
            int offset,
            int maxBytes) throws IOException {
        // If b is null, a NullPointerException is thrown.
        if (null == b)
            throw new NullPointerException();

        // Verify offset and maxBytes against target buffer if we're reading (as opposed to skipping).
        // If offset is negative, or maxBytes is negative, or offset+maxBytes
        // is greater than the length of the array b, then an IndexOutOfBoundsException is thrown.
        if (offset < 0 || maxBytes < 0 || offset + maxBytes > b.length)
            throw new IndexOutOfBoundsException();

        checkClosed();

        return readBytes(b, offset, maxBytes);
    }

    /**
     * Reads available data into supplied byte array b.
     * 
     * @param b
     *            array of bytes to fill. If b is null, method will skip over data.
     * @param offset
     *            the offset into array b where to start writing.
     * @param maxBytes
     *            the max number of bytes to write into b.
     * @return the number of bytes read or 0 meaning no bytes read or -1 meaning EOS.
     * @exception IOException
     *                if an I/O error occurs.
     */
    int readBytes(byte[] b,
            int offset,
            int maxBytes) throws IOException {
        // If maxBytes is zero, then no bytes are read and 0 is returned
        // This must be done here rather than in readBytesInternal since a 0-byte read
        // there may return -1 at EOS.
        if (0 == maxBytes)
            return 0;

        try {
            return readBytesInternal(b, offset, maxBytes);
        }
        catch (SQLServerException e) {
            throw new IOException(e.getMessage());
        }
    }

    private int readBytesInternal(byte b[],
            int offset,
            int maxBytes) throws SQLServerException {
        // If we're at EOS, say so.
        // Note: For back compat, this special case needs to always be handled
        // before checking user-supplied arguments below.
        if (PLP_EOS == currentChunkRemain)
            return -1;

        // Save off the current TDSReader position, wherever it is, and start reading
        // from where we left off last time.

        int bytesRead = 0;
        for (;;) {
            // Check that we have bytes left to read from the current chunk.
            // If not then figure out the size of the next chunk or
            // determine that we have reached the end of the stream.
            if (0 == currentChunkRemain) {
                currentChunkRemain = (int) tdsReader.readUnsignedInt();
                assert currentChunkRemain >= 0;
                if (0 == currentChunkRemain) {
                    currentChunkRemain = PLP_EOS;
                    break;
                }
            }

            if (bytesRead == maxBytes)
                break;

            // Now we know there are bytes to be read in the current chunk.
            // Further limit the max number of bytes we can read to whatever
            // remains in the current chunk.
            int bytesToRead = maxBytes - bytesRead;
            if (bytesToRead > currentChunkRemain)
                bytesToRead = currentChunkRemain;

            // Skip/Read as many bytes as we can, given the constraints.
            if (null == b)
                tdsReader.skip(bytesToRead);
            else
                tdsReader.readBytes(b, offset + bytesRead, bytesToRead);

            bytesRead += bytesToRead;
            currentChunkRemain -= bytesToRead;
        }

        if (bytesRead > 0) {
            if (isReadLimitSet && leftOverReadLimit > 0) {
                leftOverReadLimit = leftOverReadLimit - bytesRead;
                if (leftOverReadLimit < 0)
                    clearCurrentMark();
            }
            return bytesRead;
        }

        if (PLP_EOS == currentChunkRemain)
            return -1;

        return 0;
    }

    /**
     * Marks the current position in this input stream.
     * 
     * @param readlimit
     *            the number of bytes to hold (this implementation ignores this).
     */
    public void mark(int readLimit) {
        // Save off current position and how much of the current chunk remains
        // cant throw if the tdsreader is null
        if (null != tdsReader && readLimit > 0) {
            currentMark = tdsReader.mark();
            markedChunkRemain = currentChunkRemain;
            leftOverReadLimit = readLimit;
            setReadLimit(readLimit);
        }
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

        while (skip(tdsReader.getConnection().getTDSPacketSize()) != 0)
            ;
        // Release ref to tdsReader and parentRS here, shut down stream state.
        closeHelper();
    }

    /**
     * Resets stream to saved mark position.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     */
    public void reset() throws IOException {
        resetHelper();
        leftOverReadLimit = readLimit;
        currentChunkRemain = markedChunkRemain;
    }
}

/**
 * Implements an XML binary stream with BOM header.
 * 
 * Class extends a normal PLPInputStream class and prepends the XML BOM (0xFFFE) token then steps out of the way and forwards the rest of the
 * InputStream calls to the super class PLPInputStream.
 */
final class PLPXMLInputStream extends PLPInputStream {
    // XML BOM header (the first two header bytes sent to caller).
    private final static byte[] xmlBOM = {(byte) 0xFF, (byte) 0xFE};
    private final ByteArrayInputStream bomStream = new ByteArrayInputStream(xmlBOM);

    final static PLPXMLInputStream makeXMLStream(TDSReader tdsReader,
            InputStreamGetterArgs getterArgs,
            ServerDTVImpl dtv) throws SQLServerException {
        // Read total length of PLP stream.
        long payloadLength = tdsReader.readLong();

        // If length is PLP_NULL, then return a null PLP value.
        if (PLP_NULL == payloadLength)
            return null;

        PLPXMLInputStream is = new PLPXMLInputStream(tdsReader, payloadLength, getterArgs, dtv);
        is.setLoggingInfo(getterArgs.logContext);

        return is;
    }

    PLPXMLInputStream(TDSReader tdsReader,
            long statedPayloadLength,
            InputStreamGetterArgs getterArgs,
            ServerDTVImpl dtv) throws SQLServerException {
        super(tdsReader, statedPayloadLength, getterArgs.isAdaptive, getterArgs.isStreaming, dtv);
    }

    public void close() throws IOException {
        super.close();
    }

    int readBytes(byte[] b,
            int offset,
            int maxBytes) throws IOException {
        assert offset >= 0;
        assert maxBytes >= 0;
        // If maxBytes is zero, then no bytes are read and 0 is returned.
        if (0 == maxBytes)
            return 0;

        int bytesRead = 0;
        int xmlBytesRead = 0;

        // Read/Skip BOM bytes first. When all BOM bytes have been consumed ...
        if (null == b) {
            for (int bomBytesSkipped; bytesRead < maxBytes
                    && 0 != (bomBytesSkipped = (int) bomStream.skip(((long) maxBytes) - ((long) bytesRead))); bytesRead += bomBytesSkipped)
                ;
        }
        else {
            for (int bomBytesRead; bytesRead < maxBytes
                    && -1 != (bomBytesRead = bomStream.read(b, offset + bytesRead, maxBytes - bytesRead)); bytesRead += bomBytesRead)
                ;
        }

        // ... then read/skip bytes from the underlying PLPInputStream
        for (; bytesRead < maxBytes && -1 != (xmlBytesRead = super.readBytes(b, offset + bytesRead, maxBytes - bytesRead)); bytesRead += xmlBytesRead)
            ;

        if (bytesRead > 0)
            return bytesRead;

        // No bytes read - should have been EOF since 0-byte reads are handled above
        assert -1 == xmlBytesRead;
        return -1;
    }

    public void mark(int readLimit) {
        bomStream.mark(xmlBOM.length);
        super.mark(readLimit);
    }

    public void reset() throws IOException {
        bomStream.reset();
        super.reset();
    }

    /**
     * Helper function to convert the entire PLP stream into a contiguous byte array. This call is inefficient (in terms of memory usage and run time)
     * for very large PLPs. Use it only if a contiguous byte array is required.
     */
    byte[] getBytes() throws SQLServerException {
        // Look to see if the BOM has been read
        byte[] bom = new byte[2];
        try {
            int bytesread = bomStream.read(bom);
            byte[] valueWithoutBOM = super.getBytes();

            if (bytesread > 0) {
                assert 2 == bytesread;
                byte[] valueWithBOM = new byte[valueWithoutBOM.length + bytesread];
                System.arraycopy(bom, 0, valueWithBOM, 0, bytesread);
                System.arraycopy(valueWithoutBOM, 0, valueWithBOM, bytesread, valueWithoutBOM.length);
                return valueWithBOM;
            }
            else
                return valueWithoutBOM;
        }
        catch (IOException e) {
            SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
        }

        return null;
    }
}
