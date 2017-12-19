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
import java.io.Reader;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.text.MessageFormat;

/**
 * InputStream adapter for Readers.
 *
 * This class implements an InputStream whose bytes are encoded character values that are read on demand from a wrapped Reader using the suplied
 * Charset.
 *
 * Character values pass through through the following in their transformation to bytes: Reader .. CharBuffer .. Charset (CharsetEncoder) ..
 * ByteBuffer .. InputStream
 *
 * To minimize memory usage, the CharBuffer and ByteBuffer instances used by this class are created on demand when InputStream read methods are
 * called.
 */
class ReaderInputStream extends InputStream {
    // The Reader that this ReaderInputStream adapts.
    private final Reader reader;

    // The character set used to encode character values as
    // they are read from the stream.
    private final Charset charset;

    // Length of the Reader, if known, in characters
    private final long readerLength;

    // Count of characters read from the reader across all calls to encodeChars()
    private long readerCharsRead = 0;

    // Flag indicating whether the stream has reached the end of its data
    private boolean atEndOfStream = false;

    // Internal character buffer used to transfer character values
    // between the Reader and the Charset encoder.
    private CharBuffer rawChars = null;
    private static final int MAX_CHAR_BUFFER_SIZE = DataTypes.SHORT_VARTYPE_MAX_CHARS;

    // Most recent set of bytes that were encoded from rawChars.
    // This value is null initially and when the end of stream is reached.
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private ByteBuffer encodedChars = EMPTY_BUFFER;

    ReaderInputStream(Reader reader,
            Charset charset,
            long readerLength) {
        assert reader != null;
        assert charset != null;
        assert DataTypes.UNKNOWN_STREAM_LENGTH == readerLength || readerLength >= 0;

        this.reader = reader;
        this.charset = charset;
        this.readerLength = readerLength;
    }

    /**
     * Returns the number of bytes that can be read (or skipped over) from this input stream without blocking by the next caller of a method for this
     * input stream.
     *
     * @return - the number of bytes that can be read from this input stream without blocking
     * @throws IOException
     *             - if an I/O error occurs
     */
    public int available() throws IOException {
        assert null != reader;
        assert null != encodedChars;

        // If we know the reader to be empty, then take the short cut
        if (0 == readerLength)
            return 0;

        // If there are encoded characters remaining in the buffer then that's our best guess.
        if (encodedChars.remaining() > 0)
            return encodedChars.remaining();

        // If there are no encoded characters left in the buffer (or the buffer hasn't yet been populated)
        // then ask the Reader whether a call to its read() method would block. If reading wouldn't block,
        // then there's at least 1 byte that can be encoded, possibly more.
        if (reader.ready())
            return 1;

        // If there are no encoded characters, and reading characters from the underlying Reader object
        // would block, then nothing (more) can be read from this stream without blocking.
        return 0;
    }

    private final byte[] oneByte = new byte[1];

    public int read() throws IOException {
        return (-1 == readInternal(oneByte, 0, oneByte.length)) ? -1 : oneByte[0];
    }

    public int read(byte[] b) throws IOException {
        return readInternal(b, 0, b.length);
    }

    public int read(byte[] b,
            int off,
            int len) throws IOException {
        return readInternal(b, off, len);
    }

    private int readInternal(byte[] b,
            int off,
            int len) throws IOException {
        assert null != b;
        assert 0 <= off && off <= b.length;
        assert 0 <= len && len <= b.length;
        assert off <= b.length - len;

        if (0 == len)
            return 0;

        int bytesRead = 0;
        while (bytesRead < len && encodeChars()) {
            // Read the lesser of the number of bytes remaining
            // in the encoded character buffer and the number
            // of bytes remaining for this read request.
            int bytesToRead = encodedChars.remaining();
            if (bytesToRead > len - bytesRead)
                bytesToRead = len - bytesRead;

            // We should actually be attempting to read something here,
            // or we'll be in an infinite loop...
            assert bytesToRead > 0;

            encodedChars.get(b, off + bytesRead, bytesToRead);
            bytesRead += bytesToRead;
        }

        // Return number of bytes read, which may be less than
        // the number of bytes requested, or -1 at end of stream.
        return (0 == bytesRead && atEndOfStream) ? -1 : bytesRead;
    }

    /**
     * Determines whether encoded characters are available, encoding them on demand by reading them from the reader as necessary.
     *
     * @return true when encoded characters are available
     * @return false when no more encoded characters are available (i.e. end of stream)
     * @exception IOException
     *                if an I/O error occurs reading from the reader or encoding the characters
     */
    private boolean encodeChars() throws IOException {
        // Once at the end of the stream, no more characters can be encoded.
        if (atEndOfStream)
            return false;

        // Not at end of stream; check whether there are any encoded characters
        // remaining in the byte buffer. If there are, don't encode any more
        // characters this time.
        if (encodedChars.hasRemaining())
            return true;

        // Encoded byte buffer is either exhausted or has never been filled
        // (i.e. first time through). In that case, we need to repopulate
        // the encoded character buffer by encoding raw characters.
        //
        // To do that, there needs to be raw characters available to encode.
        // If there are no raw characters available (because the raw character
        // buffer has been exhausted or never filled), then try to read in
        // raw characters from the reader.
        if (null == rawChars || !rawChars.hasRemaining()) {
            if (null == rawChars) {
                assert MAX_CHAR_BUFFER_SIZE <= Integer.MAX_VALUE;
                rawChars = CharBuffer.allocate((DataTypes.UNKNOWN_STREAM_LENGTH == readerLength || readerLength > MAX_CHAR_BUFFER_SIZE)
                        ? MAX_CHAR_BUFFER_SIZE : Math.max((int) readerLength, 1));
            }
            else {
                // Flip the buffer to be ready for put (reader read) operations.
                ((Buffer)rawChars).clear();
            }

            // Try to fill up the raw character buffer by reading available characters
            // from the reader into it.
            //
            // This loop continues until one of the following conditions is satisfied:
            // - the raw character buffer has been filled,
            // - the reader reaches end-of-stream
            // - the reader throws any kind of Exception (driver throws an IOException)
            // - the reader violates its interface contract (driver throws an IOException)
            while (rawChars.hasRemaining()) {
                int lastPosition = ((Buffer)rawChars).position();
                int charsRead = 0;

                // Try reading from the app-supplied Reader
                try {
                    charsRead = reader.read(rawChars);
                }

                // Catch any kind of exception and translate it to an IOException.
                // The app-supplied reader cannot be trusted just to throw IOExceptions...
                catch (Exception e) {
                    String detailMessage = e.getMessage();
                    if (null == detailMessage)
                        detailMessage = SQLServerException.getErrString("R_streamReadReturnedInvalidValue");
                    IOException ioException = new IOException(detailMessage);
                    ioException.initCause(e);
                    throw ioException;
                }

                if (charsRead < -1 || 0 == charsRead)
                    throw new IOException(SQLServerException.getErrString("R_streamReadReturnedInvalidValue"));

                if (-1 == charsRead) {
                    // If the reader violates its interface contract then throw an exception.
                    if (((Buffer)rawChars).position() != lastPosition)
                        throw new IOException(SQLServerException.getErrString("R_streamReadReturnedInvalidValue"));

                    // Check that the reader has returned exactly the amount of data we expect
                    if (DataTypes.UNKNOWN_STREAM_LENGTH != readerLength && 0 != readerLength - readerCharsRead) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
                        throw new IOException(form.format(new Object[] {readerLength, readerCharsRead}));
                    }

                    // If there are no characters left to encode then we're done.
                    if (0 == ((Buffer)rawChars).position()) {
                        rawChars = null;
                        atEndOfStream = true;
                        return false;
                    }

                    // Otherwise, we've filled the buffer as much as we can.
                    break;
                }

                assert charsRead > 0;

                // If the reader violates its interface contract then throw an exception.
                if (charsRead != ((Buffer)rawChars).position() - lastPosition)
                    throw new IOException(SQLServerException.getErrString("R_streamReadReturnedInvalidValue"));

                // Check that the reader isn't trying to return more data than we expect
                if (DataTypes.UNKNOWN_STREAM_LENGTH != readerLength && charsRead > readerLength - readerCharsRead) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
                    throw new IOException(form.format(new Object[] {readerLength, readerCharsRead}));
                }

                readerCharsRead += charsRead;
            }

            // The raw character buffer may now have characters available for encoding.
            // Flip the buffer back to be ready for get (charset encode) operations.
            ((Buffer)rawChars).flip();
        }

        // If the raw character buffer remains empty, despite our efforts to (re)populate it,
        // then no characters can be encoded at this time. This can happen if the reader reports
        // that no characters were ready to be read.
        if (!rawChars.hasRemaining())
            return false;

        // Raw characters are now available to be encoded, so encode them.
        encodedChars = charset.encode(rawChars);
        return true;
    }
}
