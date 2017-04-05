/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.microsoft.sqlserver.testframework.Utils.DBBinaryStream;
import com.microsoft.sqlserver.testframework.Utils.DBCharacterStream;

/**
 * Prepared invalid stream types
 */
public class DBInvalidUtil {
    public static final Logger log = Logger.getLogger("DBInvalidUtils");

    /**
     * 
     * InvalidClob : stream with invalid behavior 1) getCharacterStream returns InvalidCharacterStream 2) length returns invalid lengths
     *
     */
    public class InvalidClob implements Clob {
        final int diff = 5;

        Object expected = null;
        public long length;
        private long actualLength;
        private long invalidLength = -1;
        private boolean returnValid = false;
        public InvalidCharacterStream stream = null; // keep a handle on any stream

        /**
         * Constructor
         * 
         * @param expected
         * @param returnValid
         */
        public InvalidClob(Object expected,
                boolean returnValid) {
            this.expected = expected;
            actualLength = this.value().length();
            this.returnValid = returnValid;
        }

        protected String value() {
            return ((String) expected);
        }

        /**
         * return length
         */
        public long length() throws SQLException {
            long ret = actualLength;
            long actual = ret;
            if (invalidLength == -1) {
                int choose = ThreadLocalRandom.current().nextInt(5);
                int randomInt = 1 + ThreadLocalRandom.current().nextInt(diff);

                switch (choose) {
                    case 0: // more than ret
                        actual = ret + randomInt;
                        break;
                    case 1: // less than ret
                        actual = ret - randomInt;
                        break;
                    case 2: // 0
                        actual = 0;
                        break;
                    case 3: // return > SQL Server Limit
                        actual = Long.MAX_VALUE;
                        break;
                    default: // always < -1
                        actual = -1 - randomInt;
                }
                invalidLength = actual;
                length = actual;
                returnValid = true;
            }

            log.fine("invalidClob.length(): Actual chars=" + actualLength + " Returned chars=" + length);
            return length;
        }

        public Reader getCharacterStream() throws SQLException {
            stream = new InvalidCharacterStream(this.value(), returnValid);
            return stream;
        }

        @Override
        public String getSubString(long pos,
                int length) throws SQLException {
            assertTrue(false, "Not implemented");
            return null;
        }

        @Override
        public InputStream getAsciiStream() throws SQLException {
            assertTrue(false, "Not implemented");
            return null;
        }

        @Override
        public long position(String searchstr,
                long start) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public long position(Clob searchstr,
                long start) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public int setString(long pos,
                String str) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public int setString(long pos,
                String str,
                int offset,
                int len) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public OutputStream setAsciiStream(long pos) throws SQLException {
            assertTrue(false, "Not implemented");
            return null;
        }

        @Override
        public Writer setCharacterStream(long pos) throws SQLException {
            assertTrue(false, "Not implemented");
            return null;
        }

        @Override
        public void truncate(long len) throws SQLException {
            assertTrue(false, "Not implemented");

        }

        @Override
        public void free() throws SQLException {
            assertTrue(false, "Not implemented");

        }

        @Override
        public Reader getCharacterStream(long pos,
                long length) throws SQLException {
            assertTrue(false, "Not implemented");
            return null;
        }
    }

    /**
     * 
     * invalidCharacterStream : stream with invalid behavior 1) Read can throw IOException 2) Read can return data length > or < than actual
     */
    public class InvalidCharacterStream extends DBCharacterStream {
        final int diff = 5;

        private boolean returnValid = false;   // Perfom invalid actions at most once
        public boolean threwException = false;
        public static final String IOExceptionMsg = "invalidCharacterStream.read() throws IOException";

        // Constructor
        public InvalidCharacterStream(String value,
                boolean returnValid) {
            super(value);
            this.returnValid = returnValid;
        }

        public int read(char[] cbuf,
                int off,
                int len) throws IOException {
            int ret = super.read(cbuf, off, len);
            int actual = ret;
            if (!returnValid) {
                int choose = ThreadLocalRandom.current().nextInt(5);
                int randomInt = 1 + ThreadLocalRandom.current().nextInt(diff);

                switch (choose) {
                    case 0: // more than ret
                        actual = ret + randomInt;
                        break;
                    case 1: // less than ret
                        actual = ret - randomInt;
                        break;
                    case 2: // 0
                        actual = 0;
                        break;
                    case 3: // always < -1
                        actual = -1 - randomInt;
                        break;
                    default:
                        log.fine(IOExceptionMsg);
                        threwException = true;
                        throw new IOException(IOExceptionMsg);
                }
                returnValid = true;
            }
            log.fine("invalidCharacterStream.read(): Actual bytes=" + ret + " Returned bytes=" + actual);
            return actual;
        }
    }

    /**
     * InvalidBlob : stream with invalid behavior 1) getBinaryStream returns InvalidBinaryStream 2) Length returns invalid lengths
     */
    public class InvalidBlob implements Blob {
        final int diff = 5;

        private Object expected = null;
        public long length;
        private long actualLength;
        private long invalidLength = -1;
        private boolean returnValid = false;
        private InvalidBinaryStream stream = null; // keep a handle on any stream

        // Constructor
        public InvalidBlob(Object expected,
                boolean returnValid) {
            this.expected = expected;
            actualLength = this.value().length;
            this.returnValid = returnValid;
        }

        protected byte[] value() {
            return ((byte[]) expected);
        }

        @Override
        public long length() throws SQLException {
            long ret = actualLength;
            long actual = ret;
            if (invalidLength == -1) {
                int choose = ThreadLocalRandom.current().nextInt(5);
                int randomInt = 1 + ThreadLocalRandom.current().nextInt(diff);

                switch (choose) {
                    case 0: // more than ret
                        actual = ret + randomInt;
                        break;
                    case 1: // less than ret
                        actual = ret - randomInt;
                        break;
                    case 2: // 0
                        actual = 0;
                        break;
                    case 3: // return > SQL Server Limit
                        actual = Long.MAX_VALUE;
                        break;
                    default: // always < -1
                        actual = -1 - randomInt;
                }
                invalidLength = actual;
                length = actual;
                returnValid = true;
            }

            log.fine("invalidBlob.length(): Actual bytes=" + actualLength + " Returned bytes=" + length);
            return length;
        }

        @Override
        public InputStream getBinaryStream() throws SQLException {
            stream = new InvalidBinaryStream(this.value(), returnValid);
            return stream;
        }

        @Override
        public byte[] getBytes(long pos,
                int length) throws SQLException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public long position(byte[] pattern,
                long start) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public long position(Blob pattern,
                long start) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public int setBytes(long pos,
                byte[] bytes) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public int setBytes(long pos,
                byte[] bytes,
                int offset,
                int len) throws SQLException {
            assertTrue(false, "Not implemented");
            return 0;
        }

        @Override
        public OutputStream setBinaryStream(long pos) throws SQLException {
            assertTrue(false, "Not implemented");
            return null;
        }

        @Override
        public void truncate(long len) throws SQLException {
            assertTrue(false, "Not implemented");
        }

        @Override
        public void free() throws SQLException {
            assertTrue(false, "Not implemented");

        }

        @Override
        public InputStream getBinaryStream(long pos,
                long length) throws SQLException {
            assertTrue(false, "Not implemented");
            return null;
        }

    }

    /**
     * invalidBinaryStream : stream with invalid behavior Read can return data length > or < than actual
     *
     */
    public class InvalidBinaryStream extends DBBinaryStream {
        final int diff = 5;
        private boolean _returnValid = false;   // Perfom invalid actions at most once

        /**
         * Constructor
         * 
         * @param value
         * @param returnValid
         */
        public InvalidBinaryStream(byte[] value,
                boolean returnValid) {
            super(value);
            _returnValid = returnValid;
        }

        @Override
        public int read(byte[] bytes,
                int off,
                int len) {
            int ret = super.read(bytes, off, len);
            int actual = ret;
            if (!_returnValid) {
                int choose = ThreadLocalRandom.current().nextInt(4);
                int randomInt = 1 + ThreadLocalRandom.current().nextInt(diff);
                switch (choose) {
                    case 0: // greater than ret
                        actual = ret + randomInt;
                        break;
                    case 1: // less than ret
                        actual = ret - randomInt;
                        break;
                    case 2: // always < -1
                        actual = -1 - randomInt;
                        break;
                    default: // 0
                        actual = 0;
                }
                // Return invalid only once per stream
                _returnValid = true;
            }
            log.fine("invalidBinaryStream.read(): Actual bytes=" + ret + " Returned bytes=" + actual);
            return actual;
        }
    }

}