/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * ParameterUtils provides utility a set of methods to manipulate parameter values.
 */

final class ParameterUtils {
    static byte[] HexToBin(String hexV) throws SQLServerException {
        int len = hexV.length();
        char orig[] = hexV.toCharArray();
        if ((len % 2) != 0)
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_stringNotInHex"), null, false);
        byte[] bin = new byte[len / 2];
        for (int i = 0; i < len / 2; i++) {
            bin[i] = (byte) ((CharToHex(orig[2 * i]) << 4) + CharToHex(orig[2 * i + 1]));
        }
        return bin;
    }

    // conversion routine valid values 0-9 a-f A-F
    // throws exception when failed to convert
    //
    static byte CharToHex(char CTX) throws SQLServerException {
        byte ret = 0;
        if (CTX >= 'A' && CTX <= 'F') {
            ret = (byte) (CTX - 'A' + 10);
        }
        else if (CTX >= 'a' && CTX <= 'f') {
            ret = (byte) (CTX - 'a' + 10);
        }
        else if (CTX >= '0' && CTX <= '9') {
            ret = (byte) (CTX - '0');
        }
        else {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_stringNotInHex"), null, false);
        }
        return ret;
    }

    /**
     * Locates the first occurrence of [c] in [sql] starting at [offset], where [sql] is a SQL statement string, which may contain any combination of:
     *
     * - Literals, enclosed in single quotes (') - Literals, enclosed in double quotes (") - Escape sequences, enclosed in square brackets ([]) -
     * Escaped escapes or literal delimiters (i.e. '', "", or ]]) in the above - Single-line comments, beginning in -- and continuing to EOL -
     * Multi-line comments, enclosed in C-style comment delimiters
     *
     * and [c] is not contained any of the above.
     *
     * @param c
     *            the character to search for
     * @param sql
     *            the SQL string to search in
     * @param offset
     *            the offset into [sql] to start searching from
     * @return Offset into [sql] where [c] occurs, or sql.length if [c] is not found.
     * @throws SQLServerException
     *             when [sql] does not follow
     */
    @SuppressWarnings({"fallthrough"})
    static int scanSQLForChar(char ch,
            String sql,
            int offset) {
        char chQuote;
        char chTmp;
        final int len = sql.length();

        while (offset < len) {
            switch (chTmp = sql.charAt(offset++)) {
                case '/':
                    if (offset == len)
                        break;

                    if (sql.charAt(offset) == '*') {   // If '/* ... */' comment
                        while (++offset < len) {   // Go thru comment.
                            if (sql.charAt(offset) == '*' && offset + 1 < len && sql.charAt(offset + 1) == '/') {   // If end of comment
                                offset += 2;
                                break;
                            }
                        }
                        break;
                    }
                    else if (sql.charAt(offset) == '-')
                        break;

                    // Fall through - will fail next if and end up in default case
                case '-':
                    if (sql.charAt(offset) == '-') {   // If '-- ... \n' comment
                        while (++offset < len) {   // Go thru comment.
                            if (sql.charAt(offset) == '\n' || sql.charAt(offset) == '\r') {   // If end of comment
                                offset++;
                                break;
                            }
                        }
                        break;
                    }
                    // Fall through to test character
                default:
                    if (ch == chTmp)
                        return offset - 1;
                    break;

                case '[':
                    chTmp = ']';
                case '\'':
                case '"':
                    chQuote = chTmp;
                    while (offset < len) {
                        if (sql.charAt(offset++) == chQuote) {
                            if (len == offset || sql.charAt(offset) != chQuote)
                                break;

                            ++offset;
                        }
                    }
                    break;
            }
        }

        return len;
    }
}
