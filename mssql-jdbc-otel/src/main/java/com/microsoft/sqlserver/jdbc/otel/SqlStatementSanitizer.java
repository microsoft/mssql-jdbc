/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;


/** Bounded lexical SQL masking. Raw SQL never leaves the synchronous callback invocation. */
final class SqlStatementSanitizer {
    private static final int MAX_INPUT = 16384;
    private static final int MAX_OUTPUT = 4096;

    private SqlStatementSanitizer() {}

    static String mask(String sql) {
        if (sql == null || sql.isEmpty() || sql.length() > MAX_INPUT) {
            return null;
        }
        StringBuilder output = new StringBuilder(Math.min(sql.length(), MAX_OUTPUT));
        int length = sql.length();
        for (int i = 0; i < length && output.length() < MAX_OUTPUT; ) {
            char current = sql.charAt(i);
            if (current == '-' && i + 1 < length && sql.charAt(i + 1) == '-') {
                i += 2;
                while (i < length && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
                    i++;
                }
                appendSpace(output);
            } else if (current == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
                i += 2;
                int depth = 1;
                while (i < length && depth > 0) {
                    if (i + 1 < length && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
                        depth++;
                        i += 2;
                    } else if (i + 1 < length && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
                        depth--;
                        i += 2;
                    } else {
                        i++;
                    }
                }
                if (depth != 0) {
                    return null;
                }
                appendSpace(output);
            } else if ((current == 'N' || current == 'n') && i + 1 < length && sql.charAt(i + 1) == '\'') {
                appendMask(output);
                i = skipQuoted(sql, i + 1, '\'');
                if (i < 0) {
                    return null;
                }
            } else if (current == '\'') {
                appendMask(output);
                i = skipQuoted(sql, i, '\'');
                if (i < 0) {
                    return null;
                }
            } else if (current == '[') {
                int end = skipBracketIdentifier(sql, i);
                if (end < 0 || !appendBounded(output, sql, i, end)) {
                    return null;
                }
                i = end;
            } else if (current == '"') {
                int end = skipQuoted(sql, i, '"');
                if (end < 0 || !appendBounded(output, sql, i, end)) {
                    return null;
                }
                i = end;
            } else if (isNumberStart(sql, i)) {
                appendMask(output);
                i = skipNumber(sql, i);
            } else if (Character.isWhitespace(current)) {
                appendSpace(output);
                i++;
            } else {
                output.append(current);
                i++;
            }
        }
        String masked = output.toString().trim();
        return masked.isEmpty() || output.length() >= MAX_OUTPUT && sql.length() > MAX_OUTPUT ? null : masked;
    }

    private static int skipQuoted(String sql, int start, char quote) {
        for (int i = start + 1; i < sql.length(); i++) {
            if (sql.charAt(i) == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    i++;
                } else {
                    return i + 1;
                }
            }
        }
        return -1;
    }

    private static int skipBracketIdentifier(String sql, int start) {
        for (int i = start + 1; i < sql.length(); i++) {
            if (sql.charAt(i) == ']') {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == ']') {
                    i++;
                } else {
                    return i + 1;
                }
            }
        }
        return -1;
    }

    private static boolean isNumberStart(String sql, int index) {
        char value = sql.charAt(index);
        if (!Character.isDigit(value) && !(value == '.' && index + 1 < sql.length()
                && Character.isDigit(sql.charAt(index + 1)))) {
            return false;
        }
        return index == 0 || !isIdentifierPart(sql.charAt(index - 1));
    }

    private static int skipNumber(String sql, int start) {
        int index = start;
        if (index + 1 < sql.length() && sql.charAt(index) == '0'
                && (sql.charAt(index + 1) == 'x' || sql.charAt(index + 1) == 'X')) {
            index += 2;
            while (index < sql.length() && Character.digit(sql.charAt(index), 16) >= 0) {
                index++;
            }
            return index;
        }
        boolean exponent = false;
        while (index < sql.length()) {
            char value = sql.charAt(index);
            if (Character.isDigit(value) || value == '.') {
                index++;
            } else if (!exponent && (value == 'e' || value == 'E')) {
                exponent = true;
                index++;
                if (index < sql.length() && (sql.charAt(index) == '+' || sql.charAt(index) == '-')) {
                    index++;
                }
            } else {
                break;
            }
        }
        return index;
    }

    private static boolean isIdentifierPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_' || value == '$' || value == '#';
    }

    private static void appendMask(StringBuilder output) {
        if (output.length() == 0 || output.charAt(output.length() - 1) != '?') {
            output.append('?');
        }
    }

    private static void appendSpace(StringBuilder output) {
        if (output.length() > 0 && output.charAt(output.length() - 1) != ' ') {
            output.append(' ');
        }
    }

    private static boolean appendBounded(StringBuilder output, String source, int start, int end) {
        if (output.length() + end - start > MAX_OUTPUT) {
            return false;
        }
        output.append(source, start, end);
        return true;
    }
}
