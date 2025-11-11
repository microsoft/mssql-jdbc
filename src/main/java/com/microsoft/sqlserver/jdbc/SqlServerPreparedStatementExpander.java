/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Robust SQL Expander for SQL Server.
 *
 * - Accurately replaces '?' placeholders with bound literals.
 * - Skips placeholders inside string literals and comments.
 * - Detects operators/keywords before the placeholder and rewrites
 *   = ?  -> IS NULL
 *   <> ? -> IS NOT NULL
 *   != ? -> IS NOT NULL
 *   IS ? -> IS NULL
 *   IS NOT ? -> IS NOT NULL
 *
 * NOTE: Intended to generate runnable T-SQL (for direct execution with Statement)
 *       Use with care — this writes values inline. Do NOT use for SQL containing
 *       sensitive user-supplied values unless you understand the security implications.
 */
public final class SqlServerPreparedStatementExpander {

    private static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat TIME_FMT = new SimpleDateFormat("HH:mm:ss");
    private static final SimpleDateFormat TIMESTAMP_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private SqlServerPreparedStatementExpander() {}

    /**
     * Expand SQL with '?' placeholders using provided parameters.
     *
     * @param sql    The SQL text containing '?' placeholders.
     * @param params List of bound parameter values in order.
     * @return Expanded SQL string ready to execute on SQL Server.
     */
    public static String expand(String sql, List<Object> params) {
        if (sql == null) return null;
        if (params == null || params.isEmpty()) return sql;

        StringBuilder out = new StringBuilder(sql.length() + params.size() * 20);
        int paramIdx = 0;

        // scanning state
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        char prev = 0;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);

            // handle comment/quote entry/exit
            if (inLineComment) {
                out.append(ch);
                if (ch == '\n') inLineComment = false;
                prev = ch;
                continue;
            }

            if (inBlockComment) {
                out.append(ch);
                if (prev == '*' && ch == '/') inBlockComment = false;
                prev = ch;
                continue;
            }

            if (inSingleQuote) {
                out.append(ch);
                if (ch == '\'') {
                    // Check if this is an escaped quote (doubled '')
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                        // This is an escaped quote. Append the next quote and skip it in the next
                        // iteration
                        i++; // skip next quote
                        out.append('\'');
                    } else {
                        // This is the closing quote
                        inSingleQuote = false;
                    }
                }
                prev = ch;
                continue;
            }

            if (inDoubleQuote) {
                out.append(ch);
                if (ch == '"') {
                    // Check if this is an escaped quote (doubled "")
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
                        // This is an escaped quote. Append the next quote and skip it
                        i++;
                        out.append('"');
                    } else {
                        // This is the closing quote
                        inDoubleQuote = false;
                    }
                }
                prev = ch;
                continue;
            }

            // not inside quotes/comments
            // detect start of line or block comment
            if (ch == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                out.append(ch);
                inLineComment = true;
                prev = ch;
                continue;
            }
            if (ch == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                out.append(ch);
                inBlockComment = true;
                prev = ch;
                continue;
            }

            // detect entering quotes
            if (ch == '\'') {
                out.append(ch);
                inSingleQuote = true;
                prev = ch;
                continue;
            }
            if (ch == '"') {
                out.append(ch);
                inDoubleQuote = true;
                prev = ch;
                continue;
            }

            // handle placeholder
            if (ch == '?' && paramIdx < params.size()) {
                Object val = params.get(paramIdx++);

                // Determine operator/keyword context by scanning backwards on 'out' (which has everything written so far)
                // We'll locate last non-space token before '?' in original sql by scanning original backwards.
                // Find the start index in original SQL for the token region we care about.
                int tokenEndIndex = i - 1; // index before '?'
                // Skip whitespace backwards
                while (tokenEndIndex >= 0 && Character.isWhitespace(sql.charAt(tokenEndIndex))) tokenEndIndex--;

                // Now determine token type
                String prevOp = null; // "=", "<>", "!=", "IS", "NOT", "IS NOT"
                int opStartInOut = -1; // how many chars to remove from out end if rewriting operator form

                if (tokenEndIndex >= 0) {
                    // single/double char operators
                    char c = sql.charAt(tokenEndIndex);
                    if (c == '=' || c == '>' || c == '<' || c == '!') {
                        // Check for two-char operators first
                        char before = tokenEndIndex - 1 >= 0 ? sql.charAt(tokenEndIndex - 1) : 0;
                        String two = (before != 0) ? "" + before + c : null;
                        if ("<>".equals(two) || "!=".equals(two) || "<=".equals(two) || ">=".equals(two)) {
                            prevOp = two;
                            opStartInOut = findOpStartInOut(out, two);
                        } else {
                            // Single character operator
                            prevOp = String.valueOf(c);
                            opStartInOut = findOpStartInOut(out, String.valueOf(c));
                        }
                    } else {
                        // scan backward for word tokens (letters) up to two words to detect 'IS' or 'IS NOT' or 'NOT'
                        int j = tokenEndIndex;
                        // collect last word
                        while (j >= 0 && Character.isWhitespace(sql.charAt(j))) j--;
                        int wordEnd = j;
                        while (j >= 0 && (Character.isLetter(sql.charAt(j)) || sql.charAt(j) == '_')) j--;
                        int wordStart = j + 1;
                        String lastWord = (wordStart <= wordEnd) ? sql.substring(wordStart, wordEnd + 1).toUpperCase(Locale.ROOT) : null;

                        // check previous word before lastWord
                        String prevWord = null;
                        if (lastWord != null) {
                            int k = wordStart - 1;
                            while (k >= 0 && Character.isWhitespace(sql.charAt(k))) k--;
                            int prevWordEnd = k;
                            while (k >= 0 && (Character.isLetter(sql.charAt(k)) || sql.charAt(k) == '_')) k--;
                            int prevWordStart = k + 1;
                            prevWord = (prevWordStart <= prevWordEnd) ? sql.substring(prevWordStart, prevWordEnd + 1).toUpperCase(Locale.ROOT) : null;
                        }

                        if ("IS".equals(lastWord)) {
                            prevOp = "IS";
                            opStartInOut = findWordStartInOut(out, "IS");
                        } else if ("NOT".equals(lastWord) && "IS".equals(prevWord)) {
                            prevOp = "IS NOT";
                            opStartInOut = findWordStartInOut(out, "IS NOT");
                        } else if ("NOT".equals(lastWord)) {
                            prevOp = "NOT";
                            opStartInOut = findWordStartInOut(out, "NOT");
                        } else {
                            prevOp = null;
                        }
                    }
                } // end token detection

                // Now decide replacement
                if (val == null) {
                    // null-specific rewrites
                    if ("=".equals(prevOp)) {
                        // remove '=' in output, replace with IS NULL
                        if (opStartInOut >= 0) out.setLength(opStartInOut);
                        out.append("IS NULL");
                    } else if ("<>".equals(prevOp) || "!=".equals(prevOp)) {
                        if (opStartInOut >= 0) out.setLength(opStartInOut);
                        out.append("IS NOT NULL");
                    } else if ("IS".equals(prevOp)) {
                        // keep 'IS' and just append NULL
                        out.append("NULL");
                    } else if ("IS NOT".equals(prevOp)) {
                        out.append("NULL"); // becomes IS NOT NULL
                    } else {
                        // default: just put NULL
                        out.append("NULL");
                    }
                } else {
                    // normal formatted replacement
                    out.append(formatLiteral(val));
                }

                prev = ch;
                continue;
            }

            // normal char write
            out.append(ch);
            prev = ch;
        } // end for

        return out.toString();
    }

    // Helper to find the start index in the output buffer where the given operator string begins (case-insensitive)
    // It scans backward from end of out and matches the operator possibly surrounded by whitespace.
    private static int findOpStartInOut(StringBuilder out, String op) {
        if (op == null || out.length() == 0) return -1;

        // Scan backwards to find the operator, skipping any trailing whitespace
        int pos = out.length() - 1;

        // Skip any trailing whitespace at the end of the buffer
        while (pos >= 0 && Character.isWhitespace(out.charAt(pos))) {
            pos--;
        }

        // Now pos should point to the last non-whitespace character
        // Check if the operator matches ending at this position
        int opLen = op.length();
        if (pos - opLen + 1 < 0)
            return -1;

        int opStart = pos - opLen + 1;
        String found = out.substring(opStart, pos + 1);

        if (found.equals(op)) {
            // Found the operator. Now we need to decide where to truncate.
            // We want to keep at least one space before where we'll insert the new text
            // So scan back to find any spaces before the operator
            int trimPos = opStart;
            while (trimPos > 0 && Character.isWhitespace(out.charAt(trimPos - 1))) {
                trimPos--;
            }
            // Keep one space
            if (trimPos < opStart) {
                trimPos++;
            }
            return trimPos;
        }

        // Fallback: search for last occurrence of the operator
        String outStr = out.toString();
        int lastIdx = outStr.lastIndexOf(op);
        if (lastIdx >= 0) {
            int trimPos = lastIdx;
            while (trimPos > 0 && Character.isWhitespace(out.charAt(trimPos - 1))) {
                trimPos--;
            }
            if (trimPos < lastIdx) {
                trimPos++;
            }
            return trimPos;
        }

        return -1;
    }

    // Helper to remove a prior word (like IS or NOT). Scans backwards in out to find the last matching word (case-insensitive).
    private static int findWordStartInOut(StringBuilder out, String word) {
        if (word == null || out.length() == 0) return -1;
        String upper = out.toString().toUpperCase(Locale.ROOT);
        String w = word.toUpperCase(Locale.ROOT);

        // find last occurrence of the word as separate token
        int idx = -1;
        for (int pos = upper.length() - w.length(); pos >= 0; pos--) {
            if (upper.regionMatches(pos, w, 0, w.length())) {
                boolean leftOk = (pos - 1 < 0) || !Character.isLetterOrDigit(upper.charAt(pos - 1));
                boolean rightOk = (pos + w.length() >= upper.length()) || !Character.isLetterOrDigit(upper.charAt(pos + w.length()));
                if (leftOk && rightOk) {
                    idx = pos;
                    break;
                }
            }
        }
        if (idx >= 0) {
            // Keep one space before the word to maintain proper spacing
            int trim = idx;
            while (trim - 1 >= 0 && Character.isWhitespace(out.charAt(trim - 1))) trim--;
            if (trim < idx && idx < out.length()) {
                trim++; // keep one space
            }
            return trim;
        }
        return -1;
    }

    // Format Java value into a T-SQL literal safe for SQL Server
    private static String formatLiteral(Object value) {
        if (value == null) return "NULL";

        if (value instanceof String) {
            return "N'" + escapeString((String) value) + "'";
        }

        if (value instanceof Character) {
            return "N'" + escapeString(value.toString()) + "'";
        }

        if (value instanceof Boolean) {
            return ((Boolean) value) ? "1" : "0";
        }

        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
            || value instanceof Float || value instanceof Double || value instanceof java.math.BigDecimal) {
            return value.toString();
        }

        if (value instanceof java.sql.Date) {
            return "CAST('" + DATE_FMT.format((java.util.Date) value) + "' AS DATE)";
        }

        if (value instanceof java.sql.Time) {
            return "CAST('" + TIME_FMT.format((java.util.Date) value) + "' AS TIME)";
        }

        if (value instanceof java.sql.Timestamp) {
            return "CAST('" + TIMESTAMP_FMT.format((java.util.Date) value) + "' AS DATETIME2)";
        }

        if (value instanceof java.util.Date) {
            // generic date -> timestamp
            return "CAST('" + TIMESTAMP_FMT.format((java.util.Date) value) + "' AS DATETIME2)";
        }

        if (value instanceof byte[]) {
            return bytesToHexLiteral((byte[]) value);
        }

        if (value instanceof Blob) {
            try {
                Blob b = (Blob) value;
                int len = (int) b.length();
                byte[] bytes = b.getBytes(1, len);
                return bytesToHexLiteral(bytes);
            } catch (Exception e) {
                return "NULL";
            }
        }

        if (value instanceof Clob) {
            try {
                Clob c = (Clob) value;
                String s = c.getSubString(1, (int) c.length());
                return "N'" + escapeString(s) + "'";
            } catch (Exception e) {
                return "NULL";
            }
        }

        if (value instanceof InputStreamWrapper) {
            try {
                byte[] bytes = ((InputStreamWrapper) value).getBytes();
                return bytesToHexLiteral(bytes);
            } catch (Exception e) {
                return "NULL";
            }
        }

        // fallback
        return "N'" + escapeString(value.toString()) + "'";
    }

    private static String escapeString(String s) {
        if (s == null || s.isEmpty()) return "";
        // double single quotes
        return s.replace("'", "''");
    }

    private static String bytesToHexLiteral(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "0x";
        StringBuilder sb = new StringBuilder(bytes.length * 2 + 2);
        sb.append("0x");
        for (byte b : bytes) sb.append(String.format("%02X", b));
        return sb.toString();
    }

    // --- Helper wrapper for InputStream-based binary parameters if you want to pass them in tests
    public static final class InputStreamWrapper {
        private final byte[] bytes;
        public InputStreamWrapper(byte[] bytes) { this.bytes = bytes; }
        public byte[] getBytes() { return bytes; }
    }

}
