/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;


class SQLServerFMTQuery {

    private static final String FMT_ON = "SET FMTONLY ON;";
    private static final String SELECT = "SELECT ";
    private static final String FROM = " FROM ";
    private static final String FMT_OFF = ";SET FMTONLY OFF;";

    private String prefix = "";
    private ArrayList<? extends Token> tokenList = null;
    private List<String> userColumns = new ArrayList<>();
    private List<String> tableTarget = new ArrayList<>();
    private List<String> possibleAliases = new ArrayList<>();
    private List<List<String>> valuesList = new ArrayList<>();

    List<String> getColumns() {
        return userColumns;
    }

    List<String> getTableTarget() {
        return tableTarget;
    }

    List<List<String>> getValuesList() {
        return valuesList;
    }

    List<String> getAliases() {
        return possibleAliases;
    }

    /**
     * Takes the list of user parameters ('?') and appends their respective parsed column names together. In the case of
     * an INSERT INTO table VALUES(?,?,?...), we need to wait for the server to reply to know the column names. The
     * parser uses an '*' followed by placeholder '?'s to indicate these unknown columns, and we can't include the '?'s
     * in column targets. This method is used to generate the column targets in the FMT Select query: SELECT
     * {constructColumnTargets} FROM ... .
     */
    String constructColumnTargets() {
        if (userColumns.contains("?")) {
            return userColumns.stream().filter(s -> !"?".equals(s)).map(s -> "".equals(s) ? "NULL" : s)
                    .collect(Collectors.joining(","));
        } else {
            return userColumns.isEmpty() ? "*" : userColumns.stream().map(s -> "".equals(s) ? "NULL" : s)
                    .collect(Collectors.joining(","));
        }
    }

    String constructTableTargets() {
        return tableTarget.stream().distinct().filter(s -> !possibleAliases.contains(s))
                .collect(Collectors.joining(","));
    }

    String getFMTQuery() {
        StringBuilder sb = new StringBuilder(FMT_ON);
        if (!"".equals(prefix)) {
            sb.append(prefix);
        }
        sb.append(SELECT);
        sb.append(constructColumnTargets());
        if (!tableTarget.isEmpty()) {
            sb.append(FROM);
            sb.append(constructTableTargets());
        }
        sb.append(FMT_OFF);
        return sb.toString();
    }

    // Do not allow default instantiation, class must be used with sql query
    @SuppressWarnings("unused")
    private SQLServerFMTQuery() {};

    SQLServerFMTQuery(String userSql) throws SQLServerException {
        if (null != userSql && 0 != userSql.length()) {
            InputStream stream = new ByteArrayInputStream(userSql.getBytes(StandardCharsets.UTF_8));

            SQLServerLexer lexer = null;
            try {
                lexer = new SQLServerLexer(CharStreams.fromStream(stream));
            } catch (IOException e) {
                SQLServerException.makeFromDriverError(null, userSql, e.getLocalizedMessage(), null, false);
            }
            if (null != lexer) {
                lexer.removeErrorListeners();
                lexer.addErrorListener(new SQLServerErrorListener());
                this.tokenList = (ArrayList<? extends Token>) lexer.getAllTokens();
                if (tokenList.size() <= 0) {
                    SQLServerException.makeFromDriverError(null, this,
                            SQLServerResource.getResource("R_noTokensFoundInUserQuery"), null, false);
                }
                SQLServerTokenIterator iter = new SQLServerTokenIterator(tokenList);
                this.prefix = SQLServerParser.getCTE(iter);
                SQLServerParser.parseQuery(iter, this);
            } else {
                SQLServerException.makeFromDriverError(null, userSql,
                        SQLServerResource.getResource("R_noTokensFoundInUserQuery"), null, false);
            }
        } else {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_noTokensFoundInUserQuery"), null, false);
        }
    }
}


class SQLServerErrorListener extends BaseErrorListener {
    static final private java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerFMTQuery");

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
            String msg, RecognitionException e) {
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine("Error occured during token parsing: " + msg);
            logger.fine("line " + line + ":" + charPositionInLine + " token recognition error at: "
                    + offendingSymbol.toString());
        }
    }
}
