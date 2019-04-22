package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;

class SQLServerFMTQuery {
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

    String constructColumnTargets() {
        if (userColumns.contains("?")) {
            return userColumns.stream().filter(s -> !s.equalsIgnoreCase("?")).map(s -> s.equals("") ? "NULL" : s)
                    .collect(Collectors.joining(","));
        } else {
            return userColumns.isEmpty() ? "*" : userColumns.stream().map(s -> s.equals("") ? "NULL" : s)
                    .collect(Collectors.joining(","));
        }
    }

    String constructTableTargets() {
        return tableTarget.stream().distinct().filter(s -> !possibleAliases.contains(s))
                .collect(Collectors.joining(","));
    }

    String getFMTQuery() {
        StringBuilder sb = new StringBuilder("SET FMTONLY ON;");

        if (prefix != "") {
            sb.append(prefix);
        }
        sb.append("SELECT ");
        sb.append(constructColumnTargets());
        if (!tableTarget.isEmpty()) {
            sb.append(" FROM ");
            sb.append(constructTableTargets());
        }

        sb.append(";SET FMTONLY OFF;");
        return sb.toString();
    }

    // Do not allow default instantiation, class must be used with sql query
    @SuppressWarnings("unused")
    private SQLServerFMTQuery() {};

    SQLServerFMTQuery(String userSql) throws SQLServerException {
        InputStream stream = new ByteArrayInputStream(userSql.getBytes(StandardCharsets.UTF_8));
        SQLServerLexer lexer = null;
        try {
            lexer = new SQLServerLexer(CharStreams.fromStream(stream));
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, userSql, e.getLocalizedMessage(), "", false);
        }

        this.tokenList = (ArrayList<? extends Token>) lexer.getAllTokens();
        ListIterator<? extends Token> iter = this.tokenList.listIterator();
        this.prefix = SQLServerParser.getCTE(iter);
        SQLServerParser.parseQuery(iter, this);
    }
}
