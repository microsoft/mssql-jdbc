package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Token;


class SQLServerParser {
    /*
     * Retrieves the table target from a single query.
     */
    static List<String> getTableName(ListIterator<? extends Token> iter,
            ArrayList<? extends Token> tokenList) throws SQLServerException {
        List<String> openingBrackets = Arrays.asList("'", "\"", "(", "[", "{");
        List<String> tableNames = new ArrayList<>();
        Token t = null;
        while (iter.hasNext()) {
            t = iter.next();
            switch (t.getType()) {
                case SQLServerLexer.SELECT:
                    int selectIndex = iter.nextIndex();
                    boolean foundFrom = false;
                    while (t.getType() != SQLServerLexer.SEMI && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.FROM) {
                            foundFrom = true;
                            tableNames.add(
                                    getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.GROUP,
                                            SQLServerLexer.HAVING, SQLServerLexer.ORDER, SQLServerLexer.OPTION)));
                        } else if (openingBrackets.contains(t.getText())) {
                            skipBracket(iter, t.getType());
                        }
                    }
                    if (!foundFrom) {
                        // Go back to where we first found our SELECT
                        iter = tokenList.listIterator(selectIndex);
                        try {
                            // Get next chunk
                            tableNames.add(
                                    getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.GROUP,
                                            SQLServerLexer.HAVING, SQLServerLexer.ORDER, SQLServerLexer.OPTION)));
                        } catch (NoSuchElementException e) {
                            SQLServerException.makeFromDriverError(null, tokenList, e.getLocalizedMessage(), "", false);
                        }
                    }
                    break;
                case SQLServerLexer.INSERT:
                    try {
                        t = skipTop(iter);
                        // Check for optional keyword INTO and move the iterator back if it isn't there
                        if (t.getType() != SQLServerLexer.INTO) {
                            t = iter.previous();
                        }
                        tableNames.add(getTableTargetChunk(iter,
                                Arrays.asList(SQLServerLexer.VALUES, SQLServerLexer.OUTPUT, SQLServerLexer.LR_BRACKET,
                                        SQLServerLexer.SELECT, SQLServerLexer.EXECUTE, SQLServerLexer.WITH,
                                        SQLServerLexer.DEFAULT)));
                    } catch (NoSuchElementException e) {
                        SQLServerException.makeFromDriverError(null, tokenList, e.getLocalizedMessage(), "", false);
                    }
                    break;
                case SQLServerLexer.DELETE:
                    t = skipTop(iter);
                    // Check for optional keyword FROM and move the iterator back if it isn't there
                    if (t.getType() != SQLServerLexer.FROM) {
                        t = iter.previous();
                    }
                    int deleteIndex = iter.nextIndex();
                    foundFrom = false;
                    while (t.getType() != SQLServerLexer.SEMI && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.FROM) {
                            foundFrom = true;
                            tableNames.add(getTableTargetChunk(iter,
                                    Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.OPTION)));
                        } else if (openingBrackets.contains(t.getText())) {
                            skipBracket(iter, t.getType());
                        }
                    }
                    if (!foundFrom) {
                        // Go back to where we first found our DELETE
                        iter = tokenList.listIterator(deleteIndex);
                        try {
                            // Get next chunk
                            tableNames.add(getTableTargetChunk(iter,
                                    Arrays.asList(SQLServerLexer.OPTION, SQLServerLexer.WHERE, SQLServerLexer.OUTPUT)));
                        } catch (NoSuchElementException e) {
                            SQLServerException.makeFromDriverError(null, tokenList, e.getLocalizedMessage(), "", false);
                        }
                    }
                    break;
                case SQLServerLexer.UPDATE:
                    skipTop(iter);
                    iter.previous();
                    try {
                        // Get next chunk
                        tableNames.add(getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.SET,
                                SQLServerLexer.OUTPUT, SQLServerLexer.WHERE, SQLServerLexer.OPTION)));
                    } catch (NoSuchElementException e) {
                        SQLServerException.makeFromDriverError(null, tokenList, e.getLocalizedMessage(), "", false);
                    }
                    while (t.getType() != SQLServerLexer.SEMI && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.FROM) {
                            foundFrom = true;
                            tableNames.add(getTableTargetChunk(iter,
                                    Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.OPTION)));
                        } else if (openingBrackets.contains(t.getText())) {
                            skipBracket(iter, t.getType());
                        }
                    }
                    break;
                case SQLServerLexer.EXECUTE:// covers both exec and execute
                    break;
                default:
                    // skip, we only support SELECT/UPDATE/INSERT/DELETE/EXEC/EXECUTE
                    break;
            }
        }
        return tableNames;
    }

    /*
     * Doesn't only cover brackets. Covers ', ", (, [, and {.
     */
    private static void skipBracket(ListIterator<? extends Token> iter, int firstBracket) throws SQLServerException {
        Stack<Integer> s = new Stack<>();
        s.push(firstBracket);
        while (iter.hasNext() && !s.empty()) {
            Token t = iter.next();
            if (s.peek() == SQLServerLexer.SINGLE_QUOTE || s.peek() == SQLServerLexer.DOUBLE_QUOTE
                    || s.peek() == SQLServerLexer.RS_BRACKET) {
                handleLiteral(t, s, iter);
            } else {
                handleNonLiteral(t, s);
            }
        }
        if (!s.empty()) {
            SQLServerException.makeFromDriverError(null, null, "SQL Syntax Error", "", false);
        }
    }

    /*
     * 
     */
    private static String getBracket(ListIterator<? extends Token> iter, int firstBracket) throws SQLServerException {
        Stack<Integer> s = new Stack<>();
        StringBuilder sb = new StringBuilder();
        s.push(firstBracket);
        while (iter.hasNext() && !s.empty()) {
            Token t = iter.next();
            sb.append(" ").append(t.getText());
            if (s.peek() == SQLServerLexer.SINGLE_QUOTE || s.peek() == SQLServerLexer.DOUBLE_QUOTE
                    || s.peek() == SQLServerLexer.RS_BRACKET) {
                handleLiteral(t, s, iter);
            } else {
                handleNonLiteral(t, s);
            }
        }
        if (!s.empty()) {
            SQLServerException.makeFromDriverError(null, null, "SQL Syntax Error", "", false);
        }
        return sb.toString();
    }

    /*
     * Moves the iterator past the TOP clause to the next token
     */
    private static Token skipTop(ListIterator<? extends Token> iter) throws SQLServerException {
        // Look for the TOP token
        Token t = iter.next();
        if (t.getType() == SQLServerLexer.TOP) {
            t = iter.next();
            if (t.getType() == SQLServerLexer.LR_BRACKET) {
                skipBracket(iter, SQLServerLexer.LR_BRACKET);
            }
            t = iter.next();

            // Look for PERCENT, must come before WITH TIES
            if (t.getType() == SQLServerLexer.PERCENT) {
                t = iter.next();
            }

            // Look for WITH TIES
            if (t.getType() == SQLServerLexer.WITH) {
                t = iter.next();
                if (t.getType() == SQLServerLexer.TIES) {
                    t = iter.next();
                } else {
                    // It's not a WITH TIES clause, move the iterator back
                    t = iter.previous();
                }
            }
        }
        return t;
    }

    static String getCTE(ListIterator<? extends Token> iter) throws SQLServerException {
        Token t = iter.next();
        if (t.getType() == SQLServerLexer.WITH) {
            StringBuilder sb = new StringBuilder("WITH ");
            getCTESegment(iter, sb);
            return sb.toString();
        } else {
            iter.previous();
            return "";
        }
    }

    private static void getCTESegment(ListIterator<? extends Token> iter, StringBuilder sb) throws SQLServerException {
        try {
            sb.append(getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.AS)));
        } catch (NoSuchElementException e) {
            SQLServerException.makeFromDriverError(null, SQLServerLexer.AS,
                    "Invalid syntax: WITH <cte> expressions must contain 'AS' keyword.", "", false);
        }
        sb.append("AS ");
        Token t = iter.next();
        if (t.getType() != SQLServerLexer.LR_BRACKET) {
            SQLServerException.makeFromDriverError(null, SQLServerLexer.AS,
                    "Invalid syntax: AS must be followed by ( ) in <ctw> expressions.", "", false);
        }
        int leftRoundBracketCount = 0;
        do {
            sb.append(t.getText()).append(' ');
            if (t.getType() == SQLServerLexer.LR_BRACKET) {
                leftRoundBracketCount++;
            } else if (t.getType() == SQLServerLexer.RR_BRACKET) {
                leftRoundBracketCount--;
            }
            t = iter.next();
        } while (leftRoundBracketCount > 0);

        if (t.getType() == SQLServerLexer.COMMA) {
            sb.append(", ");
            getCTESegment(iter, sb);
        } else {
            iter.previous();
        }
    }

    private static void handleNonLiteral(Token t, Stack<Integer> s) {
        switch (t.getType()) {
            case SQLServerLexer.LR_BRACKET:
                s.push(SQLServerLexer.LR_BRACKET);
                break;
            case SQLServerLexer.RR_BRACKET:
                if (s.peek() == SQLServerLexer.LR_BRACKET)
                    s.pop();
                break;
            case SQLServerLexer.LC_BRACKET:
                s.push(SQLServerLexer.LC_BRACKET);
                break;
            case SQLServerLexer.RC_BRACKET:
                if (s.peek() == SQLServerLexer.LC_BRACKET)
                    s.pop();
                break;
            case SQLServerLexer.LS_BRACKET:
                s.push(SQLServerLexer.LS_BRACKET);
                break;
            case SQLServerLexer.SINGLE_QUOTE:
                s.push(SQLServerLexer.SINGLE_QUOTE);
                break;
            case SQLServerLexer.DOUBLE_QUOTE:
                s.push(SQLServerLexer.DOUBLE_QUOTE);
                break;
            default:
                break;
        }
    }

    private static void handleLiteral(Token t, Stack<Integer> s, ListIterator<? extends Token> iter) {
        switch (t.getType()) {
            case SQLServerLexer.RS_BRACKET:
                if ((s.peek() == SQLServerLexer.RS_BRACKET)) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.RS_BRACKET) {
                            // Do nothing, continue skipping
                        } else {
                            iter.previous();
                            s.pop();
                        }
                    } else {
                        s.pop();
                    }
                }
                break;
            case SQLServerLexer.SINGLE_QUOTE:
                if ((s.peek() == SQLServerLexer.SINGLE_QUOTE)) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.SINGLE_QUOTE) {
                            // Do nothing, continue skipping
                        } else {
                            iter.previous();
                            s.pop();
                        }
                    } else {
                        s.pop();
                    }
                }
                break;
            case SQLServerLexer.DOUBLE_QUOTE:
                if ((s.peek() == SQLServerLexer.DOUBLE_QUOTE)) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.DOUBLE_QUOTE) {
                            // Do nothing, continue skipping
                        } else {
                            iter.previous();
                            s.pop();
                        }
                    } else {
                        s.pop();
                    }
                }
                break;
            default:
                break;
        }
    }

    private static String getTableTargetChunk(ListIterator<? extends Token> iter,
            List<Integer> delimiters) throws SQLServerException {
        StringBuilder sb = new StringBuilder();
        Token t = iter.next();
        do {
            switch (t.getType()) {
                case SQLServerLexer.LR_BRACKET:
                    sb.append("(");
                    Stack<String> s = new Stack<>();
                    s.push("(");
                    try {
                        while (!s.empty()) {
                            t = iter.next();
                            if (t.getType() == SQLServerLexer.RR_BRACKET) {
                                sb.append(")");
                                s.pop();
                            } else if (t.getType() == SQLServerLexer.LR_BRACKET) {
                                sb.append("(");
                                s.push("(");
                            } else {
                                sb.append(t.getText()).append(" ");
                            }
                        }
                    } catch (IndexOutOfBoundsException e) {
                        // TODO: throw index out of bounds
                        SQLServerException.makeFromDriverError(null, null, e.getLocalizedMessage(), "", false);
                    }
                    break;
                case SQLServerLexer.DOUBLE_QUOTE:
                    do {
                        sb.append(t.getText());
                        t = iter.next();
                    } while (t.getType() != SQLServerLexer.DOUBLE_QUOTE && iter.hasNext());
                    sb.append('"');
                    break;
                case SQLServerLexer.OPENQUERY:
                    sb.append("OPENQUERY");
                    t = iter.next();
                    if (t.getType() != SQLServerLexer.LR_BRACKET) {
                        // TODO: Make from driver error
                    }
                    sb.append(" (").append(getBracket(iter, SQLServerLexer.LR_BRACKET));
                    break;
                default:
                    sb.append(t.getText());
                    break;
            }
            if (iter.hasNext()) {
                sb.append(' ');
                t = iter.next();
            } else {
                break;
            }
        } while (!delimiters.contains(t.getType()) && t.getType() != SQLServerLexer.SEMI);

        return sb.toString().trim();
    }
}


class useFmtOnlyQuery {
    private String prefix;
    private List<String> columns;
    private List<String> tableTarget;
    private List<String> parameterDeclarations;

    String getPrefix() {
        return prefix;
    }
    
    String getPostfix() {
        StringBuilder sb = new StringBuilder();
        parameterDeclarations.stream().forEach(s -> sb.append("N'").append(s).append("',"));
        sb.setLength(sb.length()-1);
        return sb.toString();
    }

    List<String> getColumns() {
        return columns;
    }

    List<String> getTableTarget() {
        return tableTarget;
    }

    String getFMTQuery() {
        StringBuilder sb = new StringBuilder("SET FMTONLY ON;");
        if (prefix != "")
            sb.append(prefix).append("\n");
        sb.append("SELECT * FROM ");
        //sb.append(columns.stream().distinct().collect(Collectors.joining(","))).append(" FROM ");
        sb.append(tableTarget.stream().distinct().collect(Collectors.joining(","))).append(";");
        sb.append("SET FMTONLY OFF;");
        return sb.toString();
    }

    private useFmtOnlyQuery() {};

    private useFmtOnlyQuery(String prefix, List<String> columns, List<String> tableTarget) {
        this.prefix = prefix;
        this.columns = columns;
        this.tableTarget = tableTarget;
    };
    
    void setParameters(Parameter[] params) {
        columns = new ArrayList<String>();
        parameterDeclarations = new ArrayList<String>();
        for (int i = 0; i < params.length; i++) {
            columns.add("@P"+i);
            parameterDeclarations.add(params[i].getSetterValue().toString());
        }
    }

    static useFmtOnlyQuery getFmtQuery(String userSql) throws SQLServerException {
        InputStream stream = new ByteArrayInputStream(userSql.getBytes(StandardCharsets.UTF_8));
        SQLServerLexer lexer = null;
        try {
            lexer = new SQLServerLexer(CharStreams.fromStream(stream));
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, userSql, e.getLocalizedMessage(), "", false);
        }
        ArrayList<? extends Token> tokenList = (ArrayList<? extends Token>) lexer.getAllTokens();
        // tokenList.forEach(t -> System.out.println(t.toString()));

        ListIterator<? extends Token> iter = tokenList.listIterator();
        return new useFmtOnlyQuery(SQLServerParser.getCTE(iter), Arrays.asList("*"), SQLServerParser.getTableName(iter, tokenList));
    }
}
