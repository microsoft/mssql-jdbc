package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;


class SQLServerParser {

    /*
     * Retrieves the table target from a single query.
     */
    static void parseQuery(ListIterator<? extends Token> iter, ArrayList<? extends Token> tokenList,
            List<String> tableNames, List<String> columns) throws SQLServerException {
        Token t = null;
        while (iter.hasNext()) {
            t = iter.next();
            switch (t.getType()) {
                case SQLServerLexer.LR_BRACKET:
                    // read bracket chunk
                    break;
                case SQLServerLexer.SELECT:
                    t = skipTop(iter);
                    while (t.getType() != SQLServerLexer.SEMI && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.PARAMETER) {
                            int parameterIndex = iter.nextIndex();
                            columns.add(SQLServerParser.findColumnAroundParameter(iter));
                            iter = tokenList.listIterator(parameterIndex);
                        }
                        if (t.getType() == SQLServerLexer.FROM) {
                            tableNames.add(
                                    getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.GROUP,
                                            SQLServerLexer.HAVING, SQLServerLexer.ORDER, SQLServerLexer.OPTION)));
                            break;
                        }
                    }
                    break;
                case SQLServerLexer.INSERT:
                    t = skipTop(iter);
                    // Check for optional keyword INTO and move the iterator back if it isn't there
                    if (t.getType() != SQLServerLexer.INTO) {
                        t = iter.previous();
                    }
                    tableNames.add(getTableTargetChunk(iter,
                            Arrays.asList(SQLServerLexer.VALUES, SQLServerLexer.OUTPUT, SQLServerLexer.LR_BRACKET,
                                    SQLServerLexer.SELECT, SQLServerLexer.EXECUTE, SQLServerLexer.WITH,
                                    SQLServerLexer.DEFAULT)));
                    if (iter.next().getType() == SQLServerLexer.WITH) {
                        iter.next();
                        skipBracket(iter, SQLServerLexer.LR_BRACKET);
                    } else {
                        iter.previous();
                    }
                    List<String> tableValues = getValuesList(iter);
                    List<List<String>> userValues = new ArrayList<>();
                    // VALUES case
                    boolean valuesFound = false;
                    int valuesMarker = iter.nextIndex();
                    while (!valuesFound && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.VALUES) {
                            valuesFound = true;
                            if (tableValues.isEmpty()) {
                                columns.add("*");
                            }
                            do {
                                userValues.add(getValuesList(iter));
                            } while (iter.hasNext() && iter.next().getType() == SQLServerLexer.COMMA);
                            iter.previous();
                        }
                    }
                    if (!valuesFound) {
                        resetIteratorIndex(iter, valuesMarker);
                    }
                    if (!userValues.isEmpty()) {
                        for (List<String> ls : userValues) {
                            for (int i = 0; i < ls.size(); i++) {
                                if (ls.get(i).equalsIgnoreCase("?")) {
                                    columns.add((tableValues.size() == 0) ? "?" : tableValues.get(i));
                                }
                            }
                        }
                    }
                    break;
                case SQLServerLexer.DELETE:
                    t = skipTop(iter);
                    // Check for optional keyword FROM and move the iterator back if it isn't there
                    if (t.getType() != SQLServerLexer.FROM) {
                        t = iter.previous();
                    }
                    tableNames.add(getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.OPTION, SQLServerLexer.WHERE,
                            SQLServerLexer.OUTPUT, SQLServerLexer.FROM)));
                    break;
                case SQLServerLexer.UPDATE:
                    skipTop(iter);
                    iter.previous();
                    // Get next chunk
                    tableNames.add(getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.SET, SQLServerLexer.OUTPUT,
                            SQLServerLexer.WHERE, SQLServerLexer.OPTION)));
                    while (t.getType() != SQLServerLexer.SEMI && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.FROM) {
                            tableNames.add(getTableTargetChunk(iter,
                                    Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.OPTION)));
                            break;
                        }
                    }
                    break;
                case SQLServerLexer.EXECUTE:// covers both exec and execute
                    break;
                case SQLServerLexer.PARAMETER:
                    int parameterIndex = iter.nextIndex();
                    columns.add(SQLServerParser.findColumnAroundParameter(iter));
                    iter = tokenList.listIterator(parameterIndex);
                    break;
                default:
                    // skip, we only support SELECT/UPDATE/INSERT/DELETE/EXEC/EXECUTE
                    break;
            }
        }
    }

    /*
     * Covers ', ", (, [, and {.
     */
    private static void skipBracket(ListIterator<? extends Token> iter, int firstBracket) throws SQLServerException {
        Deque<Integer> d = new ArrayDeque<>();
        d.push(firstBracket);
        while (iter.hasNext() && !d.isEmpty()) {
            Token t = iter.next();
            if (d.peek() == SQLServerLexer.SINGLE_QUOTE || d.peek() == SQLServerLexer.DOUBLE_QUOTE
                    || d.peek() == SQLServerLexer.RS_BRACKET) {
                handleLiteral(t, d, iter);
            } else {
                handleNonLiteral(t, d);
            }
        }
        if (!d.isEmpty()) {
            SQLServerException.makeFromDriverError(null, null, "SQL Syntax Error", "", false);
        }
    }

    private static final List<Integer> OPERATORS = Arrays.asList(SQLServerLexer.EQUAL, SQLServerLexer.GREATER,
            SQLServerLexer.LESS, SQLServerLexer.GREATER_EQUAL, SQLServerLexer.LESS_EQUAL, SQLServerLexer.NOT_EQUAL,
            SQLServerLexer.PLUS_ASSIGN, SQLServerLexer.MINUS_ASSIGN, SQLServerLexer.MULT_ASSIGN,
            SQLServerLexer.DIV_ASSIGN, SQLServerLexer.MOD_ASSIGN, SQLServerLexer.AND_ASSIGN, SQLServerLexer.XOR_ASSIGN,
            SQLServerLexer.OR_ASSIGN, SQLServerLexer.STAR, SQLServerLexer.DIVIDE, SQLServerLexer.MODULE,
            SQLServerLexer.PLUS, SQLServerLexer.MINUS, SQLServerLexer.LIKE, SQLServerLexer.DOLLAR, SQLServerLexer.IN);

    static String findColumnAroundParameter(ListIterator<? extends Token> iter) {
        int index = iter.nextIndex();
        String value = findColumnAfterParameter(iter);
        resetIteratorIndex(iter, index - 1);
        if (value.equalsIgnoreCase("")) {
            value = findColumnBeforeParameter(iter);
            resetIteratorIndex(iter, index);
        }
        return value;
    }

    static void resetIteratorIndex(ListIterator<? extends Token> iter, int index) {
        if (iter.nextIndex() < index) {
            while (iter.nextIndex() != index) {
                iter.next();
            }
        } else if (iter.nextIndex() > index) {
            while (iter.nextIndex() != index) {
                iter.previous();
            }
        } else {
            return;
        }
    }

    private static String getRoundBracketChunk(ListIterator<? extends Token> iter, Token t) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        Stack<String> s = new Stack<>();
        s.push("(");
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
        return sb.toString();
    }

    private static String getRoundBracketChunkBefore(ListIterator<? extends Token> iter, Token t) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        Stack<String> s = new Stack<>();
        s.push(")");
        while (!s.empty()) {
            t = iter.previous();
            if (t.getType() == SQLServerLexer.RR_BRACKET) {
                sb.append("(");
                s.push(")");
            } else if (t.getType() == SQLServerLexer.LR_BRACKET) {
                sb.append(")");
                s.pop();
            } else {
                sb.append(t.getText()).append(" ");
            }
        }
        return sb.toString();
    }

    private static String findColumnAfterParameter(ListIterator<? extends Token> iter) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() == 0 && iter.hasNext()) {
            Token t = iter.next();
            if (OPERATORS.contains(t.getType()) && iter.hasNext()) {
                t = iter.next();
                if (t.getType() != SQLServerLexer.PARAMETER) {
                    if (t.getType() == SQLServerLexer.LR_BRACKET) {
                        sb.append(getRoundBracketChunk(iter, t));
                    } else {
                        sb.append(t.getText());
                    }
                    for (int i = 0; i < 3; i++) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.DOT) {
                            sb.append(".");
                            t = iter.next();
                            if (t.getType() == SQLServerLexer.LR_BRACKET) {
                                sb.append(getRoundBracketChunk(iter, t));
                            } else {
                                sb.append(t.getText());
                            }
                        } else {
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }
        return sb.toString();
    }

    private static String findColumnBeforeParameter(ListIterator<? extends Token> iter) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() == 0 && iter.hasPrevious()) {
            Token t = iter.previous();
            if (OPERATORS.contains(t.getType()) && iter.hasPrevious()) {
                if (t.getType() == SQLServerLexer.DOLLAR) {
                    iter.previous(); // skip if it's a $ sign
                }
                t = iter.previous();
                if (t.getType() != SQLServerLexer.PARAMETER) {
                    Deque<String> d = new ArrayDeque<>();
                    if (t.getType() == SQLServerLexer.RR_BRACKET) {
                        d.push(getRoundBracketChunkBefore(iter, t));
                    } else {
                        d.push(t.getText());
                    }
                    // Linked-servers can have a maximum of 4 parts
                    for (int i = 0; i < 3; i++) {
                        t = iter.previous();
                        if (t.getType() == SQLServerLexer.DOT) {
                            d.push(".");
                            t = iter.previous();
                            if (t.getType() == SQLServerLexer.RR_BRACKET) {
                                d.push(getRoundBracketChunkBefore(iter, t));
                            } else {
                                d.push(t.getText());
                            }
                        } else {
                            break;
                        }
                    }
                    d.stream().forEach(sb::append);
                }
            } else {
                break;
            }
        }
        return sb.toString();
    }

    /*
     * 
     */
    static String getBracket(ListIterator<? extends Token> iter, int firstBracket) throws SQLServerException {
        Deque<Integer> d = new ArrayDeque<>();
        StringBuilder sb = new StringBuilder();
        d.push(firstBracket);
        while (iter.hasNext() && !d.isEmpty()) {
            Token t = iter.next();
            sb.append(" ").append(t.getText());
            if (d.peek() == SQLServerLexer.SINGLE_QUOTE || d.peek() == SQLServerLexer.DOUBLE_QUOTE
                    || d.peek() == SQLServerLexer.RS_BRACKET) {
                handleLiteral(t, d, iter);
            } else {
                handleNonLiteral(t, d);
            }
        }
        if (!d.isEmpty()) {
            SQLServerException.makeFromDriverError(null, null, "SQL Syntax Error", "", false);
        }
        return sb.toString();
    }

    static List<String> getValuesList(ListIterator<? extends Token> iter) {
        Token t = iter.next();
        if (t.getType() == SQLServerLexer.LR_BRACKET) {
            ArrayList<String> parameterColumns = new ArrayList<>();
            Deque<Integer> d = new ArrayDeque<>();
            StringBuilder sb = new StringBuilder();
            do {
                switch (t.getType()) {
                    case SQLServerLexer.LR_BRACKET:
                        if (!d.isEmpty()) {
                            sb.append('(');
                        }
                        d.push(SQLServerLexer.LR_BRACKET);
                        break;
                    case SQLServerLexer.RR_BRACKET:
                        if (d.peek() == SQLServerLexer.LR_BRACKET) {
                            d.pop();
                        }
                        if (!d.isEmpty()) {
                            sb.append(')');
                        } else {
                            parameterColumns.add(sb.toString().trim());
                        }
                        break;
                    // handle curly brackets?
                    case SQLServerLexer.COMMA:
                        if (d.size() == 1) {
                            parameterColumns.add(sb.toString().trim());
                            sb = new StringBuilder();
                        } else {
                            sb.append(',');
                        }
                        break;
                    default:
                        sb.append(t.getText());
                        break;
                }
                if (iter.hasNext() && !d.isEmpty()) {
                    t = iter.next();
                }
            } while (!d.isEmpty());
            return parameterColumns;
        } else {
            return new ArrayList<>();
        }
    }

    private static void handleLiteral(Token t, Deque<Integer> d, ListIterator<? extends Token> iter) {
        switch (t.getType()) {
            case SQLServerLexer.RS_BRACKET:
                if ((d.peek() == SQLServerLexer.RS_BRACKET)) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.RS_BRACKET) {
                            // Do nothing, continue skipping
                        } else {
                            iter.previous();
                            d.pop();
                        }
                    } else {
                        d.pop();
                    }
                }
                break;
            case SQLServerLexer.SINGLE_QUOTE:
                if ((d.peek() == SQLServerLexer.SINGLE_QUOTE)) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.SINGLE_QUOTE) {
                            // Do nothing, continue skipping
                        } else {
                            iter.previous();
                            d.pop();
                        }
                    } else {
                        d.pop();
                    }
                }
                break;
            case SQLServerLexer.DOUBLE_QUOTE:
                if ((d.peek() == SQLServerLexer.DOUBLE_QUOTE)) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.DOUBLE_QUOTE) {
                            // Do nothing, continue skipping
                        } else {
                            iter.previous();
                            d.pop();
                        }
                    } else {
                        d.pop();
                    }
                }
                break;
            default:
                break;
        }
    }

    /*
     * Moves the iterator past the TOP clause to the next token. Returns the first token after the TOP clause.
     */
    static Token skipTop(ListIterator<? extends Token> iter) throws SQLServerException {
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
                    /*
                     * It's not a WITH TIES clause, move the iterator back. Note: this clause is probably not a valid
                     * T-SQL clause.
                     */
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

    static void getCTESegment(ListIterator<? extends Token> iter, StringBuilder sb) throws SQLServerException {
        try {
            sb.append(getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.AS)));
        } catch (NoSuchElementException e) {
            SQLServerException.makeFromDriverError(null, SQLServerLexer.AS,
                    "Invalid syntax: WITH <cte> expressions must contain 'AS' keyword.", "", false);
        }
        iter.next();
        Token t = iter.next();
        sb.append(" AS ");
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

    private static void handleNonLiteral(Token t, Deque<Integer> d) {
        switch (t.getType()) {
            case SQLServerLexer.LR_BRACKET:
                d.push(SQLServerLexer.LR_BRACKET);
                break;
            case SQLServerLexer.RR_BRACKET:
                if (d.peek() == SQLServerLexer.LR_BRACKET)
                    d.pop();
                break;
            case SQLServerLexer.LC_BRACKET:
                d.push(SQLServerLexer.LC_BRACKET);
                break;
            case SQLServerLexer.RC_BRACKET:
                if (d.peek() == SQLServerLexer.LC_BRACKET)
                    d.pop();
                break;
            case SQLServerLexer.LS_BRACKET:
                d.push(SQLServerLexer.LS_BRACKET);
                break;
            case SQLServerLexer.SINGLE_QUOTE:
                d.push(SQLServerLexer.SINGLE_QUOTE);
                break;
            case SQLServerLexer.DOUBLE_QUOTE:
                d.push(SQLServerLexer.DOUBLE_QUOTE);
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
                    sb.append(getRoundBracketChunk(iter, t));
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
        if (iter.hasNext()) {
            iter.previous();
        }
        return sb.toString().trim();
    }
}


class useFmtOnlyQuery {
    private String prefix;
    private List<String> userColumns;
    private List<String> tableTarget;

    String getPrefix() {
        return prefix;
    }

    List<String> getColumns() {
        return userColumns;
    }

    List<String> getTableTarget() {
        return tableTarget;
    }

    String getFMTQuery() {
        StringBuilder sb = new StringBuilder("SET FMTONLY ON;");
        if (prefix != "")
            sb.append(prefix);
        sb.append("SELECT ");
        sb.append(userColumns.isEmpty() ? "*" : userColumns.stream().filter(s -> !s.equalsIgnoreCase("?"))
                .collect(Collectors.joining(",")));
        if (!tableTarget.isEmpty()) {
            sb.append(" FROM ");
            sb.append(tableTarget.stream().distinct().collect(Collectors.joining(",")));
        }
        sb.append(";SET FMTONLY OFF;");
        return sb.toString();
    }

    private useFmtOnlyQuery() {};

    private useFmtOnlyQuery(String prefix, List<String> columns, List<String> tableTarget) {
        this.prefix = prefix;
        this.userColumns = columns;
        this.tableTarget = tableTarget;
    };

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
        String cte = SQLServerParser.getCTE(iter);
        List<String> tableNames = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        SQLServerParser.parseQuery(iter, tokenList, tableNames, columns);
        return new useFmtOnlyQuery(cte, columns, tableNames);
    }
}
