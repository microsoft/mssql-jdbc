/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.antlr.v4.runtime.Token;


final class SQLServerParser {

    private static final List<Integer> SELECT_DELIMITING_WORDS = Arrays.asList(SQLServerLexer.WHERE,
            SQLServerLexer.GROUP, SQLServerLexer.HAVING, SQLServerLexer.ORDER, SQLServerLexer.OPTION);
    private static final List<Integer> INSERT_DELIMITING_WORDS = Arrays.asList(SQLServerLexer.VALUES,
            SQLServerLexer.OUTPUT, SQLServerLexer.LR_BRACKET, SQLServerLexer.SELECT, SQLServerLexer.EXECUTE,
            SQLServerLexer.WITH, SQLServerLexer.DEFAULT);
    private static final List<Integer> DELETE_DELIMITING_WORDS = Arrays.asList(SQLServerLexer.OPTION,
            SQLServerLexer.WHERE, SQLServerLexer.OUTPUT, SQLServerLexer.FROM);
    private static final List<Integer> UPDATE_DELIMITING_WORDS = Arrays.asList(SQLServerLexer.SET,
            SQLServerLexer.OUTPUT, SQLServerLexer.WHERE, SQLServerLexer.OPTION);
    private static final List<Integer> FROM_DELIMITING_WORDS = Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.GROUP,
            SQLServerLexer.HAVING, SQLServerLexer.ORDER, SQLServerLexer.OPTION, SQLServerLexer.AND);

    /*
     * Retrieves the table target from a single query.
     */
    static void parseQuery(SQLServerTokenIterator iter, SQLServerFMTQuery query) throws SQLServerException {
        Token t = null;
        while (iter.hasNext()) {
            t = iter.next();
            switch (t.getType()) {
                case SQLServerLexer.SELECT:
                    t = skipTop(iter);
                    while (t.getType() != SQLServerLexer.SEMI) {
                        if (t.getType() == SQLServerLexer.PARAMETER) {
                            String columnName = SQLServerParser.findColumnAroundParameter(iter);
                            query.getColumns().add(columnName);
                        }
                        if (t.getType() == SQLServerLexer.FROM) {
                            query.getTableTarget()
                                    .add(getTableTargetChunk(iter, query.getAliases(), SELECT_DELIMITING_WORDS));
                            break;
                        }
                        if (iter.hasNext()) {
                            t = iter.next();
                        } else {
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
                    query.getTableTarget().add(getTableTargetChunk(iter, query.getAliases(), INSERT_DELIMITING_WORDS));

                    if (iter.hasNext()) {
                        List<String> tableValues = getValuesList(iter);
                        // VALUES case
                        boolean valuesFound = false;
                        int valuesMarker = iter.nextIndex();
                        while (!valuesFound && iter.hasNext()) {
                            t = iter.next();
                            if (t.getType() == SQLServerLexer.VALUES) {
                                valuesFound = true;
                                do {
                                    query.getValuesList().add(getValuesList(iter));
                                } while (iter.hasNext() && iter.next().getType() == SQLServerLexer.COMMA);
                                iter.previous();
                            }
                        }
                        if (!valuesFound) {
                            resetIteratorIndex(iter, valuesMarker);
                        }
                        if (!query.getValuesList().isEmpty()) {
                            for (List<String> ls : query.getValuesList()) {
                                if (tableValues.isEmpty()) {
                                    query.getColumns().add("*");
                                }
                                for (int i = 0; i < ls.size(); i++) {
                                    if ("?".equalsIgnoreCase(ls.get(i))) {
                                        if (0 == tableValues.size()) {
                                            query.getColumns().add("?");
                                        } else {
                                            if (i < tableValues.size()) {
                                                query.getColumns().add(tableValues.get(i));
                                            } else {
                                                SQLServerException.makeFromDriverError(null, null,
                                                        SQLServerResource.getResource("R_invalidInsertValuesQuery"),
                                                        null, false);
                                            }
                                        }
                                    }
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
                    query.getTableTarget().add(getTableTargetChunk(iter, query.getAliases(), DELETE_DELIMITING_WORDS));
                    break;
                case SQLServerLexer.UPDATE:
                    skipTop(iter);
                    t = iter.previous();
                    // Get next chunk
                    query.getTableTarget().add(getTableTargetChunk(iter, query.getAliases(), UPDATE_DELIMITING_WORDS));
                    break;
                case SQLServerLexer.FROM:
                    query.getTableTarget().add(getTableTargetChunk(iter, query.getAliases(), FROM_DELIMITING_WORDS));
                    break;
                case SQLServerLexer.PARAMETER:
                    int parameterIndex = iter.nextIndex();
                    String columnName = SQLServerParser.findColumnAroundParameter(iter);
                    query.getColumns().add(columnName);
                    resetIteratorIndex(iter, parameterIndex);
                    break;
                default:
                    // skip, we only support SELECT/UPDATE/INSERT/DELETE
                    break;
            }
        }
    }

    static void resetIteratorIndex(SQLServerTokenIterator iter, int index) {
        if (iter.nextIndex() < index) {
            while (iter.nextIndex() != index) {
                iter.next();
            }
        } else if (iter.nextIndex() > index) {
            while (iter.nextIndex() != index) {
                iter.previous();
            }
        }
    }

    private static String getRoundBracketChunk(SQLServerTokenIterator iter) throws SQLServerException {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        Stack<String> s = new Stack<>();
        s.push("(");
        while (!s.empty() && iter.hasNext()) {
            Token t = iter.next();
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

    private static String getRoundBracketChunkBefore(SQLServerTokenIterator iter) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        Stack<String> s = new Stack<>();
        s.push(")");
        while (!s.empty()) {
            Token t = iter.previous();
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

    private static final List<Integer> OPERATORS = Arrays.asList(SQLServerLexer.EQUAL, SQLServerLexer.GREATER,
            SQLServerLexer.LESS, SQLServerLexer.GREATER_EQUAL, SQLServerLexer.LESS_EQUAL, SQLServerLexer.NOT_EQUAL,
            SQLServerLexer.PLUS_ASSIGN, SQLServerLexer.MINUS_ASSIGN, SQLServerLexer.MULT_ASSIGN,
            SQLServerLexer.DIV_ASSIGN, SQLServerLexer.MOD_ASSIGN, SQLServerLexer.AND_ASSIGN, SQLServerLexer.XOR_ASSIGN,
            SQLServerLexer.OR_ASSIGN, SQLServerLexer.STAR, SQLServerLexer.DIVIDE, SQLServerLexer.MODULE,
            SQLServerLexer.PLUS, SQLServerLexer.MINUS, SQLServerLexer.LIKE, SQLServerLexer.IN, SQLServerLexer.BETWEEN);

    static String findColumnAroundParameter(SQLServerTokenIterator iter) throws SQLServerException {
        int index = iter.nextIndex();
        iter.previous();
        String value = findColumnBeforeParameter(iter);
        resetIteratorIndex(iter, index);
        if ("".equalsIgnoreCase(value)) {
            value = findColumnAfterParameter(iter);
            resetIteratorIndex(iter, index);
        }
        return value;
    }

    private static String findColumnAfterParameter(SQLServerTokenIterator iter) throws SQLServerException {
        StringBuilder sb = new StringBuilder();
        while (0 == sb.length() && iter.hasNext()) {
            Token t = iter.next();
            if (t.getType() == SQLServerLexer.NOT && iter.hasNext()) {
                t = iter.next(); // skip NOT
            }
            if (OPERATORS.contains(t.getType()) && iter.hasNext()) {
                t = iter.next();
                if (t.getType() != SQLServerLexer.PARAMETER) {
                    if (t.getType() == SQLServerLexer.LR_BRACKET) {
                        sb.append(getRoundBracketChunk(iter));
                    } else {
                        sb.append(t.getText());
                    }
                    for (int i = 0; i < 3 && iter.hasNext(); i++) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.DOT) {
                            sb.append(".");
                            if (iter.hasNext()) {
                                t = iter.next();
                                sb.append(t.getText());
                            }
                        }
                    }
                }
            } else {
                return "";
            }
        }
        return sb.toString();
    }

    private static String findColumnBeforeParameter(SQLServerTokenIterator iter) {
        StringBuilder sb = new StringBuilder();
        while (0 == sb.length() && iter.hasPrevious()) {
            Token t = iter.previous();
            if (t.getType() == SQLServerLexer.DOLLAR && iter.hasPrevious()) {
                t = iter.previous(); // skip if it's a $ sign
            }
            if (t.getType() == SQLServerLexer.AND && iter.hasPrevious()) {
                t = iter.previous();
                if (iter.hasPrevious()) {
                    t = iter.previous(); // try to find BETWEEN
                    if (t.getType() == SQLServerLexer.BETWEEN && iter.hasNext()) {
                        iter.next();
                        continue;
                    } else {
                        return "";
                    }
                }
            }
            if (OPERATORS.contains(t.getType()) && iter.hasPrevious()) {
                t = iter.previous();
                if (t.getType() == SQLServerLexer.NOT) {
                    t = iter.previous(); // skip NOT if found
                }
                if (t.getType() != SQLServerLexer.PARAMETER) {
                    Deque<String> d = new ArrayDeque<>();
                    if (t.getType() == SQLServerLexer.RR_BRACKET) {
                        d.push(getRoundBracketChunkBefore(iter));
                    } else {
                        d.push(t.getText());
                    }
                    // Linked-servers can have a maximum of 4 parts
                    for (int i = 0; i < 3 && iter.hasPrevious(); i++) {
                        t = iter.previous();
                        if (t.getType() == SQLServerLexer.DOT) {
                            d.push(".");
                            if (iter.hasPrevious()) {
                                t = iter.previous();
                                d.push(t.getText());
                            }
                        }
                    }
                    d.stream().forEach(sb::append);
                }
            } else {
                return "";
            }
        }
        return sb.toString();
    }

    static List<String> getValuesList(SQLServerTokenIterator iter) throws SQLServerException {
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
                } else if (!iter.hasNext() && !d.isEmpty()) {
                    SQLServerException.makeFromDriverError(null, null,
                            SQLServerResource.getResource("R_invalidValuesList"), null, false);
                }
            } while (!d.isEmpty());
            return parameterColumns;
        } else {
            iter.previous();
            return new ArrayList<>();
        }
    }

    /*
     * Moves the iterator past the TOP clause to the next token. Returns the first token after the TOP clause.
     */
    static Token skipTop(SQLServerTokenIterator iter) throws SQLServerException {
        // Look for the TOP token
        if (!iter.hasNext()) {
            SQLServerException.makeFromDriverError(null, null, SQLServerResource.getResource("R_invalidUserSQL"), null,
                    false);
        }
        Token t = iter.next();
        if (t.getType() == SQLServerLexer.TOP) {
            t = iter.next();
            if (t.getType() == SQLServerLexer.LR_BRACKET) {
                getRoundBracketChunk(iter);
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

    static String getCTE(SQLServerTokenIterator iter) throws SQLServerException {
        if (iter.hasNext()) {
            Token t = iter.next();
            if (t.getType() == SQLServerLexer.WITH) {
                StringBuilder sb = new StringBuilder("WITH ");
                getCTESegment(iter, sb);
                return sb.toString();
            } else {
                iter.previous();
            }
        }
        return "";
    }

    static void getCTESegment(SQLServerTokenIterator iter, StringBuilder sb) throws SQLServerException {
        try {
            sb.append(getTableTargetChunk(iter, null, Arrays.asList(SQLServerLexer.AS)));
            iter.next();
            Token t = iter.next();
            sb.append(" AS ");
            if (t.getType() != SQLServerLexer.LR_BRACKET) {
                SQLServerException.makeFromDriverError(null, null, SQLServerResource.getResource("R_invalidCTEFormat"),
                        null, false);
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
        } catch (java.util.NoSuchElementException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerResource.getResource("R_invalidCTEFormat"),
                    null, false);
        }
    }

    private static String getTableTargetChunk(SQLServerTokenIterator iter, List<String> possibleAliases,
            List<Integer> delimiters) throws SQLServerException {
        StringBuilder sb = new StringBuilder();
        if (iter.hasNext()) {
            Token t = iter.next();
            do {
                switch (t.getType()) {
                    case SQLServerLexer.LR_BRACKET:
                        sb.append(getRoundBracketChunk(iter));
                        break;
                    case SQLServerLexer.OPENDATASOURCE:
                    case SQLServerLexer.OPENJSON:
                    case SQLServerLexer.OPENQUERY:
                    case SQLServerLexer.OPENROWSET:
                    case SQLServerLexer.OPENXML:
                        sb.append(t.getText());
                        t = iter.next();
                        if (t.getType() != SQLServerLexer.LR_BRACKET) {
                            SQLServerException.makeFromDriverError(null, null,
                                    SQLServerResource.getResource("R_invalidOpenqueryCall"), null, false);
                        }
                        sb.append(getRoundBracketChunk(iter));
                        break;
                    case SQLServerLexer.AS:
                        sb.append(t.getText());
                        if (iter.hasNext()) {
                            String s = iter.next().getText();
                            if (possibleAliases != null) {
                                possibleAliases.add(s);
                            } else {
                                SQLServerException.makeFromDriverError(null, null,
                                        SQLServerResource.getResource("R_invalidCTEFormat"), null, false);
                            }
                            sb.append(" ").append(s);
                        }
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
        }
        return sb.toString().trim();
    }
}


final class SQLServerTokenIterator {
    private final AtomicInteger index;
    private final int listSize;
    private final ListIterator<? extends Token> iter;

    SQLServerTokenIterator(ArrayList<? extends Token> tokenList) {
        this.iter = tokenList.listIterator();
        this.index = new AtomicInteger(0);
        this.listSize = tokenList.size();
    }

    Token next() {
        index.incrementAndGet();
        return iter.next();
    }

    Token previous() {
        index.decrementAndGet();
        return iter.previous();
    }

    boolean hasNext() {
        return (index.intValue() < listSize);
    }

    boolean hasPrevious() {
        return (index.intValue() > 0);
    }

    int nextIndex() {
        return index.intValue() + 1;
    }
}
