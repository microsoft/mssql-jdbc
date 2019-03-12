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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;


class SQLServerParser {

    /*
     * Retrieves the table target from a single query.
     */
    static String getTableName(String userSQL) throws SQLServerException {
        InputStream stream = new ByteArrayInputStream(userSQL.getBytes(StandardCharsets.UTF_8));
        SQLServerLexer lexer = null;
        try {
            lexer = new SQLServerLexer(CharStreams.fromStream(stream));
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, userSQL, e.getLocalizedMessage(), "", false);
        }
        ArrayList<? extends Token> tokenList = (ArrayList<? extends Token>) lexer.getAllTokens();
        //tokenList.forEach(t -> System.out.println(t.toString()));
        ListIterator<? extends Token> iter = tokenList.listIterator();
        List<String> openingBrackets = Arrays.asList("'","\"","(","[","{");
        while (iter.hasNext()) {
            Token t = iter.next();
            switch (t.getType()) {
                case SQLServerLexer.SELECT:
                    int selectIndex = iter.nextIndex();
                    boolean foundFrom = false;
                    while (t.getType() != SQLServerLexer.SEMI && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.FROM) {
                            foundFrom = true;
                            return getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.GROUP,
                                    SQLServerLexer.HAVING, SQLServerLexer.ORDER, SQLServerLexer.OPTION));
                        } else if (openingBrackets.contains(t.getText())) {
                            skipBracket(iter, t.getText());
                        }
                    }
                    if (!foundFrom) {
                        // Go back to where we first found our SELECT
                        iter = tokenList.listIterator(selectIndex);
                        try {
                            // Get next chunk
                            return getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.WHERE, SQLServerLexer.GROUP,
                                    SQLServerLexer.HAVING, SQLServerLexer.ORDER, SQLServerLexer.OPTION));
                        } catch (NoSuchElementException e) {
                            SQLServerException.makeFromDriverError(null, tokenList, e.getLocalizedMessage(), "", false);
                        }
                    }
                    break;
                case SQLServerLexer.INSERT:
                    int insertIndex = iter.nextIndex();
                    boolean foundInto = false;
                    while (t.getType() != SQLServerLexer.SEMI && iter.hasNext()) {
                        t = iter.next();
                        if (t.getType() == SQLServerLexer.INTO) {
                            foundFrom = true;
                            return getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.VALUES, SQLServerLexer.OUTPUT,
                                    SQLServerLexer.RR_BRACKET));
                        }
                    }
                    if (!foundInto) {
                        // Go back to where we first found our INSERT
                        iter = tokenList.listIterator(insertIndex);
                        try {
                            // Get next chunk
                            return getTableTargetChunk(iter, Arrays.asList(SQLServerLexer.VALUES, SQLServerLexer.OUTPUT,
                                    SQLServerLexer.RR_BRACKET));
                        } catch (NoSuchElementException e) {
                            SQLServerException.makeFromDriverError(null, tokenList, e.getLocalizedMessage(), "", false);
                        }
                    }
                case SQLServerLexer.DELETE:

                    break;
                case SQLServerLexer.UPDATE:
                    break;
                    
                case SQLServerLexer.EXECUTE:// covers both exec and execute
                    break;
                default:
                    // skip, we only support SELECT/UPDATE/INSERT/DELETE/EXEC/EXECUTE
                    break;
            }
            t = lexer.nextToken();
        }
        return "";
    }
    
    /*
     * Doesn't only cover brackets. Covers ', ", (, [, and {.
     */
    private static void skipBracket(ListIterator<? extends Token> iter, String firstBracket) throws SQLServerException {
        Stack<String> s = new Stack<>();
        s.push(firstBracket);
        while (iter.hasNext() && !s.empty()) {
            Token t = iter.next();
            if (s.peek().equalsIgnoreCase("\"") || s.peek().equalsIgnoreCase("[") || s.peek().equalsIgnoreCase("'")) {
                handleLiteral(t,s,iter);
            } else {
                handleNonLiteral(t,s);
            }
        }
        if (!s.empty()) {
            SQLServerException.makeFromDriverError(null, null, "SQL Syntax Error", "", false);
        }
    }
    
    private static void handleNonLiteral(Token t, Stack<String> s) {
        switch (t.getType()) {
            case SQLServerLexer.LR_BRACKET:
                s.push("(");
                break;
            case SQLServerLexer.RR_BRACKET:
                if (s.peek().equalsIgnoreCase("("))
                    s.pop();
                break;
            case SQLServerLexer.LC_BRACKET:
                s.push("{");
                break;
            case SQLServerLexer.RC_BRACKET:
                if (s.peek().equalsIgnoreCase("{"))
                    s.pop();
                break;
            case SQLServerLexer.LS_BRACKET:
                s.push("[");
                break;
            case SQLServerLexer.SINGLE_QUOTE:
                s.push("'");
                break;
            case SQLServerLexer.DOUBLE_QUOTE:
                s.push("\"");
                break;
            default:
                break;
        }
    }
    
    private static void handleLiteral(Token t, Stack<String> s, ListIterator<? extends Token> iter) {
        switch (t.getType()) {
            case SQLServerLexer.RS_BRACKET:
                if ((s.peek().equalsIgnoreCase("["))) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.RS_BRACKET) {
                            //Do nothing, continue skipping
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
                if ((s.peek().equalsIgnoreCase("'"))) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.SINGLE_QUOTE) {
                            //Do nothing, continue skipping
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
                if ((s.peek().equalsIgnoreCase("\""))) {
                    if (iter.hasNext()) {
                        Token tempToken = iter.next();
                        if (tempToken.getType() == SQLServerLexer.DOUBLE_QUOTE) {
                            //Do nothing, continue skipping
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

    private static String getTableTargetChunk(ListIterator<? extends Token> iter, List<Integer> list) throws SQLServerException {
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
                        //TODO: throw index out of bounds
                        SQLServerException.makeFromDriverError(null, null, e.getLocalizedMessage(), "", false);
                    }
                    break;
                case SQLServerLexer.LS_BRACKET:
                    sb.append("[");
                    while (iter.hasNext()) {
                        t = iter.next();
                        sb.append(t.getText());
                        if (t.getType() == SQLServerLexer.RS_BRACKET && !iter.hasNext()) {
                            // nothing more past the first ending bracket, not escaped
                            break;
                        } else if (t.getType() == SQLServerLexer.RS_BRACKET) {
                            t = iter.next();
                            if (t.getType() == SQLServerLexer.RS_BRACKET) {
                                sb.append("]");
                                if (!iter.hasNext()) {
                                    // TODO: THROW EXCEPTION IF ENDING BRACKET COUNT DOESN'T MATCH
                                    SQLServerException.makeFromDriverError(null, null, "Unclosed sqaure brackets", "", false);
                                    break;
                                }
                            } else {
                                // Not a delimited token, move the iterator back
                                t = iter.previous();
                                break;
                            }
                        }
                    }
                    break;
                case SQLServerLexer.DOUBLE_QUOTE:
                    do {
                        sb.append(t.getText());
                        t = iter.next();
                    } while (t.getType() != SQLServerLexer.DOUBLE_QUOTE && iter.hasNext());
                    sb.append('"');
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
        } while (!list.contains(t.getType()) && t.getType() != SQLServerLexer.SEMI);

        return sb.toString();
    }
}
