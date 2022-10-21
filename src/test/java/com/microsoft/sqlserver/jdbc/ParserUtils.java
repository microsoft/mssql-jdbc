package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;


public class ParserUtils {

    private static String getTableName(String s) {
        try {
            return new SQLServerFMTQuery(s).constructTableTargets();
        } catch (SQLServerException e) {
            return e.getLocalizedMessage();
        }
    }

    private static String getCTE(String s) {
        InputStream stream = new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
        SQLServerLexer lexer = null;
        try {
            lexer = new SQLServerLexer(CharStreams.fromStream(stream));
            invokeANTLRMethods(lexer);
            ArrayList<? extends Token> tokenList = (ArrayList<? extends Token>) lexer.getAllTokens();
            SQLServerTokenIterator iter = new SQLServerTokenIterator(tokenList);
            return SQLServerParser.getCTE(iter);
        } catch (IOException | SQLServerException e) {
            return e.getLocalizedMessage();
        }
    }

    public static void compareTableName(String tsql, String expected) {
        // Verbose to make debugging more friendly
        String extractedTableName = ParserUtils.getTableName(tsql).trim();
        assertEquals(expected, extractedTableName);
    }

    public static void compareCommonTableExpression(String tsql, String expected) {
        // Verbose to make debugging more friendly
        String extractedTableName = ParserUtils.getCTE(tsql).trim();
        assertEquals(expected, extractedTableName);
    }

    @SuppressWarnings("deprecation")
    private static void invokeANTLRMethods(SQLServerLexer s) {
        s.getTokenNames();
        s.getVocabulary();
        s.getGrammarFileName();
        s.getRuleNames();
        s.getSerializedATN();
        s.getChannelNames();
        s.getModeNames();
        s.getATN();
    }
}
