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


public class ParserUtils {

    public static String getTableName(String s) {
        try {
            SQLServerFMTQuery u = new SQLServerFMTQuery(s);
            
            String fmtQuery = u.getFMTQuery();
            System.out.println(fmtQuery+"\n");
            
            String tableName = u.constructTableTargets();

            return tableName;
        } catch (SQLServerException e) {
            // TODO Auto-generated catch block
            return null;
        }
    }
    
    public static String getCTE(String s) {
        InputStream stream = new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
        SQLServerLexer lexer = null;
        try {
            lexer = new SQLServerLexer(CharStreams.fromStream(stream));
            invokeANTLRMethods(lexer);
            ArrayList<? extends Token> tokenList = (ArrayList<? extends Token>) lexer.getAllTokens();
            ListIterator<? extends Token> iter = tokenList.listIterator();
            return SQLServerParser.getCTE(iter);
        } catch (IOException | SQLServerException e) {
            // TODO Auto-generated catch block
            return null;
        }
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
