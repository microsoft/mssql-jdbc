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
            useFmtOnlyQuery u = useFmtOnlyQuery.getFmtQuery(s);
            
            String fmtQuery = u.getFMTQuery();
            System.out.println(fmtQuery+"\n");
            
//            System.out.println(u.getTableTarget());

            return u.getTableTarget().stream().map(String::trim).distinct().collect(Collectors.joining(","));
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
            ArrayList<? extends Token> tokenList = (ArrayList<? extends Token>) lexer.getAllTokens();
            ListIterator<? extends Token> iter = tokenList.listIterator();
            String s2 = SQLServerParser.getCTE(iter);
            System.out.println(s2);
            return s2;
        } catch (IOException | SQLServerException e) {
            // TODO Auto-generated catch block
            return null;
        }
    }

    public static List<? extends Token> getTokens(String userSQL) {
        try {
            return new SQLServerLexer(
                    CharStreams.fromStream(new ByteArrayInputStream(userSQL.getBytes(StandardCharsets.UTF_8))))
                            .getAllTokens();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            return null;
        }
    }
}
