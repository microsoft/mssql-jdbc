package com.microsoft.sqlserver.jdbc.fmtonly;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.jdbc.SQLServerParser;


@RunWith(JUnitPlatform.class)
public class ParserTest extends AbstractTest {

    @Test
    public void removeWSCommentsTest() {
        Set<StringPair> inputs = new HashSet<>();
        /*
         * white space cases
         */
        inputs.add(new StringPair(" SELECT","SELECT"));//1 ws
        inputs.add(new StringPair(" SELECT ","SELECT "));//1 ws and 1 ending ws
        inputs.add(new StringPair(" SELECT   ","SELECT   "));//1 ws and multiple ending ws
        inputs.add(new StringPair("   SELECT","SELECT"));//multiple ws
        inputs.add(new StringPair("   SELECT ","SELECT "));//multiple ws, 1 ending ws
        inputs.add(new StringPair("   SELECT   ","SELECT   "));//multiple ws, multiple ending ws
        inputs.add(new StringPair("\nSELECT","SELECT"));//newline
        inputs.add(new StringPair("\nSELECT\n","SELECT\n"));//newline, 1 ending newline
        inputs.add(new StringPair("\nSELECT ","SELECT "));
        /*
         * Block comment cases
         */
        inputs.add(new StringPair("/**/SELECT","SELECT"));
        inputs.add(new StringPair("/*comment*/SELECT","SELECT"));
        inputs.add(new StringPair("/**/SELECT ","SELECT "));
        inputs.add(new StringPair("/*comment*/SELECT ","SELECT "));
        inputs.add(new StringPair("/**/SELECT   ","SELECT   "));
        inputs.add(new StringPair("/*comment*/SELECT   ","SELECT   "));
        inputs.add(new StringPair("/*SELECT*/",""));
        inputs.add(new StringPair("/*SELECT",""));
        /*
         * Line comment cases
         */
        inputs.add(new StringPair("--\nSELECT","SELECT"));
        inputs.add(new StringPair("--comment\nSELECT","SELECT"));
        inputs.add(new StringPair("--\nSELECT ","SELECT "));
        inputs.add(new StringPair("--comment\nSELECT ","SELECT "));
        inputs.add(new StringPair("--\nSELECT   ","SELECT   "));
        inputs.add(new StringPair("--comment\nSELECT   ","SELECT   "));
        inputs.add(new StringPair("--SELECT",""));
        /*
         * Mixed
         */
        inputs.add(new StringPair("",""));
        inputs.add(new StringPair("SELECT","SELECT"));
        inputs.add(new StringPair("/*mixed*/ --comments and ws\n SELECT","SELECT"));
        inputs.add(new StringPair("/*mixed*/ --comments and ws\n SELECT ","SELECT "));
        inputs.add(new StringPair("/*mixed*/ --comments and ws\n SELECT   ","SELECT   "));
        
        for(StringPair p : inputs) {
            execSkipWSComments(p.getFirst(),p.getSecond());
        }
    }

    private class StringPair {
        private String first;
        private String second;
        
        public StringPair(String first, String second) {
            this.first = first;
            this.second = second;
        }
        
        public String getFirst() {
            return first;
        }
        
        public String getSecond() {
            return second;
        }
    }

    private void execSkipWSComments(String testValue, String expectedValue) {
        assertEquals(SQLServerParser.skipWSComments(testValue), expectedValue);
    }
}
