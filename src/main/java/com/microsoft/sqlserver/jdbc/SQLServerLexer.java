// Generated from SQLServerLexer.g4 by ANTLR 4.7.2
package com.microsoft.sqlserver.jdbc;

import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RuntimeMetaData;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.VocabularyImpl;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SQLServerLexer extends Lexer {
        static { RuntimeMetaData.checkVersion("4.7.2", RuntimeMetaData.VERSION); }

        protected static final DFA[] _decisionToDFA;
        protected static final PredictionContextCache _sharedContextCache =
                new PredictionContextCache();
        public static final int
                SELECT=1, INSERT=2, DELETE=3, UPDATE=4, FROM=5, INTO=6, EXECUTE=7, WHERE=8, 
                HAVING=9, GROUP=10, ORDER=11, OPTION=12, BY=13, VALUES=14, OUTPUT=15, 
                OJ=16, WITH=17, AS=18, DEFAULT=19, SET=20, OPENQUERY=21, TOP=22, DISCTINCT=23, 
                PERCENT=24, TIES=25, SPACE=26, COMMENT=27, LINE_COMMENT=28, DOUBLE_QUOTE=29, 
                SINGLE_QUOTE=30, LOCAL_ID=31, DECIMAL=32, ID=33, STRING=34, DOUBLE_LITERAL=35, 
                SQUARE_LITERAL=36, BINARY=37, FLOAT=38, REAL=39, EQUAL=40, GREATER=41, 
                LESS=42, EXCLAMATION=43, PLUS_ASSIGN=44, MINUS_ASSIGN=45, MULT_ASSIGN=46, 
                DIV_ASSIGN=47, MOD_ASSIGN=48, AND_ASSIGN=49, XOR_ASSIGN=50, OR_ASSIGN=51, 
                DOUBLE_BAR=52, DOT=53, UNDERLINE=54, AT=55, SHARP=56, DOLLAR=57, LR_BRACKET=58, 
                RR_BRACKET=59, LS_BRACKET=60, RS_BRACKET=61, LC_BRACKET=62, RC_BRACKET=63, 
                COMMA=64, SEMI=65, COLON=66, STAR=67, DIVIDE=68, MODULE=69, PLUS=70, MINUS=71, 
                BIT_NOT=72, BIT_OR=73, BIT_AND=74, BIT_XOR=75, PARAMETER=76;
        public static String[] channelNames = {
                "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
        };

        public static String[] modeNames = {
                "DEFAULT_MODE"
        };

        private static String[] makeRuleNames() {
                return new String[] {
                        "SELECT", "INSERT", "DELETE", "UPDATE", "FROM", "INTO", "EXECUTE", "WHERE", 
                        "HAVING", "GROUP", "ORDER", "OPTION", "BY", "VALUES", "OUTPUT", "OJ", 
                        "WITH", "AS", "DEFAULT", "SET", "OPENQUERY", "TOP", "DISCTINCT", "PERCENT", 
                        "TIES", "SPACE", "COMMENT", "LINE_COMMENT", "DOUBLE_QUOTE", "SINGLE_QUOTE", 
                        "LOCAL_ID", "DECIMAL", "ID", "STRING", "DOUBLE_LITERAL", "SQUARE_LITERAL", 
                        "BINARY", "FLOAT", "REAL", "EQUAL", "GREATER", "LESS", "EXCLAMATION", 
                        "PLUS_ASSIGN", "MINUS_ASSIGN", "MULT_ASSIGN", "DIV_ASSIGN", "MOD_ASSIGN", 
                        "AND_ASSIGN", "XOR_ASSIGN", "OR_ASSIGN", "DOUBLE_BAR", "DOT", "UNDERLINE", 
                        "AT", "SHARP", "DOLLAR", "LR_BRACKET", "RR_BRACKET", "LS_BRACKET", "RS_BRACKET", 
                        "LC_BRACKET", "RC_BRACKET", "COMMA", "SEMI", "COLON", "STAR", "DIVIDE", 
                        "MODULE", "PLUS", "MINUS", "BIT_NOT", "BIT_OR", "BIT_AND", "BIT_XOR", 
                        "PARAMETER", "DEC_DOT_DEC", "HEX_DIGIT", "DEC_DIGIT", "A", "B", "C", 
                        "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", 
                        "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "FullWidthLetter"
                };
        }
        public static final String[] ruleNames = makeRuleNames();

        private static String[] makeLiteralNames() {
                return new String[] {
                        null, null, null, null, null, null, null, null, null, null, null, null, 
                        null, null, null, null, null, null, null, null, null, null, null, null, 
                        null, null, null, null, null, "'\"'", "'''", null, null, null, null, 
                        null, null, null, null, null, "'='", "'>'", "'<'", "'!'", "'+='", "'-='", 
                        "'*='", "'/='", "'%='", "'&='", "'^='", "'|='", "'||'", "'.'", "'_'", 
                        "'@'", "'#'", "'$'", "'('", "')'", "'['", "']'", "'{'", "'}'", "','", 
                        "';'", "':'", "'*'", "'/'", "'%'", "'+'", "'-'", "'~'", "'|'", "'&'", 
                        "'^'", "'?'"
                };
        }
        private static final String[] _LITERAL_NAMES = makeLiteralNames();
        private static String[] makeSymbolicNames() {
                return new String[] {
                        null, "SELECT", "INSERT", "DELETE", "UPDATE", "FROM", "INTO", "EXECUTE", 
                        "WHERE", "HAVING", "GROUP", "ORDER", "OPTION", "BY", "VALUES", "OUTPUT", 
                        "OJ", "WITH", "AS", "DEFAULT", "SET", "OPENQUERY", "TOP", "DISCTINCT", 
                        "PERCENT", "TIES", "SPACE", "COMMENT", "LINE_COMMENT", "DOUBLE_QUOTE", 
                        "SINGLE_QUOTE", "LOCAL_ID", "DECIMAL", "ID", "STRING", "DOUBLE_LITERAL", 
                        "SQUARE_LITERAL", "BINARY", "FLOAT", "REAL", "EQUAL", "GREATER", "LESS", 
                        "EXCLAMATION", "PLUS_ASSIGN", "MINUS_ASSIGN", "MULT_ASSIGN", "DIV_ASSIGN", 
                        "MOD_ASSIGN", "AND_ASSIGN", "XOR_ASSIGN", "OR_ASSIGN", "DOUBLE_BAR", 
                        "DOT", "UNDERLINE", "AT", "SHARP", "DOLLAR", "LR_BRACKET", "RR_BRACKET", 
                        "LS_BRACKET", "RS_BRACKET", "LC_BRACKET", "RC_BRACKET", "COMMA", "SEMI", 
                        "COLON", "STAR", "DIVIDE", "MODULE", "PLUS", "MINUS", "BIT_NOT", "BIT_OR", 
                        "BIT_AND", "BIT_XOR", "PARAMETER"
                };
        }
        private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
        public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

        /**
         * @deprecated Use {@link #VOCABULARY} instead.
         */
        @Deprecated
        public static final String[] tokenNames;
        static {
                tokenNames = new String[_SYMBOLIC_NAMES.length];
                for (int i = 0; i < tokenNames.length; i++) {
                        tokenNames[i] = VOCABULARY.getLiteralName(i);
                        if (tokenNames[i] == null) {
                                tokenNames[i] = VOCABULARY.getSymbolicName(i);
                        }

                        if (tokenNames[i] == null) {
                                tokenNames[i] = "<INVALID>";
                        }
                }
        }

        @Override
        @Deprecated
        public String[] getTokenNames() {
                return tokenNames;
        }

        @Override

        public Vocabulary getVocabulary() {
                return VOCABULARY;
        }


        public SQLServerLexer(CharStream input) {
                super(input);
                _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
        }

        @Override
        public String getGrammarFileName() { return "SQLServerLexer.g4"; }

        @Override
        public String[] getRuleNames() { return ruleNames; }

        @Override
        public String getSerializedATN() { return _serializedATN; }

        @Override
        public String[] getChannelNames() { return channelNames; }

        @Override
        public String[] getModeNames() { return modeNames; }

        @Override
        public ATN getATN() { return _ATN; }

        public static final String _serializedATN =
                "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2N\u0291\b\1\4\2\t"+
                "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
                "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
                "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
                "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
                "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
                ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
                "\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
                "\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
                "\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
                "\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
                "`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"+
                "k\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4"+
                "\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\7\3"+
                "\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u0106\n\b\3\t\3\t\3"+
                "\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3"+
                "\13\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16"+
                "\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
                "\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\24\3\24\3\24"+
                "\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26"+
                "\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30"+
                "\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32"+
                "\3\32\3\32\3\32\3\33\6\33\u0175\n\33\r\33\16\33\u0176\3\33\3\33\3\34\3"+
                "\34\3\34\3\34\3\34\7\34\u0180\n\34\f\34\16\34\u0183\13\34\3\34\3\34\3"+
                "\34\3\34\3\34\3\35\3\35\3\35\3\35\7\35\u018e\n\35\f\35\16\35\u0191\13"+
                "\35\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \6 \u019c\n \r \16 \u019d\3"+
                "!\6!\u01a1\n!\r!\16!\u01a2\3\"\3\"\5\"\u01a7\n\"\3\"\3\"\7\"\u01ab\n\""+
                "\f\"\16\"\u01ae\13\"\3#\5#\u01b1\n#\3#\3#\3#\3#\7#\u01b7\n#\f#\16#\u01ba"+
                "\13#\3#\3#\3$\3$\3$\3$\7$\u01c2\n$\f$\16$\u01c5\13$\3$\3$\3%\3%\3%\3%"+
                "\7%\u01cd\n%\f%\16%\u01d0\13%\3%\3%\3&\3&\3&\7&\u01d7\n&\f&\16&\u01da"+
                "\13&\3\'\3\'\3(\3(\5(\u01e0\n(\3(\3(\5(\u01e4\n(\3(\6(\u01e7\n(\r(\16"+
                "(\u01e8\3)\3)\3*\3*\3+\3+\3,\3,\3-\3-\3-\3.\3.\3.\3/\3/\3/\3\60\3\60\3"+
                "\60\3\61\3\61\3\61\3\62\3\62\3\62\3\63\3\63\3\63\3\64\3\64\3\64\3\65\3"+
                "\65\3\65\3\66\3\66\3\67\3\67\38\38\39\39\3:\3:\3;\3;\3<\3<\3=\3=\3>\3"+
                ">\3?\3?\3@\3@\3A\3A\3B\3B\3C\3C\3D\3D\3E\3E\3F\3F\3G\3G\3H\3H\3I\3I\3"+
                "J\3J\3K\3K\3L\3L\3M\3M\3N\6N\u023f\nN\rN\16N\u0240\3N\3N\6N\u0245\nN\r"+
                "N\16N\u0246\3N\6N\u024a\nN\rN\16N\u024b\3N\3N\3N\3N\6N\u0252\nN\rN\16"+
                "N\u0253\5N\u0256\nN\3O\3O\3P\3P\3Q\3Q\3R\3R\3S\3S\3T\3T\3U\3U\3V\3V\3"+
                "W\3W\3X\3X\3Y\3Y\3Z\3Z\3[\3[\3\\\3\\\3]\3]\3^\3^\3_\3_\3`\3`\3a\3a\3b"+
                "\3b\3c\3c\3d\3d\3e\3e\3f\3f\3g\3g\3h\3h\3i\3i\3j\3j\3k\3k\3\u0181\2l\3"+
                "\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37"+
                "\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37="+
                " ?!A\"C#E$G%I&K\'M(O)Q*S+U,W-Y.[/]\60_\61a\62c\63e\64g\65i\66k\67m8o9"+
                "q:s;u<w=y>{?}@\177A\u0081B\u0083C\u0085D\u0087E\u0089F\u008bG\u008dH\u008f"+
                "I\u0091J\u0093K\u0095L\u0097M\u0099N\u009b\2\u009d\2\u009f\2\u00a1\2\u00a3"+
                "\2\u00a5\2\u00a7\2\u00a9\2\u00ab\2\u00ad\2\u00af\2\u00b1\2\u00b3\2\u00b5"+
                "\2\u00b7\2\u00b9\2\u00bb\2\u00bd\2\u00bf\2\u00c1\2\u00c3\2\u00c5\2\u00c7"+
                "\2\u00c9\2\u00cb\2\u00cd\2\u00cf\2\u00d1\2\u00d3\2\u00d5\2\3\2\'\5\2\13"+
                "\f\17\17\"\"\4\2\f\f\17\17\7\2%&\62;B\\aac|\6\2%%C\\aac|\3\2))\3\2$$\3"+
                "\2__\4\2--//\4\2\62;CH\3\2\62;\4\2CCcc\4\2DDdd\4\2EEee\4\2FFff\4\2GGg"+
                "g\4\2HHhh\4\2IIii\4\2JJjj\4\2KKkk\4\2LLll\4\2MMmm\4\2NNnn\4\2OOoo\4\2"+
                "PPpp\4\2QQqq\4\2RRrr\4\2SSss\4\2TTtt\4\2UUuu\4\2VVvv\4\2WWww\4\2XXxx\4"+
                "\2YYyy\4\2ZZzz\4\2[[{{\4\2\\\\||\f\2\u00c2\u00d8\u00da\u00f8\u00fa\u2001"+
                "\u2c02\u3001\u3042\u3191\u3302\u3381\u3402\u4001\u4e02\ud801\uf902\ufb01"+
                "\uff02\ufff2\2\u028e\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2"+
                "\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3"+
                "\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2"+
                "\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2"+
                "\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2"+
                "\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2"+
                "\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\2Q"+
                "\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\2[\3\2\2\2\2]\3\2"+
                "\2\2\2_\3\2\2\2\2a\3\2\2\2\2c\3\2\2\2\2e\3\2\2\2\2g\3\2\2\2\2i\3\2\2\2"+
                "\2k\3\2\2\2\2m\3\2\2\2\2o\3\2\2\2\2q\3\2\2\2\2s\3\2\2\2\2u\3\2\2\2\2w"+
                "\3\2\2\2\2y\3\2\2\2\2{\3\2\2\2\2}\3\2\2\2\2\177\3\2\2\2\2\u0081\3\2\2"+
                "\2\2\u0083\3\2\2\2\2\u0085\3\2\2\2\2\u0087\3\2\2\2\2\u0089\3\2\2\2\2\u008b"+
                "\3\2\2\2\2\u008d\3\2\2\2\2\u008f\3\2\2\2\2\u0091\3\2\2\2\2\u0093\3\2\2"+
                "\2\2\u0095\3\2\2\2\2\u0097\3\2\2\2\2\u0099\3\2\2\2\3\u00d7\3\2\2\2\5\u00de"+
                "\3\2\2\2\7\u00e5\3\2\2\2\t\u00ec\3\2\2\2\13\u00f3\3\2\2\2\r\u00f8\3\2"+
                "\2\2\17\u00fd\3\2\2\2\21\u0107\3\2\2\2\23\u010d\3\2\2\2\25\u0114\3\2\2"+
                "\2\27\u011a\3\2\2\2\31\u0120\3\2\2\2\33\u0127\3\2\2\2\35\u012a\3\2\2\2"+
                "\37\u0131\3\2\2\2!\u0138\3\2\2\2#\u013b\3\2\2\2%\u0140\3\2\2\2\'\u0143"+
                "\3\2\2\2)\u014b\3\2\2\2+\u014f\3\2\2\2-\u0159\3\2\2\2/\u015d\3\2\2\2\61"+
                "\u0166\3\2\2\2\63\u016e\3\2\2\2\65\u0174\3\2\2\2\67\u017a\3\2\2\29\u0189"+
                "\3\2\2\2;\u0194\3\2\2\2=\u0196\3\2\2\2?\u0198\3\2\2\2A\u01a0\3\2\2\2C"+
                "\u01a6\3\2\2\2E\u01b0\3\2\2\2G\u01bd\3\2\2\2I\u01c8\3\2\2\2K\u01d3\3\2"+
                "\2\2M\u01db\3\2\2\2O\u01df\3\2\2\2Q\u01ea\3\2\2\2S\u01ec\3\2\2\2U\u01ee"+
                "\3\2\2\2W\u01f0\3\2\2\2Y\u01f2\3\2\2\2[\u01f5\3\2\2\2]\u01f8\3\2\2\2_"+
                "\u01fb\3\2\2\2a\u01fe\3\2\2\2c\u0201\3\2\2\2e\u0204\3\2\2\2g\u0207\3\2"+
                "\2\2i\u020a\3\2\2\2k\u020d\3\2\2\2m\u020f\3\2\2\2o\u0211\3\2\2\2q\u0213"+
                "\3\2\2\2s\u0215\3\2\2\2u\u0217\3\2\2\2w\u0219\3\2\2\2y\u021b\3\2\2\2{"+
                "\u021d\3\2\2\2}\u021f\3\2\2\2\177\u0221\3\2\2\2\u0081\u0223\3\2\2\2\u0083"+
                "\u0225\3\2\2\2\u0085\u0227\3\2\2\2\u0087\u0229\3\2\2\2\u0089\u022b\3\2"+
                "\2\2\u008b\u022d\3\2\2\2\u008d\u022f\3\2\2\2\u008f\u0231\3\2\2\2\u0091"+
                "\u0233\3\2\2\2\u0093\u0235\3\2\2\2\u0095\u0237\3\2\2\2\u0097\u0239\3\2"+
                "\2\2\u0099\u023b\3\2\2\2\u009b\u0255\3\2\2\2\u009d\u0257\3\2\2\2\u009f"+
                "\u0259\3\2\2\2\u00a1\u025b\3\2\2\2\u00a3\u025d\3\2\2\2\u00a5\u025f\3\2"+
                "\2\2\u00a7\u0261\3\2\2\2\u00a9\u0263\3\2\2\2\u00ab\u0265\3\2\2\2\u00ad"+
                "\u0267\3\2\2\2\u00af\u0269\3\2\2\2\u00b1\u026b\3\2\2\2\u00b3\u026d\3\2"+
                "\2\2\u00b5\u026f\3\2\2\2\u00b7\u0271\3\2\2\2\u00b9\u0273\3\2\2\2\u00bb"+
                "\u0275\3\2\2\2\u00bd\u0277\3\2\2\2\u00bf\u0279\3\2\2\2\u00c1\u027b\3\2"+
                "\2\2\u00c3\u027d\3\2\2\2\u00c5\u027f\3\2\2\2\u00c7\u0281\3\2\2\2\u00c9"+
                "\u0283\3\2\2\2\u00cb\u0285\3\2\2\2\u00cd\u0287\3\2\2\2\u00cf\u0289\3\2"+
                "\2\2\u00d1\u028b\3\2\2\2\u00d3\u028d\3\2\2\2\u00d5\u028f\3\2\2\2\u00d7"+
                "\u00d8\5\u00c5c\2\u00d8\u00d9\5\u00a9U\2\u00d9\u00da\5\u00b7\\\2\u00da"+
                "\u00db\5\u00a9U\2\u00db\u00dc\5\u00a5S\2\u00dc\u00dd\5\u00c7d\2\u00dd"+
                "\4\3\2\2\2\u00de\u00df\5\u00b1Y\2\u00df\u00e0\5\u00bb^\2\u00e0\u00e1\5"+
                "\u00c5c\2\u00e1\u00e2\5\u00a9U\2\u00e2\u00e3\5\u00c3b\2\u00e3\u00e4\5"+
                "\u00c7d\2\u00e4\6\3\2\2\2\u00e5\u00e6\5\u00a7T\2\u00e6\u00e7\5\u00a9U"+
                "\2\u00e7\u00e8\5\u00b7\\\2\u00e8\u00e9\5\u00a9U\2\u00e9\u00ea\5\u00c7"+
                "d\2\u00ea\u00eb\5\u00a9U\2\u00eb\b\3\2\2\2\u00ec\u00ed\5\u00c9e\2\u00ed"+
                "\u00ee\5\u00bf`\2\u00ee\u00ef\5\u00a7T\2\u00ef\u00f0\5\u00a1Q\2\u00f0"+
                "\u00f1\5\u00c7d\2\u00f1\u00f2\5\u00a9U\2\u00f2\n\3\2\2\2\u00f3\u00f4\5"+
                "\u00abV\2\u00f4\u00f5\5\u00c3b\2\u00f5\u00f6\5\u00bd_\2\u00f6\u00f7\5"+
                "\u00b9]\2\u00f7\f\3\2\2\2\u00f8\u00f9\5\u00b1Y\2\u00f9\u00fa\5\u00bb^"+
                "\2\u00fa\u00fb\5\u00c7d\2\u00fb\u00fc\5\u00bd_\2\u00fc\16\3\2\2\2\u00fd"+
                "\u00fe\5\u00a9U\2\u00fe\u00ff\5\u00cfh\2\u00ff\u0100\5\u00a9U\2\u0100"+
                "\u0105\5\u00a5S\2\u0101\u0102\5\u00c9e\2\u0102\u0103\5\u00c7d\2\u0103"+
                "\u0104\5\u00a9U\2\u0104\u0106\3\2\2\2\u0105\u0101\3\2\2\2\u0105\u0106"+
                "\3\2\2\2\u0106\20\3\2\2\2\u0107\u0108\5\u00cdg\2\u0108\u0109\5\u00afX"+
                "\2\u0109\u010a\5\u00a9U\2\u010a\u010b\5\u00c3b\2\u010b\u010c\5\u00a9U"+
                "\2\u010c\22\3\2\2\2\u010d\u010e\5\u00afX\2\u010e\u010f\5\u00a1Q\2\u010f"+
                "\u0110\5\u00cbf\2\u0110\u0111\5\u00b1Y\2\u0111\u0112\5\u00bb^\2\u0112"+
                "\u0113\5\u00adW\2\u0113\24\3\2\2\2\u0114\u0115\5\u00adW\2\u0115\u0116"+
                "\5\u00c3b\2\u0116\u0117\5\u00bd_\2\u0117\u0118\5\u00c9e\2\u0118\u0119"+
                "\5\u00bf`\2\u0119\26\3\2\2\2\u011a\u011b\5\u00bd_\2\u011b\u011c\5\u00c3"+
                "b\2\u011c\u011d\5\u00a7T\2\u011d\u011e\5\u00a9U\2\u011e\u011f\5\u00c3"+
                "b\2\u011f\30\3\2\2\2\u0120\u0121\5\u00bd_\2\u0121\u0122\5\u00bf`\2\u0122"+
                "\u0123\5\u00c7d\2\u0123\u0124\5\u00b1Y\2\u0124\u0125\5\u00bd_\2\u0125"+
                "\u0126\5\u00bb^\2\u0126\32\3\2\2\2\u0127\u0128\5\u00a3R\2\u0128\u0129"+
                "\5\u00d1i\2\u0129\34\3\2\2\2\u012a\u012b\5\u00cbf\2\u012b\u012c\5\u00a1"+
                "Q\2\u012c\u012d\5\u00b7\\\2\u012d\u012e\5\u00c9e\2\u012e\u012f\5\u00a9"+
                "U\2\u012f\u0130\5\u00c5c\2\u0130\36\3\2\2\2\u0131\u0132\5\u00bd_\2\u0132"+
                "\u0133\5\u00c9e\2\u0133\u0134\5\u00c7d\2\u0134\u0135\5\u00bf`\2\u0135"+
                "\u0136\5\u00c9e\2\u0136\u0137\5\u00c7d\2\u0137 \3\2\2\2\u0138\u0139\5"+
                "\u00bd_\2\u0139\u013a\5\u00b3Z\2\u013a\"\3\2\2\2\u013b\u013c\5\u00cdg"+
                "\2\u013c\u013d\5\u00b1Y\2\u013d\u013e\5\u00c7d\2\u013e\u013f\5\u00afX"+
                "\2\u013f$\3\2\2\2\u0140\u0141\5\u00a1Q\2\u0141\u0142\5\u00c5c\2\u0142"+
                "&\3\2\2\2\u0143\u0144\5\u00a7T\2\u0144\u0145\5\u00a9U\2\u0145\u0146\5"+
                "\u00abV\2\u0146\u0147\5\u00a1Q\2\u0147\u0148\5\u00c9e\2\u0148\u0149\5"+
                "\u00b7\\\2\u0149\u014a\5\u00c7d\2\u014a(\3\2\2\2\u014b\u014c\5\u00c5c"+
                "\2\u014c\u014d\5\u00a9U\2\u014d\u014e\5\u00c7d\2\u014e*\3\2\2\2\u014f"+
                "\u0150\5\u00bd_\2\u0150\u0151\5\u00bf`\2\u0151\u0152\5\u00a9U\2\u0152"+
                "\u0153\5\u00bb^\2\u0153\u0154\5\u00c1a\2\u0154\u0155\5\u00c9e\2\u0155"+
                "\u0156\5\u00a9U\2\u0156\u0157\5\u00c3b\2\u0157\u0158\5\u00d1i\2\u0158"+
                ",\3\2\2\2\u0159\u015a\5\u00c7d\2\u015a\u015b\5\u00bd_\2\u015b\u015c\5"+
                "\u00bf`\2\u015c.\3\2\2\2\u015d\u015e\5\u00a7T\2\u015e\u015f\5\u00b1Y\2"+
                "\u015f\u0160\5\u00c5c\2\u0160\u0161\5\u00c7d\2\u0161\u0162\5\u00b1Y\2"+
                "\u0162\u0163\5\u00bb^\2\u0163\u0164\5\u00a5S\2\u0164\u0165\5\u00c7d\2"+
                "\u0165\60\3\2\2\2\u0166\u0167\5\u00bf`\2\u0167\u0168\5\u00a9U\2\u0168"+
                "\u0169\5\u00c3b\2\u0169\u016a\5\u00a5S\2\u016a\u016b\5\u00a9U\2\u016b"+
                "\u016c\5\u00bb^\2\u016c\u016d\5\u00c7d\2\u016d\62\3\2\2\2\u016e\u016f"+
                "\5\u00c7d\2\u016f\u0170\5\u00b1Y\2\u0170\u0171\5\u00a9U\2\u0171\u0172"+
                "\5\u00c5c\2\u0172\64\3\2\2\2\u0173\u0175\t\2\2\2\u0174\u0173\3\2\2\2\u0175"+
                "\u0176\3\2\2\2\u0176\u0174\3\2\2\2\u0176\u0177\3\2\2\2\u0177\u0178\3\2"+
                "\2\2\u0178\u0179\b\33\2\2\u0179\66\3\2\2\2\u017a\u017b\7\61\2\2\u017b"+
                "\u017c\7,\2\2\u017c\u0181\3\2\2\2\u017d\u0180\5\67\34\2\u017e\u0180\13"+
                "\2\2\2\u017f\u017d\3\2\2\2\u017f\u017e\3\2\2\2\u0180\u0183\3\2\2\2\u0181"+
                "\u0182\3\2\2\2\u0181\u017f\3\2\2\2\u0182\u0184\3\2\2\2\u0183\u0181\3\2"+
                "\2\2\u0184\u0185\7,\2\2\u0185\u0186\7\61\2\2\u0186\u0187\3\2\2\2\u0187"+
                "\u0188\b\34\2\2\u01888\3\2\2\2\u0189\u018a\7/\2\2\u018a\u018b\7/\2\2\u018b"+
                "\u018f\3\2\2\2\u018c\u018e\n\3\2\2\u018d\u018c\3\2\2\2\u018e\u0191\3\2"+
                "\2\2\u018f\u018d\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0192\3\2\2\2\u0191"+
                "\u018f\3\2\2\2\u0192\u0193\b\35\2\2\u0193:\3\2\2\2\u0194\u0195\7$\2\2"+
                "\u0195<\3\2\2\2\u0196\u0197\7)\2\2\u0197>\3\2\2\2\u0198\u019b\7B\2\2\u0199"+
                "\u019c\t\4\2\2\u019a\u019c\5\u00d5k\2\u019b\u0199\3\2\2\2\u019b\u019a"+
                "\3\2\2\2\u019c\u019d\3\2\2\2\u019d\u019b\3\2\2\2\u019d\u019e\3\2\2\2\u019e"+
                "@\3\2\2\2\u019f\u01a1\5\u009fP\2\u01a0\u019f\3\2\2\2\u01a1\u01a2\3\2\2"+
                "\2\u01a2\u01a0\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3B\3\2\2\2\u01a4\u01a7"+
                "\t\5\2\2\u01a5\u01a7\5\u00d5k\2\u01a6\u01a4\3\2\2\2\u01a6\u01a5\3\2\2"+
                "\2\u01a7\u01ac\3\2\2\2\u01a8\u01ab\t\4\2\2\u01a9\u01ab\5\u00d5k\2\u01aa"+
                "\u01a8\3\2\2\2\u01aa\u01a9\3\2\2\2\u01ab\u01ae\3\2\2\2\u01ac\u01aa\3\2"+
                "\2\2\u01ac\u01ad\3\2\2\2\u01adD\3\2\2\2\u01ae\u01ac\3\2\2\2\u01af\u01b1"+
                "\7P\2\2\u01b0\u01af\3\2\2\2\u01b0\u01b1\3\2\2\2\u01b1\u01b2\3\2\2\2\u01b2"+
                "\u01b8\7)\2\2\u01b3\u01b7\n\6\2\2\u01b4\u01b5\7)\2\2\u01b5\u01b7\7)\2"+
                "\2\u01b6\u01b3\3\2\2\2\u01b6\u01b4\3\2\2\2\u01b7\u01ba\3\2\2\2\u01b8\u01b6"+
                "\3\2\2\2\u01b8\u01b9\3\2\2\2\u01b9\u01bb\3\2\2\2\u01ba\u01b8\3\2\2\2\u01bb"+
                "\u01bc\7)\2\2\u01bcF\3\2\2\2\u01bd\u01c3\7$\2\2\u01be\u01c2\n\7\2\2\u01bf"+
                "\u01c0\7$\2\2\u01c0\u01c2\7$\2\2\u01c1\u01be\3\2\2\2\u01c1\u01bf\3\2\2"+
                "\2\u01c2\u01c5\3\2\2\2\u01c3\u01c1\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c4\u01c6"+
                "\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c6\u01c7\7$\2\2\u01c7H\3\2\2\2\u01c8\u01ce"+
                "\7]\2\2\u01c9\u01cd\n\b\2\2\u01ca\u01cb\7_\2\2\u01cb\u01cd\7_\2\2\u01cc"+
                "\u01c9\3\2\2\2\u01cc\u01ca\3\2\2\2\u01cd\u01d0\3\2\2\2\u01ce\u01cc\3\2"+
                "\2\2\u01ce\u01cf\3\2\2\2\u01cf\u01d1\3\2\2\2\u01d0\u01ce\3\2\2\2\u01d1"+
                "\u01d2\7_\2\2\u01d2J\3\2\2\2\u01d3\u01d4\7\62\2\2\u01d4\u01d8\7Z\2\2\u01d5"+
                "\u01d7\5\u009dO\2\u01d6\u01d5\3\2\2\2\u01d7\u01da\3\2\2\2\u01d8\u01d6"+
                "\3\2\2\2\u01d8\u01d9\3\2\2\2\u01d9L\3\2\2\2\u01da\u01d8\3\2\2\2\u01db"+
                "\u01dc\5\u009bN\2\u01dcN\3\2\2\2\u01dd\u01e0\5A!\2\u01de\u01e0\5\u009b"+
                "N\2\u01df\u01dd\3\2\2\2\u01df\u01de\3\2\2\2\u01e0\u01e1\3\2\2\2\u01e1"+
                "\u01e3\7G\2\2\u01e2\u01e4\t\t\2\2\u01e3\u01e2\3\2\2\2\u01e3\u01e4\3\2"+
                "\2\2\u01e4\u01e6\3\2\2\2\u01e5\u01e7\5\u009fP\2\u01e6\u01e5\3\2\2\2\u01e7"+
                "\u01e8\3\2\2\2\u01e8\u01e6\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9P\3\2\2\2"+
                "\u01ea\u01eb\7?\2\2\u01ebR\3\2\2\2\u01ec\u01ed\7@\2\2\u01edT\3\2\2\2\u01ee"+
                "\u01ef\7>\2\2\u01efV\3\2\2\2\u01f0\u01f1\7#\2\2\u01f1X\3\2\2\2\u01f2\u01f3"+
                "\7-\2\2\u01f3\u01f4\7?\2\2\u01f4Z\3\2\2\2\u01f5\u01f6\7/\2\2\u01f6\u01f7"+
                "\7?\2\2\u01f7\\\3\2\2\2\u01f8\u01f9\7,\2\2\u01f9\u01fa\7?\2\2\u01fa^\3"+
                "\2\2\2\u01fb\u01fc\7\61\2\2\u01fc\u01fd\7?\2\2\u01fd`\3\2\2\2\u01fe\u01ff"+
                "\7\'\2\2\u01ff\u0200\7?\2\2\u0200b\3\2\2\2\u0201\u0202\7(\2\2\u0202\u0203"+
                "\7?\2\2\u0203d\3\2\2\2\u0204\u0205\7`\2\2\u0205\u0206\7?\2\2\u0206f\3"+
                "\2\2\2\u0207\u0208\7~\2\2\u0208\u0209\7?\2\2\u0209h\3\2\2\2\u020a\u020b"+
                "\7~\2\2\u020b\u020c\7~\2\2\u020cj\3\2\2\2\u020d\u020e\7\60\2\2\u020el"+
                "\3\2\2\2\u020f\u0210\7a\2\2\u0210n\3\2\2\2\u0211\u0212\7B\2\2\u0212p\3"+
                "\2\2\2\u0213\u0214\7%\2\2\u0214r\3\2\2\2\u0215\u0216\7&\2\2\u0216t\3\2"+
                "\2\2\u0217\u0218\7*\2\2\u0218v\3\2\2\2\u0219\u021a\7+\2\2\u021ax\3\2\2"+
                "\2\u021b\u021c\7]\2\2\u021cz\3\2\2\2\u021d\u021e\7_\2\2\u021e|\3\2\2\2"+
                "\u021f\u0220\7}\2\2\u0220~\3\2\2\2\u0221\u0222\7\177\2\2\u0222\u0080\3"+
                "\2\2\2\u0223\u0224\7.\2\2\u0224\u0082\3\2\2\2\u0225\u0226\7=\2\2\u0226"+
                "\u0084\3\2\2\2\u0227\u0228\7<\2\2\u0228\u0086\3\2\2\2\u0229\u022a\7,\2"+
                "\2\u022a\u0088\3\2\2\2\u022b\u022c\7\61\2\2\u022c\u008a\3\2\2\2\u022d"+
                "\u022e\7\'\2\2\u022e\u008c\3\2\2\2\u022f\u0230\7-\2\2\u0230\u008e\3\2"+
                "\2\2\u0231\u0232\7/\2\2\u0232\u0090\3\2\2\2\u0233\u0234\7\u0080\2\2\u0234"+
                "\u0092\3\2\2\2\u0235\u0236\7~\2\2\u0236\u0094\3\2\2\2\u0237\u0238\7(\2"+
                "\2\u0238\u0096\3\2\2\2\u0239\u023a\7`\2\2\u023a\u0098\3\2\2\2\u023b\u023c"+
                "\7A\2\2\u023c\u009a\3\2\2\2\u023d\u023f\5\u009fP\2\u023e\u023d\3\2\2\2"+
                "\u023f\u0240\3\2\2\2\u0240\u023e\3\2\2\2\u0240\u0241\3\2\2\2\u0241\u0242"+
                "\3\2\2\2\u0242\u0244\7\60\2\2\u0243\u0245\5\u009fP\2\u0244\u0243\3\2\2"+
                "\2\u0245\u0246\3\2\2\2\u0246\u0244\3\2\2\2\u0246\u0247\3\2\2\2\u0247\u0256"+
                "\3\2\2\2\u0248\u024a\5\u009fP\2\u0249\u0248\3\2\2\2\u024a\u024b\3\2\2"+
                "\2\u024b\u0249\3\2\2\2\u024b\u024c\3\2\2\2\u024c\u024d\3\2\2\2\u024d\u024e"+
                "\7\60\2\2\u024e\u0256\3\2\2\2\u024f\u0251\7\60\2\2\u0250\u0252\5\u009f"+
                "P\2\u0251\u0250\3\2\2\2\u0252\u0253\3\2\2\2\u0253\u0251\3\2\2\2\u0253"+
                "\u0254\3\2\2\2\u0254\u0256\3\2\2\2\u0255\u023e\3\2\2\2\u0255\u0249\3\2"+
                "\2\2\u0255\u024f\3\2\2\2\u0256\u009c\3\2\2\2\u0257\u0258\t\n\2\2\u0258"+
                "\u009e\3\2\2\2\u0259\u025a\t\13\2\2\u025a\u00a0\3\2\2\2\u025b\u025c\t"+
                "\f\2\2\u025c\u00a2\3\2\2\2\u025d\u025e\t\r\2\2\u025e\u00a4\3\2\2\2\u025f"+
                "\u0260\t\16\2\2\u0260\u00a6\3\2\2\2\u0261\u0262\t\17\2\2\u0262\u00a8\3"+
                "\2\2\2\u0263\u0264\t\20\2\2\u0264\u00aa\3\2\2\2\u0265\u0266\t\21\2\2\u0266"+
                "\u00ac\3\2\2\2\u0267\u0268\t\22\2\2\u0268\u00ae\3\2\2\2\u0269\u026a\t"+
                "\23\2\2\u026a\u00b0\3\2\2\2\u026b\u026c\t\24\2\2\u026c\u00b2\3\2\2\2\u026d"+
                "\u026e\t\25\2\2\u026e\u00b4\3\2\2\2\u026f\u0270\t\26\2\2\u0270\u00b6\3"+
                "\2\2\2\u0271\u0272\t\27\2\2\u0272\u00b8\3\2\2\2\u0273\u0274\t\30\2\2\u0274"+
                "\u00ba\3\2\2\2\u0275\u0276\t\31\2\2\u0276\u00bc\3\2\2\2\u0277\u0278\t"+
                "\32\2\2\u0278\u00be\3\2\2\2\u0279\u027a\t\33\2\2\u027a\u00c0\3\2\2\2\u027b"+
                "\u027c\t\34\2\2\u027c\u00c2\3\2\2\2\u027d\u027e\t\35\2\2\u027e\u00c4\3"+
                "\2\2\2\u027f\u0280\t\36\2\2\u0280\u00c6\3\2\2\2\u0281\u0282\t\37\2\2\u0282"+
                "\u00c8\3\2\2\2\u0283\u0284\t \2\2\u0284\u00ca\3\2\2\2\u0285\u0286\t!\2"+
                "\2\u0286\u00cc\3\2\2\2\u0287\u0288\t\"\2\2\u0288\u00ce\3\2\2\2\u0289\u028a"+
                "\t#\2\2\u028a\u00d0\3\2\2\2\u028b\u028c\t$\2\2\u028c\u00d2\3\2\2\2\u028d"+
                "\u028e\t%\2\2\u028e\u00d4\3\2\2\2\u028f\u0290\t&\2\2\u0290\u00d6\3\2\2"+
                "\2\36\2\u0105\u0176\u017f\u0181\u018f\u019b\u019d\u01a2\u01a6\u01aa\u01ac"+
                "\u01b0\u01b6\u01b8\u01c1\u01c3\u01cc\u01ce\u01d8\u01df\u01e3\u01e8\u0240"+
                "\u0246\u024b\u0253\u0255\3\b\2\2";
        public static final ATN _ATN =
                new ATNDeserializer().deserialize(_serializedATN.toCharArray());
        static {
                _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
                for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
                        _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
                }
        }
}