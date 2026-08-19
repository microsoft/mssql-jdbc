/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Stream;

import javax.net.SocketFactory;
import javax.net.ssl.TrustManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests the Util class
 *
 */
@RunWith(JUnitPlatform.class)
public class UtilTest {

    public static final class TestRunnable implements Runnable {
        @Override
        public void run() {
        }
    }

    @Test
    public void testNewInstanceUsesUtilClassLoader() throws Exception {
        Thread currentThread = Thread.currentThread();
        ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(new ClassLoader(null) {
        });
        try {
            Runnable instance = Util.newInstance(Runnable.class, TestRunnable.class.getName(), null,
                    new Object[] { "testClass", Runnable.class.getName() });

            assertNotNull(instance);
        } finally {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = { "", " ", "com..example.Foo", ".com.example.Foo", "com.example.Foo.",
            "1com.example.Foo", "com.example.Foo Bar", "com/example/Foo", "jar:file:Foo", "http://example/Foo" })
    public void testNewInstanceRejectsInvalidTrustManagerClassNames(String className) {
        Object[] msgArgs = { "trustManagerClass", TrustManager.class.getName() };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> Util.newInstance(TrustManager.class, className, null, msgArgs));

        assertTrue(exception.getMessage().contains("trustManagerClass"));
    }

    @Test
    public void testNewInstanceRejectsValidClassNameWithInvalidType() {
        Object[] msgArgs = { "socketFactoryClass", SocketFactory.class.getName() };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> Util.newInstance(SocketFactory.class, String.class.getName(), null, msgArgs));

        assertTrue(exception.getMessage().contains("socketFactoryClass"));
        assertTrue(exception.getMessage().contains(SocketFactory.class.getName()));
    }

    @Test
    public void testNewInstanceRejectsInvalidSocketFactoryClassName() {
        String className = "jar:file:.proc.self.fd.!.fd_SqlServerSocketFactorykanqvbkjhp";
        Object[] msgArgs = { "socketFactoryClass", SocketFactory.class.getName() };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> Util.newInstance(SocketFactory.class, className, null, msgArgs));

        assertTrue(exception.getMessage().contains("socketFactoryClass"));
        assertTrue(exception.getMessage().contains(className));
    }

    @Test
    public void testNewInstanceRejectsInvalidAccessTokenCallbackClassName() {
        String className = "jar:file:.proc.self.fd.!.fd_SQLServerAccessTokenCallback";
        Object[] msgArgs = { "accessTokenCallbackClass", SQLServerAccessTokenCallback.class.getName() };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> Util.newInstance(SQLServerAccessTokenCallback.class, className, null, msgArgs));

        assertTrue(exception.getMessage().contains("accessTokenCallbackClass"));
        assertTrue(exception.getMessage().contains(className));
    }

    @ParameterizedTest
    @ValueSource(strings = { "java.lang.Object", "java.lang.String", "java.lang.Thread",
            "java.util.ArrayList" })
    public void testNewInstanceAcceptsValidClassNames(String className) throws Exception {
        Object instance = Util.newInstance(Object.class, className, null,
                new Object[] { "testClass", Object.class.getName() });

        assertNotNull(instance);
    }

    @Test
    public void readGUIDtoUUID() throws SQLException {
        UUID expected = UUID.fromString("6F9619FF-8B86-D011-B42D-00C04FC964FF");
        byte[] guid = new byte[] {-1, 25, -106, 111, -122, -117, 17, -48, -76, 45, 0, -64, 79, -55, 100, -1};
        assertEquals(expected, Util.readGUIDtoUUID(guid));
    }

    @Test
    public void testLongConversions() {
        writeAndReadLong(Long.MIN_VALUE);
        writeAndReadLong(Long.MIN_VALUE / 2);
        writeAndReadLong(-1);
        writeAndReadLong(0);
        writeAndReadLong(1);
        writeAndReadLong(Long.MAX_VALUE / 2);
        writeAndReadLong(Long.MAX_VALUE);
    }

    @Test
    public void testparseUrl() throws SQLException {
        java.util.logging.Logger drLogger = java.util.logging.Logger
                .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
        String constr = "jdbc:sqlserver://localhost;password={pasS}};word={qq};user=username;portName=1433;databaseName=database;";
        Properties prt = Util.parseUrl(constr, drLogger);
        assertEquals(prt.getProperty("password"), "pasS};word={qq");
        assertEquals(prt.getProperty("serverName"), "localhost");
        assertEquals(prt.getProperty("user"), "username");
        assertEquals(prt.getProperty("databaseName"), "database");

        constr = "jdbc:sqlserver://localhost;password={pasS}}}";
        prt = Util.parseUrl(constr, drLogger);
        assertEquals(prt.getProperty("password"), "pasS}");

        constr = "jdbc:sqlserver://localhost;password={pasS}}} ";
        prt = Util.parseUrl(constr, drLogger);
        assertEquals(prt.getProperty("password"), "pasS}");

        constr = "jdbc:sqlserver://localhost;password={pasS}}} ;";
        prt = Util.parseUrl(constr, drLogger);
        assertEquals(prt.getProperty("password"), "pasS}");
    }

    /**
     * Tests that the cross-driver connection string aliases (SqlClient / ODBC / OLEDB spellings) are normalized to the
     * canonical JDBC property names by {@link Util#parseUrl}.
     *
     * @throws SQLException
     */
    @Test
    public void testConnectionStringAliasNormalization() throws SQLException {
        java.util.logging.Logger drLogger = java.util.logging.Logger
                .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
        String constr = "jdbc:sqlserver://localhost;uid=myUser;trusted_connection=true;app=myApp;connectTimeout=45;"
                + "columnEncryption=Enabled;quotedId=OFF;";
        Properties prt = Util.parseUrl(constr, drLogger);

        // Each alias must normalize to its canonical property name and the alias itself must not survive.
        assertEquals("myUser", prt.getProperty("user"));
        assertEquals(null, prt.getProperty("uid"));

        assertEquals("true", prt.getProperty("integratedSecurity"));
        assertEquals(null, prt.getProperty("trusted_connection"));

        assertEquals("myApp", prt.getProperty("applicationName"));
        assertEquals(null, prt.getProperty("app"));

        assertEquals("45", prt.getProperty("loginTimeout"));
        assertEquals(null, prt.getProperty("connectTimeout"));

        assertEquals("Enabled", prt.getProperty("columnEncryptionSetting"));
        assertEquals(null, prt.getProperty("columnEncryption"));

        assertEquals("OFF", prt.getProperty("quotedIdentifier"));
        assertEquals(null, prt.getProperty("quotedId"));
    }

    /**
     * Tests that connection string alias normalization is case-insensitive, matching the behavior of the existing
     * synonym handling in {@link SQLServerDriver#getNormalizedPropertyName}.
     *
     * @throws SQLException
     */
    @Test
    public void testConnectionStringAliasCaseInsensitive() throws SQLException {
        java.util.logging.Logger drLogger = java.util.logging.Logger
                .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
        String constr = "jdbc:sqlserver://localhost;UID=myUser;Trusted_Connection=true;APP=myApp;CONNECTTIMEOUT=45;"
                + "ColumnEncryption=Enabled;QUOTEDID=OFF;";
        Properties prt = Util.parseUrl(constr, drLogger);

        assertEquals("myUser", prt.getProperty("user"));
        assertEquals("true", prt.getProperty("integratedSecurity"));
        assertEquals("myApp", prt.getProperty("applicationName"));
        assertEquals("45", prt.getProperty("loginTimeout"));
        assertEquals("Enabled", prt.getProperty("columnEncryptionSetting"));
        assertEquals("OFF", prt.getProperty("quotedIdentifier"));
    }

    /**
     * Verifies a duplicate connection-string keyword does not cause a parse error and the last value
     * provided wins.
     */
    @Test
    @Tag(Constants.legacyFx)
    public void testDuplicateKeywords() throws SQLException {
        Logger drLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
        String constr = "jdbc:sqlserver://localhost;databaseName=first;databaseName=second;user=u1;user=u2;";
        Properties prt = Util.parseUrl(constr, drLogger);
        assertEquals("duplicate keyword: last value should win", "second", prt.getProperty("databaseName"));
        assertEquals("duplicate keyword: last value should win", "u2", prt.getProperty("user"));
        assertEquals("localhost", prt.getProperty("serverName"));
    }

    /**
     * Verifies a connection string with trailing token separators (semicolons and whitespace) parses
     * without error.
     */
    @Test
    @Tag(Constants.legacyFx)
    public void testTokenSeparatorsAtEnd() throws SQLException {
        Logger drLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
        String constr = "jdbc:sqlserver://localhost;databaseName=db;user=u;  ;;  ";
        Properties prt = Util.parseUrl(constr, drLogger);
        assertEquals("db", prt.getProperty("databaseName"));
        assertEquals("u", prt.getProperty("user"));
        assertEquals("localhost", prt.getProperty("serverName"));
    }

    private static String testString = "A ß € 嗨 𝄞 🙂ăѣ𝔠ծềſģȟᎥ𝒋ǩľḿꞑȯ𝘱𝑞𝗋𝘴ȶ𝞄𝜈ψ𝒙𝘆𝚣1234567890!@#$%^&*()-_=+[{]};:'\",<.>/?~𝘈Ḇ𝖢𝕯٤ḞԍНǏ𝙅ƘԸⲘ𝙉০Ρ𝗤Ɍ𝓢ȚЦ𝒱Ѡ𝓧ƳȤѧᖯć𝗱ễ𝑓𝙜Ⴙ𝞲𝑗𝒌ļṃŉо𝞎𝒒ᵲꜱ𝙩ừ𝗏ŵ𝒙𝒚ź1234567890!@#$%^&*()-_=+[{]};:'\",<.>/?~АḂⲤ𝗗𝖤𝗙ꞠꓧȊ𝐉𝜥ꓡ𝑀𝑵Ǭ𝙿𝑄Ŗ𝑆𝒯𝖴𝘝𝘞ꓫŸ𝜡ả𝘢ƀ𝖼ḋếᵮℊ𝙝Ꭵ𝕛кιṃդⱺ𝓅𝘲𝕣𝖘ŧ𝑢ṽẉ𝘅ყž1234567890!@#$%^&*()-_=+[{]};:'\",<.>/?~Ѧ𝙱ƇᗞΣℱԍҤ١𝔍К𝓛𝓜ƝȎ𝚸𝑄Ṛ𝓢ṮṺƲᏔꓫ𝚈𝚭𝜶Ꮟçძ𝑒𝖿𝗀ḧ𝗂𝐣ҝɭḿ𝕟𝐨𝝔𝕢ṛ𝓼тú𝔳ẃ⤬𝝲𝗓1234567890!@#$%^&*()-_=+[{]};:'\",<.>/?~𝖠Β𝒞𝘋𝙴𝓕ĢȞỈ𝕵ꓗʟ𝙼ℕ০𝚸𝗤ՀꓢṰǓⅤ𝔚Ⲭ𝑌𝙕𝘢𝕤";
    private static String testString2 = "ssdfsdflkjh9u0345)*&)(*&%$";
    private static String testString3 = "ss345(*&^%oujdf.';lk2345(*&()*$#~!`1\\]wer><.,/?dfsdflkjh9u0345)*&)(*&%$";

    @Test
    public void testArrayConversions() {
        char[] chars = testString.toCharArray();
        byte[] bytes = Util.charsToBytes(chars);
        char[] newChars = Util.bytesToChars(bytes);
        assertArrayEquals(chars, newChars);
        String end = String.valueOf(newChars);
        assertEquals(testString, end);
    }

    @Test
    public void testSecureStringUtil() throws SQLException {
        // Encrypt/decrypt multiple values in overlapping orders
        byte[] bytes = SecureStringUtil.getInstance().getEncryptedBytes(testString.toCharArray());
        byte[] bytes2 = SecureStringUtil.getInstance().getEncryptedBytes(testString2.toCharArray());
        String end = String.valueOf(SecureStringUtil.getInstance().getDecryptedChars(bytes));
        byte[] bytes3 = SecureStringUtil.getInstance().getEncryptedBytes(testString3.toCharArray());
        String end3 = String.valueOf(SecureStringUtil.getInstance().getDecryptedChars(bytes3));
        String end2 = String.valueOf(SecureStringUtil.getInstance().getDecryptedChars(bytes2));

        assertEquals(testString, end);
        assertEquals(testString2, end2);
        assertEquals(testString3, end3);
    }

    private void writeAndReadLong(long valueToTest) {
        byte[] buffer = new byte[8];
        Util.writeLong(valueToTest, buffer, 0);
        long newLong = Util.readLong(buffer, 0);
        assertEquals(valueToTest, newLong);
    }

    /**
     * Verifies {@link DDC#convertBigDecimalToBytes} produces the same TDS bytes on its long fast path (magnitude fits
     * in a long) as the reference {@link java.math.BigInteger}-based encoding, with explicit coverage of the
     * zero-magnitude corner case (bitLength() == 0 must still emit a single zero magnitude byte).
     */
    @Test
    public void testConvertBigDecimalToBytesFastPath() {
        // Zero is the fragile corner case: it must encode as one zero magnitude byte, matching BigInteger.ZERO.
        assertArrayEquals(referenceBigDecimalBytes(BigDecimal.ZERO),
                DDC.convertBigDecimalToBytes(BigDecimal.ZERO, 0), "zero-magnitude encoding mismatch");

        BigDecimal[] values = {new BigDecimal("0.00"), new BigDecimal("1"), new BigDecimal("-1"),
                new BigDecimal("127"), new BigDecimal("128"), new BigDecimal("255"), new BigDecimal("256"),
                new BigDecimal("-128"), new BigDecimal("123.4567"), new BigDecimal("-987654321.0001"),
                BigDecimal.valueOf(Long.MAX_VALUE, 4), BigDecimal.valueOf(Long.MIN_VALUE, 4),
                // A magnitude that does not fit in a long -> exercises the slow (BigInteger) path.
                new BigDecimal(new BigInteger("123456789012345678901234567890"), 4)};

        for (BigDecimal v : values) {
            assertArrayEquals(referenceBigDecimalBytes(v), DDC.convertBigDecimalToBytes(v, v.scale()),
                    "encoding mismatch for " + v);
        }
    }

    /** Reference TDS decimal encoding using the straightforward BigInteger.toByteArray() path. */
    private static byte[] referenceBigDecimalBytes(BigDecimal bigDecimalVal) {
        boolean isNegative = bigDecimalVal.signum() < 0;
        if (bigDecimalVal.scale() < 0)
            bigDecimalVal = bigDecimalVal.setScale(0);
        BigInteger bi = bigDecimalVal.unscaledValue();
        if (isNegative)
            bi = bi.negate();

        byte[] unscaledBytes = bi.toByteArray();
        byte[] valueBytes = new byte[unscaledBytes.length + 3];
        int j = 0;
        valueBytes[j++] = (byte) bigDecimalVal.scale();
        valueBytes[j++] = (byte) (unscaledBytes.length + 1); // data length + sign
        valueBytes[j++] = (byte) (isNegative ? 0 : 1); // 1 = +ve, 0 = -ve
        for (int i = unscaledBytes.length - 1; i >= 0; i--)
            valueBytes[j++] = unscaledBytes[i];
        return valueBytes;
    }

    // ─── escapeMultiPartIdentifier: parameterized tests ───

    static Stream<Arguments> escapeMultiPartIdentifierArgs() {
        return Stream.of(
                // Simple names
                Arguments.of("MyTable", "[MyTable]"),
                Arguments.of("Customers", "[Customers]"),
                Arguments.of("x", "[x]"),
                Arguments.of("table123", "[table123]"),
                Arguments.of("my_table", "[my_table]"),

                // Multi-part names
                Arguments.of("dbo.MyTable", "[dbo].[MyTable]"),
                Arguments.of("MyDB.dbo.MyTable", "[MyDB].[dbo].[MyTable]"),
                Arguments.of("AdventureWorks.Sales.Customer", "[AdventureWorks].[Sales].[Customer]"),

                // Already bracket-quoted (preserved as-is)
                Arguments.of("[dbo].[MyTable]", "[dbo].[MyTable]"),
                Arguments.of("[MyDB].[dbo].[MyTable]", "[MyDB].[dbo].[MyTable]"),
                Arguments.of("[Order Details]", "[Order Details]"),

                // Mixed quoting (some parts bracketed, some not)
                Arguments.of("[dbo].MyTable", "[dbo].[MyTable]"),

                // Double-quoted identifiers (preserved as-is)
                Arguments.of("\"dbo\".\"MyTable\"", "\"dbo\".\"MyTable\""),

                // Temp tables
                Arguments.of("#TempTable", "[#TempTable]"),
                Arguments.of("##GlobalTemp", "[##GlobalTemp]"),
                Arguments.of("dbo.#TempTable", "[dbo].[#TempTable]"),

                // Special characters
                Arguments.of("My Table", "[My Table]"),
                Arguments.of("@variable_table", "[@variable_table]"),
                Arguments.of("$system_table", "[$system_table]"),

                // Closing bracket doubled
                Arguments.of("My]Table", "[My]]Table]"),
                Arguments.of("dbo.My]Table", "[dbo].[My]]Table]"),
                Arguments.of("]", "[]]]"),
                Arguments.of("]]", "[]]]]]"),

                // Null and empty
                Arguments.of(null, null),
                Arguments.of("", ""),

                // Unicode
                Arguments.of("\u30C6\u30FC\u30D6\u30EB", "[\u30C6\u30FC\u30D6\u30EB]"),

                // SQL injection payloads from vulnerability report
                // Attack 1: RCE via xp_cmdshell
                Arguments.of(
                        "(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--",
                        "[(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--]"),
                // Semicolon injection
                Arguments.of(
                        "table1; DROP TABLE users--",
                        "[table1; DROP TABLE users--]"),
                // UNION injection
                Arguments.of(
                        "t UNION SELECT password FROM users--",
                        "[t UNION SELECT password FROM users--]"),
                // EXEC injection
                Arguments.of(
                        "t; EXEC sp_addlogin 'hacker','password'--",
                        "[t; EXEC sp_addlogin 'hacker','password'--]"),
                // FMTONLY bypass
                Arguments.of(
                        "x; SET FMTONLY OFF; EXEC xp_cmdshell 'net user hacker P@ss /add'--",
                        "[x; SET FMTONLY OFF; EXEC xp_cmdshell 'net user hacker P@ss /add'--]"),
                // Comment injection
                Arguments.of("table1 /*", "[table1 /*]"),
                // Single quote in payload
                Arguments.of("it's a table", "[it's a table]"),
                // Bracket escape breakout attempt
                Arguments.of(
                        "table]; DROP TABLE users--",
                        "[table]]; DROP TABLE users--]")
        );
    }

    @ParameterizedTest
    @MethodSource("escapeMultiPartIdentifierArgs")
    public void testEscapeMultiPartIdentifier(String input, String expected) {
        assertEquals(expected, Util.escapeMultiPartIdentifier(input));
    }

    // ─── INSERT BULK command context (SQLServerBulkCopy.createInsertBulkCommand) ───

    static Stream<Arguments> insertBulkCommandArgs() {
        return Stream.of(
                Arguments.of("dbo.MyTable",
                        "INSERT BULK [dbo].[MyTable] (col1 INT)"),
                Arguments.of("MyDB.dbo.MyTable",
                        "INSERT BULK [MyDB].[dbo].[MyTable] (col1 INT)"),
                Arguments.of("#TempTable",
                        "INSERT BULK [#TempTable] (col1 INT)"),
                // RCE payload
                Arguments.of("(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--",
                        "INSERT BULK [(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--] (col1 INT)"),
                // DROP TABLE payload
                Arguments.of("table1; DROP TABLE users--",
                        "INSERT BULK [table1; DROP TABLE users--] (col1 INT)"),
                // Bracket breakout attempt
                Arguments.of("table]; DROP TABLE users--",
                        "INSERT BULK [table]]; DROP TABLE users--] (col1 INT)")
        );
    }

    @ParameterizedTest
    @MethodSource("insertBulkCommandArgs")
    public void testInsertBulkCommand(String tableName, String expected) {
        String cmd = "INSERT BULK " + Util.escapeMultiPartIdentifier(tableName) + " (col1 INT)";
        assertEquals(expected, cmd);
    }

    // ─── sp_executesql context (SQLServerBulkCopy.getDestinationMetadata + SQLServerPreparedStatement batch insert) ───

    static Stream<Arguments> spExecuteSqlQueryArgs() {
        return Stream.of(
                Arguments.of("dbo.MyTable",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [dbo].[MyTable] '"),
                Arguments.of("MyDB.dbo.MyTable",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [MyDB].[dbo].[MyTable] '"),
                Arguments.of("[dbo].[Order Details]",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [dbo].[Order Details] '"),
                // RCE payload — single quotes in 'whoami' get doubled in string context
                Arguments.of("(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell ''whoami''--] '"),
                // Credential theft payload
                Arguments.of("(SELECT 1 a) t; SET FMTONLY OFF; SELECT name, password_hash INTO ##creds FROM sys.sql_logins--",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [(SELECT 1 a) t; SET FMTONLY OFF; SELECT name, password_hash INTO ##creds FROM sys.sql_logins--] '"),
                // Privilege escalation with brackets in payload
                Arguments.of("(SELECT 1 a) t; SET FMTONLY OFF; CREATE LOGIN [backdoor] WITH PASSWORD='x'--",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [(SELECT 1 a) t; SET FMTONLY OFF; CREATE LOGIN [backdoor]] WITH PASSWORD=''x''--] '"),
                // Simple DROP injection
                Arguments.of("table1; DROP TABLE users--",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [table1; DROP TABLE users--] '"),
                // Bracket breakout attempt — ] doubled, quotes doubled
                Arguments.of("t'; DROP TABLE users--",
                        "sp_executesql N'SET FMTONLY ON SELECT * FROM [t''; DROP TABLE users--] '")
        );
    }

    @ParameterizedTest
    @MethodSource("spExecuteSqlQueryArgs")
    public void testSpExecuteSqlQuery(String tableName, String expected) {
        String escaped = Util.escapeMultiPartIdentifier(tableName);
        String query = "sp_executesql N'SET FMTONLY ON SELECT * FROM "
                + Util.escapeSingleQuotes(escaped) + " '";
        assertEquals(expected, query);
    }

    // ─── OBJECT_ID query context (SQLServerBulkCopy.setDestinationColumnMetadata) ───

    static Stream<Arguments> objectIdQueryArgs() {
        return Stream.of(
                Arguments.of("dbo.MyTable",
                        "object_id=OBJECT_ID('[dbo].[MyTable]')"),
                Arguments.of("MyDB.dbo.MyTable",
                        "object_id=OBJECT_ID('[MyDB].[dbo].[MyTable]')"),
                // RCE payload
                Arguments.of("(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--",
                        "object_id=OBJECT_ID('[(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell ''whoami''--]')"),
                // Bracket breakout with quote
                Arguments.of("t'; DROP TABLE users--",
                        "object_id=OBJECT_ID('[t''; DROP TABLE users--]')"),
                // Bracket breakout with closing bracket
                Arguments.of("t]; DROP TABLE users--",
                        "object_id=OBJECT_ID('[t]]; DROP TABLE users--]')")
        );
    }

    @ParameterizedTest
    @MethodSource("objectIdQueryArgs")
    public void testObjectIdQuery(String tableName, String expected) {
        String escaped = Util.escapeMultiPartIdentifier(tableName);
        String query = "object_id=OBJECT_ID('" + Util.escapeSingleQuotes(escaped) + "')";
        assertEquals(expected, query);
    }

    // ─── escapeMultiPartIdentifier: additional quoting edge cases ───

    private static Stream<Arguments> additionalQuotingArgs() {
        return Stream.of(
                // already-bracketed with escaped brackets
                Arguments.of("[table]]name]", "[table]]name]"),
                Arguments.of("[a]]b]]c]", "[a]]b]]c]"),
                // dot inside brackets is not a separator
                Arguments.of("[db].[dbo].[my.table]", "[db].[dbo].[my.table]"),
                // space and empty inside brackets
                Arguments.of("[ ]", "[ ]"),
                Arguments.of("[]", "[]"),
                // single quote in name
                Arguments.of("O'Brien", "[O'Brien]"),
                // unterminated or mismatched delimiters are ordinary characters
                Arguments.of("[abc", "[[abc]"),
                Arguments.of("[abc\"", "[[abc\"]"),
                Arguments.of("\"abc", "[\"abc]"),
                // delimiter not at start of part is an ordinary character
                Arguments.of("a[b].c", "[a[b]]].[c]"),
                Arguments.of("sch[ema.table", "[sch[ema].[table]"),
                // unicode
                Arguments.of("T\u00ef\u00f1\u00e9s", "[T\u00ef\u00f1\u00e9s]"),
                Arguments.of("\u6570\u636e\u8868", "[\u6570\u636e\u8868]"),
                Arguments.of("dbo.\u00d1\u00e4me", "[dbo].[\u00d1\u00e4me]"));
    }

    @ParameterizedTest(name = "escapeMultiPartIdentifier(\"{0}\") = \"{1}\"")
    @MethodSource("additionalQuotingArgs")
    public void testEscapeMultiPartIdentifierAdditionalQuoting(String input, String expected) {
        assertEquals(expected, Util.escapeMultiPartIdentifier(input));
    }

    // ─── escapeMultiPartIdentifier: additional payload tests ───

    private static Stream<Arguments> additionalPayloadArgs() {
        return Stream.of(
                Arguments.of("; DROP TABLE users--", "[; DROP TABLE users--]"),
                Arguments.of("t]; DROP TABLE x--", "[t]]; DROP TABLE x--]"),
                Arguments.of("table--; DROP TABLE x", "[table--; DROP TABLE x]"),
                Arguments.of("table /* comment */ ; EXEC xp_cmdshell 'cmd'",
                        "[table /* comment */ ; EXEC xp_cmdshell 'cmd']"),
                Arguments.of("table GO EXEC xp_cmdshell 'cmd'", "[table GO EXEC xp_cmdshell 'cmd']"),
                // invalid bracketed identifier: starts/ends with [] but internal ] not doubled
                Arguments.of("[t]; DROP TABLE x--]", "[[t]]; DROP TABLE x--]]]"),
                Arguments.of("[a] PRINT 1 --[b]", "[[a]] PRINT 1 --[b]]]"));
    }

    @ParameterizedTest(name = "escapeMultiPartIdentifier(\"{0}\") neutralizes payload")
    @MethodSource("additionalPayloadArgs")
    public void testEscapeMultiPartIdentifierNeutralizesPayload(String input, String expected) {
        assertEquals(expected, Util.escapeMultiPartIdentifier(input));
    }

    // ─── Composition: single-quote doubling after bracket quoting ───

    @Test
    public void testEscapeMultiPartIdentifierWithSingleQuote() {
        assertEquals("[O''Brien]", Util.escapeSingleQuotes(Util.escapeMultiPartIdentifier("O'Brien")));
        assertEquals("[dbo].[O''Brien]", Util.escapeSingleQuotes(Util.escapeMultiPartIdentifier("dbo.O'Brien")));
        assertEquals("[O''Brien]", Util.escapeSingleQuotes(Util.escapeMultiPartIdentifier("[O'Brien]")));
    }

    /**
     * After bracket-quoting and single-quote doubling, no unescaped single quote can close the N'...' literal
     * used in sp_executesql.
     */
    @ParameterizedTest
    @ValueSource(strings = {"O'Brien", "t'; EXEC xp_cmdshell 'whoami'--",
            "(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--"})
    public void testEscapeMultiPartIdentifierIsInertInStringLiteral(String input) {
        String escaped = Util.escapeSingleQuotes(Util.escapeMultiPartIdentifier(input));
        for (int i = 0; i < escaped.length(); i++) {
            if (escaped.charAt(i) == '\'') {
                assertTrue(i + 1 < escaped.length() && escaped.charAt(i + 1) == '\'',
                        "unescaped quote at " + i + " would close the N'...' literal: " + escaped);
                i++;
            }
        }
    }

    /**
     * The escaped result consists only of bracket-quoted parts separated by dots — no text can appear outside
     * an identifier.
     */
    @ParameterizedTest
    @ValueSource(strings = {"employees", "dbo.employees", "mydb.dbo.employees",
            "[dbo].[My Table]", "table]name", "t]; DROP TABLE x--",
            "(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--"})
    public void testEscapeMultiPartIdentifierIsWellFormed(String input) {
        String quoted = Util.escapeMultiPartIdentifier(input);
        int i = 0;
        while (true) {
            if (i < quoted.length() && quoted.charAt(i) == '[') {
                int j = i + 1;
                while (j < quoted.length()) {
                    if (quoted.charAt(j) == ']') {
                        if (j + 1 < quoted.length() && quoted.charAt(j + 1) == ']') {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j++;
                }
                assertTrue(j < quoted.length() && quoted.charAt(j) == ']',
                        "unterminated identifier in " + quoted);
                i = j + 1;
            }
            if (i == quoted.length()) {
                return;
            }
            assertTrue(quoted.charAt(i) == '.',
                    "unexpected text outside an identifier in " + quoted);
            i++;
        }
    }

    @Test
    public void testEscapeMultiPartIdentifierVeryLongName() {
        char[] chars = new char[128];
        java.util.Arrays.fill(chars, 'a');
        String longName = new String(chars);
        assertEquals("[" + longName + "]", Util.escapeMultiPartIdentifier(longName));
    }

}
