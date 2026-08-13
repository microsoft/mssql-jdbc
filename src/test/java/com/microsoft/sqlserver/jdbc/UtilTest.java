/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

    private static Stream<Arguments> sanitizeIdentifierQuotesArgs() {
        return Stream.of(
                // regular names
                Arguments.of("employees", "[employees]"),
                Arguments.of("dbo.employees", "[dbo].[employees]"),
                Arguments.of("mydb.dbo.employees", "[mydb].[dbo].[employees]"),
                // four-part (linked server) names are supported, matching SqlClient's MultipartIdentifier
                Arguments.of("srv.mydb.dbo.employees", "[srv].[mydb].[dbo].[employees]"),
                // an empty qualifier defers to the default schema and has to be preserved
                Arguments.of("mydb..employees", "[mydb]..[employees]"),
                Arguments.of("srv..dbo.employees", "[srv]..[dbo].[employees]"),
                // caller-delimited names are not double-quoted
                Arguments.of("[dbo].[My Table]", "[dbo].[My Table]"),
                Arguments.of("[table]]name]", "[table]]name]"),
                Arguments.of("[a]]b]]c]", "[a]]b]]c]"),
                Arguments.of("[db].[dbo].[my.table]", "[db].[dbo].[my.table]"),
                Arguments.of("\"dbo\".\"My Table\"", "[dbo].[My Table]"),
                Arguments.of("\"my\"\"table\"", "[my\"table]"),
                // whitespace around a part is not part of the name, whitespace inside a delimited part is
                Arguments.of("  dbo  .  t  ", "[dbo].[t]"),
                Arguments.of("[ ]", "[ ]"),
                Arguments.of("[]", "[]"),
                // temp tables
                Arguments.of("#tempTable", "[#tempTable]"),
                Arguments.of("##globalTemp", "[##globalTemp]"),
                // characters that must be escaped rather than allowed to close the identifier
                Arguments.of("table]name", "[table]]name]"),
                Arguments.of("O'Brien", "[O'Brien]"),
                // unterminated or mismatched delimiters are ordinary characters, not the start of an identifier
                Arguments.of("[abc", "[[abc]"),
                Arguments.of("[abc\"", "[[abc\"]"),
                Arguments.of("\"abc", "[\"abc]"),
                // a delimiter that does not start the part is an ordinary character, so later dots still split
                Arguments.of("a[b].c", "[a[b]]].[c]"),
                Arguments.of("sch[ema.table", "[sch[ema].[table]"),
                // unicode
                Arguments.of("T\u00ef\u00f1\u00e9s", "[T\u00ef\u00f1\u00e9s]"),
                Arguments.of("\u6570\u636e\u8868", "[\u6570\u636e\u8868]"),
                Arguments.of("dbo.\u00d1\u00e4me", "[dbo].[\u00d1\u00e4me]"));
    }

    /**
     * Names that are quoted rather than rejected. Every result is a well-formed bracket-quoted identifier, so text
     * that is not a valid object name names one non-existent object rather than adding SQL.
     */
    @ParameterizedTest(name = "sanitizeIdentifier(\"{0}\") = \"{1}\"")
    @MethodSource("sanitizeIdentifierQuotesArgs")
    public void testSanitizeIdentifierQuotes(String input, String expected) throws SQLException {
        assertEquals(expected, Util.sanitizeIdentifier(input));
    }

    private static Stream<Arguments> sanitizeIdentifierPayloadArgs() {
        return Stream.of(
                Arguments.of("(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--",
                        "[(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--]"),
                Arguments.of("; DROP TABLE users--", "[; DROP TABLE users--]"),
                Arguments.of("t]; DROP TABLE x--", "[t]]; DROP TABLE x--]"),
                Arguments.of("table--; DROP TABLE x", "[table--; DROP TABLE x]"),
                Arguments.of("table /* comment */ ; EXEC xp_cmdshell 'cmd'",
                        "[table /* comment */ ; EXEC xp_cmdshell 'cmd']"),
                Arguments.of("table GO EXEC xp_cmdshell 'cmd'", "[table GO EXEC xp_cmdshell 'cmd']"),
                Arguments.of("(SELECT 1 a) t; SET FMTONLY OFF; SELECT name, password_hash INTO ##creds FROM sys.sql_logins--",
                        "[(SELECT 1 a) t; SET FMTONLY OFF; SELECT name, password_hash INTO ##creds FROM sys].[sql_logins--]"),
                // starts and ends with brackets but is not a single identifier, so it is escaped whole instead of
                // being trusted and unwrapped
                Arguments.of("[a] PRINT 1 --[b]", "[[a]] PRINT 1 --[b]]]"),
                Arguments.of("[a] SET FMTONLY OFF; SELECT 1 INTO ##x--[b]", "[[a]] SET FMTONLY OFF; SELECT 1 INTO ##x--[b]]]"),
                // same shape with double quotes
                Arguments.of("\"a\" PRINT 1 --\"b\"", "[\"a\" PRINT 1 --\"b\"]"));
    }

    /**
     * Payloads that try to break out of the identifier and run as SQL. Each is quoted whole, so it names one
     * non-existent object instead of adding statements.
     */
    @ParameterizedTest(name = "sanitizeIdentifier(\"{0}\") = \"{1}\"")
    @MethodSource("sanitizeIdentifierPayloadArgs")
    public void testSanitizeIdentifierNeutralizesPayload(String input, String expected) throws SQLException {
        assertEquals(expected, Util.sanitizeIdentifier(input));
    }

    /**
     * Names that cannot identify an object, rejected up front so the caller gets a clear error instead of an
     * unrelated server-side one.
     */
    @ParameterizedTest(name = "sanitizeIdentifier(\"{0}\") throws")
    @ValueSource(strings = {"", "   ", "\t",
            // no object part
            ".", "dbo.", "dbo..", "  .  ",
            // more parts than [server].[database].[schema].[object]
            "a.b.c.d.e", "[a].[b].[c].[d].[e]", "[a].[b].[c].[d].e", "srv.db.schema.tbl.extra"})
    public void testSanitizeIdentifierRejects(String input) {
        assertThrows(SQLServerException.class, () -> Util.sanitizeIdentifier(input));
    }

    @Test
    public void testSanitizeIdentifierNullThrows() {
        assertThrows(SQLServerException.class, () -> Util.sanitizeIdentifier(null));
    }

    /**
     * A name containing a single quote survives both steps getDestinationMetadata applies: it is bracket-quoted so it
     * reads as an object name, and its quotes are doubled so it can sit in the enclosing N'...' literal.
     */
    @Test
    public void testSanitizeIdentifierWithSingleQuote() throws SQLException {
        assertEquals("[O''Brien]", Util.escapeSingleQuotes(Util.sanitizeIdentifier("O'Brien")));
        assertEquals("[dbo].[O''Brien]", Util.escapeSingleQuotes(Util.sanitizeIdentifier("dbo.O'Brien")));
        assertEquals("[O''Brien]", Util.escapeSingleQuotes(Util.sanitizeIdentifier("[O'Brien]")));
    }

    /**
     * A quoted name is inert inside the N'...' literal the metadata query builds: every remaining quote is doubled,
     * so the literal cannot be closed early.
     */
    @ParameterizedTest
    @ValueSource(strings = {"O'Brien", "t'; EXEC xp_cmdshell 'whoami'--",
            "(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--"})
    public void testSanitizeIdentifierIsInertInStringLiteral(String input) throws SQLException {
        String escaped = Util.escapeSingleQuotes(Util.sanitizeIdentifier(input));
        for (int i = 0; i < escaped.length(); i++) {
            if (escaped.charAt(i) == '\'') {
                assertTrue("unescaped quote at " + i + " would close the N'...' literal: " + escaped,
                        i + 1 < escaped.length() && escaped.charAt(i + 1) == '\'');
                i++;
            }
        }
    }

    /**
     * A quoted name is a single bracketed identifier per part, so no part can close its brackets early and be read
     * as anything other than an object name.
     */
    @ParameterizedTest
    @ValueSource(strings = {"employees", "dbo.employees", "srv.mydb.dbo.employees", "mydb..employees",
            "[dbo].[My Table]", "table]name", "t]; DROP TABLE x--", "[a] PRINT 1 --[b]",
            "(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'whoami'--"})
    public void testSanitizeIdentifierIsWellFormed(String input) throws SQLException {
        String quoted = Util.sanitizeIdentifier(input);
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
                assertTrue("unterminated identifier in " + quoted, j < quoted.length() && quoted.charAt(j) == ']');
                i = j + 1;
            }
            if (i == quoted.length()) {
                return;
            }
            assertTrue("unexpected text outside an identifier in " + quoted, quoted.charAt(i) == '.');
            i++;
        }
    }

    /** SQL Server allows identifiers of up to 128 characters; the name must not be truncated. */
    @Test
    public void testSanitizeIdentifierVeryLongName() throws SQLException {
        char[] chars = new char[128];
        java.util.Arrays.fill(chars, 'a');
        String longName = new String(chars);
        assertEquals("[" + longName + "]", Util.sanitizeIdentifier(longName));
    }

}
