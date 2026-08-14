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

import javax.net.SocketFactory;
import javax.net.ssl.TrustManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
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

}
