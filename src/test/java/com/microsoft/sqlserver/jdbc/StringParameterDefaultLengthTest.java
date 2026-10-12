/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.DriverPropertyInfo;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.microsoft.sqlserver.testframework.Constants;


@Tag("Unit")
public class StringParameterDefaultLengthTest {
    private static final String PROPERTY = "stringParameterDefaultLength";

    @Test
    public void testInterfaceCompatibilityDefaults() throws Exception {
        assertTrue(ISQLServerDataSource.class.getMethod("getStringParameterDefaultLength").isDefault());
        assertTrue(ISQLServerDataSource.class.getMethod("setStringParameterDefaultLength", int.class).isDefault());
        ISQLServerDataSource source = mock(ISQLServerDataSource.class, CALLS_REAL_METHODS);
        assertEquals(0, source.getStringParameterDefaultLength());
        source.setStringParameterDefaultLength(5000);
        assertEquals(0, source.getStringParameterDefaultLength());
    }

    @Test
    public void testPropertyInfoDefault() throws Exception {
        DriverPropertyInfo property = findProperty(
                new SQLServerDriver().getPropertyInfo(Constants.JDBC_PREFIX, new Properties()));
        assertEquals("0", property.value);
        assertFalse(property.required);
        assertNull(property.choices);
        assertNotNull(property.description);
    }

    @Test
    public void testUrlAndPropertiesRegistration() throws Exception {
        SQLServerDriver driver = new SQLServerDriver();
        String url = Constants.JDBC_PREFIX + ";STRINGPARAMETERDEFAULTLENGTH=5000";
        assertEquals("5000", findProperty(driver.getPropertyInfo(url, new Properties())).value);
        Properties properties = new Properties();
        properties.setProperty("stringparameterdefaultlength", "8001");
        assertEquals("8001", findProperty(driver.getPropertyInfo(url, properties)).value);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, 4000, 5000, 8000, 8001, Integer.MAX_VALUE})
    public void testDataSourceStorageAndSerialization(int value) throws Exception {
        SQLServerDataSource[] sources = {new SQLServerDataSource(), new SQLServerConnectionPoolDataSource(),
                new SQLServerXADataSource()};
        for (SQLServerDataSource source : sources) {
            ISQLServerDataSource api = source;
            assertEquals(0, api.getStringParameterDefaultLength());
            assertNull(source.getReference().get(PROPERTY));
            api.setStringParameterDefaultLength(value);
            assertEquals(value, api.getStringParameterDefaultLength());
            assertEquals(Integer.toString(value), source.getReference().get(PROPERTY).getContent());
            SQLServerDataSource restored = (SQLServerDataSource) new SQLServerDataSourceObjectFactory()
                    .getObjectInstance(source.getReference(), null, null, null);
            assertEquals(source.getClass(), restored.getClass());
            assertEquals(value, restored.getStringParameterDefaultLength());

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
                output.writeObject(source);
            }
            try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                restored = (SQLServerDataSource) input.readObject();
                assertEquals(source.getClass(), restored.getClass());
                assertEquals(value, restored.getStringParameterDefaultLength());
            }
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"0", "-1", "-2147483648", "1", "4000", "5000", "8000", "8001", "2147483647"})
    public void testConnectionParsingAndWarnings(String value) throws Exception {
        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerConnection");
        Level previousLevel = logger.getLevel();
        List<LogRecord> warnings = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING && record.getMessage().contains(PROPERTY)) {
                    warnings.add(record);
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        logger.addHandler(handler);
        logger.setLevel(Level.WARNING);
        try (SQLServerConnection connection = new SQLServerConnection("property-test")) {
            Properties properties = parsingProperties(value);
            SQLServerException exception = assertThrows(SQLServerException.class,
                    () -> connection.connect(properties, null));
            assertEquals(MessageFormat.format(SQLServerException.getErrString("R_invalidPacketSize"), "1"),
                    exception.getMessage());
            int configured = null == value ? 0 : Integer.parseInt(value);
            assertEquals(Math.max(0, configured), connection.getStringParameterDefaultLength());
            assertEquals(null != value && configured <= 0 ? 1 : 0, warnings.size());
            if (!warnings.isEmpty()) {
                assertEquals(connection + " The stringParameterDefaultLength value is not positive; "
                        + "using legacy parameter sizing.", warnings.get(0).getMessage());
            }
        } finally {
            logger.removeHandler(handler);
            logger.setLevel(previousLevel);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "abc", "1.5", "2147483648", "-2147483649"})
    public void testMalformedConfigurationRejectedBeforeNetwork(String value) throws Exception {
        try (SQLServerConnection connection = new SQLServerConnection("property-test")) {
            SQLServerException exception = assertThrows(SQLServerException.class,
                    () -> connection.connect(parsingProperties(value), null));
            assertEquals(SQLServerException.getErrString("R_invalidStringParameterDefaultLength"),
                    exception.getMessage());
        }
    }

    private static Properties parsingProperties(String value) {
        Properties properties = new Properties();
        if (null != value) {
            properties.setProperty(PROPERTY, value);
        }
        // A later validation error stops connection setup before any network access.
        properties.setProperty("packetSize", "1");
        properties.setProperty("connectRetryCount", "0");
        return properties;
    }

    private static DriverPropertyInfo findProperty(DriverPropertyInfo[] properties) {
        for (DriverPropertyInfo property : properties) {
            if (PROPERTY.equals(property.name)) {
                return property;
            }
        }
        return fail("Missing property: " + PROPERTY);
    }
}
