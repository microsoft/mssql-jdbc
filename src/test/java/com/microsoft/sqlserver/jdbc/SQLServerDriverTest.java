package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class SQLServerDriverTest extends AbstractTest {

    String randomServer = RandomUtil.getIdentifier("Server");
    static final Logger logger = Logger.getLogger("SQLServerDriverTest");

    /**
     * Tests the stream<Driver> drivers() methods in java.sql.DriverManager
     * 
     * @since 1.9
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testDriverDM() throws SQLException, ClassNotFoundException {
        Driver driver = DriverManager.getDriver(connectionString);
        assertEquals(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDriver"), driver.getClass(),
                TestResource.getResource("R_parrentLoggerNameWrong"));
    }

    /**
     * Tests deRegister Driver
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void testDriverRegistrationDM() throws SQLException, ClassNotFoundException {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        Driver current = null;
        while (drivers.hasMoreElements()) {
            current = drivers.nextElement();
            DriverManager.deregisterDriver(current);
        }
        Stream<Driver> currentDrivers = DriverManager.drivers();
        Object[] driversArray = currentDrivers.toArray();
        assertEquals(0, driversArray.length);
        DriverManager.registerDriver(current);
    }

    @Test
    public void testDriverIsRegistered() {
        assertTrue(SQLServerDriver.isRegistered(), TestResource.getResource("R_valuesAreDifferent"));
    }

    @Test
    public void testDriverRegistrationInternal() throws SQLException {
        if (SQLServerDriver.isRegistered()) {
            try {
                SQLServerDriver.register();
            } catch (SQLException e) {
                fail(e.getMessage());
            }
            SQLServerDriver.deregister();
            SQLServerDriver.register();
        }
    }

    @Test
    public void testConnect() {
        SQLServerDriver driver = new SQLServerDriver();
        try (Connection con = driver.connect(connectionString, null); Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            while (rs.next()) {
                assertEquals(1, rs.getInt(1));
            }
        } catch (SQLException e) {
            fail();
        }
        try (Connection con = driver.connect(null, null)) {
            fail();
        } catch (SQLException e) {
            assert (e instanceof SQLServerException);
            assert (null != e.getMessage());
            assert (e.getMessage().contains(TestResource.getResource("R_ConnectionURLNull")));
        }
    }

    @Test
    public void testAcceptsUrl() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        assert (driver.acceptsURL(connectionString));
        assert (!driver.acceptsURL("jdbc:somethingelse"));
        try {
            driver.acceptsURL(null);
            fail();
        } catch (SQLException e) {
            assert (e instanceof SQLServerException);
            assert (null != e.getMessage());
            assert (e.getMessage().contains(TestResource.getResource("R_ConnectionURLNull")));
        }
    }

    @Test
    public void testJDBCCompliant() {
        assert (new SQLServerDriver().jdbcCompliant());
    }

    @Test
    public void testNullPropertyInfo() {
        try {
            new SQLServerDriver().getPropertyInfo(null, null);
            fail();
        } catch (SQLException e) {
            assert (e instanceof SQLServerException);
            assert (null != e.getMessage());
            assert (e.getMessage().contains(TestResource.getResource("R_ConnectionURLNull")));
        }
    }

    @Test
    public void testParentLogger() throws SQLFeatureNotSupportedException {
        SQLServerDriver serverDriver = new SQLServerDriver();
        Logger logger = serverDriver.getParentLogger();
        assertEquals(logger.getName(), Constants.MSSQL_JDBC_PACKAGE,
                TestResource.getResource("R_parrentLoggerNameWrong"));
    }

    /**
     * test connection properties
     * 
     * @throws SQLException
     */
    @Test
    public void testConnectionDriver() throws SQLException {
        SQLServerDriver d = new SQLServerDriver();
        Properties info = new Properties();
        StringBuffer url = new StringBuffer();
        url.append(Constants.JDBC_PREFIX + randomServer + ";packetSize=512;");
        // test defaults
        DriverPropertyInfo[] infoArray = d.getPropertyInfo(url.toString(), info);
        for (DriverPropertyInfo anInfoArray1 : infoArray) {
            logger.fine(anInfoArray1.name);
            logger.fine(anInfoArray1.description);
            logger.fine(Boolean.valueOf(anInfoArray1.required).toString());
            logger.fine(anInfoArray1.value);
        }

        url.append("encrypt=true; trustStore=someStore; trustStorePassword=somepassword;");
        url.append("hostNameInCertificate=someHost; trustServerCertificate=true");
        infoArray = d.getPropertyInfo(url.toString(), info);
        for (DriverPropertyInfo anInfoArray : infoArray) {
            if (anInfoArray.name.equalsIgnoreCase(Constants.ENCRYPT)) {
                assertTrue(anInfoArray.value.equalsIgnoreCase(Boolean.TRUE.toString()),
                        TestResource.getResource("R_valuesAreDifferent"));
            }
            if (anInfoArray.name.equalsIgnoreCase(Constants.TRUST_STORE)) {
                assertTrue(anInfoArray.value.equalsIgnoreCase("someStore"),
                        TestResource.getResource("R_valuesAreDifferent"));
            }
            if (anInfoArray.name.equalsIgnoreCase(Constants.TRUST_STORE_SECRET_PROPERTY)) {
                assertTrue(anInfoArray.value.equalsIgnoreCase("somepassword"),
                        TestResource.getResource("R_valuesAreDifferent"));
            }
            if (anInfoArray.name.equalsIgnoreCase(Constants.HOST_NAME_IN_CERTIFICATE)) {
                assertTrue(anInfoArray.value.equalsIgnoreCase("someHost"),
                        TestResource.getResource("R_valuesAreDifferent"));
            }
        }
    }
}
