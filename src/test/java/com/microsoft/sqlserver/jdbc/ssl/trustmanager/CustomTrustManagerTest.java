package com.microsoft.sqlserver.jdbc.ssl.trustmanager;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.*;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class CustomTrustManagerTest extends AbstractTest {

    /**
     * Connect with a permissive Trust Manager that always accepts the X509Certificate chain offered to it.
     * 
     * @throws Exception
     */
    @Test
    public void testWithPermissiveX509TrustManager() throws Exception {
        String url = connectionString + ";trustManagerClass=" + PermissiveTrustManager.class.getName() + ";encrypt=true;";
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(url)) {
            assertTrue(con != null);
        }
    }

    /**
     * Connect with a Trust Manager that requires trustManagerConstructorArg.
     * 
     * @throws Exception
     */
    @Test
    public void testWithTrustManagerConstructorArg() throws Exception {
        String url = connectionString + ";trustManagerClass=" + TrustManagerWithConstructorArg.class.getName()
                + ";trustManagerConstructorArg=dummyString;" + ";encrypt=true;";
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(url)) {
            assertTrue(con != null);
        }
    }

    /**
     * Test with a custom Trust Manager that does not implement X509TrustManager.
     * 
     * @throws Exception
     */
    @Test
    public void testWithInvalidTrustManager() throws Exception {
        String url = connectionString + ";trustManagerClass=" + InvalidTrustManager.class.getName() + ";encrypt=true;";
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(url)) {
            fail();
        }
        catch (SQLException e) {
            assertTrue(e.getMessage().contains("The class specified by the trustManagerClass property must implement javax.net.ssl.TrustManager"));
        }
    }
}
