package com.microsoft.sqlserver.jdbc.CustomTrustManager;

import java.sql.DriverManager;

import static org.junit.Assert.*;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class CustomTrustManagerTest extends AbstractTest {

    /**
     * Connect with a permissive Trust Manager that always accepts the X509Certificate chain offered to it. The test assumes PermissiveTrustManager
     * class exists in the current package.
     * 
     * @throws Exception
     */
    @Test
    public void testWithPermissiveX509TrustManager() throws Exception {
        String url = connectionString + ";trustManagerClass=" + this.getClass().getPackage().getName() + ".PermissiveTrustManager" + ";encrypt=true;";
        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(url);
        assertTrue(con != null);
    }

    /**
     * Connect with a Trust Manager that requires trustManagerConstructorArg. The test assumes TrustManagerWithConstructorArg class exists in the
     * current package.
     * 
     * @throws Exception
     */
    @Test
    public void testWithTrustManagerConstructorArg() throws Exception {
        String url = connectionString + ";trustManagerClass=" + this.getClass().getPackage().getName() + ".TrustManagerWithConstructorArg"
                + ";trustManagerConstructorArg=dummyString;" + ";encrypt=true;";
        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(url);
        assertTrue(con != null);
    }

    /**
     * Test with a custom Trust Manager that does not implement X509TrustManager The test assumes InvalidTrustManager class exists in the current
     * package.
     * 
     * @throws Exception
     */
    @Test
    public void testWithInvalidTrustManager() throws Exception {
        try {
            String url = connectionString + ";trustManagerClass=" + this.getClass().getPackage().getName() + ".InvalidTrustManager"
                    + ";encrypt=true;";
            SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(url);
            assertTrue(con == null);
        }
        catch (SQLServerException e) {
            assertTrue(e.getMessage().contains("The class specified by the trustManagerClass property must implement javax.net.ssl.TrustManager"));
        }
    }
}
