import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * Sample of use of platform gss integration
 *
 * It is necessary to set the -Dsun.security.jgss.native=true . Based on the host, it would either look for the GSSAPI
 * shared object library or DLL (Security Services Provider Interface or GSSAPI DLL if MIT Kerberos for Windows is installed)
 *
 * Based on "Native Platform GSS Integration" in https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html
 */
public class PlatformGSS {

    // Connection properties
    private static final String DRIVER_CLASS_NAME ="com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String CONNECTION_URI = "jdbc:sqlserver:// URI of the SQLServer";

    private static final Properties driverProperties;

    static {

        driverProperties = new Properties();
        driverProperties.setProperty("integratedSecurity", "true");
        driverProperties.setProperty("authenticationScheme", "JavaKerberos");
        driverProperties.setProperty("usePlatformGssCredentials", "true"); // Additional property to be added

        System.setProperty("sun.security.jgss.native", "true");
    }

    public static void main(String... args) throws Exception {

        Class.forName(DRIVER_CLASS_NAME).getConstructor().newInstance();

        try (Connection con = createConnection()) {
            System.out.println("Connection succesfully: " + con);
        }

    }

    /**
     * Obtains a connection using platform GSS
     */
    private static Connection createConnection() throws SQLException {
        return DriverManager.getConnection(CONNECTION_URI, driverProperties);
    }

}
