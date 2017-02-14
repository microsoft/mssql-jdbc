/**
 * 
 */
package com.microsoft.sqlserver.testframework;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author v-afrafi
 *
 */
public class DBOptions extends AbstractParentWrapper{
    public static final Logger log = Logger.getLogger("DBOptions");

    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBOptions(AbstractParentWrapper parent,
            Object internal,
            String name) {
        super(parent, internal, name);
        // TODO Auto-generated constructor stub
    }

    // 'SQL' represents SQL Server, while 'SQLAzure' represents SQL Azure. 
    private static String _serverType         = null;  
    public  static final  String  SERVER_TYPE_SQL_SERVER = "SQL";
    public  static final  String  SERVER_TYPE_SQL_AZURE  = "SQLAzure";
    
    public static String servertype()
    {
        if (_serverType == null)
        {
            String serverTypeProperty = DBOptions.get("server.type");
            if (serverTypeProperty == null)
            {
                // default to SQL Server
                _serverType = SERVER_TYPE_SQL_SERVER;
            }
            else if (serverTypeProperty.equalsIgnoreCase(SERVER_TYPE_SQL_AZURE))
            {
                _serverType = SERVER_TYPE_SQL_AZURE;
            } 
            else if (serverTypeProperty.equalsIgnoreCase(SERVER_TYPE_SQL_SERVER))
            {
                _serverType = SERVER_TYPE_SQL_SERVER;
            }
            else 
            {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Server.type '" + serverTypeProperty +"' is not supported yet. Default to SQL Server");
                }
                _serverType = SERVER_TYPE_SQL_SERVER;
            }
        }
        return _serverType;
    }
    
    public static String get(String property){
      //Otherwise delegate to Java, as it was probably passed in.
        return System.getProperty(property);
    }
    
}
