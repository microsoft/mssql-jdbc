//---------------------------------------------------------------------------------------------------------------------------------
// File: KerbAuthentication.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 
 
package com.microsoft.sqlserver.jdbc;
import java.util.logging.*;
import org.ietf.jgss.*;
import javax.security.auth.Subject;
import java.util.*;
import javax.security.auth.login.*;
import java.net.IDN;
import java.security.*;


/**
* KerbAuthentication for int auth.
*/
final class KerbAuthentication extends SSPIAuthentication
{
    private final static String CONFIGNAME = "SQLJDBCDriver";
    private final static java.util.logging.Logger authLogger 	= java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.KerbAuthentication");

	private final SQLServerConnection con;
    private final String spn;

    private final GSSManager manager = GSSManager.getInstance();
    private LoginContext lc = null;    
    private GSSCredential peerCredentials = null;
	private GSSContext peerContext = null;

    
    static 
	{
	    // The driver on load will look to see if there is a configuration set for the SQLJDBCDriver, if not it will install its 
        // own configuration. Note it is possible that there is a configuration exists but it does not contain a configuration entry 
        // for the driver in that case, we will override the configuration but will flow the configuration requests to existing
        // config for anything other than SQLJDBCDriver
        // 
    	class SQLJDBCDriverConfig extends Configuration
    	{
    	    Configuration current = null;
            AppConfigurationEntry[] driverConf;

            SQLJDBCDriverConfig()
            {
                try 
                {
                     current = Configuration.getConfiguration();
                }
                catch (SecurityException e)
                {
                    // if we cant get the configuration, it is likely that no configuration has been specified. So go ahead and set the config
                    authLogger.finer(toString() + " No configurations provided, setting driver default");
                }
                AppConfigurationEntry[] config = null;
                
                if(null != current)
                {
                    config = current.getAppConfigurationEntry(CONFIGNAME);
                }
                // If there is user provided configuration we leave use that and not install our configuration
                if(null == config)
                {
                    if (authLogger.isLoggable(Level.FINER)) 
                        authLogger.finer(toString() + " SQLJDBCDriver configuration entry is not provided, setting driver default");
                    
                    AppConfigurationEntry appConf;
    				if(Util.isIBM())
                    {
                        Map<String, String> confDetails = new HashMap<String, String>();
        				confDetails.put("useDefaultCcache", "true");
                        confDetails.put("moduleBanner", "false");
                        appConf = new AppConfigurationEntry("com.ibm.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,  confDetails);
                        if (authLogger.isLoggable(Level.FINER)) 
                            authLogger.finer(toString() + " Setting IBM Krb5LoginModule");    
                    }
                    else
                    {
                        Map<String, String> confDetails = new HashMap<String, String>();
        				confDetails.put("useTicketCache", "true");
        				confDetails.put("doNotPrompt", "true");
    				    appConf = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,  confDetails);
                        if (authLogger.isLoggable(Level.FINER)) 
                            authLogger.finer(toString() + " Setting Sun Krb5LoginModule");    
                    }
    				driverConf = new AppConfigurationEntry[1];
    				driverConf[0] = appConf;
            		Configuration.setConfiguration(this);
                }
                
            }
            
    		public AppConfigurationEntry[] getAppConfigurationEntry(String name) 
    		{   
    		    // we should only handle anything that is related to our part, everything else is handled by the configuration 
    		    // already existing configuration if there is one. 
    		    if(name.equals(CONFIGNAME))
                {         
                    return driverConf;
                }
                else
                {
                    if(null != current)
                        return current.getAppConfigurationEntry(name);
                    else
                        return null;
                }
    		}
    		
    		public void refresh()
    		{
    			if(null != current)
                    current.refresh();
    		}
    	}
        SQLJDBCDriverConfig driverconfig = new SQLJDBCDriverConfig();
	}    
    
    private void intAuthInit() throws SQLServerException
	{
		try
		{
            // If we need to support NTLM as well, we can use null
            // Kerberos OID
            Oid kerberos = new Oid("1.2.840.113554.1.2.2"); 
            Subject currentSubject=null;
            try 
            {
                AccessControlContext context = AccessController.getContext();
        		currentSubject = Subject.getSubject(context);
                if(null == currentSubject)
                {
                    lc = new LoginContext(CONFIGNAME);
                    lc.login();
                    // per documentation LoginContext will instantiate a new subject. 
                    currentSubject = lc.getSubject();
                }
            } 
            catch (LoginException le) 
            {
                con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_integratedAuthenticationFailed"), le);
            }
            
            // http://blogs.sun.com/harcey/entry/of_java_kerberos_and_access
            // We pass null to indicate that the system should interpret the SPN as it is. 
            GSSName remotePeerName = manager.createName(spn, null);
            if (authLogger.isLoggable(Level.FINER)) 
    		{
    			authLogger.finer(toString() + " Getting client credentials");
    		} 
            peerCredentials = getClientCredential(currentSubject,manager, kerberos);
            if (authLogger.isLoggable(Level.FINER)) 
    		{
    			authLogger.finer(toString() + " creating security context");
    		}

            peerContext = manager.createContext(remotePeerName,
                    kerberos,
                    peerCredentials,
                    GSSContext.DEFAULT_LIFETIME);
            // The following flags should be inline with our native implementation.
            peerContext.requestCredDeleg(true);
            peerContext.requestMutualAuth(true);
            peerContext.requestInteg(true);
		}

		catch(GSSException ge) 
		{
		    authLogger.finer(toString() + "initAuthInit failed GSSException:-" + ge);
            con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);            
		}
        catch(PrivilegedActionException ge) 
		{
		    authLogger.finer(toString() + "initAuthInit failed privileged exception:-" + ge);
            con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);            
		}
        

	}

    // We have to do a privileged action to create the credential of the user in the current context
    private static GSSCredential getClientCredential(final Subject subject, final GSSManager MANAGER, final Oid kerboid)
            throws PrivilegedActionException 
    {
         final PrivilegedExceptionAction<GSSCredential> action = 
            new PrivilegedExceptionAction<GSSCredential>() {
                public GSSCredential run() throws GSSException {
                    return MANAGER.createCredential(
                        null // use the default principal
                        , GSSCredential.DEFAULT_LIFETIME
                        ,kerboid 
                        , GSSCredential.INITIATE_ONLY);
                } 
            };
        // TO support java 5, 6 we have to do this
        // The signature for Java 5 returns an object 6 returns GSSCredential, immediate casting throws 
        // warning in Java 6.
        Object credential = Subject.doAs(subject, action);
        return (GSSCredential)credential;
    }
    private byte [] intAuthHandShake(byte[] pin,   boolean[] done) throws SQLServerException
    {
        try
        {
            if (authLogger.isLoggable(Level.FINER)) 
    		{
    			authLogger.finer(toString() + " Sending token to server over secure context");
    		}
			byte [] byteToken = peerContext.initSecContext(pin, 0,  pin.length);
            
	        if (peerContext.isEstablished())
            {
                done[0] = true;
                if (authLogger.isLoggable(Level.FINER)) 
                    authLogger.finer(toString() + "Authentication done.");
  		    }
            else
                if (null == byteToken)
                 {
                    // The documentation is not clear on when this can happen but it does say this could happen
                    authLogger.info(toString() + "byteToken is null in initSecContext.");
                    con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_integratedAuthenticationFailed"));        
                 }
           return byteToken;
        }
        catch(GSSException ge) 
		{
		    authLogger.finer(toString() + "initSecContext Failed :-" + ge);
            con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);            
		}
        // keep the compiler happy
        return null;
    }
    
	private String makeSpn(String server, int port) throws SQLServerException
    {
		if (authLogger.isLoggable(Level.FINER)) 
		{
			authLogger.finer(toString() + " Server: " + server + " port: " +  port);
		}
        StringBuilder spn = new StringBuilder("MSSQLSvc/");
		//Format is MSSQLSvc/myhost.domain.company.com:1433
		// FQDN must be provided
        if(con.serverNameAsACE())
        {
        	spn.append(IDN.toASCII(server));
        }
        else
        {
        	spn.append(server);
        }		
		spn.append(":");
		spn.append(port);
		String strSPN = spn.toString();
		if (authLogger.isLoggable(Level.FINER)) 
		{
			authLogger.finer(toString() + " SPN: " + strSPN);
		}
		return strSPN;
	}

    // Package visible members below. 
	KerbAuthentication(SQLServerConnection con, String address, int port) throws SQLServerException
	{
		this.con = con;
		// Get user provided SPN string; if not provided then build the generic one
		String userSuppliedServerSpn = con.activeConnectionProperties.
				getProperty(SQLServerDriverStringProperty.SERVER_SPN.toString());
		
		if (null != userSuppliedServerSpn)
		{
			// serverNameAsACE is true, translate the user supplied serverSPN to ASCII
			if(con.serverNameAsACE())
			{
				int slashPos = userSuppliedServerSpn.indexOf("/");
				spn = userSuppliedServerSpn.substring(0,slashPos+1)
						+ IDN.toASCII(userSuppliedServerSpn.substring(slashPos+1));
			}
			else
			{
				spn = userSuppliedServerSpn;
			}
		}
        else
        {
        	spn =  makeSpn(address, port);
        }
	}
	
    byte[] GenerateClientContext(byte[] pin,   boolean[] done ) throws SQLServerException
    {
        if(null == peerContext)
        {
            intAuthInit();
        }
        return intAuthHandShake(pin, done);
    }
    int ReleaseClientContext() throws SQLServerException
    {
        try 
        {
            if(null != peerCredentials)
                peerCredentials.dispose();
            if(null != peerContext)
                peerContext.dispose();
            if(null != lc)
                lc.logout();
        }
        catch(LoginException e)
        {
            // yes we are eating exceptions here but this should not fail in the normal circumstances and we do not want to eat previous
            // login errors if caused before which is more useful to the user than the cleanup errors.
            authLogger.fine(toString() + " Release of the credentials failed LoginException: " + e);
        }
        catch(GSSException e)
        {
            // yes we are eating exceptions here but this should not fail in the normal circumstances and we do not want to eat previous
            // login errors if caused before which is more useful to the user than the cleanup errors.
            authLogger.fine(toString() + " Release of the credentials failed GSSException: " + e);
        }
        return 0;
    }
}


