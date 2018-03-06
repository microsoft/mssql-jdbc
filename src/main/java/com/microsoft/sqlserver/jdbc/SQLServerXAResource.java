/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Transaction id implementation used to recover transactions.
 */

final class XidImpl implements Xid {
    private final int formatId;
    private final byte gtrid[];
    private final byte bqual[];
    private final String traceID;

    /*
     * XA Flags public static final int TMENDRSCAN = 8388608; public static final int TMFAIL = 536870912; public static final int TMJOIN = 2097152;
     * public static final int TMNOFLAGS = 0; public static final int TMONEPHASE = 1073741824; public static final int TMRESUME = 134217728; public
     * static final int TMSTARTRSCAN = 16777216; public static final int TMSUCCESS = 67108864; public static final int TMSUSPEND = 33554432; public
     * static final int XA_RDONLY = 3; public static final int XA_OK = 0;
     */

    /**
     * Create a new XID
     * 
     * @param formatId
     *            format id
     * @param gtrid
     *            global id
     * @param bqual
     *            branch id
     */
    /* L0 */ public XidImpl(int formatId,
            byte gtrid[],
            byte bqual[]) {
        this.formatId = formatId;
        this.gtrid = gtrid;
        this.bqual = bqual;
        traceID = " XID:" + xidDisplay(this);
    }

    /* L0 */ public byte[] getGlobalTransactionId() {
        return gtrid;
    }

    /* L0 */ public byte[] getBranchQualifier() {
        return bqual;
    }

    /* L0 */ public int getFormatId() {
        return formatId;
    }

    /**
     * Used for tracing
     * 
     * @return traceID string
     */
    public String toString() {
        return traceID;
    }

    // Returns displayable representation of xid for logging purposes.
    static String xidDisplay(Xid xid) {
        if (null == xid)
            return "(null)";
        StringBuilder sb = new StringBuilder(300);
        sb.append("formatId=");
        sb.append(xid.getFormatId());
        sb.append(" gtrid=");
        sb.append(Util.byteToHexDisplayString(xid.getGlobalTransactionId()));
        sb.append(" bqual=");
        sb.append(Util.byteToHexDisplayString(xid.getBranchQualifier()));
        return sb.toString();
    }

}

final class XAReturnValue {
    int nStatus;
    byte bData[];
}

/**
 * SQLServerXAResource provides an XAResource for XA distributed transaction management. XA transactions are implemented over SQL Server using
 * Microsoft Distributed Transaction Manager (DTC). SQLServerXAResource makes calls to a SQL Server extended dll called SQLServer_XA.dll which
 * interfaces with DTC.
 * 
 * XA calls received by SQLServerXAResource (XA_START, XA_END, XA_PREPARE etc) are mapped to the corresponding calls to DTC functions.
 * 
 * SQLServerXAResource may also be configured not to use DTC. In this case distributed transactions are simply implemented as local transactions.
 */

public final class SQLServerXAResource implements javax.transaction.xa.XAResource {
    /*
     * In the Java transaction API doc a 'resource manager' appears to be (for JDBC) a 'particular DBMS server that participates in distributed
     * transaction'. More accurately an instance of a connection to a database since commit/rollback is done at the DB connection level. A resource
     * adapter is the implementation below
     */

    /*
     * In the JDBC XA spec the 'middle tier server' is the application server. We assume that this module implements the pooling of connections since
     * it must also pass the XAResouce obtained when a connection is handed to an application to the transaction manager. IE JPoolingDataSource is not
     * used - the JConnectionPoolDataSource and JPoolied connections are managed for pooling by the app server.
     */

    /* Examples http://oradoc.photo.net/ora816/java.816/a81354/xadistr1.htm#1064452 */

    /*
     * Note that EJB componenents performing getConnection() may be using the same XAConnection/XAResource since it is a pooled connection
     */

    private int timeoutSeconds;

    final static int XA_START = 0;
    final static int XA_END = 1;
    final static int XA_PREPARE = 2;
    final static int XA_COMMIT = 3;
    final static int XA_ROLLBACK = 4;
    final static int XA_FORGET = 5;
    final static int XA_RECOVER = 6;
    final static int XA_PREPARE_EX = 7;
    final static int XA_ROLLBACK_EX = 8;
    final static int XA_FORGET_EX = 9;
    final static int XA_INIT = 10;

    private SQLServerConnection controlConnection;
    private SQLServerConnection con; // original connection

    private boolean serverInfoRetrieved;
    private String version, instanceName;
    private int ArchitectureMSSQL, ArchitectureOS;

    private static boolean xaInitDone;
    private static final Object xaInitLock;
    private String sResourceManagerId;
    private int enlistedTransactionCount;
    final private Logger xaLogger;
    static private final AtomicInteger baseResourceID = new AtomicInteger(0);	// Unique id generator for each instance (used for logging).
    private int tightlyCoupled = 0;
    private int isTransacrionTimeoutSet = 0;	// set to 1 if setTransactionTimeout() is called

    public static final int SSTRANSTIGHTLYCPLD = 0x8000;
    private SQLServerCallableStatement[] xaStatements = {null, null, null, null, null, null, null, null, null, null};
    private final String traceID;
    /**
     * Variable that shows how many times we attempt the recovery, e.g in case of MSDTC restart
     */
    private int recoveryAttempt = 0;
    static {
        xaInitLock = new Object();
    }

    public String toString() {
        return traceID;
    }

    /* L0 */ SQLServerXAResource(SQLServerConnection original,
            SQLServerConnection control,
            String loginfo) {
        traceID = " XAResourceID:" + nextResourceID();
        // Grab SQLServerXADataSource's static XA logger instance.
        xaLogger = SQLServerXADataSource.xaLogger;
        controlConnection = control;
        con = original;
        Properties p = original.activeConnectionProperties;
        if (p == null)
            sResourceManagerId = "";
        else {
            sResourceManagerId = p.getProperty(SQLServerDriverStringProperty.SERVER_NAME.toString()) + "."
                    + p.getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString()) + "."
                    + p.getProperty(SQLServerDriverIntProperty.PORT_NUMBER.toString());
        }
        if (xaLogger.isLoggable(Level.FINE))
            xaLogger.fine(toString() + " created by (" + loginfo + ")");

        // Information about the server, needed for XA timeout logic in the DLL.
        serverInfoRetrieved = false;
        version = "0";
        instanceName = "";
        ArchitectureMSSQL = 0;
        ArchitectureOS = 0;

    }

    private synchronized SQLServerCallableStatement getXACallableStatementHandle(int number) throws SQLServerException {
        assert number >= XA_START && number <= XA_FORGET_EX;
        assert number < xaStatements.length;
        if (null != xaStatements[number])
            return xaStatements[number];

        CallableStatement CS = null;

        switch (number) {
            case SQLServerXAResource.XA_START:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_start(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_END:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_end(?, ?, ?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_PREPARE:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_prepare(?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_COMMIT:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_commit(?, ?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_ROLLBACK:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_rollback(?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_FORGET:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_forget(?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_RECOVER:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_recover(?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_PREPARE_EX:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_prepare_ex(?, ?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_ROLLBACK_EX:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_rollback_ex(?, ?, ?, ?, ?, ?)}");
                break;
            case SQLServerXAResource.XA_FORGET_EX:
                CS = controlConnection.prepareCall("{call master..xp_sqljdbc_xa_forget_ex(?, ?, ?, ?, ?, ?)}");
                break;
            default:
                assert false : "Bad handle request:" + number;
                break;
        }

        xaStatements[number] = (SQLServerCallableStatement) CS;
        return xaStatements[number];
    }

    private synchronized void closeXAStatements() throws SQLServerException {
        for (int i = 0; i < xaStatements.length; i++)
            if (null != xaStatements[i]) {
                xaStatements[i].close();
                xaStatements[i] = null;
            }
    }

    final synchronized void close() throws SQLServerException {
        try {
            closeXAStatements();
        }
        catch (Exception e) {
            if (xaLogger.isLoggable(Level.WARNING))
                xaLogger.warning(toString() + "Closing exception ignored: " + e);
        }

        if (null != controlConnection)
            controlConnection.close();
    }

    // Returns displayable representation of XID flags for logging purposes.
    private String flagsDisplay(int flags) {
        // Handle default most common case first.
        // Note TMNOFLAGS is 0 so this means no other bits are set.
        if (TMNOFLAGS == flags)
            return "TMNOFLAGS";

        // Build displayable bitmask of rest of flags.
        StringBuilder sb = new StringBuilder(100);

        if (0 != (TMENDRSCAN & flags))
            sb.append("TMENDRSCAN");

        if (0 != (TMFAIL & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("TMFAIL");
        }
        if (0 != (TMJOIN & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("TMJOIN");
        }
        if (0 != (TMONEPHASE & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("TMONEPHASE");
        }
        if (0 != (TMRESUME & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("TMRESUME");
        }
        if (0 != (TMSTARTRSCAN & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("TMSTARTRSCAN");
        }
        if (0 != (TMSUCCESS & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("TMSUCCESS");
        }
        if (0 != (TMSUSPEND & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("TMSUSPEND");
        }

        if (0 != (SSTRANSTIGHTLYCPLD & flags)) {
            if (sb.length() > 0)
                sb.append("|");
            sb.append("SSTRANSTIGHTLYCPLD");
        }
        return sb.toString();
    }

    // Returns displayable representation of XID cookie for logging purposes.
    private String cookieDisplay(byte[] cookie) {
        return Util.byteToHexDisplayString(cookie);
    }

    // Returns displayable representation of XA type flag.
    private String typeDisplay(int type) {
        switch (type) {
            case XA_START:
                return "XA_START";
            case XA_END:
                return "XA_END";
            case XA_PREPARE:
                return "XA_PREPARE";
            case XA_COMMIT:
                return "XA_COMMIT";
            case XA_ROLLBACK:
                return "XA_ROLLBACK";
            case XA_FORGET:
                return "XA_FORGET";
            case XA_RECOVER:
                return "XA_RECOVER";
            default:
                return "UNKNOWN" + type;
        }

    }

    /* L0 */ private XAReturnValue DTC_XA_Interface(int nType,
            Xid xid,
            int xaFlags) throws XAException {

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(toString() + " Calling XA function for type:" + typeDisplay(nType) + " flags:" + flagsDisplay(xaFlags) + " xid:"
                    + XidImpl.xidDisplay(xid));

        int formatId = 0;
        byte gid[] = null;
        byte bid[] = null;
        if (xid != null) {
            formatId = xid.getFormatId();
            gid = xid.getGlobalTransactionId();
            bid = xid.getBranchQualifier();
        }

        String sContext = "DTC_XA_";
        int n = 1;
        int nStatus = 0;
        XAReturnValue returnStatus = new XAReturnValue();

        SQLServerCallableStatement cs = null;
        try {
            synchronized (this) {
                if (!xaInitDone) {  
                    try {
                        synchronized (xaInitLock) {
                            SQLServerCallableStatement initCS = null;

                            initCS = (SQLServerCallableStatement) controlConnection.prepareCall("{call master..xp_sqljdbc_xa_init_ex(?, ?,?)}");
                            initCS.registerOutParameter(1, Types.INTEGER); // Return status
                            initCS.registerOutParameter(2, Types.CHAR);    // Return error message
                            initCS.registerOutParameter(3, Types.CHAR);    // Return version number
                            try {
                                initCS.execute();
                            }
                            catch (SQLServerException eX) {
                                try {
                                    initCS.close();
                                    // Mapping between control connection and xaresource is 1:1
                                    controlConnection.close();
                                }
                                catch (SQLException e3) {
                                    // we really want to ignore this failue
                                    if (xaLogger.isLoggable(Level.FINER))
                                        xaLogger.finer(toString() + " Ignoring exception when closing failed execution. exception:" + e3);
                                }
                                if (xaLogger.isLoggable(Level.FINER))
                                    xaLogger.finer(toString() + " exception:" + eX);
                                throw eX;
                            } catch (SQLTimeoutException e4) {
                                if (xaLogger.isLoggable(Level.FINER))
                                    xaLogger.finer(toString() + " exception:" + e4);
                                throw new SQLServerException(e4.getMessage(), SQLState.STATEMENT_CANCELED, DriverError.NOT_SET, null);
			    }

                            // Check for error response from xp_sqljdbc_xa_init.
                            int initStatus = initCS.getInt(1);
                            String initErr = initCS.getString(2);
                            String versionNumberXADLL = initCS.getString(3);
                            if (xaLogger.isLoggable(Level.FINE))
                                xaLogger.fine(toString() + " Server XA DLL version:" + versionNumberXADLL);
                            initCS.close();
                            if (XA_OK != initStatus) {
                                assert null != initErr && initErr.length() > 1;
                                controlConnection.close();

                                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedToInitializeXA"));
                                Object[] msgArgs = {String.valueOf(initStatus), initErr};
                                XAException xex = new XAException(form.format(msgArgs));
                                xex.errorCode = initStatus;
                                if (xaLogger.isLoggable(Level.FINER))
                                    xaLogger.finer(toString() + " exception:" + xex);
                                throw xex;
                            }
                        }
                    }
                    catch (SQLServerException e1) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedToCreateXAConnection"));
                        Object[] msgArgs = {e1.getMessage()};
                        if (xaLogger.isLoggable(Level.FINER))
                            xaLogger.finer(toString() + " exception:" + form.format(msgArgs));
                        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
                    }
                    xaInitDone = true;
                }
            }

            switch (nType) {
                case XA_START:

                    if (!serverInfoRetrieved) {
                        Statement stmt = null;
                        try {
                            serverInfoRetrieved = true;
                            // data are converted to varchar as type variant returned by SERVERPROPERTY is not supported by driver
                            String query = "select convert(varchar(100), SERVERPROPERTY('Edition'))as edition, "
                                    + " convert(varchar(100), SERVERPROPERTY('InstanceName'))as instance,"
                                    + " convert(varchar(100), SERVERPROPERTY('ProductVersion')) as version,"
                                    + " SUBSTRING(@@VERSION, CHARINDEX('<', @@VERSION)+2, 2)";

                            stmt = controlConnection.createStatement();
                            ResultSet rs = stmt.executeQuery(query);
                            rs.next();

                            String edition = rs.getString(1);
                            ArchitectureMSSQL = ((null != edition) && (edition.contains("(64-bit)"))) ? 64 : 32;

                            // if InstanceName is null use the default instance without name (MSSQLSERVER)
                            instanceName = (rs.getString(2) == null) ? "MSSQLSERVER" : rs.getString(2);
                            version = rs.getString(3);
                            if (null == version) {
                                version = "0";
                            }
                            else if (-1 != version.indexOf('.')) {
                                version = version.substring(0, version.indexOf('.'));
                            }

                            // @@VERSION returns single nvarchar string with SQL version, architecture, build date, edition and OS version
                            // Version of the OS running MS SQL is retrieved as substring
                            ArchitectureOS = Integer.parseInt(rs.getString(4));

                            rs.close();
                        }
                        // Got caught in static analysis. Catch only the thrown exceptions, do not catch
                        // run time exceptions.
                        catch (Exception e) {
                            if (xaLogger.isLoggable(Level.WARNING))
                                xaLogger.warning(toString() + " Cannot retrieve server information: :" + e.getMessage());
                        }
                        finally {
                            if (null != stmt)
                                try {
                                    stmt.close();
                                }
                                catch (SQLException e) {
                                    if (xaLogger.isLoggable(Level.FINER))
                                        xaLogger.finer(toString());
                                }
                        }
                    }

                    sContext = "START:";
                    cs = getXACallableStatementHandle(XA_START);
                    cs.registerOutParameter(n++, Types.INTEGER);	// Return status
                    cs.registerOutParameter(n++, Types.CHAR);		// Return error message
                    cs.setBytes(n++, gid);							// Global XID
                    cs.setBytes(n++, bid);							// Branch ID
                    cs.setInt(n++, xaFlags);						// XA transaction flags
                    cs.registerOutParameter(n++, Types.BINARY);		// Returned OLE transaction cookie
                    cs.setInt(n++, timeoutSeconds);					// Transaction timeout in seconds.
                    cs.setInt(n++, formatId);						// Format ID
                    cs.registerOutParameter(n++, Types.CHAR);		// DLL Version number
                    cs.setInt(n++, Integer.parseInt(version));		// Version of SQL Server
                    cs.setInt(n++, instanceName.length());			// Length of SQL Server instance name
                    cs.setBytes(n++, instanceName.getBytes());		// SQL Server instance name
                    cs.setInt(n++, ArchitectureMSSQL);				// Architecture of SQL Server
                    cs.setInt(n++, ArchitectureOS);					// Architecture of OS running SQL Server
                    cs.setInt(n++, isTransacrionTimeoutSet);		// pass 1 if setTransactionTimeout() is called
                    cs.registerOutParameter(n++, Types.BINARY);		// Return UoW

                    break;

                case XA_END:
                    sContext = "END:";
                    cs = getXACallableStatementHandle(XA_END);
                    cs.registerOutParameter(n++, Types.INTEGER);
                    cs.registerOutParameter(n++, Types.CHAR);
                    cs.setBytes(n++, gid);
                    cs.setBytes(n++, bid);
                    cs.setInt(n++, xaFlags);
                    cs.setInt(n++, formatId);
                    cs.registerOutParameter(n++, Types.BINARY);		// Return UoW
                    break;

                case XA_PREPARE:
                    sContext = "PREPARE:";
                    if ((SSTRANSTIGHTLYCPLD & xaFlags) == SSTRANSTIGHTLYCPLD)
                        cs = getXACallableStatementHandle(XA_PREPARE_EX);
                    else
                        cs = getXACallableStatementHandle(XA_PREPARE);

                    cs.registerOutParameter(n++, Types.INTEGER);
                    cs.registerOutParameter(n++, Types.CHAR);
                    cs.setBytes(n++, gid);
                    cs.setBytes(n++, bid);
                    if ((SSTRANSTIGHTLYCPLD & xaFlags) == SSTRANSTIGHTLYCPLD)
                        cs.setInt(n++, xaFlags);					// XA transaction flags
                    cs.setInt(n++, formatId);						// Format ID n=5 for loosely coupled, n=6 for tightly coupled
                    break;

                case XA_COMMIT:
                    sContext = "COMMIT:";
                    cs = getXACallableStatementHandle(XA_COMMIT);
                    cs.registerOutParameter(n++, Types.INTEGER);
                    cs.registerOutParameter(n++, Types.CHAR);
                    cs.setBytes(n++, gid);
                    cs.setBytes(n++, bid);
                    cs.setInt(n++, xaFlags);
                    cs.setInt(n++, formatId);
                    break;

                case XA_ROLLBACK:
                    sContext = "ROLLBACK:";
                    if ((SSTRANSTIGHTLYCPLD & xaFlags) == SSTRANSTIGHTLYCPLD)
                        cs = getXACallableStatementHandle(XA_ROLLBACK_EX);
                    else
                        cs = getXACallableStatementHandle(XA_ROLLBACK);

                    cs.registerOutParameter(n++, Types.INTEGER);
                    cs.registerOutParameter(n++, Types.CHAR);
                    cs.setBytes(n++, gid);
                    cs.setBytes(n++, bid);
                    if ((SSTRANSTIGHTLYCPLD & xaFlags) == SSTRANSTIGHTLYCPLD)
                        cs.setInt(n++, xaFlags);					// XA transaction flags
                    cs.setInt(n++, formatId);						// Format ID n=5 for loosely coupled, n=6 for tightly coupled
                    break;

                case XA_FORGET:
                    sContext = "FORGET:";
                    if ((SSTRANSTIGHTLYCPLD & xaFlags) == SSTRANSTIGHTLYCPLD)
                        cs = getXACallableStatementHandle(XA_FORGET_EX);
                    else
                        cs = getXACallableStatementHandle(XA_FORGET);
                    cs.registerOutParameter(n++, Types.INTEGER);
                    cs.registerOutParameter(n++, Types.CHAR);
                    cs.setBytes(n++, gid);
                    cs.setBytes(n++, bid);
                    if ((SSTRANSTIGHTLYCPLD & xaFlags) == SSTRANSTIGHTLYCPLD)
                        cs.setInt(n++, xaFlags);					// XA transaction flags
                    cs.setInt(n++, formatId);						// Format ID n=5 for loosely coupled, n=6 for tightly coupled
                    break;

                case XA_RECOVER:
                    sContext = "RECOVER:";
                    cs = getXACallableStatementHandle(XA_RECOVER);
                    cs.registerOutParameter(n++, Types.INTEGER);
                    cs.registerOutParameter(n++, Types.CHAR);
                    cs.setInt(n++, xaFlags);
                    cs.registerOutParameter(n++, Types.BINARY);
                    // Format Id need not be sent for recover action
                    break;
                default:
                    assert false : "Unknown execution type:" + nType;
                    break;

            }

            /* execute the interface procedure */

            cs.execute();
            nStatus = cs.getInt(1);
            String sErr = cs.getString(2);
            if (nType == XA_START) {
                String versionNumberXADLL = cs.getString(9);
                if (xaLogger.isLoggable(Level.FINE)) {
                    xaLogger.fine(toString() + " Server XA DLL version:" + versionNumberXADLL);
                    if (null != cs.getString(16)) {
                        StringBuffer strBuf = new StringBuffer(cs.getString(16));
                        strBuf.insert(20, '-');
                        strBuf.insert(16, '-');
                        strBuf.insert(12, '-');
                        strBuf.insert(8, '-');
                        xaLogger.fine(toString() + " XID to UoW mapping for XA type:XA_START XID: " + XidImpl.xidDisplay(xid) + " UoW: "
                                + strBuf.toString());
                    }
                }
            }
            if (nType == XA_END) {
                if (xaLogger.isLoggable(Level.FINE)) {
                    if (null != cs.getString(7)) {
                        StringBuffer strBuf = new StringBuffer(cs.getString(7));
                        strBuf.insert(20, '-');
                        strBuf.insert(16, '-');
                        strBuf.insert(12, '-');
                        strBuf.insert(8, '-');
                        xaLogger.fine(
                                toString() + " XID to UoW mapping for XA type:XA_END XID: " + XidImpl.xidDisplay(xid) + " UoW: " + strBuf.toString());
                    }
                }
            }
            if (XA_RECOVER == nType && XA_OK != nStatus && recoveryAttempt < 1) {
                // if recover failed, attempt to start again - adding the variable to check to attempt only once otherwise throw exception that recovery fails
                // this is added since before this change, if we restart the MSDTC and attempt to do recovery, driver will throw exception
                //"The function RECOVER: failed. The status is: -3"
                recoveryAttempt++;
                DTC_XA_Interface(XA_START, xid, TMNOFLAGS);
                return DTC_XA_Interface(XA_RECOVER, xid, xaFlags);
            }
            // prepare and end can return XA_RDONLY
            // Think should we just check for nStatus to be greater than or equal to zero instead of this check
            if (((XA_RDONLY == nStatus) && (XA_END != nType && XA_PREPARE != nType)) || (XA_OK != nStatus && XA_RDONLY != nStatus)) {
                assert (null != sErr) && (sErr.length() > 1);
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedFunctionXA"));
                Object[] msgArgs = {sContext, String.valueOf(nStatus), sErr};
                XAException e = new XAException(form.format(msgArgs));
                e.errorCode = nStatus;
                // if the request is end make sure we delist from the DTC transaction on rm failure.
                if (nType == XA_END && (XAException.XAER_RMFAIL == nStatus)) {
                    try {
                        if (xaLogger.isLoggable(Level.FINER))
                            xaLogger.finer(toString() + " Begin un-enlist, enlisted count:" + enlistedTransactionCount);
                        con.JTAUnenlistConnection();
                        enlistedTransactionCount--;
                        if (xaLogger.isLoggable(Level.FINER))
                            xaLogger.finer(toString() + " End un-enlist, enlisted count:" + enlistedTransactionCount);
                    }
                    catch (SQLServerException e1) {
                        // ignore this message as the previous error message is more important.
                        if (xaLogger.isLoggable(Level.FINER))
                            xaLogger.finer(toString() + " Ignoring exception:" + e1);
                    }
                }
                throw e;
            }
            else {
                if (nType == XA_START) {
                    // A physical connection may not have been enlisted yet so always enlist.
                    byte transactionCookie[] = cs.getBytes(6);
                    if (transactionCookie == null) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_noTransactionCookie"));
                        Object[] msgArgs = {sContext};
                        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);

                    }
                    else {
                        try {
                            if (xaLogger.isLoggable(Level.FINER))
                                xaLogger.finer(toString() + " Begin enlisting, cookie:" + cookieDisplay(transactionCookie) + " enlisted count:"
                                        + enlistedTransactionCount);
                            con.JTAEnlistConnection(transactionCookie);
                            enlistedTransactionCount++;
                            if (xaLogger.isLoggable(Level.FINER))
                                xaLogger.finer(toString() + " End enlisting, cookie:" + cookieDisplay(transactionCookie) + " enlisted count:"
                                        + enlistedTransactionCount);
                        }
                        catch (SQLServerException e1) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedToEnlist"));
                            Object[] msgArgs = {e1.getMessage()};
                            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
                        }
                    }
                }
                if (nType == XA_END) {
                    try {
                        if (xaLogger.isLoggable(Level.FINER))
                            xaLogger.finer(toString() + " Begin un-enlist, enlisted count:" + enlistedTransactionCount);
                        con.JTAUnenlistConnection();
                        enlistedTransactionCount--;
                        if (xaLogger.isLoggable(Level.FINER))
                            xaLogger.finer(toString() + " End un-enlist, enlisted count:" + enlistedTransactionCount);
                    }
                    catch (SQLServerException e1) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedToUnEnlist"));
                        Object[] msgArgs = {e1.getMessage()};
                        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
                    }
                }
                if (nType == XA_RECOVER)

                {
                    try {
                        returnStatus.bData = cs.getBytes(4);
                    }
                    catch (SQLServerException e1) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_failedToReadRecoveryXIDs"));
                        Object[] msgArgs = {e1.getMessage()};
                        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
                    }
                }
            }
        }
        catch (SQLServerException | SQLTimeoutException ex) {
            if (xaLogger.isLoggable(Level.FINER))
                xaLogger.finer(toString() + " exception:" + ex);
            XAException e = new XAException(ex.toString());
            e.errorCode = XAException.XAER_RMERR;
            throw e;
        }

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(toString() + " Status:" + nStatus);

        returnStatus.nStatus = nStatus;
        return returnStatus;
    }

    /* L0 */ public void start(Xid xid,
            int flags) throws XAException {
        /*
         * Transaction mgr will use this resource in the global transaction. After this call the app server will call getConnection() to get a
         * connection to give the application
         * 
         * The xid holds the global transaction id + the transaction branch id. The getGlobalTransactionId should be the same for each call until the
         * transaction is committed
         */

        /*
         * XA API DOC : Start work on behalf of a transaction branch specified in xid If TMJOIN is specified, the start is for joining a transaction
         * previously seen by the resource manager. If TMRESUME is specified, the start is to resume a suspended transaction specified in the
         * parameter xid. If neither TMJOIN nor TMRESUME is specified and the transaction specified by xid has previously been seen by the resource
         * manager, the resource manager throws the XAException exception with XAER_DUPID error code.
         */

        // TMNOFLAGS indicates this is the first time this physical connection has seen the transaction.
        // EG if the physical connection has generated multiple connection handles only work on the first
        // of those will be prefixed by the transaction manager with a call to start with TMNOFLAGS

        tightlyCoupled = flags & SSTRANSTIGHTLYCPLD;
        DTC_XA_Interface(XA_START, xid, flags);
    }

    /* L0 */ public void end(Xid xid,
            int flags) throws XAException {
        // Called by the transaction mgr after the app closes the connection it was given from this physical
        // connection
        /*
         * Ends the work performed on behalf of a transaction branch. The resource manager disassociates the XA resource from the transaction branch
         * specified and let the transaction be completed. If TMSUSPEND is specified in flags, the transaction branch is temporarily suspended in
         * incomplete state. The transaction context is in suspened state and must be resumed via start with TMRESUME specified. If TMFAIL is
         * specified, the portion of work has failed. The resource manager may mark the transaction as rollback-only. If TMSUCCESS is specified, the
         * portion of work has completed successfully.
         */
        DTC_XA_Interface(XA_END, xid, flags | tightlyCoupled);
    }

    /* L0 */ public int prepare(Xid xid) throws XAException {
        /*
         * Ask the resource manager to prepare for a transaction commit of the transaction specified in xid. Parameters: xid - A global transaction
         * identifier Returns: A value indicating the resource manager's vote on the outcome of the transaction. The possible values are: XA_RDONLY or
         * XA_OK. If the resource manager wants to roll back the transaction, it should do so by raising an appropriate XAException in the prepare
         * method.
         */
        int nStatus = XA_OK;
        XAReturnValue r = DTC_XA_Interface(XA_PREPARE, xid, tightlyCoupled);
        nStatus = r.nStatus;

        return nStatus;
    }

    /* L0 */ public void commit(Xid xid,
            boolean onePhase) throws XAException {
        DTC_XA_Interface(XA_COMMIT, xid, ((onePhase) ? TMONEPHASE : TMNOFLAGS) | tightlyCoupled);
    }

    /* L0 */ public void rollback(Xid xid) throws XAException {
        DTC_XA_Interface(XA_ROLLBACK, xid, tightlyCoupled);
    }

    /* L0 */ public void forget(Xid xid) throws XAException {
        DTC_XA_Interface(XA_FORGET, xid, tightlyCoupled);
    }

    /* L0 */ public Xid[] recover(int flags) throws XAException {
        XAReturnValue r = DTC_XA_Interface(XA_RECOVER, null, flags | tightlyCoupled);
        int offset = 0;
        ArrayList<XidImpl> al = new ArrayList<>();

        // If no XID's found, return zero length XID array (don't return null).
        //
        // Per Java 1.4.2 spec:
        //
        // The resource manager returns zero or more XIDs of the transaction branches
        // that are currently in a prepared or heuristically completed state. If an
        // error occurs during the operation, the resource manager should throw the
        // appropriate XAException.

        if (null == r.bData)
            return new XidImpl[0];

        while (offset < r.bData.length) {
            int power = 1;
            int formatId = 0;
            for (int i = 0; i < 4; i++) {
                int x = (r.bData[offset + i] & 0x00FF);
                x = x * power;
                formatId += x;
                power = power * 256;
            }
            offset += 4;
            int gid_len = (r.bData[offset++] & 0x00FF);
            int bid_len = (r.bData[offset++] & 0x00FF);
            byte gid[] = new byte[gid_len];
            byte bid[] = new byte[bid_len];
            System.arraycopy(r.bData, offset, gid, 0, gid_len);
            offset += gid_len;
            System.arraycopy(r.bData, offset, bid, 0, bid_len);
            offset += bid_len;
            XidImpl xid = new XidImpl(formatId, gid, bid);
            al.add(xid);
        }
        XidImpl xids[] = new XidImpl[al.size()];
        for (int i = 0; i < al.size(); i++) {
            xids[i] = al.get(i);
            if (xaLogger.isLoggable(Level.FINER))
                xaLogger.finer(toString() + xids[i].toString());
        }
        return xids;
    }

    /* L0 */ public boolean isSameRM(XAResource xares) throws XAException {
        // A Resource Manager (RM) is an instance of a connection to a DB

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(toString() + " xares:" + xares);

        // Change to return true if its the same database physical connection
        if (!(xares instanceof SQLServerXAResource))
            return false;
        SQLServerXAResource jxa = (SQLServerXAResource) xares;
        return jxa.sResourceManagerId.equals(this.sResourceManagerId);
    }

    /* L0 */ public boolean setTransactionTimeout(int seconds) throws XAException {

        isTransacrionTimeoutSet = 1;
        timeoutSeconds = seconds;
        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(toString() + " TransactionTimeout:" + seconds);
        return true;
    }

    /* L0 */ public int getTransactionTimeout() throws XAException {
        return timeoutSeconds;
    }

    // Returns unique id for each PooledConnection instance.
    private static int nextResourceID() {
        return baseResourceID.incrementAndGet();
    }

}
