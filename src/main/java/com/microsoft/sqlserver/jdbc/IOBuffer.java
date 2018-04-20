/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.nio.Buffer;

final class TDS {
    // TDS protocol versions
    static final int VER_DENALI = 0x74000004; // TDS 7.4
    static final int VER_KATMAI = 0x730B0003; // TDS 7.3B(includes null bit compression)
    static final int VER_YUKON = 0x72090002; // TDS 7.2
    static final int VER_UNKNOWN = 0x00000000; // Unknown/uninitialized

    static final int TDS_RET_STAT = 0x79;
    static final int TDS_COLMETADATA = 0x81;
    static final int TDS_TABNAME = 0xA4;
    static final int TDS_COLINFO = 0xA5;
    static final int TDS_ORDER = 0xA9;
    static final int TDS_ERR = 0xAA;
    static final int TDS_MSG = 0xAB;
    static final int TDS_RETURN_VALUE = 0xAC;
    static final int TDS_LOGIN_ACK = 0xAD;
    static final int TDS_FEATURE_EXTENSION_ACK = 0xAE;
    static final int TDS_ROW = 0xD1;
    static final int TDS_NBCROW = 0xD2;
    static final int TDS_ENV_CHG = 0xE3;
    static final int TDS_SSPI = 0xED;
    static final int TDS_DONE = 0xFD;
    static final int TDS_DONEPROC = 0xFE;
    static final int TDS_DONEINPROC = 0xFF;
    static final int TDS_FEDAUTHINFO = 0xEE;

    // FedAuth
    static final int TDS_FEATURE_EXT_FEDAUTH = 0x02;
    static final int TDS_FEDAUTH_LIBRARY_SECURITYTOKEN = 0x01;
    static final int TDS_FEDAUTH_LIBRARY_ADAL = 0x02;
    static final int TDS_FEDAUTH_LIBRARY_RESERVED = 0x7F;
    static final byte ADALWORKFLOW_ACTIVEDIRECTORYPASSWORD = 0x01;
    static final byte ADALWORKFLOW_ACTIVEDIRECTORYINTEGRATED = 0x02;
    static final byte FEDAUTH_INFO_ID_STSURL = 0x01; // FedAuthInfoData is token endpoint URL from which to acquire fed auth token
    static final byte FEDAUTH_INFO_ID_SPN = 0x02; // FedAuthInfoData is the SPN to use for acquiring fed auth token

    // AE constants
    static final int TDS_FEATURE_EXT_AE = 0x04;
    static final int MAX_SUPPORTED_TCE_VERSION = 0x01; // max version
    static final int CUSTOM_CIPHER_ALGORITHM_ID = 0; // max version
    static final int AES_256_CBC = 1;
    static final int AEAD_AES_256_CBC_HMAC_SHA256 = 2;
    static final int AE_METADATA = 0x08;

    static final int TDS_TVP = 0xF3;
    static final int TVP_ROW = 0x01;
    static final int TVP_NULL_TOKEN = 0xFFFF;
    static final int TVP_STATUS_DEFAULT = 0x02;

    static final int TVP_ORDER_UNIQUE_TOKEN = 0x10;
    // TVP_ORDER_UNIQUE_TOKEN flags
    static final byte TVP_ORDERASC_FLAG = 0x1;
    static final byte TVP_ORDERDESC_FLAG = 0x2;
    static final byte TVP_UNIQUE_FLAG = 0x4;

    // TVP flags, may be used in other places
    static final int FLAG_NULLABLE = 0x01;
    static final int FLAG_TVP_DEFAULT_COLUMN = 0x200;

    static final int FEATURE_EXT_TERMINATOR = -1;
    
    // Sql_variant length
    static final int SQL_VARIANT_LENGTH = 8009;

    static final String getTokenName(int tdsTokenType) {
        switch (tdsTokenType) {
            case TDS_RET_STAT:
                return "TDS_RET_STAT (0x79)";
            case TDS_COLMETADATA:
                return "TDS_COLMETADATA (0x81)";
            case TDS_TABNAME:
                return "TDS_TABNAME (0xA4)";
            case TDS_COLINFO:
                return "TDS_COLINFO (0xA5)";
            case TDS_ORDER:
                return "TDS_ORDER (0xA9)";
            case TDS_ERR:
                return "TDS_ERR (0xAA)";
            case TDS_MSG:
                return "TDS_MSG (0xAB)";
            case TDS_RETURN_VALUE:
                return "TDS_RETURN_VALUE (0xAC)";
            case TDS_LOGIN_ACK:
                return "TDS_LOGIN_ACK (0xAD)";
            case TDS_FEATURE_EXTENSION_ACK:
                return "TDS_FEATURE_EXTENSION_ACK (0xAE)";
            case TDS_ROW:
                return "TDS_ROW (0xD1)";
            case TDS_NBCROW:
                return "TDS_NBCROW (0xD2)";
            case TDS_ENV_CHG:
                return "TDS_ENV_CHG (0xE3)";
            case TDS_SSPI:
                return "TDS_SSPI (0xED)";
            case TDS_DONE:
                return "TDS_DONE (0xFD)";
            case TDS_DONEPROC:
                return "TDS_DONEPROC (0xFE)";
            case TDS_DONEINPROC:
                return "TDS_DONEINPROC (0xFF)";
            case TDS_FEDAUTHINFO:
                return "TDS_FEDAUTHINFO (0xEE)";
            default:
                return "unknown token (0x" + Integer.toHexString(tdsTokenType).toUpperCase() + ")";
        }
    }

    // RPC ProcIDs for use with RPCRequest (PKT_RPC) calls
    static final short PROCID_SP_CURSOR = 1;
    static final short PROCID_SP_CURSOROPEN = 2;
    static final short PROCID_SP_CURSORPREPARE = 3;
    static final short PROCID_SP_CURSOREXECUTE = 4;
    static final short PROCID_SP_CURSORPREPEXEC = 5;
    static final short PROCID_SP_CURSORUNPREPARE = 6;
    static final short PROCID_SP_CURSORFETCH = 7;
    static final short PROCID_SP_CURSOROPTION = 8;
    static final short PROCID_SP_CURSORCLOSE = 9;
    static final short PROCID_SP_EXECUTESQL = 10;
    static final short PROCID_SP_PREPARE = 11;
    static final short PROCID_SP_EXECUTE = 12;
    static final short PROCID_SP_PREPEXEC = 13;
    static final short PROCID_SP_PREPEXECRPC = 14;
    static final short PROCID_SP_UNPREPARE = 15;

    // Constants for use with cursor RPCs
    static final short SP_CURSOR_OP_UPDATE = 1;
    static final short SP_CURSOR_OP_DELETE = 2;
    static final short SP_CURSOR_OP_INSERT = 4;
    static final short SP_CURSOR_OP_REFRESH = 8;
    static final short SP_CURSOR_OP_LOCK = 16;
    static final short SP_CURSOR_OP_SETPOSITION = 32;
    static final short SP_CURSOR_OP_ABSOLUTE = 64;

    // Constants for server-cursored result sets.
    // See the Engine Cursors Functional Specification for details.
    static final int FETCH_FIRST = 1;
    static final int FETCH_NEXT = 2;
    static final int FETCH_PREV = 4;
    static final int FETCH_LAST = 8;
    static final int FETCH_ABSOLUTE = 16;
    static final int FETCH_RELATIVE = 32;
    static final int FETCH_REFRESH = 128;
    static final int FETCH_INFO = 256;
    static final int FETCH_PREV_NOADJUST = 512;
    static final byte RPC_OPTION_NO_METADATA = (byte) 0x02;

    // Transaction manager request types
    static final short TM_GET_DTC_ADDRESS = 0;
    static final short TM_PROPAGATE_XACT = 1;
    static final short TM_BEGIN_XACT = 5;
    static final short TM_PROMOTE_PROMOTABLE_XACT = 6;
    static final short TM_COMMIT_XACT = 7;
    static final short TM_ROLLBACK_XACT = 8;
    static final short TM_SAVE_XACT = 9;

    static final byte PKT_QUERY = 1;
    static final byte PKT_RPC = 3;
    static final byte PKT_REPLY = 4;
    static final byte PKT_CANCEL_REQ = 6;
    static final byte PKT_BULK = 7;
    static final byte PKT_DTC = 14;
    static final byte PKT_LOGON70 = 16; // 0x10
    static final byte PKT_SSPI = 17;
    static final byte PKT_PRELOGIN = 18; // 0x12
    static final byte PKT_FEDAUTH_TOKEN_MESSAGE = 8;	// Authentication token for federated authentication

    static final byte STATUS_NORMAL = 0x00;
    static final byte STATUS_BIT_EOM = 0x01;
    static final byte STATUS_BIT_ATTENTION = 0x02;// this is called ignore bit in TDS spec
    static final byte STATUS_BIT_RESET_CONN = 0x08;

    // Various TDS packet size constants
    static final int INVALID_PACKET_SIZE = -1;
    static final int INITIAL_PACKET_SIZE = 4096;
    static final int MIN_PACKET_SIZE = 512;
    static final int MAX_PACKET_SIZE = 32767;
    static final int DEFAULT_PACKET_SIZE = 8000;
    static final int SERVER_PACKET_SIZE = 0; // Accept server's configured packet size

    // TDS packet header size and offsets
    static final int PACKET_HEADER_SIZE = 8;
    static final int PACKET_HEADER_MESSAGE_TYPE = 0;
    static final int PACKET_HEADER_MESSAGE_STATUS = 1;
    static final int PACKET_HEADER_MESSAGE_LENGTH = 2;
    static final int PACKET_HEADER_SPID = 4;
    static final int PACKET_HEADER_SEQUENCE_NUM = 6;
    static final int PACKET_HEADER_WINDOW = 7; // Reserved/Not used

    // MARS header length:
    // 2 byte header type
    // 8 byte transaction descriptor
    // 4 byte outstanding request count
    static final int MARS_HEADER_LENGTH = 18; // 2 byte header type, 8 byte transaction descriptor,
    static final int TRACE_HEADER_LENGTH = 26; // header length (4) + header type (2) + guid (16) + Sequence number size (4)

    static final short HEADERTYPE_TRACE = 3;  // trace header type

    // Message header length
    static final int MESSAGE_HEADER_LENGTH = MARS_HEADER_LENGTH + 4; // length includes message header itself

    static final byte B_PRELOGIN_OPTION_VERSION = 0x00;
    static final byte B_PRELOGIN_OPTION_ENCRYPTION = 0x01;
    static final byte B_PRELOGIN_OPTION_INSTOPT = 0x02;
    static final byte B_PRELOGIN_OPTION_THREADID = 0x03;
    static final byte B_PRELOGIN_OPTION_MARS = 0x04;
    static final byte B_PRELOGIN_OPTION_TRACEID = 0x05;
    static final byte B_PRELOGIN_OPTION_FEDAUTHREQUIRED = 0x06;
    static final byte B_PRELOGIN_OPTION_TERMINATOR = (byte) 0xFF;

    // Login option byte 1
    static final byte LOGIN_OPTION1_ORDER_X86 = 0x00;
    static final byte LOGIN_OPTION1_ORDER_6800 = 0x01;
    static final byte LOGIN_OPTION1_CHARSET_ASCII = 0x00;
    static final byte LOGIN_OPTION1_CHARSET_EBCDIC = 0x02;
    static final byte LOGIN_OPTION1_FLOAT_IEEE_754 = 0x00;
    static final byte LOGIN_OPTION1_FLOAT_VAX = 0x04;
    static final byte LOGIN_OPTION1_FLOAT_ND5000 = 0x08;
    static final byte LOGIN_OPTION1_DUMPLOAD_ON = 0x00;
    static final byte LOGIN_OPTION1_DUMPLOAD_OFF = 0x10;
    static final byte LOGIN_OPTION1_USE_DB_ON = 0x00;
    static final byte LOGIN_OPTION1_USE_DB_OFF = 0x20;
    static final byte LOGIN_OPTION1_INIT_DB_WARN = 0x00;
    static final byte LOGIN_OPTION1_INIT_DB_FATAL = 0x40;
    static final byte LOGIN_OPTION1_SET_LANG_OFF = 0x00;
    static final byte LOGIN_OPTION1_SET_LANG_ON = (byte) 0x80;

    // Login option byte 2
    static final byte LOGIN_OPTION2_INIT_LANG_WARN = 0x00;
    static final byte LOGIN_OPTION2_INIT_LANG_FATAL = 0x01;
    static final byte LOGIN_OPTION2_ODBC_OFF = 0x00;
    static final byte LOGIN_OPTION2_ODBC_ON = 0x02;
    static final byte LOGIN_OPTION2_TRAN_BOUNDARY_OFF = 0x00;
    static final byte LOGIN_OPTION2_TRAN_BOUNDARY_ON = 0x04;
    static final byte LOGIN_OPTION2_CACHE_CONNECTION_OFF = 0x00;
    static final byte LOGIN_OPTION2_CACHE_CONNECTION_ON = 0x08;
    static final byte LOGIN_OPTION2_USER_NORMAL = 0x00;
    static final byte LOGIN_OPTION2_USER_SERVER = 0x10;
    static final byte LOGIN_OPTION2_USER_REMUSER = 0x20;
    static final byte LOGIN_OPTION2_USER_SQLREPL = 0x30;
    static final byte LOGIN_OPTION2_INTEGRATED_SECURITY_OFF = 0x00;
    static final byte LOGIN_OPTION2_INTEGRATED_SECURITY_ON = (byte) 0x80;

    // Login option byte 3
    static final byte LOGIN_OPTION3_DEFAULT = 0x00;
    static final byte LOGIN_OPTION3_CHANGE_PASSWORD = 0x01;
    static final byte LOGIN_OPTION3_SEND_YUKON_BINARY_XML = 0x02;
    static final byte LOGIN_OPTION3_USER_INSTANCE = 0x04;
    static final byte LOGIN_OPTION3_UNKNOWN_COLLATION_HANDLING = 0x08;
    static final byte LOGIN_OPTION3_FEATURE_EXTENSION = 0x10;

    // Login type flag (bits 5 - 7 reserved for future use)
    static final byte LOGIN_SQLTYPE_DEFAULT = 0x00;
    static final byte LOGIN_SQLTYPE_TSQL = 0x01;
    static final byte LOGIN_SQLTYPE_ANSI_V1 = 0x02;
    static final byte LOGIN_SQLTYPE_ANSI89_L1 = 0x03;
    static final byte LOGIN_SQLTYPE_ANSI89_L2 = 0x04;
    static final byte LOGIN_SQLTYPE_ANSI89_IEF = 0x05;
    static final byte LOGIN_SQLTYPE_ANSI89_ENTRY = 0x06;
    static final byte LOGIN_SQLTYPE_ANSI89_TRANS = 0x07;
    static final byte LOGIN_SQLTYPE_ANSI89_INTER = 0x08;
    static final byte LOGIN_SQLTYPE_ANSI89_FULL = 0x09;

    static final byte LOGIN_OLEDB_OFF = 0x00;
    static final byte LOGIN_OLEDB_ON = 0x10;

    static final byte LOGIN_READ_ONLY_INTENT = 0x20;
    static final byte LOGIN_READ_WRITE_INTENT = 0x00;

    static final byte ENCRYPT_OFF = 0x00;
    static final byte ENCRYPT_ON = 0x01;
    static final byte ENCRYPT_NOT_SUP = 0x02;
    static final byte ENCRYPT_REQ = 0x03;
    static final byte ENCRYPT_INVALID = (byte) 0xFF;

    static final String getEncryptionLevel(int level) {
        switch (level) {
            case ENCRYPT_OFF:
                return "OFF";
            case ENCRYPT_ON:
                return "ON";
            case ENCRYPT_NOT_SUP:
                return "NOT SUPPORTED";
            case ENCRYPT_REQ:
                return "REQUIRED";
            default:
                return "unknown encryption level (0x" + Integer.toHexString(level).toUpperCase() + ")";
        }
    }

    // Prelogin packet length, including the tds header,
    // version, encrpytion, and traceid data sessions.
    // For detailed info, please check the definition of
    // preloginRequest in Prelogin function.
    static final byte B_PRELOGIN_MESSAGE_LENGTH = 67;
    static final byte B_PRELOGIN_MESSAGE_LENGTH_WITH_FEDAUTH = 73;

    // Scroll options and concurrency options lifted out
    // of the the Yukon cursors spec for sp_cursoropen.
    final static int SCROLLOPT_KEYSET = 1;
    final static int SCROLLOPT_DYNAMIC = 2;
    final static int SCROLLOPT_FORWARD_ONLY = 4;
    final static int SCROLLOPT_STATIC = 8;
    final static int SCROLLOPT_FAST_FORWARD = 16;

    final static int SCROLLOPT_PARAMETERIZED_STMT = 4096;
    final static int SCROLLOPT_AUTO_FETCH = 8192;
    final static int SCROLLOPT_AUTO_CLOSE = 16384;

    final static int CCOPT_READ_ONLY = 1;
    final static int CCOPT_SCROLL_LOCKS = 2;
    final static int CCOPT_OPTIMISTIC_CC = 4;
    final static int CCOPT_OPTIMISTIC_CCVAL = 8;
    final static int CCOPT_ALLOW_DIRECT = 8192;
    final static int CCOPT_UPDT_IN_PLACE = 16384;

    // Result set rows include an extra, "hidden" ROWSTAT column which indicates
    // the overall success or failure of the row fetch operation. With a keyset
    // cursor, the value in the ROWSTAT column indicates whether the row has been
    // deleted from the database.
    static final int ROWSTAT_FETCH_SUCCEEDED = 1;
    static final int ROWSTAT_FETCH_MISSING = 2;

    // ColumnInfo status
    final static int COLINFO_STATUS_EXPRESSION = 0x04;
    final static int COLINFO_STATUS_KEY = 0x08;
    final static int COLINFO_STATUS_HIDDEN = 0x10;
    final static int COLINFO_STATUS_DIFFERENT_NAME = 0x20;

    final static int MAX_FRACTIONAL_SECONDS_SCALE = 7;

    final static Timestamp MAX_TIMESTAMP = Timestamp.valueOf("2079-06-06 23:59:59");
    final static Timestamp MIN_TIMESTAMP = Timestamp.valueOf("1900-01-01 00:00:00");

    static int nanosSinceMidnightLength(int scale) {
        final int[] scaledLengths = {3, 3, 3, 4, 4, 5, 5, 5};
        assert scale >= 0;
        assert scale <= MAX_FRACTIONAL_SECONDS_SCALE;
        return scaledLengths[scale];
    }

    final static int DAYS_INTO_CE_LENGTH = 3;
    final static int MINUTES_OFFSET_LENGTH = 2;

    // Number of days in a "normal" (non-leap) year according to SQL Server.
    final static int DAYS_PER_YEAR = 365;

    final static int BASE_YEAR_1900 = 1900;
    final static int BASE_YEAR_1970 = 1970;
    final static String BASE_DATE_1970 = "1970-01-01";

    static int timeValueLength(int scale) {
        return nanosSinceMidnightLength(scale);
    }

    static int datetime2ValueLength(int scale) {
        return DAYS_INTO_CE_LENGTH + nanosSinceMidnightLength(scale);
    }

    static int datetimeoffsetValueLength(int scale) {
        return DAYS_INTO_CE_LENGTH + MINUTES_OFFSET_LENGTH + nanosSinceMidnightLength(scale);
    }

    // TDS is just a namespace - it can't be instantiated.
    private TDS() {
    }
}

class Nanos {
    static final int PER_SECOND = 1000000000;
    static final int PER_MAX_SCALE_INTERVAL = PER_SECOND / (int) Math.pow(10, TDS.MAX_FRACTIONAL_SECONDS_SCALE);
    static final int PER_MILLISECOND = PER_SECOND / 1000;
    static final long PER_DAY = 24 * 60 * 60 * (long) PER_SECOND;

    private Nanos() {
    }
}

// Constants relating to the historically accepted Julian-Gregorian calendar cutover date (October 15, 1582).
//
// Used in processing SQL Server temporal data types whose date component may precede that date.
//
// Scoping these constants to a class defers their initialization to first use.
class GregorianChange {
    // Cutover date for a pure Gregorian calendar - that is, a proleptic Gregorian calendar with
    // Gregorian leap year behavior throughout its entire range. This is the cutover date is used
    // with temporal server values, which are represented in terms of number of days relative to a
    // base date.
    static final java.util.Date PURE_CHANGE_DATE = new java.util.Date(Long.MIN_VALUE);

    // The standard Julian to Gregorian cutover date (October 15, 1582) that the JDBC temporal
    // classes (Time, Date, Timestamp) assume when converting to and from their UTC milliseconds
    // representations.
    static final java.util.Date STANDARD_CHANGE_DATE = (new GregorianCalendar(Locale.US)).getGregorianChange();

    // A hint as to the number of days since 1/1/0001, past which we do not need to
    // not rationalize the difference between SQL Server behavior (pure Gregorian)
    // and Java behavior (standard Gregorian).
    //
    // Not having to rationalize the difference has a substantial (measured) performance benefit
    // for temporal getters.
    //
    // The hint does not need to be exact, as long as it's later than the actual change date.
    static final int DAYS_SINCE_BASE_DATE_HINT = DDC.daysSinceBaseDate(1583, 1, 1);

    // Extra days that need to added to a pure gregorian date, post the gergorian
    // cut over date, to match the default julian-gregorain calendar date of java.
    static final int EXTRA_DAYS_TO_BE_ADDED;

    static {
        // This issue refers to the following bugs in java(same issue).
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7109480
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6459836
        // The issue is fixed in JRE 1.7
        // and exists in all the older versions.
        // Due to the above bug, in older JVM versions(1.6 and before),
        // the date calculation is incorrect at the Gregorian cut over date.
        // i.e. the next date after Oct 4th 1582 is Oct 17th 1582, where as
        // it should have been Oct 15th 1582.
        // We intentionally do not make a check based on JRE version.
        // If we do so, our code would break if the bug is fixed in a later update
        // to an older JRE. So, we check for the existence of the bug instead.

        GregorianCalendar cal = new GregorianCalendar(Locale.US);
        cal.clear();
        cal.set(1, Calendar.FEBRUARY, 577738, 0, 0, 0);// 577738 = 1+577737(no of days since epoch that brings us to oct 15th 1582)
        if (cal.get(Calendar.DAY_OF_MONTH) == 15) {
            // If the date calculation is correct(the above bug is fixed),
            // post the default gregorian cut over date, the pure gregorian date
            // falls short by two days for all dates compared to julian-gregorian date.
            // so, we add two extra days for functional correctness.
            // Note: other ways, in which this issue can be fixed instead of
            // trying to detect the JVM bug is
            // a) use unoptimized code path in the function convertTemporalToObject
            // b) use cal.add api instead of cal.set api in the current optimized code path
            // In both the above approaches, the code is about 6-8 times slower,
            // resulting in an overall perf regression of about (10-30)% for perf test cases
            EXTRA_DAYS_TO_BE_ADDED = 2;
        }
        else
            EXTRA_DAYS_TO_BE_ADDED = 0;
    }

    private GregorianChange() {
    }
}

final class UTC {

    // UTC/GMT time zone singleton.
    static final TimeZone timeZone = new SimpleTimeZone(0, "UTC");

    private UTC() {
    }
}

final class TDSChannel {
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Channel");

    final Logger getLogger() {
        return logger;
    }

    private final String traceID;

    final public String toString() {
        return traceID;
    }

    private final SQLServerConnection con;

    private final TDSWriter tdsWriter;

    final TDSWriter getWriter() {
        return tdsWriter;
    }

    final TDSReader getReader(TDSCommand command) {
        return new TDSReader(this, con, command);
    }

    // Socket for raw TCP/IP communications with SQL Server
    private Socket tcpSocket;

    // Socket for SSL-encrypted communications with SQL Server
    private SSLSocket sslSocket;

    // Socket providing the communications interface to the driver.
    // For SSL-encrypted connections, this is the SSLSocket wrapped
    // around the TCP socket. For unencrypted connections, it is
    // just the TCP socket itself.
    private Socket channelSocket;

    // Implementation of a Socket proxy that can switch from TDS-wrapped I/O
    // (using the TDSChannel itself) during SSL handshake to raw I/O over
    // the TCP/IP socket.
    ProxySocket proxySocket = null;

    // I/O streams for raw TCP/IP communications with SQL Server
    private InputStream tcpInputStream;
    private OutputStream tcpOutputStream;

    // I/O streams providing the communications interface to the driver.
    // For SSL-encrypted connections, these are streams obtained from
    // the SSL socket above. They wrap the underlying TCP streams.
    // For unencrypted connections, they are just the TCP streams themselves.
    private InputStream inputStream;
    private OutputStream outputStream;

    /** TDS packet payload logger */
    private static Logger packetLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.DATA");
    private final boolean isLoggingPackets = packetLogger.isLoggable(Level.FINEST);

    final boolean isLoggingPackets() {
        return isLoggingPackets;
    }

    // Number of TDS messages sent to and received from the server
    int numMsgsSent = 0;
    int numMsgsRcvd = 0;

    // Last SPID received from the server. Used for logging and to tag subsequent outgoing
    // packets to facilitate diagnosing problems from the server side.
    private int spid = 0;

    void setSPID(int spid) {
        this.spid = spid;
    }

    int getSPID() {
        return spid;
    }

    void resetPooledConnection() {
        tdsWriter.resetPooledConnection();
    }

    TDSChannel(SQLServerConnection con) {
        this.con = con;
        traceID = "TDSChannel (" + con.toString() + ")";
        this.tcpSocket = null;
        this.sslSocket = null;
        this.channelSocket = null;
        this.tcpInputStream = null;
        this.tcpOutputStream = null;
        this.inputStream = null;
        this.outputStream = null;
        this.tdsWriter = new TDSWriter(this, con);
    }

    /**
     * Opens the physical communications channel (TCP/IP socket and I/O streams) to the SQL Server.
     */
    final void open(String host,
            int port,
            int timeoutMillis,
            boolean useParallel,
            boolean useTnir,
            boolean isTnirFirstAttempt,
            int timeoutMillisForFullTimeout) throws SQLServerException {
        if (logger.isLoggable(Level.FINER))
            logger.finer(this.toString() + ": Opening TCP socket...");

        SocketFinder socketFinder = new SocketFinder(traceID, con);
        channelSocket = tcpSocket = socketFinder.findSocket(host, port, timeoutMillis, useParallel, useTnir, isTnirFirstAttempt,
                timeoutMillisForFullTimeout);

        try {

            // Set socket options
            tcpSocket.setTcpNoDelay(true);
            tcpSocket.setKeepAlive(true);

            // set SO_TIMEOUT
            int socketTimeout = con.getSocketTimeoutMilliseconds();
            tcpSocket.setSoTimeout(socketTimeout);

            inputStream = tcpInputStream = tcpSocket.getInputStream();
            outputStream = tcpOutputStream = tcpSocket.getOutputStream();
        }
        catch (IOException ex) {
            SQLServerException.ConvertConnectExceptionToSQLServerException(host, port, con, ex);
        }
    }

    /**
     * Disables SSL on this TDS channel.
     */
    void disableSSL() {
        if (logger.isLoggable(Level.FINER))
            logger.finer(toString() + " Disabling SSL...");

        /*
         * The mission: To close the SSLSocket and release everything that it is holding onto other than the TCP/IP socket and streams.
         *
         * The challenge: Simply closing the SSLSocket tries to do additional, unnecessary shutdown I/O over the TCP/IP streams that are bound to the
         * socket proxy, resulting in a not responding and confusing SQL Server.
         *
         * Solution: Rewire the ProxySocket's input and output streams (one more time) to closed streams. SSLSocket sees that the streams are already
         * closed and does not attempt to do any further I/O on them before closing itself.
         */

        // Create a couple of cheap closed streams
        InputStream is = new ByteArrayInputStream(new byte[0]);
        try {
            is.close();
        }
        catch (IOException e) {
            // No reason to expect a brand new ByteArrayInputStream not to close,
            // but just in case...
            logger.fine("Ignored error closing InputStream: " + e.getMessage());
        }

        OutputStream os = new ByteArrayOutputStream();
        try {
            os.close();
        }
        catch (IOException e) {
            // No reason to expect a brand new ByteArrayOutputStream not to close,
            // but just in case...
            logger.fine("Ignored error closing OutputStream: " + e.getMessage());
        }

        // Rewire the proxy socket to the closed streams
        if (logger.isLoggable(Level.FINEST))
            logger.finest(toString() + " Rewiring proxy streams for SSL socket close");
        proxySocket.setStreams(is, os);

        // Now close the SSL socket. It will see that the proxy socket's streams
        // are closed and not try to do any further I/O over them.
        try {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Closing SSL socket");

            sslSocket.close();
        }
        catch (IOException e) {
            // Don't care if we can't close the SSL socket. We're done with it anyway.
            logger.fine("Ignored error closing SSLSocket: " + e.getMessage());
        }

        // Do not close the proxy socket. Doing so would close our TCP socket
        // to which the proxy socket is bound. Instead, just null out the reference
        // to free up the few resources it holds onto.
        proxySocket = null;

        // Finally, with all of the SSL support out of the way, put the TDSChannel
        // back to using the TCP/IP socket and streams directly.
        inputStream = tcpInputStream;
        outputStream = tcpOutputStream;
        channelSocket = tcpSocket;
        sslSocket = null;

        if (logger.isLoggable(Level.FINER))
            logger.finer(toString() + " SSL disabled");
    }

    /**
     * Used during SSL handshake, this class implements an InputStream that reads SSL handshake response data (framed in TDS messages) from the TDS
     * channel.
     */
    private class SSLHandshakeInputStream extends InputStream {
        private final TDSReader tdsReader;
        private final SSLHandshakeOutputStream sslHandshakeOutputStream;

        private final Logger logger;
        private final String logContext;

        SSLHandshakeInputStream(TDSChannel tdsChannel,
                SSLHandshakeOutputStream sslHandshakeOutputStream) {
            this.tdsReader = tdsChannel.getReader(null);
            this.sslHandshakeOutputStream = sslHandshakeOutputStream;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (SSLHandshakeInputStream):";
        }

        /**
         * If there is no handshake response data available to be read from existing packets then this method ensures that the SSL handshake output
         * stream has been flushed to the server, and reads another packet (starting the next TDS response message).
         *
         * Note that simply using TDSReader.ensurePayload isn't sufficient as it does not automatically start the new response message.
         */
        private void ensureSSLPayload() throws IOException {
            if (0 == tdsReader.available()) {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext + " No handshake response bytes available. Flushing SSL handshake output stream.");

                try {
                    sslHandshakeOutputStream.endMessage();
                }
                catch (SQLServerException e) {
                    logger.finer(logContext + " Ending TDS message threw exception:" + e.getMessage());
                    throw new IOException(e.getMessage());
                }

                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext + " Reading first packet of SSL handshake response");

                try {
                    tdsReader.readPacket();
                }
                catch (SQLServerException e) {
                    logger.finer(logContext + " Reading response packet threw exception:" + e.getMessage());
                    throw new IOException(e.getMessage());
                }
            }
        }

        public long skip(long n) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Skipping " + n + " bytes...");

            if (n <= 0)
                return 0;

            if (n > Integer.MAX_VALUE)
                n = Integer.MAX_VALUE;

            ensureSSLPayload();

            try {
                tdsReader.skip((int) n);
            }
            catch (SQLServerException e) {
                logger.finer(logContext + " Skipping bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }

            return n;
        }

        private final byte oneByte[] = new byte[1];

        public int read() throws IOException {
            int bytesRead;

            while (0 == (bytesRead = readInternal(oneByte, 0, oneByte.length)))
                ;

            assert 1 == bytesRead || -1 == bytesRead;
            return 1 == bytesRead ? oneByte[0] : -1;
        }

        public int read(byte[] b) throws IOException {
            return readInternal(b, 0, b.length);
        }

        public int read(byte b[],
                int offset,
                int maxBytes) throws IOException {
            return readInternal(b, offset, maxBytes);
        }

        private int readInternal(byte b[],
                int offset,
                int maxBytes) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Reading " + maxBytes + " bytes...");

            ensureSSLPayload();

            try {
                tdsReader.readBytes(b, offset, maxBytes);
            }
            catch (SQLServerException e) {
                logger.finer(logContext + " Reading bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }

            return maxBytes;
        }
    }

    /**
     * Used during SSL handshake, this class implements an OutputStream that writes SSL handshake request data (framed in TDS messages) to the TDS
     * channel.
     */
    private class SSLHandshakeOutputStream extends OutputStream {
        private final TDSWriter tdsWriter;

        /** Flag indicating when it is necessary to start a new prelogin TDS message */
        private boolean messageStarted;

        private final Logger logger;
        private final String logContext;

        SSLHandshakeOutputStream(TDSChannel tdsChannel) {
            this.tdsWriter = tdsChannel.getWriter();
            this.messageStarted = false;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (SSLHandshakeOutputStream):";
        }

        public void flush() throws IOException {
            // It seems that the security provider implementation in some JVMs
            // (notably SunJSSE in the 6.0 JVM) likes to add spurious calls to
            // flush the SSL handshake output stream during SSL handshaking.
            // We need to ignore these calls because the SSL handshake payload
            // needs to be completely encapsulated in TDS. The SSL handshake
            // input stream always ensures that this output stream has been flushed
            // before trying to read the response.
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Ignored a request to flush the stream");
        }

        void endMessage() throws SQLServerException {
            // We should only be asked to end the message if we have started one
            assert messageStarted;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Finishing TDS message");

            // Flush any remaining bytes through the writer. Since there may be fewer bytes
            // ready to send than a full TDS packet, we end the message here and start a new
            // one later if additional handshake data needs to be sent.
            tdsWriter.endMessage();
            messageStarted = false;
        }

        private final byte singleByte[] = new byte[1];

        public void write(int b) throws IOException {
            singleByte[0] = (byte) (b & 0xFF);
            writeInternal(singleByte, 0, singleByte.length);
        }

        public void write(byte[] b) throws IOException {
            writeInternal(b, 0, b.length);
        }

        public void write(byte[] b,
                int off,
                int len) throws IOException {
            writeInternal(b, off, len);
        }

        private void writeInternal(byte[] b,
                int off,
                int len) throws IOException {
            try {
                // Start out the handshake request in a new prelogin message. Subsequent
                // writes just add handshake data to the request until flushed.
                if (!messageStarted) {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(logContext + " Starting new TDS packet...");

                    tdsWriter.startMessage(null, TDS.PKT_PRELOGIN);
                    messageStarted = true;
                }

                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext + " Writing " + len + " bytes...");

                tdsWriter.writeBytes(b, off, len);
            }
            catch (SQLServerException e) {
                logger.finer(logContext + " Writing bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }
        }
    }

    /**
     * This class implements an InputStream that just forwards all of its methods to an underlying InputStream.
     *
     * It is more predictable than FilteredInputStream which forwards some of its read methods directly to the underlying stream, but not others.
     */
    private final class ProxyInputStream extends InputStream {
        private InputStream filteredStream;

        ProxyInputStream(InputStream is) {
            filteredStream = is;
        }

        final void setFilteredStream(InputStream is) {
            filteredStream = is;
        }

        public long skip(long n) throws IOException {
            long bytesSkipped;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Skipping " + n + " bytes");

            bytesSkipped = filteredStream.skip(n);

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Skipped " + n + " bytes");

            return bytesSkipped;
        }

        public int available() throws IOException {
            int bytesAvailable = filteredStream.available();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " " + bytesAvailable + " bytes available");

            return bytesAvailable;
        }

        private final byte oneByte[] = new byte[1];

        public int read() throws IOException {
            int bytesRead;

            while (0 == (bytesRead = readInternal(oneByte, 0, oneByte.length)))
                ;

            assert 1 == bytesRead || -1 == bytesRead;
            return 1 == bytesRead ? oneByte[0] : -1;
        }

        public int read(byte[] b) throws IOException {
            return readInternal(b, 0, b.length);
        }

        public int read(byte b[],
                int offset,
                int maxBytes) throws IOException {
            return readInternal(b, offset, maxBytes);
        }

        private int readInternal(byte b[],
                int offset,
                int maxBytes) throws IOException {
            int bytesRead;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Reading " + maxBytes + " bytes");

            try {
                bytesRead = filteredStream.read(b, offset, maxBytes);
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " " + e.getMessage());

                logger.finer(toString() + " Reading bytes threw exception:" + e.getMessage());
                throw e;
            }

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Read " + bytesRead + " bytes");

            return bytesRead;
        }

        public boolean markSupported() {
            boolean markSupported = filteredStream.markSupported();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Returning markSupported: " + markSupported);

            return markSupported;
        }

        public void mark(int readLimit) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Marking next " + readLimit + " bytes");

            filteredStream.mark(readLimit);
        }

        public void reset() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Resetting to previous mark");

            filteredStream.reset();
        }

        public void close() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Closing");

            filteredStream.close();
        }
    }

    /**
     * This class implements an OutputStream that just forwards all of its methods to an underlying OutputStream.
     *
     * This class essentially does what FilteredOutputStream does, but is more efficient for our usage. FilteredOutputStream transforms block writes
     * to sequences of single-byte writes.
     */
    final class ProxyOutputStream extends OutputStream {
        private OutputStream filteredStream;

        ProxyOutputStream(OutputStream os) {
            filteredStream = os;
        }

        final void setFilteredStream(OutputStream os) {
            filteredStream = os;
        }

        public void close() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Closing");

            filteredStream.close();
        }

        public void flush() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Flushing");

            filteredStream.flush();
        }

        private final byte singleByte[] = new byte[1];

        public void write(int b) throws IOException {
            singleByte[0] = (byte) (b & 0xFF);
            writeInternal(singleByte, 0, singleByte.length);
        }

        public void write(byte[] b) throws IOException {
            writeInternal(b, 0, b.length);
        }

        public void write(byte[] b,
                int off,
                int len) throws IOException {
            writeInternal(b, off, len);
        }

        private void writeInternal(byte[] b,
                int off,
                int len) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Writing " + len + " bytes");

            filteredStream.write(b, off, len);
        }
    }

    /**
     * This class implements a Socket whose I/O streams can be switched from using a TDSChannel for I/O to using its underlying TCP/IP socket.
     *
     * The SSL socket binds to a ProxySocket. The initial SSL handshake is done over TDSChannel I/O streams so that the handshake payload is framed in
     * TDS packets. The I/O streams are then switched to TCP/IP I/O streams using setStreams, and SSL communications continue directly over the TCP/IP
     * I/O streams.
     *
     * Most methods other than those for getting the I/O streams are simply forwarded to the TDSChannel's underlying TCP/IP socket. Methods that
     * change the socket binding or provide direct channel access are disallowed.
     */
    private class ProxySocket extends Socket {
        private final TDSChannel tdsChannel;
        private final Logger logger;
        private final String logContext;
        private final ProxyInputStream proxyInputStream;
        private final ProxyOutputStream proxyOutputStream;

        ProxySocket(TDSChannel tdsChannel) {
            this.tdsChannel = tdsChannel;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (ProxySocket):";

            // Create the I/O streams
            SSLHandshakeOutputStream sslHandshakeOutputStream = new SSLHandshakeOutputStream(tdsChannel);
            SSLHandshakeInputStream sslHandshakeInputStream = new SSLHandshakeInputStream(tdsChannel, sslHandshakeOutputStream);
            this.proxyOutputStream = new ProxyOutputStream(sslHandshakeOutputStream);
            this.proxyInputStream = new ProxyInputStream(sslHandshakeInputStream);
        }

        void setStreams(InputStream is,
                OutputStream os) {
            proxyInputStream.setFilteredStream(is);
            proxyOutputStream.setFilteredStream(os);
        }

        public InputStream getInputStream() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Getting input stream");

            return proxyInputStream;
        }

        public OutputStream getOutputStream() throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Getting output stream");

            return proxyOutputStream;
        }

        // Allow methods that should just forward to the underlying TCP socket or return fixed values
        public InetAddress getInetAddress() {
            return tdsChannel.tcpSocket.getInetAddress();
        }

        public boolean getKeepAlive() throws SocketException {
            return tdsChannel.tcpSocket.getKeepAlive();
        }

        public InetAddress getLocalAddress() {
            return tdsChannel.tcpSocket.getLocalAddress();
        }

        public int getLocalPort() {
            return tdsChannel.tcpSocket.getLocalPort();
        }

        public SocketAddress getLocalSocketAddress() {
            return tdsChannel.tcpSocket.getLocalSocketAddress();
        }

        public boolean getOOBInline() throws SocketException {
            return tdsChannel.tcpSocket.getOOBInline();
        }

        public int getPort() {
            return tdsChannel.tcpSocket.getPort();
        }

        public int getReceiveBufferSize() throws SocketException {
            return tdsChannel.tcpSocket.getReceiveBufferSize();
        }

        public SocketAddress getRemoteSocketAddress() {
            return tdsChannel.tcpSocket.getRemoteSocketAddress();
        }

        public boolean getReuseAddress() throws SocketException {
            return tdsChannel.tcpSocket.getReuseAddress();
        }

        public int getSendBufferSize() throws SocketException {
            return tdsChannel.tcpSocket.getSendBufferSize();
        }

        public int getSoLinger() throws SocketException {
            return tdsChannel.tcpSocket.getSoLinger();
        }

        public int getSoTimeout() throws SocketException {
            return tdsChannel.tcpSocket.getSoTimeout();
        }

        public boolean getTcpNoDelay() throws SocketException {
            return tdsChannel.tcpSocket.getTcpNoDelay();
        }

        public int getTrafficClass() throws SocketException {
            return tdsChannel.tcpSocket.getTrafficClass();
        }

        public boolean isBound() {
            return true;
        }

        public boolean isClosed() {
            return false;
        }

        public boolean isConnected() {
            return true;
        }

        public boolean isInputShutdown() {
            return false;
        }

        public boolean isOutputShutdown() {
            return false;
        }

        public String toString() {
            return tdsChannel.tcpSocket.toString();
        }

        public SocketChannel getChannel() {
            return null;
        }

        // Disallow calls to methods that would change the underlying TCP socket
        public void bind(SocketAddress bindPoint) throws IOException {
            logger.finer(logContext + " Disallowed call to bind.  Throwing IOException.");
            throw new IOException();
        }

        public void connect(SocketAddress endpoint) throws IOException {
            logger.finer(logContext + " Disallowed call to connect (without timeout).  Throwing IOException.");
            throw new IOException();
        }

        public void connect(SocketAddress endpoint,
                int timeout) throws IOException {
            logger.finer(logContext + " Disallowed call to connect (with timeout).  Throwing IOException.");
            throw new IOException();
        }

        // Ignore calls to methods that would otherwise allow the SSL socket
        // to directly manipulate the underlying TCP socket
        public void close() throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " Ignoring close");
        }

        public void setReceiveBufferSize(int size) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setReceiveBufferSize size:" + size);
        }

        public void setSendBufferSize(int size) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setSendBufferSize size:" + size);
        }

        public void setReuseAddress(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setReuseAddress");
        }

        public void setSoLinger(boolean on,
                int linger) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setSoLinger");
        }

        public void setSoTimeout(int timeout) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setSoTimeout");
        }

        public void setTcpNoDelay(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setTcpNoDelay");
        }

        public void setTrafficClass(int tc) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setTrafficClass");
        }

        public void shutdownInput() throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring shutdownInput");
        }

        public void shutdownOutput() throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring shutdownOutput");
        }

        public void sendUrgentData(int data) throws IOException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring sendUrgentData");
        }

        public void setKeepAlive(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setKeepAlive");
        }

        public void setOOBInline(boolean on) throws SocketException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Ignoring setOOBInline");
        }
    }

    /**
     * This class implements an X509TrustManager that always accepts the X509Certificate chain offered to it.
     *
     * A PermissiveX509TrustManager is used to "verify" the authenticity of the server when the trustServerCertificate connection property is set to
     * true.
     */
    private final class PermissiveX509TrustManager implements X509TrustManager {
        private final TDSChannel tdsChannel;
        private final Logger logger;
        private final String logContext;

        PermissiveX509TrustManager(TDSChannel tdsChannel) {
            this.tdsChannel = tdsChannel;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (PermissiveX509TrustManager):";
        }

        public void checkClientTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " Trusting client certificate (!)");
        }

        public void checkServerTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " Trusting server certificate");
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    /**
     * This class implements an X509TrustManager that hostname for validation.
     *
     * This validates the subject name in the certificate with the host name
     */
    private final class HostNameOverrideX509TrustManager implements X509TrustManager {
        private final Logger logger;
        private final String logContext;
        private final X509TrustManager defaultTrustManager;
        private String hostName;

        HostNameOverrideX509TrustManager(TDSChannel tdsChannel,
                X509TrustManager tm,
                String hostName) {
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (HostNameOverrideX509TrustManager):";
            defaultTrustManager = tm;
            // canonical name is in lower case so convert this to lowercase too.
            this.hostName = hostName.toLowerCase(Locale.ENGLISH);
            ;
        }

        // Parse name in RFC 2253 format
        // Returns the common name if successful, null if failed to find the common name.
        // The parser tuned to be safe than sorry so if it sees something it cant parse correctly it returns null
        private String parseCommonName(String distinguishedName) {
            int index;
            // canonical name converts entire name to lowercase
            index = distinguishedName.indexOf("cn=");
            if (index == -1) {
                return null;
            }
            distinguishedName = distinguishedName.substring(index + 3);
            // Parse until a comma or end is reached
            // Note the parser will handle gracefully (essentially will return empty string) , inside the quotes (e.g cn="Foo, bar") however
            // RFC 952 says that the hostName cant have commas however the parser should not (and will not) crash if it sees a , within quotes.
            for (index = 0; index < distinguishedName.length(); index++) {
                if (distinguishedName.charAt(index) == ',') {
                    break;
                }
            }
            String commonName = distinguishedName.substring(0, index);
            // strip any quotes
            if (commonName.length() > 1 && ('\"' == commonName.charAt(0))) {
                if ('\"' == commonName.charAt(commonName.length() - 1))
                    commonName = commonName.substring(1, commonName.length() - 1);
                else {
                    // Be safe the name is not ended in " return null so the common Name wont match
                    commonName = null;
                }
            }
            return commonName;
        }

        private boolean validateServerName(String nameInCert) throws CertificateException {
            // Failed to get the common name from DN or empty CN
            if (null == nameInCert) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(logContext + " Failed to parse the name from the certificate or name is empty.");
                return false;
            }

            // Verify that the name in certificate matches exactly with the host name
            if (!nameInCert.equals(hostName)) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(logContext + " The name in certificate " + nameInCert + " does not match with the server name " + hostName + ".");
                return false;
            }

            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " The name in certificate:" + nameInCert + " validated against server name " + hostName + ".");

            return true;
        }

        public void checkClientTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Forwarding ClientTrusted.");
            defaultTrustManager.checkClientTrusted(chain, authType);
        }

        public void checkServerTrusted(X509Certificate[] chain,
                String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Forwarding Trusting server certificate");
            defaultTrustManager.checkServerTrusted(chain, authType);
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " default serverTrusted succeeded proceeding with server name validation");

            validateServerNameInCertificate(chain[0]);
        }

        private void validateServerNameInCertificate(X509Certificate cert) throws CertificateException {
            String nameInCertDN = cert.getSubjectX500Principal().getName("canonical");
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(logContext + " Validating the server name:" + hostName);
                logger.finer(logContext + " The DN name in certificate:" + nameInCertDN);
            }

            boolean isServerNameValidated;

            // the name in cert is in RFC2253 format parse it to get the actual subject name
            String subjectCN = parseCommonName(nameInCertDN);

            isServerNameValidated = validateServerName(subjectCN);

            if (!isServerNameValidated) {

                Collection<List<?>> sanCollection = cert.getSubjectAlternativeNames();

                if (sanCollection != null) {
                    // find a subjectAlternateName entry corresponding to DNS Name
                    for (List<?> sanEntry : sanCollection) {

                        if (sanEntry != null && sanEntry.size() >= 2) {
                            Object key = sanEntry.get(0);
                            Object value = sanEntry.get(1);

                            if (logger.isLoggable(Level.FINER)) {
                                logger.finer(logContext + "Key: " + key + "; KeyClass:" + (key != null ? key.getClass() : null) + ";value: " + value
                                        + "; valueClass:" + (value != null ? value.getClass() : null));

                            }

                            // From Documentation(http://download.oracle.com/javase/6/docs/api/java/security/cert/X509Certificate.html):
                            // "Note that the Collection returned may contain
                            // more than one name of the same type."
                            // So, more than one entry of dnsNameType can be present.
                            // Java docs guarantee that the first entry in the list will be an integer.
                            // 2 is the sequence no of a dnsName
                            if ((key != null) && (key instanceof Integer) && ((Integer) key == 2)) {
                                // As per RFC2459, the DNSName will be in the
                                // "preferred name syntax" as specified by RFC
                                // 1034 and the name can be in upper or lower case.
                                // And no significance is attached to case.
                                // Java docs guarantee that the second entry in the list
                                // will be a string for dnsName
                                if (value != null && value instanceof String) {
                                    String dnsNameInSANCert = (String) value;

                                    // Use English locale to avoid Turkish i issues.
                                    // Note that, this conversion was not necessary for
                                    // cert.getSubjectX500Principal().getName("canonical");
                                    // as the above API already does this by default as per documentation.
                                    dnsNameInSANCert = dnsNameInSANCert.toLowerCase(Locale.ENGLISH);

                                    isServerNameValidated = validateServerName(dnsNameInSANCert);

                                    if (isServerNameValidated) {
                                        if (logger.isLoggable(Level.FINER)) {
                                            logger.finer(logContext + " found a valid name in certificate: " + dnsNameInSANCert);
                                        }
                                        break;
                                    }
                                }

                                if (logger.isLoggable(Level.FINER)) {
                                    logger.finer(logContext + " the following name in certificate does not match the serverName: " + value);
                                }
                            }

                        }
                        else {
                            if (logger.isLoggable(Level.FINER)) {
                                logger.finer(logContext + " found an invalid san entry: " + sanEntry);
                            }
                        }
                    }

                }
            }

            if (!isServerNameValidated) {
                String msg = SQLServerException.getErrString("R_certNameFailed");
                throw new CertificateException(msg);
            }
        }

        public X509Certificate[] getAcceptedIssuers() {
            return defaultTrustManager.getAcceptedIssuers();
        }
    }

    enum SSLHandhsakeState {
        SSL_HANDHSAKE_NOT_STARTED,
        SSL_HANDHSAKE_STARTED,
        SSL_HANDHSAKE_COMPLETE
    };

    /**
     * Enables SSL Handshake.
     * 
     * @param host
     *            Server Host Name for SSL Handshake
     * @param port
     *            Server Port for SSL Handshake
     * @throws SQLServerException
     */
    void enableSSL(String host,
            int port) throws SQLServerException {
        // If enabling SSL fails, which it can for a number of reasons, the following items
        // are used in logging information to the TDS channel logger to help diagnose the problem.
        Provider tmfProvider = null;        // TrustManagerFactory provider
        Provider sslContextProvider = null; // SSLContext provider
        Provider ksProvider = null;         // KeyStore provider
        String tmfDefaultAlgorithm = null;  // Default algorithm (typically X.509) used by the TrustManagerFactory
        SSLHandhsakeState handshakeState = SSLHandhsakeState.SSL_HANDHSAKE_NOT_STARTED;

        boolean isFips = false;
        String trustStoreType = null;
        String sslProtocol = null;

        // If anything in here fails, terminate the connection and throw an exception
        try {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Enabling SSL...");

            String trustStoreFileName = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_STORE.toString());
            String trustStorePassword = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString());
            String hostNameInCertificate = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString());

            trustStoreType = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString());
            
            if(StringUtils.isEmpty(trustStoreType)) {
                trustStoreType = SQLServerDriverStringProperty.TRUST_STORE_TYPE.getDefaultValue();
            }
            
            isFips = Boolean.valueOf(con.activeConnectionProperties.getProperty(SQLServerDriverBooleanProperty.FIPS.toString())); 
            sslProtocol = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.SSL_PROTOCOL.toString());
            
            if (isFips) {
                validateFips(trustStoreType, trustStoreFileName);
            }

            assert TDS.ENCRYPT_OFF == con.getRequestedEncryptionLevel() || // Login only SSL
                    TDS.ENCRYPT_ON == con.getRequestedEncryptionLevel();   // Full SSL

            assert TDS.ENCRYPT_OFF == con.getNegotiatedEncryptionLevel() || // Login only SSL
                    TDS.ENCRYPT_ON == con.getNegotiatedEncryptionLevel() || // Full SSL
                    TDS.ENCRYPT_REQ == con.getNegotiatedEncryptionLevel();   // Full SSL

            // If we requested login only SSL or full SSL without server certificate validation,
            // then we'll "validate" the server certificate using a naive TrustManager that trusts
            // everything it sees.
            TrustManager[] tm = null;
            if (TDS.ENCRYPT_OFF == con.getRequestedEncryptionLevel()
                    || (TDS.ENCRYPT_ON == con.getRequestedEncryptionLevel() && con.trustServerCertificate())) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " SSL handshake will trust any certificate");

                tm = new TrustManager[] {new PermissiveX509TrustManager(this)};
            }
            // Otherwise, we'll check if a specific TrustManager implemenation has been requested and
            // if so instantiate it, optionally specifying a constructor argument to customize it.
            else if (con.getTrustManagerClass() != null) {
                Class<?> tmClass = Class.forName(con.getTrustManagerClass());
                if (!TrustManager.class.isAssignableFrom(tmClass)) {
                    throw new IllegalArgumentException(
                            "The class specified by the trustManagerClass property must implement javax.net.ssl.TrustManager");
                }
                String constructorArg = con.getTrustManagerConstructorArg();
                if (constructorArg == null) {
                    tm = new TrustManager[] {(TrustManager) tmClass.getDeclaredConstructor().newInstance()};
                }
                else {
                    tm = new TrustManager[] {(TrustManager) tmClass.getDeclaredConstructor(String.class).newInstance(constructorArg)};
                }
            }
            // Otherwise, we'll validate the certificate using a real TrustManager obtained
            // from the a security provider that is capable of validating X.509 certificates.
            else {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " SSL handshake will validate server certificate");

                KeyStore ks = null;

                // If we are using the system default trustStore and trustStorePassword
                // then we can skip all of the KeyStore loading logic below.
                // The security provider's implementation takes care of everything for us.
                if (null == trustStoreFileName && null == trustStorePassword) {
                    if (logger.isLoggable(Level.FINER))
                        logger.finer(toString() + " Using system default trust store and password");
                }

                // Otherwise either the trustStore, trustStorePassword, or both was specified.
                // In that case, we need to load up a KeyStore ourselves.
                else {
                    // First, obtain an interface to a KeyStore that can load trust material
                    // stored in Java Key Store (JKS) format.
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(toString() + " Finding key store interface");


                    ks = KeyStore.getInstance(trustStoreType);
                    ksProvider = ks.getProvider();

                    // Next, load up the trust store file from the specified location.
                    // Note: This function returns a null InputStream if the trust store cannot
                    // be loaded. This is by design. See the method comment and documentation
                    // for KeyStore.load for details.
                    InputStream is = loadTrustStore(trustStoreFileName);

                    // Finally, load the KeyStore with the trust material (if any) from the
                    // InputStream and close the stream.
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(toString() + " Loading key store");

                    try {
                        ks.load(is, (null == trustStorePassword) ? null : trustStorePassword.toCharArray());
                    }
                    finally {
                        // We are done with the trustStorePassword (if set). Clear it for better security.
                        con.activeConnectionProperties.remove(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString());

                        // We are also done with the trust store input stream.
                        if (null != is) {
                            try {
                                is.close();
                            }
                            catch (IOException e) {
                                if (logger.isLoggable(Level.FINE))
                                    logger.fine(toString() + " Ignoring error closing trust material InputStream...");
                            }
                        }
                    }
                }

                // Either we now have a KeyStore populated with trust material or we are using the
                // default source of trust material (cacerts). Either way, we are now ready to
                // use a TrustManagerFactory to create a TrustManager that uses the trust material
                // to validate the server certificate.

                // Next step is to get a TrustManagerFactory that can produce TrustManagers
                // that understands X.509 certificates.
                TrustManagerFactory tmf = null;

                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Locating X.509 trust manager factory");

                tmfDefaultAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
                tmf = TrustManagerFactory.getInstance(tmfDefaultAlgorithm);
                tmfProvider = tmf.getProvider();

                // Tell the TrustManagerFactory to give us TrustManagers that we can use to
                // validate the server certificate using the trust material in the KeyStore.
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Getting trust manager");

                tmf.init(ks);
                tm = tmf.getTrustManagers();

                // if the host name in cert provided use it or use the host name Only if it is not FIPS
                if (!isFips) {
                    if (null != hostNameInCertificate) {
                        tm = new TrustManager[] {new HostNameOverrideX509TrustManager(this, (X509TrustManager) tm[0], hostNameInCertificate)};
                    }
                    else {
                        tm = new TrustManager[] {new HostNameOverrideX509TrustManager(this, (X509TrustManager) tm[0], host)};
                    }
                }
            } // end if (!con.trustServerCertificate())

            // Now, with a real or fake TrustManager in hand, get a context for creating a
            // SSL sockets through a SSL socket factory. We require at least TLS support.
            SSLContext sslContext = null;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Getting TLS or better SSL context");

            sslContext = SSLContext.getInstance(sslProtocol);
            sslContextProvider = sslContext.getProvider();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Initializing SSL context");

            sslContext.init(null, tm, null);

            // Got the SSL context. Now create an SSL socket over our own proxy socket
            // which we can toggle between TDS-encapsulated and raw communications.
            // Initially, the proxy is set to encapsulate the SSL handshake in TDS packets.
            proxySocket = new ProxySocket(this);

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Creating SSL socket");

            sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(proxySocket, host, port, false); // don't close proxy when SSL socket
                                                                                                                // is closed     
            // At long last, start the SSL handshake ...
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Starting SSL handshake");

            // TLS 1.2 intermittent exception happens here.
            handshakeState = SSLHandhsakeState.SSL_HANDHSAKE_STARTED;
            sslSocket.startHandshake();
            handshakeState = SSLHandhsakeState.SSL_HANDHSAKE_COMPLETE;

            // After SSL handshake is complete, rewire proxy socket to use raw TCP/IP streams ...
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Rewiring proxy streams after handshake");

            proxySocket.setStreams(inputStream, outputStream);

            // ... and rewire TDSChannel to use SSL streams.
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Getting SSL InputStream");

            inputStream = sslSocket.getInputStream();

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Getting SSL OutputStream");

            outputStream = sslSocket.getOutputStream();

            // SSL is now enabled; switch over the channel socket
            channelSocket = sslSocket;

            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " SSL enabled");
        }
        catch (Exception e) {
            // Log the original exception and its source at FINER level
            if (logger.isLoggable(Level.FINER))
                logger.log(Level.FINER, e.getMessage(), e);

            // If enabling SSL fails, the following information may help diagnose the problem.
            // Do not use Level INFO or above which is sent to standard output/error streams.
            // This is because due to an intermittent TLS 1.2 connection issue, we will be retrying the connection and
            // do not want to print this message in console.
            if (logger.isLoggable(Level.FINER))
                logger.log(Level.FINER,
                        "java.security path: " + JAVA_SECURITY + "\n" + "Security providers: " + Arrays.asList(Security.getProviders()) + "\n"
                                + ((null != sslContextProvider) ? ("SSLContext provider info: " + sslContextProvider.getInfo() + "\n"
                                        + "SSLContext provider services:\n" + sslContextProvider.getServices() + "\n") : "")
                                + ((null != tmfProvider) ? ("TrustManagerFactory provider info: " + tmfProvider.getInfo() + "\n") : "")
                                + ((null != tmfDefaultAlgorithm) ? ("TrustManagerFactory default algorithm: " + tmfDefaultAlgorithm + "\n") : "")
                                + ((null != ksProvider) ? ("KeyStore provider info: " + ksProvider.getInfo() + "\n") : "") + "java.ext.dirs: "
                                + System.getProperty("java.ext.dirs"));

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_sslFailed"));
            Object[] msgArgs = {e.getMessage()};

            // It is important to get the localized message here, otherwise error messages won't match for different locales.
            String errMsg = e.getLocalizedMessage();
            // If the message is null replace it with the non-localized message or a dummy string. This can happen if a custom
            // TrustManager implementation is specified that does not provide localized messages.
            if (errMsg == null) {
                errMsg = e.getMessage();
            }
            if (errMsg == null) {
                errMsg = "";
            }
            // The error message may have a connection id appended to it. Extract the message only for comparison.
            // This client connection id is appended in method checkAndAppendClientConnId().
            if (errMsg.contains(SQLServerException.LOG_CLIENT_CONNECTION_ID_PREFIX)) {
                errMsg = errMsg.substring(0, errMsg.indexOf(SQLServerException.LOG_CLIENT_CONNECTION_ID_PREFIX));
            }

            // Isolate the TLS1.2 intermittent connection error.
            if (e instanceof IOException && (SSLHandhsakeState.SSL_HANDHSAKE_STARTED == handshakeState)
                    && (errMsg.equals(SQLServerException.getErrString("R_truncatedServerResponse")))) {
                con.terminate(SQLServerException.DRIVER_ERROR_INTERMITTENT_TLS_FAILED, form.format(msgArgs), e);
            }
            else {
                con.terminate(SQLServerException.DRIVER_ERROR_SSL_FAILED, form.format(msgArgs), e);
            }
        }
    }

    /**
     * Validate FIPS if fips set as true
     * 
     * Valid FIPS settings:
     * <LI>Encrypt should be true
     * <LI>trustServerCertificate should be false
     * <LI>if certificate is not installed TrustStoreType should be present.
     * 
     * @param trustStoreType
     * @param trustStoreFileName
     * @throws SQLServerException
     * @since 6.1.4
     */
    private void validateFips(final String trustStoreType,
            final String trustStoreFileName) throws SQLServerException {
        boolean isValid = false;
        boolean isEncryptOn;
        boolean isValidTrustStoreType;
        boolean isValidTrustStore;
        boolean isTrustServerCertificate;

        String strError = SQLServerException.getErrString("R_invalidFipsConfig");

        isEncryptOn = (TDS.ENCRYPT_ON == con.getRequestedEncryptionLevel());

        isValidTrustStoreType = !StringUtils.isEmpty(trustStoreType);
        isValidTrustStore = !StringUtils.isEmpty(trustStoreFileName);
        isTrustServerCertificate = con.trustServerCertificate();

        if (isEncryptOn && !isTrustServerCertificate) {          
            isValid = true;
            if (isValidTrustStore && !isValidTrustStoreType) {
            // In case of valid trust store we need to check TrustStoreType.
                isValid = false;               
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + "TrustStoreType is required alongside with TrustStore.");
            }
        }

        if (!isValid) {
            throw new SQLServerException(strError, null, 0, null);
        }

    }

    private final static String SEPARATOR = System.getProperty("file.separator");
    private final static String JAVA_HOME = System.getProperty("java.home");
    private final static String JAVA_SECURITY = JAVA_HOME + SEPARATOR + "lib" + SEPARATOR + "security";
    private final static String JSSECACERTS = JAVA_SECURITY + SEPARATOR + "jssecacerts";
    private final static String CACERTS = JAVA_SECURITY + SEPARATOR + "cacerts";

    /**
     * Loads the contents of a trust store into an InputStream.
     *
     * When a location to a trust store is specified, this method attempts to load that store. Otherwise, it looks for and attempts to load the
     * default trust store using essentially the same logic (outlined in the JSSE Reference Guide) as the default X.509 TrustManagerFactory.
     *
     * @return an InputStream containing the contents of the loaded trust store
     * @return null if the trust store cannot be loaded.
     *
     *         Note: It is by design that this function returns null when the trust store cannot be loaded rather than throwing an exception. The
     *         reason is that KeyStore.load, which uses the returned InputStream, interprets a null InputStream to mean that there are no trusted
     *         certificates, which mirrors the behavior of the default (no trust store, no password specified) path.
     */
    final InputStream loadTrustStore(String trustStoreFileName) {
        FileInputStream is = null;

        // First case: Trust store filename was specified
        if (null != trustStoreFileName) {
            try {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Opening specified trust store: " + trustStoreFileName);

                is = new FileInputStream(trustStoreFileName);
            }
            catch (FileNotFoundException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(toString() + " Trust store not found: " + e.getMessage());

                // If the trustStoreFileName connection property is set, but the file is not found,
                // then treat it as if the file was empty so that the TrustManager reports
                // that no certificate is found.
            }
        }

        // Second case: Trust store filename derived from javax.net.ssl.trustStore system property
        else if (null != (trustStoreFileName = System.getProperty("javax.net.ssl.trustStore"))) {
            try {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Opening default trust store (from javax.net.ssl.trustStore): " + trustStoreFileName);

                is = new FileInputStream(trustStoreFileName);
            }
            catch (FileNotFoundException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(toString() + " Trust store not found: " + e.getMessage());

                // If the javax.net.ssl.trustStore property is set, but the file is not found,
                // then treat it as if the file was empty so that the TrustManager reports
                // that no certificate is found.
            }
        }

        // Third case: No trust store specified and no system property set. Use jssecerts/cacerts.
        else {
            try {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Opening default trust store: " + JSSECACERTS);

                is = new FileInputStream(JSSECACERTS);
            }
            catch (FileNotFoundException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(toString() + " Trust store not found: " + e.getMessage());
            }

            // No jssecerts. Try again with cacerts...
            if (null == is) {
                try {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(toString() + " Opening default trust store: " + CACERTS);

                    is = new FileInputStream(CACERTS);
                }
                catch (FileNotFoundException e) {
                    if (logger.isLoggable(Level.FINE))
                        logger.fine(toString() + " Trust store not found: " + e.getMessage());

                    // No jssecerts or cacerts. Treat it as if the trust store is empty so that
                    // the TrustManager reports that no certificate is found.
                }
            }
        }

        return is;
    }

    final int read(byte[] data,
            int offset,
            int length) throws SQLServerException {
        try {
            return inputStream.read(data, offset, length);
        }
        catch (IOException e) {
            if (logger.isLoggable(Level.FINE))
                logger.fine(toString() + " read failed:" + e.getMessage());

            if (e instanceof SocketTimeoutException) {
                con.terminate(SQLServerException.ERROR_SOCKET_TIMEOUT, e.getMessage(), e);
            }
            else {
                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
            }

            return 0; // Keep the compiler happy.
        }
    }

    final void write(byte[] data,
            int offset,
            int length) throws SQLServerException {
        try {
            outputStream.write(data, offset, length);
        }
        catch (IOException e) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " write failed:" + e.getMessage());

            con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
        }
    }

    final void flush() throws SQLServerException {
        try {
            outputStream.flush();
        }
        catch (IOException e) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " flush failed:" + e.getMessage());

            con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
        }
    }

    final void close() {
        if (null != sslSocket)
            disableSSL();

        if (null != inputStream) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Closing inputStream...");

            try {
                inputStream.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing inputStream", e);
            }
        }

        if (null != outputStream) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Closing outputStream...");

            try {
                outputStream.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing outputStream", e);
            }
        }

        if (null != tcpSocket) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing TCP socket...");

            try {
                tcpSocket.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing socket", e);
            }
        }
    }

    /**
     * Logs TDS packet data to the com.microsoft.sqlserver.jdbc.TDS.DATA logger
     *
     * @param data
     *            the buffer containing the TDS packet payload data to log
     * @param nStartOffset
     *            offset into the above buffer from where to start logging
     * @param nLength
     *            length (in bytes) of payload
     * @param messageDetail
     *            other loggable details about the payload
     */
    /* L0 */ void logPacket(byte data[],
            int nStartOffset,
            int nLength,
            String messageDetail) {
        assert 0 <= nLength && nLength <= data.length;
        assert 0 <= nStartOffset && nStartOffset <= data.length;

        final char hexChars[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

        final char printableChars[] = {'.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', ' ', '!', '\"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?', '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '[', '\\', ']', '^', '_', '`', 'a', 'b', 'c', 'd',
                'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'};

        // Log message body lines have this form:
        //
        // "XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX ................"
        // 012345678911111111112222222222333333333344444444445555555555666666
        // 01234567890123456789012345678901234567890123456789012345
        //
        final char lineTemplate[] = {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',

                ' ', ' ',

                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'};

        char logLine[] = new char[lineTemplate.length];
        System.arraycopy(lineTemplate, 0, logLine, 0, lineTemplate.length);

        // Logging builds up a string buffer for the entire log trace
        // before writing it out. So use an initial size large enough
        // that the buffer doesn't have to resize itself.
        StringBuilder logMsg = new StringBuilder(messageDetail.length() + // Message detail
                4 * nLength +            // 2-digit hex + space + ASCII, per byte
                4 * (1 + nLength / 16) +     // 2 extra spaces + CR/LF, per line (16 bytes per line)
                80);                   // Extra fluff: IP:Port, Connection #, SPID, ...

        // Format the headline like so:
        // /157.55.121.182:2983 Connection 1, SPID 53, Message info here ...
        //
        // Note: the log formatter itself timestamps what we write so we don't have
        // to do it again here.
        logMsg.append(tcpSocket.getLocalAddress().toString() + ":" + tcpSocket.getLocalPort() + " SPID:" + spid + " " + messageDetail + "\r\n");

        // Fill in the body of the log message, line by line, 16 bytes per line.
        int nBytesLogged = 0;
        int nBytesThisLine;
        while (true) {
            // Fill up the line with as many bytes as we can (up to 16 bytes)
            for (nBytesThisLine = 0; nBytesThisLine < 16 && nBytesLogged < nLength; nBytesThisLine++, nBytesLogged++) {
                int nUnsignedByteVal = (data[nStartOffset + nBytesLogged] + 256) % 256;
                logLine[3 * nBytesThisLine] = hexChars[nUnsignedByteVal / 16];
                logLine[3 * nBytesThisLine + 1] = hexChars[nUnsignedByteVal % 16];
                logLine[50 + nBytesThisLine] = printableChars[nUnsignedByteVal];
            }

            // Pad out the remainder with whitespace
            for (int nBytesJustified = nBytesThisLine; nBytesJustified < 16; nBytesJustified++) {
                logLine[3 * nBytesJustified] = ' ';
                logLine[3 * nBytesJustified + 1] = ' ';
            }

            logMsg.append(logLine, 0, 50 + nBytesThisLine);
            if (nBytesLogged == nLength)
                break;

            logMsg.append("\r\n");
        }

        if (packetLogger.isLoggable(Level.FINEST)) {
            packetLogger.finest(logMsg.toString());
        }
    }

    /**
     * Get the current socket SO_TIMEOUT value.
     *
     * @return the current socket timeout value
     * @throws IOException thrown if the socket timeout cannot be read
     */
    final int getNetworkTimeout() throws IOException {
        return tcpSocket.getSoTimeout();
    }

    /**
     * Set the socket SO_TIMEOUT value.
     *
     * @param timeout the socket timeout in milliseconds
     * @throws IOException thrown if the socket timeout cannot be set
     */
    final void setNetworkTimeout(int timeout) throws IOException {
        tcpSocket.setSoTimeout(timeout);
    }
}

/**
 * SocketFinder is used to find a server socket to which a connection can be made. This class abstracts the logic of finding a socket from TDSChannel
 * class.
 * 
 * In the case when useParallel is set to true, this is achieved by trying to make parallel connections to multiple IP addresses. This class is
 * responsible for spawning multiple threads and keeping track of the search result and the connected socket or exception to be thrown.
 * 
 * In the case where multiSubnetFailover is false, we try our old logic of trying to connect to the first ip address
 * 
 * Typical usage of this class is SocketFinder sf = new SocketFinder(traceId, conn); Socket = sf.getSocket(hostName, port, timeout);
 */
final class SocketFinder {
    /**
     * Indicates the result of a search
     */
    enum Result {
        UNKNOWN,// search is still in progress
        SUCCESS,// found a socket
        FAILURE// failed in finding a socket
    }

    // Thread pool - the values in the constructor are chosen based on the
    // explanation given in design_connection_director_multisubnet.doc
    private static final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());

    // When parallel connections are to be used, use minimum timeout slice of 1500 milliseconds.
    private static final int minTimeoutForParallelConnections = 1500;

    // lock used for synchronization while updating
    // data within a socketFinder object
    private final Object socketFinderlock = new Object();

    // lock on which the parent thread would wait
    // after spawning threads.
    private final Object parentThreadLock = new Object();

    // indicates whether the socketFinder has succeeded or failed
    // in finding a socket or is still trying to find a socket
    private volatile Result result = Result.UNKNOWN;

    // total no of socket connector threads
    // spawned by a socketFinder object
    private int noOfSpawnedThreads = 0;

    // no of threads that finished their socket connection
    // attempts and notified socketFinder about their result
    private int noOfThreadsThatNotified = 0;

    // If a valid connected socket is found, this value would be non-null,
    // else this would be null
    private volatile Socket selectedSocket = null;

    // This would be one of the exceptions returned by the
    // socketConnector threads
    private volatile IOException selectedException = null;

    // Logging variables
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SocketFinder");
    private final String traceID;

    // maximum number of IP Addresses supported
    private static final int ipAddressLimit = 64;

    // necessary for raising exceptions so that the connection pool can be notified
    private final SQLServerConnection conn;

    /**
     * Constructs a new SocketFinder object with appropriate traceId
     * 
     * @param callerTraceID
     *            traceID of the caller
     * @param sqlServerConnection
     *            the SQLServer connection
     */
    SocketFinder(String callerTraceID,
            SQLServerConnection sqlServerConnection) {
        traceID = "SocketFinder(" + callerTraceID + ")";
        conn = sqlServerConnection;
    }

    /**
     * Used to find a socket to which a connection can be made
     * 
     * @param hostName
     * @param portNumber
     * @param timeoutInMilliSeconds
     * @return connected socket
     * @throws IOException
     */
    Socket findSocket(String hostName,
            int portNumber,
            int timeoutInMilliSeconds,
            boolean useParallel,
            boolean useTnir,
            boolean isTnirFirstAttempt,
            int timeoutInMilliSecondsForFullTimeout) throws SQLServerException {
        assert timeoutInMilliSeconds != 0 : "The driver does not allow a time out of 0";

        try {
            InetAddress[] inetAddrs = null;

            // inetAddrs is only used if useParallel is true or TNIR is true. Skip resolving address if that's not the case.
            if (useParallel || useTnir) {
                // Ignore TNIR if host resolves to more than 64 IPs. Make sure we are using original timeout for this.
                inetAddrs = InetAddress.getAllByName(hostName);

                if ((useTnir) && (inetAddrs.length > ipAddressLimit)) {
                    useTnir = false;
                    timeoutInMilliSeconds = timeoutInMilliSecondsForFullTimeout;
                }
            }

            if (!useParallel) {
                // MSF is false. TNIR could be true or false. DBMirroring could be true or false.
                // For TNIR first attempt, we should do existing behavior including how host name is resolved.
                if (useTnir && isTnirFirstAttempt) {
                    return getDefaultSocket(hostName, portNumber, SQLServerConnection.TnirFirstAttemptTimeoutMs);
                }
                else if (!useTnir) {
                    return getDefaultSocket(hostName, portNumber, timeoutInMilliSeconds);
                }
            }

            // Code reaches here only if MSF = true or (TNIR = true and not TNIR first attempt)

            if (logger.isLoggable(Level.FINER)) {
                StringBuilder loggingString = new StringBuilder(this.toString());
                loggingString.append(" Total no of InetAddresses: ");
                loggingString.append(inetAddrs.length);
                loggingString.append(". They are: ");
                
                for (InetAddress inetAddr : inetAddrs) {
                    loggingString.append(inetAddr.toString() + ";");
                }

                logger.finer(loggingString.toString());
            }

            if (inetAddrs.length > ipAddressLimit) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ipAddressLimitWithMultiSubnetFailover"));
                Object[] msgArgs = {Integer.toString(ipAddressLimit)};
                String errorStr = form.format(msgArgs);
                // we do not want any retry to happen here. So, terminate the connection
                // as the config is unsupported.
                conn.terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, errorStr);
            }

            if (Util.isIBM()) {
                timeoutInMilliSeconds = Math.max(timeoutInMilliSeconds, minTimeoutForParallelConnections);
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(this.toString() + "Using Java NIO with timeout:" + timeoutInMilliSeconds);
                }
                findSocketUsingJavaNIO(inetAddrs, portNumber, timeoutInMilliSeconds);
            }
            else {
                LinkedList<InetAddress> inet4Addrs = new LinkedList<>();
                LinkedList<InetAddress> inet6Addrs = new LinkedList<>();

                for (InetAddress inetAddr : inetAddrs) {
                    if (inetAddr instanceof Inet4Address) {
                        inet4Addrs.add((Inet4Address) inetAddr);
                    }
                    else {
                        assert inetAddr instanceof Inet6Address : "Unexpected IP address " + inetAddr.toString();
                        inet6Addrs.add((Inet6Address) inetAddr);
                    }
                }

                // use half timeout only if both IPv4 and IPv6 addresses are present
                int timeoutForEachIPAddressType;
                if ((!inet4Addrs.isEmpty()) && (!inet6Addrs.isEmpty())) {
                    timeoutForEachIPAddressType = Math.max(timeoutInMilliSeconds / 2, minTimeoutForParallelConnections);
                }
                else
                    timeoutForEachIPAddressType = Math.max(timeoutInMilliSeconds, minTimeoutForParallelConnections);

                if (!inet4Addrs.isEmpty()) {
                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString() + "Using Java Threading with timeout:" + timeoutForEachIPAddressType);
                    }

                    findSocketUsingThreading(inet4Addrs, portNumber, timeoutForEachIPAddressType);
                }

                if (!result.equals(Result.SUCCESS)) {
                    // try threading logic
                    if (!inet6Addrs.isEmpty()) {
                        // do not start any threads if there is only one ipv6 address
                        if (inet6Addrs.size() == 1) {
                            return getConnectedSocket(inet6Addrs.get(0), portNumber, timeoutForEachIPAddressType);
                        }

                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer(this.toString() + "Using Threading with timeout:" + timeoutForEachIPAddressType);
                        }

                        findSocketUsingThreading(inet6Addrs, portNumber, timeoutForEachIPAddressType);
                    }
                }
            }

            // If the thread continued execution due to timeout, the result may not be known.
            // In that case, update the result to failure. Note that this case is possible
            // for both IPv4 and IPv6.
            // Using double-checked locking for performance reasons.
            if (result.equals(Result.UNKNOWN)) {
                synchronized (socketFinderlock) {
                    if (result.equals(Result.UNKNOWN)) {
                        result = Result.FAILURE;
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer(this.toString() + " The parent thread updated the result to failure");
                        }
                    }
                }
            }

            // After we reach this point, there is no need for synchronization any more.
            // Because, the result would be known(success/failure).
            // And no threads would update SocketFinder
            // as their function calls would now be no-ops.
            if (result.equals(Result.FAILURE)) {
                if (selectedException == null) {
                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString()
                                + " There is no selectedException. The wait calls timed out before any connect call returned or timed out.");
                    }
                    String message = SQLServerException.getErrString("R_connectionTimedOut");
                    selectedException = new IOException(message);
                }
                throw selectedException;
            }

        }
        catch (InterruptedException ex) {
            // re-interrupt the current thread, in order to restore the thread's interrupt status.
            Thread.currentThread().interrupt();
            
            close(selectedSocket);
            SQLServerException.ConvertConnectExceptionToSQLServerException(hostName, portNumber, conn, ex);
        }
        catch (IOException ex) {
            close(selectedSocket);
            // The code below has been moved from connectHelper.
            // If we do not move it, the functions open(caller of findSocket)
            // and findSocket will have to
            // declare both IOException and SQLServerException in the throws clause
            // as we throw custom SQLServerExceptions(eg:IPAddressLimit, wrapping other exceptions
            // like interruptedException) in findSocket.
            // That would be a bit awkward, because connecthelper(the caller of open)
            // just wraps IOException into SQLServerException and throws SQLServerException.
            // Instead, it would be good to wrap all exceptions at one place - Right here, their origin.
            SQLServerException.ConvertConnectExceptionToSQLServerException(hostName, portNumber, conn, ex);

        }

        assert result.equals(Result.SUCCESS);
        assert selectedSocket != null : "Bug in code. Selected Socket cannot be null here.";

        return selectedSocket;
    }

    /**
     * This function uses java NIO to connect to all the addresses in inetAddrs with in a specified timeout. If it succeeds in connecting, it closes
     * all the other open sockets and updates the result to success.
     * 
     * @param inetAddrs
     *            the array of inetAddress to which connection should be made
     * @param portNumber
     *            the port number at which connection should be made
     * @param timeoutInMilliSeconds
     * @throws IOException
     */
    private void findSocketUsingJavaNIO(InetAddress[] inetAddrs,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException {
        // The driver does not allow a time out of zero.
        // Also, the unit of time the user can specify in the driver is seconds.
        // So, even if the user specifies 1 second(least value), the least possible
        // value that can come here as timeoutInMilliSeconds is 500 milliseconds.
        assert timeoutInMilliSeconds != 0 : "The timeout cannot be zero";
        assert inetAddrs.length != 0 : "Number of inetAddresses should not be zero in this function";

        Selector selector = null;
        LinkedList<SocketChannel> socketChannels = new LinkedList<>();
        SocketChannel selectedChannel = null;

        try {
            selector = Selector.open();

            for (InetAddress inetAddr : inetAddrs) {
                SocketChannel sChannel = SocketChannel.open();
                socketChannels.add(sChannel);

                // make the channel non-blocking
                sChannel.configureBlocking(false);

                // register the channel for connect event
                int ops = SelectionKey.OP_CONNECT;
                SelectionKey key = sChannel.register(selector, ops);

                sChannel.connect(new InetSocketAddress(inetAddr, portNumber));

                if (logger.isLoggable(Level.FINER))
                    logger.finer(this.toString() + " initiated connection to address: " + inetAddr + ", portNumber: " + portNumber);
            }

            long timerNow = System.currentTimeMillis();
            long timerExpire = timerNow + timeoutInMilliSeconds;

            // Denotes the no of channels that still need to processed
            int noOfOutstandingChannels = inetAddrs.length;

            while (true) {
                long timeRemaining = timerExpire - timerNow;
                // if the timeout expired or a channel is selected or there are no more channels left to processes
                if ((timeRemaining <= 0) || (selectedChannel != null) || (noOfOutstandingChannels <= 0))
                    break;

                // denotes the no of channels that are ready to be processed. i.e. they are either connected
                // or encountered an exception while trying to connect
                int readyChannels = selector.select(timeRemaining);

                if (logger.isLoggable(Level.FINER))
                    logger.finer(this.toString() + " no of channels ready: " + readyChannels);

                // There are no real time guarantees on the time out of the select API used above.
                // This check is necessary
                // a) to guard against cases where the select returns faster than expected.
                // b) for cases where no channels could connect with in the time out
                if (readyChannels != 0) {
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                    while (keyIterator.hasNext()) {

                        SelectionKey key = keyIterator.next();
                        SocketChannel ch = (SocketChannel) key.channel();

                        if (logger.isLoggable(Level.FINER))
                            logger.finer(this.toString() + " processing the channel :" + ch);// this traces the IP by default

                        boolean connected = false;
                        try {
                            connected = ch.finishConnect();

                            // ch.finishConnect should either return true or throw an exception
                            // as we have subscribed for OP_CONNECT.
                            assert connected == true : "finishConnect on channel:" + ch + " cannot be false";

                            selectedChannel = ch;

                            if (logger.isLoggable(Level.FINER))
                                logger.finer(this.toString() + " selected the channel :" + selectedChannel);

                            break;
                        }
                        catch (IOException ex) {
                            if (logger.isLoggable(Level.FINER))
                                logger.finer(this.toString() + " the exception: " + ex.getClass() + " with message: " + ex.getMessage()
                                        + " occured while processing the channel: " + ch);
                            updateSelectedException(ex, this.toString());
                            // close the channel pro-actively so that we do not
                            // rely to network resources
                            ch.close();
                        }

                        // unregister the key and remove from the selector's selectedKeys
                        key.cancel();
                        keyIterator.remove();
                        noOfOutstandingChannels--;
                    }
                }

                timerNow = System.currentTimeMillis();
            }
        }
        catch (IOException ex) {
            // in case of an exception, close the selected channel.
            // All other channels will be closed in the finally block,
            // as they need to be closed irrespective of a success/failure
            close(selectedChannel);
            throw ex;
        }
        finally {
            // close the selector
            // As per java docs, on selector.close(), any uncancelled keys still
            // associated with this
            // selector are invalidated, their channels are deregistered, and any other
            // resources associated with this selector are released.
            // So, its not necessary to cancel each key again
            close(selector);

            // Close all channels except the selected one.
            // As we close channels pro-actively in the try block,
            // its possible that we close a channel twice.
            // Closing a channel second time is a no-op.
            // This code is should be in the finally block to guard against cases where
            // we pre-maturely exit try block due to an exception in selector or other places.
            for (SocketChannel s : socketChannels) {
                if (s != selectedChannel) {
                    close(s);
                }
            }
        }

        // if a channel was selected, make the necessary updates
        if (selectedChannel != null) {
            // Note that this must be done after selector is closed. Otherwise,
            // we would get an illegalBlockingMode exception at run time.
            selectedChannel.configureBlocking(true);
            selectedSocket = selectedChannel.socket();

            result = Result.SUCCESS;
        }
    }

    // This method contains the old logic of connecting to
    // a socket of one of the IPs corresponding to a given host name.
    // In the old code below, the logic around 0 timeout has been removed as
    // 0 timeout is not allowed. The code has been re-factored so that the logic
    // is common for hostName or InetAddress.
    private Socket getDefaultSocket(String hostName,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException {
        // Open the socket, with or without a timeout, throwing an UnknownHostException
        // if there is a failure to resolve the host name to an InetSocketAddress.
        //
        // Note that Socket(host, port) throws an UnknownHostException if the host name
        // cannot be resolved, but that InetSocketAddress(host, port) does not - it sets
        // the returned InetSocketAddress as unresolved.
        InetSocketAddress addr = new InetSocketAddress(hostName, portNumber);
        return getConnectedSocket(addr, timeoutInMilliSeconds);
    }

    private Socket getConnectedSocket(InetAddress inetAddr,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(inetAddr, portNumber);
        return getConnectedSocket(addr, timeoutInMilliSeconds);
    }

    private Socket getConnectedSocket(InetSocketAddress addr,
            int timeoutInMilliSeconds) throws IOException {
        assert timeoutInMilliSeconds != 0 : "timeout cannot be zero";
        if (addr.isUnresolved())
            throw new java.net.UnknownHostException();
        selectedSocket = new Socket();
        selectedSocket.connect(addr, timeoutInMilliSeconds);
        return selectedSocket;
    }

    private void findSocketUsingThreading(LinkedList<InetAddress> inetAddrs,
            int portNumber,
            int timeoutInMilliSeconds) throws IOException, InterruptedException {
        assert timeoutInMilliSeconds != 0 : "The timeout cannot be zero";
        
        assert inetAddrs.isEmpty() == false : "Number of inetAddresses should not be zero in this function";

        LinkedList<Socket> sockets = new LinkedList<>();
        LinkedList<SocketConnector> socketConnectors = new LinkedList<>();

        try {

            // create a socket, inetSocketAddress and a corresponding socketConnector per inetAddress
            noOfSpawnedThreads = inetAddrs.size();
            for (InetAddress inetAddress : inetAddrs) {
                Socket s = new Socket();
                sockets.add(s);

                InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, portNumber);

                SocketConnector socketConnector = new SocketConnector(s, inetSocketAddress, timeoutInMilliSeconds, this);
                socketConnectors.add(socketConnector);
            }

            // acquire parent lock and spawn all threads
            synchronized (parentThreadLock) {
                for (SocketConnector sc : socketConnectors) {
                    threadPoolExecutor.execute(sc);
                }

                long timerNow = System.currentTimeMillis();
                long timerExpire = timerNow + timeoutInMilliSeconds;

                // The below loop is to guard against the spurious wake up problem
                while (true) {
                    long timeRemaining = timerExpire - timerNow;

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString() + " TimeRemaining:" + timeRemaining + "; Result:" + result + "; Max. open thread count: "
                                + threadPoolExecutor.getLargestPoolSize() + "; Current open thread count:" + threadPoolExecutor.getActiveCount());
                    }

                    // if there is no time left or if the result is determined, break.
                    // Note that a dirty read of result is totally fine here.
                    // Since this thread holds the parentThreadLock, even if we do a dirty
                    // read here, the child thread, after updating the result, would not be
                    // able to call notify on the parentThreadLock
                    // (and thus finish execution) as it would be waiting on parentThreadLock
                    // held by this thread(the parent thread).
                    // So, this thread will wait again and then be notified by the childThread.
                    // On the other hand, if we try to take socketFinderLock here to avoid
                    // dirty read, we would introduce a dead lock due to the
                    // reverse order of locking in updateResult method.
                    if (timeRemaining <= 0 || (!result.equals(Result.UNKNOWN)))
                        break;

                    parentThreadLock.wait(timeRemaining);

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(this.toString() + " The parent thread wokeup.");
                    }

                    timerNow = System.currentTimeMillis();
                }

            }

        }
        finally {
            // Close all sockets except the selected one.
            // As we close sockets pro-actively in the child threads,
            // its possible that we close a socket twice.
            // Closing a socket second time is a no-op.
            // If a child thread is waiting on the connect call on a socket s,
            // closing the socket s here ensures that an exception is thrown
            // in the child thread immediately. This mitigates the problem
            // of thread explosion by ensuring that unnecessary threads die
            // quickly without waiting for "min(timeOut, 21)" seconds
            for (Socket s : sockets) {
                if (s != selectedSocket) {
                    close(s);
                }
            }
        }
        
        if (selectedSocket != null) {          
            result = Result.SUCCESS;
        }
    }

    /**
     * search result
     */
    Result getResult() {
        return result;
    }

    void close(Selector selector) {
        if (null != selector) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing Selector");

            try {
                selector.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored the following error while closing Selector", e);
            }
        }
    }

    void close(Socket socket) {
        if (null != socket) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing TCP socket:" + socket);

            try {
                socket.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored the following error while closing socket", e);
            }
        }
    }

    void close(SocketChannel socketChannel) {
        if (null != socketChannel) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing TCP socket channel:" + socketChannel);

            try {
                socketChannel.close();
            }
            catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + "Ignored the following error while closing socketChannel", e);
            }
        }
    }

    /**
     * Used by socketConnector threads to notify the socketFinder of their connection attempt result(a connected socket or exception). It updates the
     * result, socket and exception variables of socketFinder object. This method notifies the parent thread if a socket is found or if all the
     * spawned threads have notified. It also closes a socket if it is not selected for use by socketFinder.
     * 
     * @param socket
     *            the SocketConnector's socket
     * @param exception
     *            Exception that occurred in socket connector thread
     * @param threadId
     *            Id of the calling Thread for diagnosis
     */
    void updateResult(Socket socket,
            IOException exception,
            String threadId) {
        if (result.equals(Result.UNKNOWN)) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("The following child thread is waiting for socketFinderLock:" + threadId);
            }

            synchronized (socketFinderlock) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer("The following child thread acquired socketFinderLock:" + threadId);
                }

                if (result.equals(Result.UNKNOWN)) {
                    // if the connection was successful and no socket has been
                    // selected yet
                    if (exception == null && selectedSocket == null) {
                        selectedSocket = socket;
                        result = Result.SUCCESS;
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer("The socket of the following thread has been chosen:" + threadId);
                        }
                    }

                    // if an exception occurred
                    if (exception != null) {
                        updateSelectedException(exception, threadId);
                    }
                }

                noOfThreadsThatNotified++;

                // if all threads notified, but the result is still unknown,
                // update the result to failure
                if ((noOfThreadsThatNotified >= noOfSpawnedThreads) && result.equals(Result.UNKNOWN)) {
                    result = Result.FAILURE;
                }

                if (!result.equals(Result.UNKNOWN)) {
                    // 1) Note that at any point of time, there is only one
                    // thread(parent/child thread) competing for parentThreadLock.
                    // 2) The only time where a child thread could be waiting on
                    // parentThreadLock is before the wait call in the parentThread
                    // 3) After the above happens, the parent thread waits to be
                    // notified on parentThreadLock. After being notified,
                    // it would be the ONLY thread competing for the lock.
                    // for the following reasons
                    // a) The parentThreadLock is taken while holding the socketFinderLock.
                    // So, all child threads, except one, block on socketFinderLock
                    // (not parentThreadLock)
                    // b) After parentThreadLock is notified by a child thread, the result
                    // would be known(Refer the double-checked locking done at the
                    // start of this method). So, all child threads would exit
                    // as no-ops and would never compete with parent thread
                    // for acquiring parentThreadLock
                    // 4) As the parent thread is the only thread that competes for the
                    // parentThreadLock, it need not wait to acquire the lock once it wakes
                    // up and gets scheduled.
                    // This results in better performance as it would close unnecessary
                    // sockets and thus help child threads die quickly.

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer("The following child thread is waiting for parentThreadLock:" + threadId);
                    }

                    synchronized (parentThreadLock) {
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer("The following child thread acquired parentThreadLock:" + threadId);
                        }

                        parentThreadLock.notify();
                    }

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer("The following child thread released parentThreadLock and notified the parent thread:" + threadId);
                    }
                }
            }

            if (logger.isLoggable(Level.FINER)) {
                logger.finer("The following child thread released socketFinderLock:" + threadId);
            }
        }

    }

    /**
     * Updates the selectedException if
     * <p>
     * a) selectedException is null
     * <p>
     * b) ex is a non-socketTimeoutException and selectedException is a socketTimeoutException
     * <p>
     * If there are multiple exceptions, that are not related to socketTimeout the first non-socketTimeout exception is picked. If all exceptions are
     * related to socketTimeout, the first exception is picked. Note: This method is not thread safe. The caller should ensure thread safety.
     * 
     * @param ex
     *            the IOException
     * @param traceId
     *            the traceId of the thread
     */
    public void updateSelectedException(IOException ex,
            String traceId) {
        boolean updatedException = false;
        if (selectedException == null ||
        	(!(ex instanceof SocketTimeoutException)) && (selectedException instanceof SocketTimeoutException)) {
            selectedException = ex;
            updatedException = true;
        }

        if (updatedException) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("The selected exception is updated to the following: ExceptionType:" + ex.getClass() + "; ExceptionMessage:"
                        + ex.getMessage() + "; by the following thread:" + traceId);
            }
        }
    }

    /**
     * Used fof tracing
     * 
     * @return traceID string
     */
    public String toString() {
        return traceID;
    }
}

/**
 * This is used to connect a socket in a separate thread
 */
final class SocketConnector implements Runnable {
    // socket on which connection attempt would be made
    private final Socket socket;

    // the socketFinder associated with this connector
    private final SocketFinder socketFinder;

    // inetSocketAddress to connect to
    private final InetSocketAddress inetSocketAddress;

    // timeout in milliseconds
    private final int timeoutInMilliseconds;

    // Logging variables
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SocketConnector");
    private final String traceID;

    // Id of the thread. used for diagnosis
    private final String threadID;

    // a counter used to give unique IDs to each connector thread.
    // this will have the id of the thread that was last created.
    private static long lastThreadID = 0;

    /**
     * Constructs a new SocketConnector object with the associated socket and socketFinder
     */
    SocketConnector(Socket socket,
            InetSocketAddress inetSocketAddress,
            int timeOutInMilliSeconds,
            SocketFinder socketFinder) {
        this.socket = socket;
        this.inetSocketAddress = inetSocketAddress;
        this.timeoutInMilliseconds = timeOutInMilliSeconds;
        this.socketFinder = socketFinder;
        this.threadID = Long.toString(nextThreadID());
        this.traceID = "SocketConnector:" + this.threadID + "(" + socketFinder.toString() + ")";
    }

    /**
     * If search for socket has not finished, this function tries to connect a socket(with a timeout) synchronously. It further notifies the
     * socketFinder the result of the connection attempt
     */
    public void run() {
        IOException exception = null;
        // Note that we do not need socketFinder lock here
        // as we update nothing in socketFinder based on the condition.
        // So, its perfectly fine to make a dirty read.
        SocketFinder.Result result = socketFinder.getResult();
        if (result.equals(SocketFinder.Result.UNKNOWN)) {
            try {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(
                            this.toString() + " connecting to InetSocketAddress:" + inetSocketAddress + " with timeout:" + timeoutInMilliseconds);
                }

                socket.connect(inetSocketAddress, timeoutInMilliseconds);
            }
            catch (IOException ex) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(this.toString() + " exception:" + ex.getClass() + " with message:" + ex.getMessage()
                            + " occured while connecting to InetSocketAddress:" + inetSocketAddress);
                }
                exception = ex;
            }

            socketFinder.updateResult(socket, exception, this.toString());
        }

    }

    /**
     * Used for tracing
     * 
     * @return traceID string
     */
    public String toString() {
        return traceID;
    }

    /**
     * Generates the next unique thread id.
     */
    private static synchronized long nextThreadID() {
        if (lastThreadID == Long.MAX_VALUE) {
            if (logger.isLoggable(Level.FINER))
                logger.finer("Resetting the Id count");
            lastThreadID = 1;
        }
        else {
            lastThreadID++;
        }
        return lastThreadID;
    }
}

/**
 * TDSWriter implements the client to server TDS data pipe.
 */
final class TDSWriter {
    private static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Writer");
    private final String traceID;

    final public String toString() {
        return traceID;
    }

    private final TDSChannel tdsChannel;
    private final SQLServerConnection con;

    // Flag to indicate whether data written via writeXXX() calls
    // is loggable. Data is normally loggable. But sensitive
    // data, such as user credentials, should never be logged for
    // security reasons.
    private boolean dataIsLoggable = true;

    void setDataLoggable(boolean value) {
        dataIsLoggable = value;
    }

    private TDSCommand command = null;

    // TDS message type (Query, RPC, DTC, etc.) sent at the beginning
    // of every TDS message header. Value is set when starting a new
    // TDS message of the specified type.
    private byte tdsMessageType;

    private volatile int sendResetConnection = 0;

    // Size (in bytes) of the TDS packets to/from the server.
    // This size is normally fixed for the life of the connection,
    // but it can change once after the logon packet because packet
    // size negotiation happens at logon time.
    private int currentPacketSize = 0;

    // Size of the TDS packet header, which is:
    // byte type
    // byte status
    // short length
    // short SPID
    // byte packet
    // byte window
    private final static int TDS_PACKET_HEADER_SIZE = 8;
    private final static byte[] placeholderHeader = new byte[TDS_PACKET_HEADER_SIZE];

    // Intermediate array used to convert typically "small" values such as fixed-length types
    // (byte, int, long, etc.) and Strings from their native form to bytes for sending to
    // the channel buffers.
    private byte valueBytes[] = new byte[256];

    // Monotonically increasing packet number associated with the current message
    private int packetNum = 0;

    // Bytes for sending decimal/numeric data
    private final static int BYTES4 = 4;
    private final static int BYTES8 = 8;
    private final static int BYTES12 = 12;
    private final static int BYTES16 = 16;
    public final static int BIGDECIMAL_MAX_LENGTH = 0x11;

    // is set to true when EOM is sent for the current message.
    // Note that this variable will never be accessed from multiple threads
    // simultaneously and so it need not be volatile
    private boolean isEOMSent = false;

    boolean isEOMSent() {
        return isEOMSent;
    }

    // Packet data buffers
    private ByteBuffer stagingBuffer;
    private ByteBuffer socketBuffer;
    private ByteBuffer logBuffer;

    private CryptoMetadata cryptoMeta = null;

    TDSWriter(TDSChannel tdsChannel,
            SQLServerConnection con) {
        this.tdsChannel = tdsChannel;
        this.con = con;
        traceID = "TDSWriter@" + Integer.toHexString(hashCode()) + " (" + con.toString() + ")";
    }

    // TDS message start/end operations

    void preparePacket() throws SQLServerException {
        if (tdsChannel.isLoggingPackets()) {
            Arrays.fill(logBuffer.array(), (byte) 0xFE);
            ((Buffer)logBuffer).clear();
        }

        // Write a placeholder packet header. This will be replaced
        // with the real packet header when the packet is flushed.
        writeBytes(placeholderHeader);
    }

    /**
     * Start a new TDS message.
     */
    void writeMessageHeader() throws SQLServerException {
        // TDS 7.2 & later:
        // Include ALL_Headers/MARS header in message's first packet
        // Note: The PKT_BULK message does not nees this ALL_HEADERS
        if ((TDS.PKT_QUERY == tdsMessageType || TDS.PKT_DTC == tdsMessageType || TDS.PKT_RPC == tdsMessageType)) {
            boolean includeTraceHeader = false;
            int totalHeaderLength = TDS.MESSAGE_HEADER_LENGTH;
            if (TDS.PKT_QUERY == tdsMessageType || TDS.PKT_RPC == tdsMessageType) {
                if (con.isDenaliOrLater() && !ActivityCorrelator.getCurrent().IsSentToServer() && Util.IsActivityTraceOn()) {
                    includeTraceHeader = true;
                    totalHeaderLength += TDS.TRACE_HEADER_LENGTH;
                }
            }
            writeInt(totalHeaderLength); // allHeaders.TotalLength (DWORD)
            writeInt(TDS.MARS_HEADER_LENGTH); // MARS header length (DWORD)
            writeShort((short) 2); // allHeaders.HeaderType(MARS header) (USHORT)
            writeBytes(con.getTransactionDescriptor());
            writeInt(1); // marsHeader.OutstandingRequestCount
            if (includeTraceHeader) {
                writeInt(TDS.TRACE_HEADER_LENGTH); // trace header length (DWORD)
                writeTraceHeaderData();
                ActivityCorrelator.setCurrentActivityIdSentFlag(); // set the flag to indicate this ActivityId is sent
            }
        }
    }

    void writeTraceHeaderData() throws SQLServerException {
        ActivityId activityId = ActivityCorrelator.getCurrent();
        final byte[] actIdByteArray = Util.asGuidByteArray(activityId.getId());
        long seqNum = activityId.getSequence();
        writeShort(TDS.HEADERTYPE_TRACE);   // trace header type
        writeBytes(actIdByteArray, 0, actIdByteArray.length);  // guid part of ActivityId
        writeInt((int) seqNum);  // sequence number of ActivityId

        if (logger.isLoggable(Level.FINER))
            logger.finer("Send Trace Header - ActivityID: " + activityId.toString());
    }

    /**
     * Convenience method to prepare the TDS channel for writing and start a new TDS message.
     *
     * @param command
     *            The TDS command
     * @param tdsMessageType
     *            The TDS message type (PKT_QUERY, PKT_RPC, etc.)
     */
    void startMessage(TDSCommand command,
            byte tdsMessageType) throws SQLServerException {
        this.command = command;
        this.tdsMessageType = tdsMessageType;
        this.packetNum = 0;
        this.isEOMSent = false;
        this.dataIsLoggable = true;

        // If the TDS packet size has changed since the last request
        // (which should really only happen after the login packet)
        // then allocate new buffers that are the correct size.
        int negotiatedPacketSize = con.getTDSPacketSize();
        if (currentPacketSize != negotiatedPacketSize) {
            socketBuffer = ByteBuffer.allocate(negotiatedPacketSize).order(ByteOrder.LITTLE_ENDIAN);
            stagingBuffer = ByteBuffer.allocate(negotiatedPacketSize).order(ByteOrder.LITTLE_ENDIAN);
            logBuffer = ByteBuffer.allocate(negotiatedPacketSize).order(ByteOrder.LITTLE_ENDIAN);
            currentPacketSize = negotiatedPacketSize;
        }

        ((Buffer) socketBuffer).position(((Buffer) socketBuffer).limit());
        ((Buffer)stagingBuffer).clear();

        preparePacket();
        writeMessageHeader();
    }

    final void endMessage() throws SQLServerException {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(toString() + " Finishing TDS message");
        writePacket(TDS.STATUS_BIT_EOM);
    }

    // If a complete request has not been sent to the server,
    // the client MUST send the next packet with both ignore bit (0x02) and EOM bit (0x01)
    // set in the status to cancel the request.
    final boolean ignoreMessage() throws SQLServerException {
        if (packetNum > 0) {
            assert !isEOMSent;

            if (logger.isLoggable(Level.FINER))
                logger.finest(toString() + " Finishing TDS message by sending ignore bit and end of message");
            writePacket(TDS.STATUS_BIT_EOM | TDS.STATUS_BIT_ATTENTION);
            return true;
        }
        return false;
    }

    final void resetPooledConnection() {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(toString() + " resetPooledConnection");
        sendResetConnection = TDS.STATUS_BIT_RESET_CONN;
    }

    // Primitive write operations

    void writeByte(byte value) throws SQLServerException {
        if (stagingBuffer.remaining() >= 1) {
            stagingBuffer.put(value);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.put(value);
                else
                    ((Buffer)logBuffer).position(((Buffer)logBuffer).position() + 1);
            }
        }
        else {
            valueBytes[0] = value;
            writeWrappedBytes(valueBytes, 1);
        }
    }

    /**
     * writing sqlCollation information for sqlVariant type when sending character types.
     * 
     * @param variantType
     * @throws SQLServerException
     */
    void writeCollationForSqlVariant(SqlVariant variantType) throws SQLServerException {
        writeInt(variantType.getCollation().getCollationInfo());
        writeByte((byte) (variantType.getCollation().getCollationSortID() & 0xFF));
    }

    void writeChar(char value) throws SQLServerException {
        if (stagingBuffer.remaining() >= 2) {
            stagingBuffer.putChar(value);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.putChar(value);
                else
                    ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + 2);
            }
        }
        else {
            Util.writeShort((short) value, valueBytes, 0);
            writeWrappedBytes(valueBytes, 2);
        }
    }

    void writeShort(short value) throws SQLServerException {
        if (stagingBuffer.remaining() >= 2) {
            stagingBuffer.putShort(value);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.putShort(value);
                else
                    ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + 2);
            }
        }
        else {
            Util.writeShort(value, valueBytes, 0);
            writeWrappedBytes(valueBytes, 2);
        }
    }

    void writeInt(int value) throws SQLServerException {
        if (stagingBuffer.remaining() >= 4) {
            stagingBuffer.putInt(value);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.putInt(value);
                else
                    ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + 4);
            }
        }
        else {
            Util.writeInt(value, valueBytes, 0);
            writeWrappedBytes(valueBytes, 4);
        }
    }

    /**
     * Append a real value in the TDS stream.
     * 
     * @param value
     *            the data value
     */
    void writeReal(Float value) throws SQLServerException {
        writeInt(Float.floatToRawIntBits(value));
    }

    /**
     * Append a double value in the TDS stream.
     * 
     * @param value
     *            the data value
     */
    void writeDouble(double value) throws SQLServerException {
        if (stagingBuffer.remaining() >= 8) {
            stagingBuffer.putDouble(value);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.putDouble(value);
                else
                    ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + 8);
            }
        }
        else {
            long bits = Double.doubleToLongBits(value);
            long mask = 0xFF;
            int nShift = 0;
            for (int i = 0; i < 8; i++) {
                writeByte((byte) ((bits & mask) >> nShift));
                nShift += 8;
                mask = mask << 8;
            }
        }
    }

    /**
     * Append a big decimal in the TDS stream.
     * 
     * @param bigDecimalVal
     *            the big decimal data value
     * @param srcJdbcType
     *            the source JDBCType
     * @param precision
     *            the precision of the data value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     */
    void writeBigDecimal(BigDecimal bigDecimalVal,
            int srcJdbcType,
            int precision,
            int scale) throws SQLServerException {
        /*
         * Length including sign byte One 1-byte unsigned integer that represents the sign of the decimal value (0 => Negative, 1 => positive) One 4-,
         * 8-, 12-, or 16-byte signed integer that represents the decimal value multiplied by 10^scale.
         */

        /*
         * setScale of all BigDecimal value based on metadata as scale is not sent seperately for individual value. Use the rounding used in Server.
         * Say, for BigDecimal("0.1"), if scale in metdadata is 0, then ArithmeticException would be thrown if RoundingMode is not set
         */
        bigDecimalVal = bigDecimalVal.setScale(scale, RoundingMode.HALF_UP);

        // data length + 1 byte for sign
        int bLength = BYTES16 + 1;
        writeByte((byte) (bLength));

        // Byte array to hold all the data and padding bytes.
        byte[] bytes = new byte[bLength];

        byte[] valueBytes = DDC.convertBigDecimalToBytes(bigDecimalVal, scale);
        // removing the precision and scale information from the valueBytes array
        System.arraycopy(valueBytes, 2, bytes, 0, valueBytes.length - 2);
        writeBytes(bytes);
    }
    
    /**
     * Append a big decimal inside sql_variant in the TDS stream.
     * 
     * @param bigDecimalVal
     *            the big decimal data value
     * @param srcJdbcType
     *            the source JDBCType
     */
    void writeSqlVariantInternalBigDecimal(BigDecimal bigDecimalVal,
            int srcJdbcType) throws SQLServerException {
        /*
         * Length including sign byte One 1-byte unsigned integer that represents the sign of the decimal value (0 => Negative, 1 => positive) One
         * 16-byte signed integer that represents the decimal value multiplied by 10^scale. In sql_variant, we send the bigdecimal with precision 38,
         * therefore we use 16 bytes for the maximum size of this integer.
         */

        boolean isNegative = (bigDecimalVal.signum() < 0);
        BigInteger bi = bigDecimalVal.unscaledValue();
        if (isNegative)
        {
            bi = bi.negate();
        }
        int bLength;
        bLength = BYTES16;

        writeByte((byte) (isNegative ? 0 : 1));

        // Get the bytes of the BigInteger value. It is in reverse order, with
        // most significant byte in 0-th element. We need to reverse it first before sending over TDS.
        byte[] unscaledBytes = bi.toByteArray();

        if (unscaledBytes.length > bLength) {
            // If precession of input is greater than maximum allowed (p><= 38) throw Exception
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
            Object[] msgArgs = {JDBCType.of(srcJdbcType)};
            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET, null);
        }

        // Byte array to hold all the reversed and padding bytes.
        byte[] bytes = new byte[bLength];

        // We need to fill up the rest of the array with zeros, as unscaledBytes may have less bytes
        // than the required size for TDS.
        int remaining = bLength - unscaledBytes.length;

        // Reverse the bytes.
        int i, j;
        for (i = 0, j = unscaledBytes.length - 1; i < unscaledBytes.length;)
            bytes[i++] = unscaledBytes[j--];

        // Fill the rest of the array with zeros.
        for (; i < remaining; i++)
        {
            bytes[i] = (byte) 0x00;
        }
        writeBytes(bytes);
    }

    void writeSmalldatetime(String value) throws SQLServerException {
        GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
        long utcMillis;    // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00 GMT)
        java.sql.Timestamp timestampValue = java.sql.Timestamp.valueOf(value);
        utcMillis = timestampValue.getTime();

        // Load the calendar with the desired value
        calendar.setTimeInMillis(utcMillis);

        // Number of days since the SQL Server Base Date (January 1, 1900)
        int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(calendar.get(Calendar.YEAR), calendar.get(Calendar.DAY_OF_YEAR), TDS.BASE_YEAR_1900);

        // Next, figure out the number of milliseconds since midnight of the current day.
        int millisSinceMidnight = 1000 * calendar.get(Calendar.SECOND) + // Seconds into the current minute
                60 * 1000 * calendar.get(Calendar.MINUTE) + // Minutes into the current hour
                60 * 60 * 1000 * calendar.get(Calendar.HOUR_OF_DAY); // Hours into the current day

        // The last millisecond of the current day is always rounded to the first millisecond
        // of the next day because DATETIME is only accurate to 1/300th of a second.
        if (1000 * 60 * 60 * 24 - 1 <= millisSinceMidnight) {
            ++daysSinceSQLBaseDate;
            millisSinceMidnight = 0;
        }

        // Number of days since the SQL Server Base Date (January 1, 1900)
        writeShort((short) daysSinceSQLBaseDate);

        int secondsSinceMidnight = (millisSinceMidnight / 1000);
        int minutesSinceMidnight = (secondsSinceMidnight / 60);

        // Values that are 29.998 seconds or less are rounded down to the nearest minute
        minutesSinceMidnight = ((secondsSinceMidnight % 60) > 29.998) ? minutesSinceMidnight + 1 : minutesSinceMidnight;

        // Minutes since midnight
        writeShort((short) minutesSinceMidnight);
    }

    void writeDatetime(String value) throws SQLServerException {
        GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
        long utcMillis;    // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00 GMT)
        int subSecondNanos;
        java.sql.Timestamp timestampValue = java.sql.Timestamp.valueOf(value);
        utcMillis = timestampValue.getTime();
        subSecondNanos = timestampValue.getNanos();

        // Load the calendar with the desired value
        calendar.setTimeInMillis(utcMillis);

        // Number of days there have been since the SQL Base Date.
        // These are based on SQL Server algorithms
        int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(calendar.get(Calendar.YEAR), calendar.get(Calendar.DAY_OF_YEAR), TDS.BASE_YEAR_1900);

        // Number of milliseconds since midnight of the current day.
        int millisSinceMidnight = (subSecondNanos + Nanos.PER_MILLISECOND / 2) / Nanos.PER_MILLISECOND + // Millis into the current second
                1000 * calendar.get(Calendar.SECOND) + // Seconds into the current minute
                60 * 1000 * calendar.get(Calendar.MINUTE) + // Minutes into the current hour
                60 * 60 * 1000 * calendar.get(Calendar.HOUR_OF_DAY); // Hours into the current day

        // The last millisecond of the current day is always rounded to the first millisecond
        // of the next day because DATETIME is only accurate to 1/300th of a second.
        if (1000 * 60 * 60 * 24 - 1 <= millisSinceMidnight) {
            ++daysSinceSQLBaseDate;
            millisSinceMidnight = 0;
        }

        // Last-ditch verification that the value is in the valid range for the
        // DATETIMEN TDS data type (1/1/1753 to 12/31/9999). If it's not, then
        // throw an exception now so that statement execution is safely canceled.
        // Attempting to put an invalid value on the wire would result in a TDS
        // exception, which would close the connection.
        // These are based on SQL Server algorithms
        if (daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1753, 1, TDS.BASE_YEAR_1900)
                || daysSinceSQLBaseDate >= DDC.daysSinceBaseDate(10000, 1, TDS.BASE_YEAR_1900)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
            Object[] msgArgs = {SSType.DATETIME};
            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW, DriverError.NOT_SET, null);
        }

        // Number of days since the SQL Server Base Date (January 1, 1900)
        writeInt(daysSinceSQLBaseDate);

        // Milliseconds since midnight (at a resolution of three hundredths of a second)
        writeInt((3 * millisSinceMidnight + 5) / 10);
    }

    void writeDate(String value) throws SQLServerException {
        GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
        long utcMillis;
        java.sql.Date dateValue = java.sql.Date.valueOf(value);
        utcMillis = dateValue.getTime();

        // Load the calendar with the desired value
        calendar.setTimeInMillis(utcMillis);

        writeScaledTemporal(calendar, 0, // subsecond nanos (none for a date value)
                0, // scale (dates are not scaled)
                SSType.DATE);
    }

    void writeTime(java.sql.Timestamp value,
            int scale) throws SQLServerException {
        GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
        long utcMillis;    // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00 GMT)
        int subSecondNanos;
        utcMillis = value.getTime();
        subSecondNanos = value.getNanos();

        // Load the calendar with the desired value
        calendar.setTimeInMillis(utcMillis);

        writeScaledTemporal(calendar, subSecondNanos, scale, SSType.TIME);
    }

    void writeDateTimeOffset(Object value,
            int scale,
            SSType destSSType) throws SQLServerException {
        GregorianCalendar calendar;
        TimeZone timeZone; // Time zone to associate with the value in the Gregorian calendar
        long utcMillis;    // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00 GMT)
        int subSecondNanos;
        int minutesOffset;

        microsoft.sql.DateTimeOffset dtoValue = (microsoft.sql.DateTimeOffset) value;
        utcMillis = dtoValue.getTimestamp().getTime();
        subSecondNanos = dtoValue.getTimestamp().getNanos();
        minutesOffset = dtoValue.getMinutesOffset();

        // If the target data type is DATETIMEOFFSET, then use UTC for the calendar that
        // will hold the value, since writeRPCDateTimeOffset expects a UTC calendar.
        // Otherwise, when converting from DATETIMEOFFSET to other temporal data types,
        // use a local time zone determined by the minutes offset of the value, since
        // the writers for those types expect local calendars.
        timeZone = (SSType.DATETIMEOFFSET == destSSType) ? UTC.timeZone : new SimpleTimeZone(minutesOffset * 60 * 1000, "");

        calendar = new GregorianCalendar(timeZone, Locale.US);
        calendar.setLenient(true);
        calendar.clear();
        calendar.setTimeInMillis(utcMillis);

        writeScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);

        writeShort((short) minutesOffset);
    }

    void writeOffsetDateTimeWithTimezone(OffsetDateTime offsetDateTimeValue,
            int scale) throws SQLServerException {
        GregorianCalendar calendar;
        TimeZone timeZone;
        long utcMillis;
        int subSecondNanos;
        int minutesOffset = 0;

        try {
            // offsetTimeValue.getOffset() returns a ZoneOffset object which has only hours and minutes
            // components. So the result of the division will be an integer always. SQL Server also supports
            // offsets in minutes precision.
            minutesOffset = offsetDateTimeValue.getOffset().getTotalSeconds() / 60;
        }
        catch (Exception e) {
            throw new SQLServerException(SQLServerException.getErrString("R_zoneOffsetError"), null, // SQLState is null as this error is generated in
                                                                                                     // the driver
                    0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
                    e);
        }
        subSecondNanos = offsetDateTimeValue.getNano();

        // writeScaledTemporal() expects subSecondNanos in 9 digits precssion
        // but getNano() used in OffsetDateTime returns precession based on nanoseconds read from csv
        // padding zeros to match the expectation of writeScaledTemporal()
        int padding = 9 - String.valueOf(subSecondNanos).length();
        while (padding > 0) {
            subSecondNanos = subSecondNanos * 10;
            padding--;
        }

        // For TIME_WITH_TIMEZONE, use UTC for the calendar that will hold the value
        timeZone = UTC.timeZone;

        // The behavior is similar to microsoft.sql.DateTimeOffset
        // In Timestamp format, only YEAR needs to have 4 digits. The leading zeros for the rest of the fields can be omitted.
        String offDateTimeStr = String.format("%04d", offsetDateTimeValue.getYear()) + '-' + offsetDateTimeValue.getMonthValue() + '-'
                + offsetDateTimeValue.getDayOfMonth() + ' ' + offsetDateTimeValue.getHour() + ':' + offsetDateTimeValue.getMinute() + ':'
                + offsetDateTimeValue.getSecond();
        utcMillis = Timestamp.valueOf(offDateTimeStr).getTime();
        calendar = initializeCalender(timeZone);
        calendar.setTimeInMillis(utcMillis);

        // Local timezone value in minutes
        int minuteAdjustment = ((TimeZone.getDefault().getRawOffset()) / (60 * 1000));
        // check if date is in day light savings and add daylight saving minutes
        if (TimeZone.getDefault().inDaylightTime(calendar.getTime()))
            minuteAdjustment += (TimeZone.getDefault().getDSTSavings()) / (60 * 1000);
        // If the local time is negative then positive minutesOffset must be subtracted from calender
        minuteAdjustment += (minuteAdjustment < 0) ? (minutesOffset * (-1)) : minutesOffset;
        calendar.add(Calendar.MINUTE, minuteAdjustment);

        writeScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);
        writeShort((short) minutesOffset);
    }

    void writeOffsetTimeWithTimezone(OffsetTime offsetTimeValue,
            int scale) throws SQLServerException {
        GregorianCalendar calendar;
        TimeZone timeZone;
        long utcMillis;
        int subSecondNanos;
        int minutesOffset = 0;

        try {
            // offsetTimeValue.getOffset() returns a ZoneOffset object which has only hours and minutes
            // components. So the result of the division will be an integer always. SQL Server also supports
            // offsets in minutes precision.
            minutesOffset = offsetTimeValue.getOffset().getTotalSeconds() / 60;
        }
        catch (Exception e) {
            throw new SQLServerException(SQLServerException.getErrString("R_zoneOffsetError"), null, // SQLState is null as this error is generated in
                                                                                                     // the driver
                    0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
                    e);
        }
        subSecondNanos = offsetTimeValue.getNano();

        // writeScaledTemporal() expects subSecondNanos in 9 digits precssion
        // but getNano() used in OffsetDateTime returns precession based on nanoseconds read from csv
        // padding zeros to match the expectation of writeScaledTemporal()
        int padding = 9 - String.valueOf(subSecondNanos).length();
        while (padding > 0) {
            subSecondNanos = subSecondNanos * 10;
            padding--;
        }

        // For TIME_WITH_TIMEZONE, use UTC for the calendar that will hold the value
        timeZone = UTC.timeZone;

        // Using TDS.BASE_YEAR_1900, based on SQL server behavious
        // If date only contains a time part, the return value is 1900, the base year.
        // https://msdn.microsoft.com/en-us/library/ms186313.aspx
        // In Timestamp format, leading zeros for the fields can be omitted.
        String offsetTimeStr = TDS.BASE_YEAR_1900 + "-01-01" + ' ' + offsetTimeValue.getHour() + ':' + offsetTimeValue.getMinute() + ':'
                + offsetTimeValue.getSecond();
        utcMillis = Timestamp.valueOf(offsetTimeStr).getTime();

        calendar = initializeCalender(timeZone);
        calendar.setTimeInMillis(utcMillis);

        int minuteAdjustment = (TimeZone.getDefault().getRawOffset()) / (60 * 1000);
        // check if date is in day light savings and add daylight saving minutes to Local timezone(in minutes)
        if (TimeZone.getDefault().inDaylightTime(calendar.getTime()))
            minuteAdjustment += ((TimeZone.getDefault().getDSTSavings()) / (60 * 1000));
        // If the local time is negative then positive minutesOffset must be subtracted from calender
        minuteAdjustment += (minuteAdjustment < 0) ? (minutesOffset * (-1)) : minutesOffset;
        calendar.add(Calendar.MINUTE, minuteAdjustment);

        writeScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);
        writeShort((short) minutesOffset);
    }

    void writeLong(long value) throws SQLServerException {
        if (stagingBuffer.remaining() >= 8) {
            stagingBuffer.putLong(value);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.putLong(value);
                else
                    ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + 8);
            }
        }
        else {
            valueBytes[0] = (byte) ((value >> 0) & 0xFF);
            valueBytes[1] = (byte) ((value >> 8) & 0xFF);
            valueBytes[2] = (byte) ((value >> 16) & 0xFF);
            valueBytes[3] = (byte) ((value >> 24) & 0xFF);
            valueBytes[4] = (byte) ((value >> 32) & 0xFF);
            valueBytes[5] = (byte) ((value >> 40) & 0xFF);
            valueBytes[6] = (byte) ((value >> 48) & 0xFF);
            valueBytes[7] = (byte) ((value >> 56) & 0xFF);
            writeWrappedBytes(valueBytes, 8);
        }
    }

    void writeBytes(byte[] value) throws SQLServerException {
        writeBytes(value, 0, value.length);
    }

    void writeBytes(byte[] value,
            int offset,
            int length) throws SQLServerException {
        assert length <= value.length;

        int bytesWritten = 0;
        int bytesToWrite;

        if (logger.isLoggable(Level.FINEST))
            logger.finest(toString() + " Writing " + length + " bytes");

        while ((bytesToWrite = length - bytesWritten) > 0) {
            if (0 == stagingBuffer.remaining())
                writePacket(TDS.STATUS_NORMAL);

            if (bytesToWrite > stagingBuffer.remaining())
                bytesToWrite = stagingBuffer.remaining();

            stagingBuffer.put(value, offset + bytesWritten, bytesToWrite);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.put(value, offset + bytesWritten, bytesToWrite);
                else
                    ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + bytesToWrite);
            }

            bytesWritten += bytesToWrite;
        }
    }

    void writeWrappedBytes(byte value[],
            int valueLength) throws SQLServerException {
        // This function should only be used to write a value that is longer than
        // what remains in the current staging buffer. However, the value must
        // be short enough to fit in an empty buffer.
        assert valueLength <= value.length;
        
        int remaining = stagingBuffer.remaining();
        assert remaining < valueLength;

        assert valueLength <= stagingBuffer.capacity();

        // Fill any remaining space in the staging buffer
        remaining = stagingBuffer.remaining();
        if (remaining > 0) {
            stagingBuffer.put(value, 0, remaining);
            if (tdsChannel.isLoggingPackets()) {
                if (dataIsLoggable)
                    logBuffer.put(value, 0, remaining);
                else
                    ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + remaining);
            }
        }

        writePacket(TDS.STATUS_NORMAL);

        // After swapping, the staging buffer should once again be empty, so the
        // remainder of the value can be written to it.
        stagingBuffer.put(value, remaining, valueLength - remaining);
        if (tdsChannel.isLoggingPackets()) {
            if (dataIsLoggable)
                logBuffer.put(value, remaining, valueLength - remaining);
            else
                ((Buffer)logBuffer).position( ((Buffer)logBuffer).position() + remaining);
        }
    }

    void writeString(String value) throws SQLServerException {
        int charsCopied = 0;
        int length = value.length();
        while (charsCopied < length) {
            int bytesToCopy = 2 * (length - charsCopied);

            if (bytesToCopy > valueBytes.length)
                bytesToCopy = valueBytes.length;

            int bytesCopied = 0;
            while (bytesCopied < bytesToCopy) {
                char ch = value.charAt(charsCopied++);
                valueBytes[bytesCopied++] = (byte) ((ch >> 0) & 0xFF);
                valueBytes[bytesCopied++] = (byte) ((ch >> 8) & 0xFF);
            }

            writeBytes(valueBytes, 0, bytesCopied);
        }
    }

    void writeStream(InputStream inputStream,
            long advertisedLength,
            boolean writeChunkSizes) throws SQLServerException {
        assert DataTypes.UNKNOWN_STREAM_LENGTH == advertisedLength || advertisedLength >= 0;

        long actualLength = 0;
        final byte[] streamByteBuffer = new byte[4 * currentPacketSize];
        int bytesRead = 0;
        int bytesToWrite;
        do {
            // Read in next chunk
            for (bytesToWrite = 0; -1 != bytesRead && bytesToWrite < streamByteBuffer.length; bytesToWrite += bytesRead) {
                try {
                    bytesRead = inputStream.read(streamByteBuffer, bytesToWrite, streamByteBuffer.length - bytesToWrite);
                }
                catch (IOException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                    Object[] msgArgs = {e.toString()};
                    error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
                }

                if (-1 == bytesRead)
                    break;

                // Check for invalid bytesRead returned from InputStream.read
                if (bytesRead < 0 || bytesRead > streamByteBuffer.length - bytesToWrite) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                    Object[] msgArgs = {SQLServerException.getErrString("R_streamReadReturnedInvalidValue")};
                    error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
                }
            }

            // Write it out
            if (writeChunkSizes)
                writeInt(bytesToWrite);

            writeBytes(streamByteBuffer, 0, bytesToWrite);
            actualLength += bytesToWrite;
        }
        while (-1 != bytesRead || bytesToWrite > 0);

        // If we were given an input stream length that we had to match and
        // the actual stream length did not match then cancel the request.
        if (DataTypes.UNKNOWN_STREAM_LENGTH != advertisedLength && actualLength != advertisedLength) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
            Object[] msgArgs = {advertisedLength, actualLength};
            error(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET);
        }
    }

    /*
     * Adding another function for writing non-unicode reader instead of re-factoring the writeReader() for performance efficiency. As this method
     * will only be used in bulk copy, it needs to be efficient. Note: Any changes in algorithm/logic should propagate to both writeReader() and
     * writeNonUnicodeReader().
     */

    void writeNonUnicodeReader(Reader reader,
            long advertisedLength,
            boolean isDestBinary,
            Charset charSet) throws SQLServerException {
        assert DataTypes.UNKNOWN_STREAM_LENGTH == advertisedLength || advertisedLength >= 0;

        long actualLength = 0;
        char[] streamCharBuffer = new char[currentPacketSize];
        // The unicode version, writeReader() allocates a byte buffer that is 4 times the currentPacketSize, not sure why.
        byte[] streamByteBuffer = new byte[currentPacketSize];
        int charsRead = 0;
        int charsToWrite;
        int bytesToWrite;
        String streamString;

        do {
            // Read in next chunk
            for (charsToWrite = 0; -1 != charsRead && charsToWrite < streamCharBuffer.length; charsToWrite += charsRead) {
                try {
                    charsRead = reader.read(streamCharBuffer, charsToWrite, streamCharBuffer.length - charsToWrite);
                }
                catch (IOException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                    Object[] msgArgs = {e.toString()};
                    error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
                }

                if (-1 == charsRead)
                    break;

                // Check for invalid bytesRead returned from Reader.read
                if (charsRead < 0 || charsRead > streamCharBuffer.length - charsToWrite) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                    Object[] msgArgs = {SQLServerException.getErrString("R_streamReadReturnedInvalidValue")};
                    error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
                }
            }

            if (!isDestBinary) {
                // Write it out
                // This also writes the PLP_TERMINATOR token after all the data in the the stream are sent.
                // The Do-While loop goes on one more time as charsToWrite is greater than 0 for the last chunk, and
                // in this last round the only thing that is written is an int value of 0, which is the PLP Terminator token(0x00000000).
                writeInt(charsToWrite);

                for (int charsCopied = 0; charsCopied < charsToWrite; ++charsCopied) {
                    if (null == charSet) {
                        streamByteBuffer[charsCopied] = (byte) (streamCharBuffer[charsCopied] & 0xFF);
                    }
                    else {
                        // encoding as per collation
                        streamByteBuffer[charsCopied] = new String(streamCharBuffer[charsCopied] + "").getBytes(charSet)[0];
                    }
                }
                writeBytes(streamByteBuffer, 0, charsToWrite);
            }
            else {
                bytesToWrite = charsToWrite;
                if (0 != charsToWrite)
                    bytesToWrite = charsToWrite / 2;

                streamString = new String(streamCharBuffer);
                byte[] bytes = ParameterUtils.HexToBin(streamString.trim());
                writeInt(bytesToWrite);
                writeBytes(bytes, 0, bytesToWrite);
            }
            actualLength += charsToWrite;
        }
        while (-1 != charsRead || charsToWrite > 0);

        // If we were given an input stream length that we had to match and
        // the actual stream length did not match then cancel the request.
        if (DataTypes.UNKNOWN_STREAM_LENGTH != advertisedLength && actualLength != advertisedLength) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
            Object[] msgArgs = {advertisedLength, actualLength};
            error(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET);
        }
    }   

    /*
     * Note: There is another method with same code logic for non unicode reader, writeNonUnicodeReader(), implemented for performance efficiency. Any
     * changes in algorithm/logic should propagate to both writeReader() and writeNonUnicodeReader().
     */
    void writeReader(Reader reader,
            long advertisedLength,
            boolean writeChunkSizes) throws SQLServerException {
        assert DataTypes.UNKNOWN_STREAM_LENGTH == advertisedLength || advertisedLength >= 0;

        long actualLength = 0;
        char[] streamCharBuffer = new char[2 * currentPacketSize];
        byte[] streamByteBuffer = new byte[4 * currentPacketSize];
        int charsRead = 0;
        int charsToWrite;
        do {
            // Read in next chunk
            for (charsToWrite = 0; -1 != charsRead && charsToWrite < streamCharBuffer.length; charsToWrite += charsRead) {
                try {
                    charsRead = reader.read(streamCharBuffer, charsToWrite, streamCharBuffer.length - charsToWrite);
                }
                catch (IOException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                    Object[] msgArgs = {e.toString()};
                    error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
                }

                if (-1 == charsRead)
                    break;

                // Check for invalid bytesRead returned from Reader.read
                if (charsRead < 0 || charsRead > streamCharBuffer.length - charsToWrite) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                    Object[] msgArgs = {SQLServerException.getErrString("R_streamReadReturnedInvalidValue")};
                    error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
                }
            }

            // Write it out
            if (writeChunkSizes)
                writeInt(2 * charsToWrite);

            // Convert from Unicode characters to bytes
            //
            // Note: The following inlined code is much faster than the equivalent
            // call to (new String(streamCharBuffer)).getBytes("UTF-16LE") because it
            // saves a conversion to String and use of Charset in that conversion.
            for (int charsCopied = 0; charsCopied < charsToWrite; ++charsCopied) {
                streamByteBuffer[2 * charsCopied] = (byte) ((streamCharBuffer[charsCopied] >> 0) & 0xFF);
                streamByteBuffer[2 * charsCopied + 1] = (byte) ((streamCharBuffer[charsCopied] >> 8) & 0xFF);
            }

            writeBytes(streamByteBuffer, 0, 2 * charsToWrite);
            actualLength += charsToWrite;
        }
        while (-1 != charsRead || charsToWrite > 0);

        // If we were given an input stream length that we had to match and
        // the actual stream length did not match then cancel the request.
        if (DataTypes.UNKNOWN_STREAM_LENGTH != advertisedLength && actualLength != advertisedLength) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
            Object[] msgArgs = {advertisedLength, actualLength};
            error(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET);
        }
    }

    GregorianCalendar initializeCalender(TimeZone timeZone) {
        GregorianCalendar calendar;

        // Create the calendar that will hold the value. For DateTimeOffset values, the calendar's
        // time zone is UTC. For other values, the calendar's time zone is a local time zone.
        calendar = new GregorianCalendar(timeZone, Locale.US);

        // Set the calendar lenient to allow setting the DAY_OF_YEAR and MILLISECOND fields
        // to roll other fields to their correct values.
        calendar.setLenient(true);

        // Clear the calendar of any existing state. The state of a new Calendar object always
        // reflects the current date, time, DST offset, etc.
        calendar.clear();

        return calendar;
    }

    final void error(String reason,
            SQLState sqlState,
            DriverError driverError) throws SQLServerException {
        assert null != command;
        command.interrupt(reason);
        throw new SQLServerException(reason, sqlState, driverError, null);
    }

    /**
     * Sends an attention signal to the server, if necessary, to tell it to stop processing the current command on this connection.
     *
     * If no packets of the command's request have yet been sent to the server, then no attention signal needs to be sent. The interrupt will be
     * handled entirely by the driver.
     *
     * This method does not need synchronization as it does not manipulate interrupt state and writing is guaranteed to occur only from one thread at
     * a time.
     */
    final boolean sendAttention() throws SQLServerException {
        // If any request packets were already written to the server then send an
        // attention signal to the server to tell it to ignore the request or
        // cancel its execution.
        if (packetNum > 0) {
            // Ideally, we would want to add the following assert here.
            // But to add that the variable isEOMSent would have to be made
            // volatile as this piece of code would be reached from multiple
            // threads. So, not doing it to avoid perf hit. Note that
            // isEOMSent would be updated in writePacket everytime an EOM is sent
            // assert isEOMSent;

            if (logger.isLoggable(Level.FINE))
                logger.fine(this + ": sending attention...");

            ++tdsChannel.numMsgsSent;

            startMessage(command, TDS.PKT_CANCEL_REQ);
            endMessage();

            return true;
        }

        return false;
    }

    private void writePacket(int tdsMessageStatus) throws SQLServerException {
        final boolean atEOM = (TDS.STATUS_BIT_EOM == (TDS.STATUS_BIT_EOM & tdsMessageStatus));
        final boolean isCancelled = ((TDS.PKT_CANCEL_REQ == tdsMessageType)
                || ((tdsMessageStatus & TDS.STATUS_BIT_ATTENTION) == TDS.STATUS_BIT_ATTENTION));
        // Before writing each packet to the channel, check if an interrupt has occurred.
        if (null != command && (!isCancelled))
            command.checkForInterrupt();

        writePacketHeader(tdsMessageStatus | sendResetConnection);
        sendResetConnection = 0;

        flush(atEOM);

        // If this is the last packet then flush the remainder of the request
        // through the socket. The first flush() call ensured that data currently
        // waiting in the socket buffer was sent, flipped the buffers, and started
        // sending data from the staging buffer (flipped to be the new socket buffer).
        // This flush() call ensures that all remaining data in the socket buffer is sent.
        if (atEOM) {
            flush(atEOM);
            isEOMSent = true;
            ++tdsChannel.numMsgsSent;
        }

        // If we just sent the first login request packet and SSL encryption was enabled
        // for login only, then disable SSL now.
        if (TDS.PKT_LOGON70 == tdsMessageType && 1 == packetNum && TDS.ENCRYPT_OFF == con.getNegotiatedEncryptionLevel()) {
            tdsChannel.disableSSL();
        }

        // Notify the currently associated command (if any) that we have written the last
        // of the response packets to the channel.
        if (null != command && (!isCancelled) && atEOM)
            command.onRequestComplete();
    }

    private void writePacketHeader(int tdsMessageStatus) {
        int tdsMessageLength =  ((Buffer)stagingBuffer).position();
        ++packetNum;

        // Write the TDS packet header back at the start of the staging buffer
        stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_TYPE, tdsMessageType);
        stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_STATUS, (byte) tdsMessageStatus);
        stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH, (byte) ((tdsMessageLength >> 8) & 0xFF));     // Note: message length is 16 bits,
        stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH + 1, (byte) ((tdsMessageLength >> 0) & 0xFF)); // written BIG ENDIAN
        stagingBuffer.put(TDS.PACKET_HEADER_SPID, (byte) ((tdsChannel.getSPID() >> 8) & 0xFF));     // Note: SPID is 16 bits,
        stagingBuffer.put(TDS.PACKET_HEADER_SPID + 1, (byte) ((tdsChannel.getSPID() >> 0) & 0xFF)); // written BIG ENDIAN
        stagingBuffer.put(TDS.PACKET_HEADER_SEQUENCE_NUM, (byte) (packetNum % 256));
        stagingBuffer.put(TDS.PACKET_HEADER_WINDOW, (byte) 0); // Window (Reserved/Not used)

        // Write the header to the log buffer too if logging.
        if (tdsChannel.isLoggingPackets()) {
            logBuffer.put(TDS.PACKET_HEADER_MESSAGE_TYPE, tdsMessageType);
            logBuffer.put(TDS.PACKET_HEADER_MESSAGE_STATUS, (byte) tdsMessageStatus);
            logBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH, (byte) ((tdsMessageLength >> 8) & 0xFF));     // Note: message length is 16 bits,
            logBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH + 1, (byte) ((tdsMessageLength >> 0) & 0xFF)); // written BIG ENDIAN
            logBuffer.put(TDS.PACKET_HEADER_SPID, (byte) ((tdsChannel.getSPID() >> 8) & 0xFF));     // Note: SPID is 16 bits,
            logBuffer.put(TDS.PACKET_HEADER_SPID + 1, (byte) ((tdsChannel.getSPID() >> 0) & 0xFF)); // written BIG ENDIAN
            logBuffer.put(TDS.PACKET_HEADER_SEQUENCE_NUM, (byte) (packetNum % 256));
            logBuffer.put(TDS.PACKET_HEADER_WINDOW, (byte) 0); // Window (Reserved/Not used);
        }
    }

    void flush(boolean atEOM) throws SQLServerException {
        // First, flush any data left in the socket buffer.
        tdsChannel.write(socketBuffer.array(), ((Buffer)socketBuffer).position(), socketBuffer.remaining());
        ((Buffer)socketBuffer).position(((Buffer)socketBuffer).limit());

        // If there is data in the staging buffer that needs to be written
        // to the socket, the socket buffer is now empty, so swap buffers
        // and start writing data from the staging buffer.
        if (((Buffer)stagingBuffer).position() >= TDS_PACKET_HEADER_SIZE) {
            // Swap the packet buffers ...
            ByteBuffer swapBuffer = stagingBuffer;
            stagingBuffer = socketBuffer;
            socketBuffer = swapBuffer;

            // ... and prepare to send data from the from the new socket
            // buffer (the old staging buffer).
            //
            // We need to use flip() rather than rewind() here so that
            // the socket buffer's limit is properly set for the last
            // packet, which may be shorter than the other packets.
            ((Buffer)socketBuffer).flip();
            ((Buffer)stagingBuffer).clear();

            // If we are logging TDS packets then log the packet we're about
            // to send over the wire now.
            if (tdsChannel.isLoggingPackets()) {
                tdsChannel.logPacket(logBuffer.array(), 0, ((Buffer) socketBuffer).limit(),
                        this.toString() + " sending packet (" + ((Buffer) socketBuffer).limit() + " bytes)");
            }

            // Prepare for the next packet
            if (!atEOM)
                preparePacket();

            // Finally, start sending data from the new socket buffer.
            tdsChannel.write(socketBuffer.array(), ((Buffer)socketBuffer).position(), socketBuffer.remaining());
            ((Buffer)socketBuffer).position( ((Buffer)socketBuffer).limit());
        }
    }

    // Composite write operations

    /**
     * Write out elements common to all RPC values.
     * 
     * @param sName
     *            the optional parameter name
     * @param bOut
     *            boolean true if the value that follows is being registered as an ouput parameter
     * @param tdsType
     *            TDS type of the value that follows
     */
    void writeRPCNameValType(String sName,
            boolean bOut,
            TDSType tdsType) throws SQLServerException {
        int nNameLen = 0;

        if (null != sName)
            nNameLen = sName.length() + 1; // The @ prefix is required for the param

        writeByte((byte) nNameLen);  // param name len
        if (nNameLen > 0) {
            writeChar('@');
            writeString(sName);
        }

        if (null != cryptoMeta)
            writeByte((byte) (bOut ? 1 | TDS.AE_METADATA : 0 | TDS.AE_METADATA)); // status
        else
            writeByte((byte) (bOut ? 1 : 0)); // status
        writeByte(tdsType.byteValue());  // type
    }

    /**
     * Append a boolean value in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param booleanValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCBit(String sName,
            Boolean booleanValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.BITN);
        writeByte((byte) 1); // max length of datatype
        if (null == booleanValue) {
            writeByte((byte) 0); // len of data bytes
        }
        else {
            writeByte((byte) 1); // length of datatype
            writeByte((byte) (booleanValue ? 1 : 0));
        }
    }

    /**
     * Append a short value in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param shortValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCByte(String sName,
            Byte byteValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.INTN);
        writeByte((byte) 1); // max length of datatype
        if (null == byteValue) {
            writeByte((byte) 0); // len of data bytes
        }
        else {
            writeByte((byte) 1); // length of datatype
            writeByte(byteValue);
        }
    }

    /**
     * Append a short value in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param shortValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCShort(String sName,
            Short shortValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.INTN);
        writeByte((byte) 2); // max length of datatype
        if (null == shortValue) {
            writeByte((byte) 0); // len of data bytes
        }
        else {
            writeByte((byte) 2); // length of datatype
            writeShort(shortValue);
        }
    }

    /**
     * Append an int value in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param intValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCInt(String sName,
            Integer intValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.INTN);
        writeByte((byte) 4); // max length of datatype
        if (null == intValue) {
            writeByte((byte) 0); // len of data bytes
        }
        else {
            writeByte((byte) 4); // length of datatype
            writeInt(intValue);
        }
    }

    /**
     * Append a long value in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param longValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCLong(String sName,
            Long longValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.INTN);
        writeByte((byte) 8); // max length of datatype
        if (null == longValue) {
            writeByte((byte) 0); // len of data bytes
        }
        else {
            writeByte((byte) 8); // length of datatype
            writeLong(longValue);
        }
    }

    /**
     * Append a real value in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param floatValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCReal(String sName,
            Float floatValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.FLOATN);

        // Data and length
        if (null == floatValue) {
            writeByte((byte) 4); // max length
            writeByte((byte) 0); // actual length (0 == null)
        }
        else {
            writeByte((byte) 4); // max length
            writeByte((byte) 4); // actual length
            writeInt(Float.floatToRawIntBits(floatValue));
        }
    }

    void writeRPCSqlVariant(String sName,
            SqlVariant sqlVariantValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.SQL_VARIANT);

        // Data and length
        if (null == sqlVariantValue) {
            writeInt(0); // max length
            writeInt(0); // actual length
        }
    }

    /**
     * Append a double value in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param doubleValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCDouble(String sName,
            Double doubleValue,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.FLOATN);

        int l = 8;
        writeByte((byte) l); // max length of datatype

        // Data and length
        if (null == doubleValue) {
            writeByte((byte) 0); // len of data bytes
        }
        else {
            writeByte((byte) l); // len of data bytes
            long bits = Double.doubleToLongBits(doubleValue);
            long mask = 0xFF;
            int nShift = 0;
            for (int i = 0; i < 8; i++) {
                writeByte((byte) ((bits & mask) >> nShift));
                nShift += 8;
                mask = mask << 8;
            }
        }
    }

    /**
     * Append a big decimal in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param bdValue
     *            the data value
     * @param nScale
     *            the desired scale
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     */
    void writeRPCBigDecimal(String sName,
            BigDecimal bdValue,
            int nScale,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.DECIMALN);
        writeByte((byte) 0x11); // maximum length
        writeByte((byte) SQLServerConnection.maxDecimalPrecision); // precision

        byte[] valueBytes = DDC.convertBigDecimalToBytes(bdValue, nScale);
        writeBytes(valueBytes, 0, valueBytes.length);
    }

    /**
     * Appends a standard v*max header for RPC parameter transmission.
     * 
     * @param headerLength
     *            the total length of the PLP data block.
     * @param isNull
     *            true if the value is NULL.
     * @param collation
     *            The SQL collation associated with the value that follows the v*max header. Null for non-textual types.
     */
    void writeVMaxHeader(long headerLength,
            boolean isNull,
            SQLCollation collation) throws SQLServerException {
        // Send v*max length indicator 0xFFFF.
        writeShort((short) 0xFFFF);

        // Send collation if requested.
        if (null != collation)
            collation.writeCollation(this);

        // Handle null here and return, we're done here if it's null.
        if (isNull) {
            // Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
            writeLong(0xFFFFFFFFFFFFFFFFL);
        }
        else if (DataTypes.UNKNOWN_STREAM_LENGTH == headerLength) {
            // Append v*max length.
            // UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
            writeLong(0xFFFFFFFFFFFFFFFEL);

            // NOTE: Don't send the first chunk length, this will be calculated by caller.
        }
        else {
            // For v*max types with known length, length is <totallength8><chunklength4>
            // We're sending same total length as chunk length (as we're sending 1 chunk).
            writeLong(headerLength);
        }
    }

    /**
     * Utility for internal writeRPCString calls
     */
    void writeRPCStringUnicode(String sValue) throws SQLServerException {
        writeRPCStringUnicode(null, sValue, false, null);
    }

    /**
     * Writes a string value as Unicode for RPC
     * 
     * @param sName
     *            the optional parameter name
     * @param sValue
     *            the data value
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     * @param collation
     *            the collation of the data value
     */
    void writeRPCStringUnicode(String sName,
            String sValue,
            boolean bOut,
            SQLCollation collation) throws SQLServerException {
        boolean bValueNull = (sValue == null);
        int nValueLen = bValueNull ? 0 : (2 * sValue.length());
        boolean isShortValue = nValueLen <= DataTypes.SHORT_VARTYPE_MAX_BYTES;

        // Textual RPC requires a collation. If none is provided, as is the case when
        // the SSType is non-textual, then use the database collation by default.
        if (null == collation)
            collation = con.getDatabaseCollation();

        // Use PLP encoding on Yukon and later with long values and OUT parameters
        boolean usePLP = (!isShortValue || bOut);
        if (usePLP) {
            writeRPCNameValType(sName, bOut, TDSType.NVARCHAR);

            // Handle Yukon v*max type header here.
            writeVMaxHeader(nValueLen,	// Length
                    bValueNull,	// Is null?
                    collation);

            // Send the data.
            if (!bValueNull) {
                if (nValueLen > 0) {
                    writeInt(nValueLen);
                    writeString(sValue);
                }

                // Send the terminator PLP chunk.
                writeInt(0);
            }
        }
        else // non-PLP type
        {
            // Write maximum length of data
            if (isShortValue) {
                writeRPCNameValType(sName, bOut, TDSType.NVARCHAR);
                writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
            }
            else {
                writeRPCNameValType(sName, bOut, TDSType.NTEXT);
                writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
            }

            collation.writeCollation(this);

            // Data and length
            if (bValueNull) {
                writeShort((short) -1); // actual len
            }
            else {
                // Write actual length of data
                if (isShortValue)
                    writeShort((short) nValueLen);
                else
                    writeInt(nValueLen);

                // If length is zero, we're done.
                if (0 != nValueLen)
                    writeString(sValue);  // data
            }
        }
    }

    void writeTVP(TVP value) throws SQLServerException {
        if (!value.isNull()) {
            writeByte((byte) 0); // status
        }
        else {
            // Default TVP
            writeByte((byte) TDS.TVP_STATUS_DEFAULT); // default TVP
        }

        writeByte((byte) TDS.TDS_TVP);

        /*
         * TVP_TYPENAME = DbName OwningSchema TypeName
         */
        // Database where TVP type resides
        if (null != value.getDbNameTVP()) {
            writeByte((byte) value.getDbNameTVP().length());
            writeString(value.getDbNameTVP());
        }
        else
            writeByte((byte) 0x00);	// empty DB name

        // Schema where TVP type resides
        if (null != value.getOwningSchemaNameTVP()) {
            writeByte((byte) value.getOwningSchemaNameTVP().length());
            writeString(value.getOwningSchemaNameTVP());
        }
        else
            writeByte((byte) 0x00);	// empty Schema name

        // TVP type name
        if (null != value.getTVPName()) {
            writeByte((byte) value.getTVPName().length());
            writeString(value.getTVPName());
        }
        else
            writeByte((byte) 0x00);	// empty TVP name

        if (!value.isNull()) {
            writeTVPColumnMetaData(value);

            // optional OrderUnique metadata
            writeTvpOrderUnique(value);
        }
        else {
            writeShort((short) TDS.TVP_NULL_TOKEN);
        }

        // TVP_END_TOKEN
        writeByte((byte) 0x00);

        try {
            writeTVPRows(value);
        }
        catch (NumberFormatException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_TVPInvalidColumnValue"), e);
        }
        catch (ClassCastException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_TVPInvalidColumnValue"), e);
        }
    }

    void writeTVPRows(TVP value) throws SQLServerException {
        boolean tdsWritterCached = false;
        ByteBuffer cachedTVPHeaders = null;
        TDSCommand cachedCommand = null;

        boolean cachedRequestComplete = false;
        boolean cachedInterruptsEnabled = false;
        boolean cachedProcessedResponse = false;

        if (!value.isNull()) {

            // If the preparedStatement and the ResultSet are created by the same connection, and TVP is set with ResultSet and Server Cursor
            // is used, the tdsWriter of the calling preparedStatement is overwritten by the SQLServerResultSet#next() method when fetching new rows.
            // Therefore, we need to send TVP data row by row before fetching new row.
            if (TVPType.ResultSet == value.tvpType) {
                if ((null != value.sourceResultSet) && (value.sourceResultSet instanceof SQLServerResultSet)) {
                    SQLServerResultSet sourceResultSet = (SQLServerResultSet) value.sourceResultSet;
                    SQLServerStatement src_stmt = (SQLServerStatement) sourceResultSet.getStatement();
                    int resultSetServerCursorId = sourceResultSet.getServerCursorId();

                    if (con.equals(src_stmt.getConnection()) && 0 != resultSetServerCursorId) {
                        cachedTVPHeaders = ByteBuffer.allocate(stagingBuffer.capacity()).order(stagingBuffer.order());
                        cachedTVPHeaders.put(stagingBuffer.array(), 0,  ((Buffer)stagingBuffer).position());

                        cachedCommand = this.command;

                        cachedRequestComplete = command.getRequestComplete();
                        cachedInterruptsEnabled = command.getInterruptsEnabled();
                        cachedProcessedResponse = command.getProcessedResponse();

                        tdsWritterCached = true;

                        if (sourceResultSet.isForwardOnly()) {
                            sourceResultSet.setFetchSize(1);
                        }
                    }
                }
            }

            Map<Integer, SQLServerMetaData> columnMetadata = value.getColumnMetadata();
            Iterator<Entry<Integer, SQLServerMetaData>> columnsIterator;

            while (value.next()) {

                // restore command and TDS header, which have been overwritten by value.next()
                if (tdsWritterCached) {
                    command = cachedCommand;

                    ((Buffer)stagingBuffer).clear();
                    ((Buffer)logBuffer).clear();
                    writeBytes(cachedTVPHeaders.array(), 0, ((Buffer)cachedTVPHeaders).position());
                }

                Object[] rowData = value.getRowData();

                // ROW
                writeByte((byte) TDS.TVP_ROW);
                columnsIterator = columnMetadata.entrySet().iterator();
                int currentColumn = 0;
                while (columnsIterator.hasNext()) {
                    Map.Entry<Integer, SQLServerMetaData> columnPair = columnsIterator.next();

                    // If useServerDefault is set, client MUST NOT emit TvpColumnData for the associated column
                    if (columnPair.getValue().useServerDefault) {
                        currentColumn++;
                        continue;
                    }

                    JDBCType jdbcType = JDBCType.of(columnPair.getValue().javaSqlType);
                    String currentColumnStringValue = null;

                    Object currentObject = null;
                    if (null != rowData) {
                        // if rowData has value for the current column, retrieve it. If not, current column will stay null.
                        if (rowData.length > currentColumn) {
                            currentObject = rowData[currentColumn];
                            if (null != currentObject) {
                                currentColumnStringValue = String.valueOf(currentObject);
                            }
                        }
                    }
                    writeInternalTVPRowValues(jdbcType, currentColumnStringValue, currentObject, columnPair, false);
                    currentColumn++;
                }

                // send this row, read its response (throw exception in case of errors) and reset command status
                if (tdsWritterCached) {
                    // TVP_END_TOKEN
                    writeByte((byte) 0x00);

                    writePacket(TDS.STATUS_BIT_EOM);

                    TDSReader tdsReader = tdsChannel.getReader(command);
                    int tokenType = tdsReader.peekTokenType();

                    if (TDS.TDS_ERR == tokenType) {
                        StreamError databaseError = new StreamError();
                        databaseError.setFromTDS(tdsReader);

                        SQLServerException.makeFromDatabaseError(con, null, databaseError.getMessage(), databaseError, false);
                    }

                    command.setInterruptsEnabled(true);
                    command.setRequestComplete(false);
                }
            }
        }

        // reset command status which have been overwritten
        if (tdsWritterCached) {
            command.setRequestComplete(cachedRequestComplete);
            command.setInterruptsEnabled(cachedInterruptsEnabled);
            command.setProcessedResponse(cachedProcessedResponse);
        }
        else {
            // TVP_END_TOKEN
            writeByte((byte) 0x00);
        }
    }

    private void writeInternalTVPRowValues(JDBCType jdbcType,
            String currentColumnStringValue,
            Object currentObject,
            Map.Entry<Integer, SQLServerMetaData> columnPair,
            boolean isSqlVariant) throws SQLServerException {
        boolean isShortValue, isNull;
        int dataLength;
        switch (jdbcType) {
            case BIGINT:
                if (null == currentColumnStringValue)
                    writeByte((byte) 0);
                else {
                    if (isSqlVariant) {
                        writeTVPSqlVariantHeader(10, TDSType.INT8.byteValue(), (byte) 0);
                    }
                    else {
                        writeByte((byte) 8);
                    }
                    writeLong(Long.valueOf(currentColumnStringValue).longValue());
                }
                break;

            case BIT:
                if (null == currentColumnStringValue)
                    writeByte((byte) 0);
                else {
                    if (isSqlVariant)
                        writeTVPSqlVariantHeader(3, TDSType.BIT1.byteValue(), (byte) 0);
                    else
                        writeByte((byte) 1);
                    writeByte((byte) (Boolean.valueOf(currentColumnStringValue).booleanValue() ? 1 : 0));
                }
                break;

            case INTEGER:
                if (null == currentColumnStringValue)
                    writeByte((byte) 0);
                else {
                    if (!isSqlVariant)
                        writeByte((byte) 4);
                    else
                        writeTVPSqlVariantHeader(6, TDSType.INT4.byteValue(), (byte) 0);
                    writeInt(Integer.valueOf(currentColumnStringValue).intValue());
                }
                break;

            case SMALLINT:
            case TINYINT:
                if (null == currentColumnStringValue)
                    writeByte((byte) 0);
                else {
                    if (isSqlVariant) {
                        writeTVPSqlVariantHeader(6, TDSType.INT4.byteValue(), (byte) 0);
                        writeInt(Integer.valueOf(currentColumnStringValue));
                    }
                    else {
                        writeByte((byte) 2); // length of datatype
                        writeShort(Short.valueOf(currentColumnStringValue).shortValue());
                    }
                }
                break;

            case DECIMAL:
            case NUMERIC:
                if (null == currentColumnStringValue)
                    writeByte((byte) 0);
                else {
                    if (isSqlVariant) {
                        writeTVPSqlVariantHeader(21, TDSType.DECIMALN.byteValue(), (byte) 2);
                        writeByte((byte) 38); // scale (byte)variantType.getScale()
                        writeByte((byte) 4); // scale (byte)variantType.getScale()
                    }
                    else {
                        writeByte((byte) TDSWriter.BIGDECIMAL_MAX_LENGTH); // maximum length
                    }
                    BigDecimal bdValue = new BigDecimal(currentColumnStringValue);

                    /*
                     * setScale of all BigDecimal value based on metadata as scale is not sent seperately for individual value. Use the rounding used
                     * in Server. Say, for BigDecimal("0.1"), if scale in metdadata is 0, then ArithmeticException would be thrown if RoundingMode is
                     * not set
                     */
                    bdValue = bdValue.setScale(columnPair.getValue().scale, RoundingMode.HALF_UP);

                    byte[] valueBytes = DDC.convertBigDecimalToBytes(bdValue, bdValue.scale());

                    // 1-byte for sign and 16-byte for integer
                    byte[] byteValue = new byte[17];

                    // removing the precision and scale information from the valueBytes array
                    System.arraycopy(valueBytes, 2, byteValue, 0, valueBytes.length - 2);
                    writeBytes(byteValue);
                }
                break;

            case DOUBLE:
                if (null == currentColumnStringValue)
                    writeByte((byte) 0); // len of data bytes
                else {
                    if (isSqlVariant) {
                        writeTVPSqlVariantHeader(10, TDSType.FLOAT8.byteValue(), (byte) 0);
                        writeDouble(Double.valueOf(currentColumnStringValue));
                        break;
                    }
                    writeByte((byte) 8); // len of data bytes
                    long bits = Double.doubleToLongBits(Double.valueOf(currentColumnStringValue).doubleValue());
                    long mask = 0xFF;
                    int nShift = 0;
                    for (int i = 0; i < 8; i++) {
                        writeByte((byte) ((bits & mask) >> nShift));
                        nShift += 8;
                        mask = mask << 8;
                    }
                }
                break;

            case FLOAT:
            case REAL:
                if (null == currentColumnStringValue)
                    writeByte((byte) 0);
                else {
                    if (isSqlVariant) {
                        writeTVPSqlVariantHeader(6, TDSType.FLOAT4.byteValue(), (byte) 0);
                        writeInt(Float.floatToRawIntBits(Float.valueOf(currentColumnStringValue).floatValue()));
                    }
                    else {
                        writeByte((byte) 4);
                        writeInt(Float.floatToRawIntBits(Float.valueOf(currentColumnStringValue).floatValue()));
                    }
                }
                break;

            case DATE:
            case TIME:
            case TIMESTAMP:
            case DATETIMEOFFSET:
            case DATETIME:
            case SMALLDATETIME:
            case TIMESTAMP_WITH_TIMEZONE:
            case TIME_WITH_TIMEZONE:
            case CHAR:
            case VARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case SQLXML:
                isShortValue = (2L * columnPair.getValue().precision) <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
                isNull = (null == currentColumnStringValue);
                dataLength = isNull ? 0 : currentColumnStringValue.length() * 2;
                if (!isShortValue) {
                    // check null
                    if (isNull) {
                        // Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
                        writeLong(0xFFFFFFFFFFFFFFFFL);
                    }
                    else if (isSqlVariant) {
                        // for now we send as bigger type, but is sendStringParameterAsUnicoe is set to false we can't send nvarchar
                        // since we are writing as nvarchar we need to write as tdstype.bigvarchar value because if we
                        // want to supprot varchar(8000) it becomes as nvarchar, 8000*2 therefore we should send as longvarchar,
                        // but we cannot send more than 8000 cause sql_variant datatype in sql server does not support it.
                        // then throw exception if user is sending more than that
                        if (dataLength > 2 * DataTypes.SHORT_VARTYPE_MAX_BYTES) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidStringValue"));
                            throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
                        }
                        int length = currentColumnStringValue.length();
                        writeTVPSqlVariantHeader(9 + length, TDSType.BIGVARCHAR.byteValue(), (byte) 0x07);
                        SQLCollation col = con.getDatabaseCollation();
                        // write collation for sql variant
                        writeInt(col.getCollationInfo());
                        writeByte((byte) col.getCollationSortID());
                        writeShort((short) (length));
                        writeBytes(currentColumnStringValue.getBytes());
                        break;
                    }

                    else if (DataTypes.UNKNOWN_STREAM_LENGTH == dataLength)
                        // Append v*max length.
                        // UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
                        writeLong(0xFFFFFFFFFFFFFFFEL);
                    else
                        // For v*max types with known length, length is <totallength8><chunklength4>
                        writeLong(dataLength);
                    if (!isNull) {
                        if (dataLength > 0) {
                            writeInt(dataLength);
                            writeString(currentColumnStringValue);
                        }
                        // Send the terminator PLP chunk.
                        writeInt(0);
                    }
                }
                else {
                    if (isNull)
                        writeShort((short) -1); // actual len
                    else {
                        if (isSqlVariant) {
                            // for now we send as bigger type, but is sendStringParameterAsUnicoe is set to false we can't send nvarchar
                            // check for this
                            int length = currentColumnStringValue.length() * 2;
                            writeTVPSqlVariantHeader(9 + length, TDSType.NVARCHAR.byteValue(), (byte) 7);
                            SQLCollation col = con.getDatabaseCollation();
                            // write collation for sql variant
                            writeInt(col.getCollationInfo());
                            writeByte((byte) col.getCollationSortID());
                            int stringLength = currentColumnStringValue.length();
                            byte[] typevarlen = new byte[2];
                            typevarlen[0] = (byte) (2 * stringLength & 0xFF);
                            typevarlen[1] = (byte) ((2 * stringLength >> 8) & 0xFF);
                            writeBytes(typevarlen);
                            writeString(currentColumnStringValue);
                            break;
                        }
                        else {
                            writeShort((short) dataLength);
                            writeString(currentColumnStringValue);
                        }
                    }
                }
                break;

            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                // Handle conversions as done in other types.
                isShortValue = columnPair.getValue().precision <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
                isNull = (null == currentObject);
                if (currentObject instanceof String)
                    dataLength = isNull ? 0 : (ParameterUtils.HexToBin(currentObject.toString())).length;
                else
                    dataLength = isNull ? 0 : ((byte[]) currentObject).length;
                if (!isShortValue) {
                    // check null
                    if (isNull)
                        // Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
                        writeLong(0xFFFFFFFFFFFFFFFFL);
                    else if (DataTypes.UNKNOWN_STREAM_LENGTH == dataLength)
                        // Append v*max length.
                        // UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
                        writeLong(0xFFFFFFFFFFFFFFFEL);
                    else
                        // For v*max types with known length, length is <totallength8><chunklength4>
                        writeLong(dataLength);
                    if (!isNull) {
                        if (dataLength > 0) {
                            writeInt(dataLength);
                            if (currentObject instanceof String)
                                writeBytes(ParameterUtils.HexToBin(currentObject.toString()));
                            else
                                writeBytes((byte[]) currentObject);
                        }
                        // Send the terminator PLP chunk.
                        writeInt(0);
                    }
                }
                else {
                    if (isNull)
                        writeShort((short) -1); // actual len
                    else {
                        writeShort((short) dataLength);
                        if (currentObject instanceof String)
                            writeBytes(ParameterUtils.HexToBin(currentObject.toString()));
                        else
                            writeBytes((byte[]) currentObject);
                    }
                }
                break;
            case SQL_VARIANT:
                boolean isShiloh = (8 >= con.getServerMajorVersion());
                if (isShiloh) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_SQLVariantSupport"));
                    throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
                }
                JDBCType internalJDBCType;
                JavaType javaType = JavaType.of(currentObject);
                internalJDBCType = javaType.getJDBCType(SSType.UNKNOWN, jdbcType);
                writeInternalTVPRowValues(internalJDBCType, currentColumnStringValue, currentObject, columnPair, true);
                break;
            default:
                assert false : "Unexpected JDBC type " + jdbcType.toString();
        }
    }

    /**
     * writes Header for sql_variant for TVP
     * @param length
     * @param tdsType
     * @param probBytes
     * @throws SQLServerException
     */
    private void writeTVPSqlVariantHeader(int length,
            byte tdsType,
            byte probBytes) throws SQLServerException {
        writeInt(length);
        writeByte(tdsType);
        writeByte(probBytes);
    }


    void writeTVPColumnMetaData(TVP value) throws SQLServerException {
        boolean isShortValue;

        // TVP_COLMETADATA
        writeShort((short) value.getTVPColumnCount());

        Map<Integer, SQLServerMetaData> columnMetadata = value.getColumnMetadata();
        /*
         * TypeColumnMetaData = UserType Flags TYPE_INFO ColName ;
         */

        for (Entry<Integer, SQLServerMetaData> pair : columnMetadata.entrySet()) {
            JDBCType jdbcType = JDBCType.of(pair.getValue().javaSqlType);
            boolean useServerDefault = pair.getValue().useServerDefault;
            // ULONG ; UserType of column
            // The value will be 0x0000 with the exceptions of TIMESTAMP (0x0050) and alias types (greater than 0x00FF).
            writeInt(0);
            /*
             * Flags = fNullable ; Column is nullable - %x01 fCaseSen -- Ignored ; usUpdateable -- Ignored ; fIdentity ; Column is identity column -
             * %x10 fComputed ; Column is computed - %x20 usReservedODBC -- Ignored ; fFixedLenCLRType-- Ignored ; fDefault ; Column is default value
             * - %x200 usReserved -- Ignored ;
             */

            short flags = TDS.FLAG_NULLABLE;
            if (useServerDefault) {
                flags |= TDS.FLAG_TVP_DEFAULT_COLUMN;
            }
            writeShort(flags);

            // Type info
            switch (jdbcType) {
                case BIGINT:
                    writeByte(TDSType.INTN.byteValue());
                    writeByte((byte) 8); // max length of datatype
                    break;
                case BIT:
                    writeByte(TDSType.BITN.byteValue());
                    writeByte((byte) 1); // max length of datatype
                    break;
                case INTEGER:
                    writeByte(TDSType.INTN.byteValue());
                    writeByte((byte) 4); // max length of datatype
                    break;
                case SMALLINT:
                case TINYINT:
                    writeByte(TDSType.INTN.byteValue());
                    writeByte((byte) 2); // max length of datatype
                    break;

                case DECIMAL:
                case NUMERIC:
                    writeByte(TDSType.NUMERICN.byteValue());
                    writeByte((byte) 0x11); // maximum length
                    writeByte((byte) pair.getValue().precision);
                    writeByte((byte) pair.getValue().scale);
                    break;

                case DOUBLE:
                    writeByte(TDSType.FLOATN.byteValue());
                    writeByte((byte) 8); // max length of datatype
                    break;

                case FLOAT:
                case REAL:
                    writeByte(TDSType.FLOATN.byteValue());
                    writeByte((byte) 4); // max length of datatype
                    break;

                case DATE:
                case TIME:
                case TIMESTAMP:
                case DATETIMEOFFSET:
                case DATETIME:
                case SMALLDATETIME:
                case TIMESTAMP_WITH_TIMEZONE:
                case TIME_WITH_TIMEZONE:
                case CHAR:
                case VARCHAR:
                case NCHAR:
                case NVARCHAR:
                case LONGVARCHAR:
                case LONGNVARCHAR:
                case SQLXML:
                    writeByte(TDSType.NVARCHAR.byteValue());
                    isShortValue = (2L * pair.getValue().precision) <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
                    // Use PLP encoding on Yukon and later with long values
                    if (!isShortValue)    // PLP
                    {
                        // Handle Yukon v*max type header here.
                        writeShort((short) 0xFFFF);
                        con.getDatabaseCollation().writeCollation(this);
                    } else    // non PLP
                    {
                        writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
                        con.getDatabaseCollation().writeCollation(this);
                    }

                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                    writeByte(TDSType.BIGVARBINARY.byteValue());
                    isShortValue = pair.getValue().precision <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
                    // Use PLP encoding on Yukon and later with long values
                    if (!isShortValue)    // PLP
                        // Handle Yukon v*max type header here.
                        writeShort((short) 0xFFFF);
                    else    // non PLP
                        writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
                    break;
                case SQL_VARIANT:
                    writeByte(TDSType.SQL_VARIANT.byteValue());
                    writeInt(TDS.SQL_VARIANT_LENGTH);// write length of sql variant 8009

                    break;

                default:
                    assert false : "Unexpected JDBC type " + jdbcType.toString();
            }
            // Column name - must be null (from TDS - TVP_COLMETADATA)
            writeByte((byte) 0x00);

            // [TVP_ORDER_UNIQUE]
            // [TVP_COLUMN_ORDERING]
        }
    }

    void writeTvpOrderUnique(TVP value) throws SQLServerException {
        /*
         * TVP_ORDER_UNIQUE = TVP_ORDER_UNIQUE_TOKEN (Count <Count>(ColNum OrderUniqueFlags))
         */

        Map<Integer, SQLServerMetaData> columnMetadata = value.getColumnMetadata();
        Iterator<Entry<Integer, SQLServerMetaData>> columnsIterator = columnMetadata.entrySet().iterator();
        LinkedList<TdsOrderUnique> columnList = new LinkedList<>();

        while (columnsIterator.hasNext()) {
            byte flags = 0;
            Map.Entry<Integer, SQLServerMetaData> pair = columnsIterator.next();
            SQLServerMetaData metaData = pair.getValue();

            if (SQLServerSortOrder.Ascending == metaData.sortOrder)
                flags = TDS.TVP_ORDERASC_FLAG;
            else if (SQLServerSortOrder.Descending == metaData.sortOrder)
                flags = TDS.TVP_ORDERDESC_FLAG;
            if (metaData.isUniqueKey)
                flags |= TDS.TVP_UNIQUE_FLAG;

            // Remember this column if any flags were set
            if (0 != flags)
                columnList.add(new TdsOrderUnique(pair.getKey(), flags));
        }

        // Write flagged columns
        if (!columnList.isEmpty()) {
            writeByte((byte) TDS.TVP_ORDER_UNIQUE_TOKEN);
            writeShort((short) columnList.size());
            for (TdsOrderUnique column : columnList) {
                writeShort((short) (column.columnOrdinal + 1));
                writeByte(column.flags);
            }
        }
    }

    private class TdsOrderUnique {
        int columnOrdinal;
        byte flags;

        TdsOrderUnique(int ordinal,
                byte flags) {
            this.columnOrdinal = ordinal;
            this.flags = flags;
        }
    }

    void setCryptoMetaData(CryptoMetadata cryptoMetaForBulk) {
        this.cryptoMeta = cryptoMetaForBulk;
    }

    CryptoMetadata getCryptoMetaData() {
        return cryptoMeta;
    }

    void writeEncryptedRPCByteArray(byte bValue[]) throws SQLServerException {
        boolean bValueNull = (bValue == null);
        long nValueLen = bValueNull ? 0 : bValue.length;
        boolean isShortValue = (nValueLen <= DataTypes.SHORT_VARTYPE_MAX_BYTES);

        boolean isPLP = (!isShortValue) && (nValueLen <= DataTypes.MAX_VARTYPE_MAX_BYTES);

        // Handle Shiloh types here.
        if (isShortValue) {
            writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
        }
        else if (isPLP) {
            writeShort((short) DataTypes.SQL_USHORTVARMAXLEN);
        }
        else {
            writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
        }

        // Data and length
        if (bValueNull) {
            writeShort((short) -1); // actual len
        }
        else {
            if (isShortValue) {
                writeShort((short) nValueLen); // actual len
            }
            else if (isPLP) {
                writeLong(nValueLen); // actual length
            }
            else {
                writeInt((int) nValueLen); // actual len
            }

            // If length is zero, we're done.
            if (0 != nValueLen) {
                if (isPLP) {
                    writeInt((int) nValueLen);
                }
                writeBytes(bValue);
            }

            if (isPLP) {
                writeInt(0); // PLP_TERMINATOR, 0x00000000
            }
        }
    }

    void writeEncryptedRPCPLP() throws SQLServerException {
        writeShort((short) DataTypes.SQL_USHORTVARMAXLEN);
        writeLong((long) 0); // actual length
        writeInt(0); // PLP_TERMINATOR, 0x00000000
    }

    void writeCryptoMetaData() throws SQLServerException {
        writeByte(cryptoMeta.cipherAlgorithmId);
        writeByte(cryptoMeta.encryptionType.getValue());
        writeInt(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).databaseId);
        writeInt(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).cekId);
        writeInt(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).cekVersion);
        writeBytes(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).cekMdVersion);
        writeByte(cryptoMeta.normalizationRuleVersion);
    }

    void writeRPCByteArray(String sName,
            byte bValue[],
            boolean bOut,
            JDBCType jdbcType,
            SQLCollation collation) throws SQLServerException {
        boolean bValueNull = (bValue == null);
        int nValueLen = bValueNull ? 0 : bValue.length;
        boolean isShortValue = (nValueLen <= DataTypes.SHORT_VARTYPE_MAX_BYTES);

        // Use PLP encoding on Yukon and later with long values and OUT parameters
        boolean usePLP = (!isShortValue || bOut);

        TDSType tdsType;

        if (null != cryptoMeta) {
            // send encrypted data as BIGVARBINARY
            tdsType = (isShortValue || usePLP) ? TDSType.BIGVARBINARY : TDSType.IMAGE;
            collation = null;
        }
        else
            switch (jdbcType) {
                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:
                default:
                    tdsType = (isShortValue || usePLP) ? TDSType.BIGVARBINARY : TDSType.IMAGE;
                    collation = null;
                    break;

                case CHAR:
                case VARCHAR:
                case LONGVARCHAR:
                case CLOB:
                    tdsType = (isShortValue || usePLP) ? TDSType.BIGVARCHAR : TDSType.TEXT;
                    if (null == collation)
                        collation = con.getDatabaseCollation();
                    break;

                case NCHAR:
                case NVARCHAR:
                case LONGNVARCHAR:
                case NCLOB:
                    tdsType = (isShortValue || usePLP) ? TDSType.NVARCHAR : TDSType.NTEXT;
                    if (null == collation)
                        collation = con.getDatabaseCollation();
                    break;
            }

        writeRPCNameValType(sName, bOut, tdsType);

        if (usePLP) {
            // Handle Yukon v*max type header here.
            writeVMaxHeader(nValueLen, bValueNull, collation);

            // Send the data.
            if (!bValueNull) {
                if (nValueLen > 0) {
                    writeInt(nValueLen);
                    writeBytes(bValue);
                }

                // Send the terminator PLP chunk.
                writeInt(0);
            }
        }
        else // non-PLP type
        {
            // Handle Shiloh types here.
            if (isShortValue) {
                writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
            }
            else {
                writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
            }

            if (null != collation)
                collation.writeCollation(this);

            // Data and length
            if (bValueNull) {
                writeShort((short) -1); // actual len
            }
            else {
                if (isShortValue)
                    writeShort((short) nValueLen); // actual len
                else
                    writeInt(nValueLen); // actual len

                // If length is zero, we're done.
                if (0 != nValueLen)
                    writeBytes(bValue);
            }
        }
    }

    /**
     * Append a timestamp in RPC transmission format as a SQL Server DATETIME data type
     * 
     * @param sName
     *            the optional parameter name
     * @param cal
     *            Pure Gregorian calendar containing the timestamp, including its associated time zone
     * @param subSecondNanos
     *            the sub-second nanoseconds (0 - 999,999,999)
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     *
     */
    void writeRPCDateTime(String sName,
            GregorianCalendar cal,
            int subSecondNanos,
            boolean bOut) throws SQLServerException {
        assert (subSecondNanos >= 0) && (subSecondNanos < Nanos.PER_SECOND) : "Invalid subNanoSeconds value: " + subSecondNanos;
        assert (cal != null) || (subSecondNanos == 0) : "Invalid subNanoSeconds value when calendar is null: " + subSecondNanos;

        writeRPCNameValType(sName, bOut, TDSType.DATETIMEN);
        writeByte((byte) 8); // max length of datatype

        if (null == cal) {
            writeByte((byte) 0); // len of data bytes
            return;
        }

        writeByte((byte) 8); // len of data bytes

        // We need to extract the Calendar's current date & time in terms
        // of the number of days since the SQL Base Date (1/1/1900) plus
        // the number of milliseconds since midnight in the current day.
        //
        // We cannot rely on any pre-calculated value for the number of
        // milliseconds in a day or the number of milliseconds since the
        // base date to do this because days with DST changes are shorter
        // or longer than "normal" days.
        //
        // ASSUMPTION: We assume we are dealing with a GregorianCalendar here.
        // If not, we have no basis in which to compare dates. E.g. if we
        // are dealing with a Chinese Calendar implementation which does not
        // use the same value for Calendar.YEAR as the GregorianCalendar,
        // we cannot meaningfully compute a value relative to 1/1/1900.

        // First, figure out how many days there have been since the SQL Base Date.
        // These are based on SQL Server algorithms
        int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR), TDS.BASE_YEAR_1900);

        // Next, figure out the number of milliseconds since midnight of the current day.
        int millisSinceMidnight = (subSecondNanos + Nanos.PER_MILLISECOND / 2) / Nanos.PER_MILLISECOND + // Millis into the current second
                1000 * cal.get(Calendar.SECOND) + // Seconds into the current minute
                60 * 1000 * cal.get(Calendar.MINUTE) + // Minutes into the current hour
                60 * 60 * 1000 * cal.get(Calendar.HOUR_OF_DAY); // Hours into the current day

        // The last millisecond of the current day is always rounded to the first millisecond
        // of the next day because DATETIME is only accurate to 1/300th of a second.
        if (millisSinceMidnight >= 1000 * 60 * 60 * 24 - 1) {
            ++daysSinceSQLBaseDate;
            millisSinceMidnight = 0;
        }

        // Last-ditch verification that the value is in the valid range for the
        // DATETIMEN TDS data type (1/1/1753 to 12/31/9999). If it's not, then
        // throw an exception now so that statement execution is safely canceled.
        // Attempting to put an invalid value on the wire would result in a TDS
        // exception, which would close the connection.
        // These are based on SQL Server algorithms
        if (daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1753, 1, TDS.BASE_YEAR_1900)
                || daysSinceSQLBaseDate >= DDC.daysSinceBaseDate(10000, 1, TDS.BASE_YEAR_1900)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
            Object[] msgArgs = {SSType.DATETIME};
            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW, DriverError.NOT_SET, null);
        }

        // And put it all on the wire...

        // Number of days since the SQL Server Base Date (January 1, 1900)
        writeInt(daysSinceSQLBaseDate);

        // Milliseconds since midnight (at a resolution of three hundredths of a second)
        writeInt((3 * millisSinceMidnight + 5) / 10);
    }

    void writeRPCTime(String sName,
            GregorianCalendar localCalendar,
            int subSecondNanos,
            int scale,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.TIMEN);
        writeByte((byte) scale);

        if (null == localCalendar) {
            writeByte((byte) 0);
            return;
        }

        writeByte((byte) TDS.timeValueLength(scale));
        writeScaledTemporal(localCalendar, subSecondNanos, scale, SSType.TIME);
    }

    void writeRPCDate(String sName,
            GregorianCalendar localCalendar,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.DATEN);
        if (null == localCalendar) {
            writeByte((byte) 0);
            return;
        }

        writeByte((byte) TDS.DAYS_INTO_CE_LENGTH);
        writeScaledTemporal(localCalendar, 0, // subsecond nanos (none for a date value)
                0, // scale (dates are not scaled)
                SSType.DATE);
    }

    void writeEncryptedRPCTime(String sName,
            GregorianCalendar localCalendar,
            int subSecondNanos,
            int scale,
            boolean bOut) throws SQLServerException {
        if (con.getSendTimeAsDatetime()) {
            throw new SQLServerException(SQLServerException.getErrString("R_sendTimeAsDateTimeForAE"), null);
        }
        writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

        if (null == localCalendar)
            writeEncryptedRPCByteArray(null);
        else
            writeEncryptedRPCByteArray(writeEncryptedScaledTemporal(localCalendar, subSecondNanos, scale, SSType.TIME, (short) 0));

        writeByte(TDSType.TIMEN.byteValue());
        writeByte((byte) scale);
        writeCryptoMetaData();
    }

    void writeEncryptedRPCDate(String sName,
            GregorianCalendar localCalendar,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

        if (null == localCalendar)
            writeEncryptedRPCByteArray(null);
        else
            writeEncryptedRPCByteArray(writeEncryptedScaledTemporal(localCalendar, 0, // subsecond nanos (none for a date value)
                    0, // scale (dates are not scaled)
                    SSType.DATE, (short) 0));

        writeByte(TDSType.DATEN.byteValue());
        writeCryptoMetaData();
    }

    void writeEncryptedRPCDateTime(String sName,
            GregorianCalendar cal,
            int subSecondNanos,
            boolean bOut,
            JDBCType jdbcType) throws SQLServerException {
        assert (subSecondNanos >= 0) && (subSecondNanos < Nanos.PER_SECOND) : "Invalid subNanoSeconds value: " + subSecondNanos;
        assert (cal != null) || (subSecondNanos == 0) : "Invalid subNanoSeconds value when calendar is null: " + subSecondNanos;

        writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

        if (null == cal)
            writeEncryptedRPCByteArray(null);
        else
            writeEncryptedRPCByteArray(getEncryptedDateTimeAsBytes(cal, subSecondNanos, jdbcType));

        if (JDBCType.SMALLDATETIME == jdbcType) {
            writeByte(TDSType.DATETIMEN.byteValue());
            writeByte((byte) 4);
        }
        else {
            writeByte(TDSType.DATETIMEN.byteValue());
            writeByte((byte) 8);
        }
        writeCryptoMetaData();
    }

    // getEncryptedDateTimeAsBytes is called if jdbcType/ssType is SMALLDATETIME or DATETIME
    byte[] getEncryptedDateTimeAsBytes(GregorianCalendar cal,
            int subSecondNanos,
            JDBCType jdbcType) throws SQLServerException {
        int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR), TDS.BASE_YEAR_1900);

        // Next, figure out the number of milliseconds since midnight of the current day.
        int millisSinceMidnight = (subSecondNanos + Nanos.PER_MILLISECOND / 2) / Nanos.PER_MILLISECOND + // Millis into the current second
                1000 * cal.get(Calendar.SECOND) + // Seconds into the current minute
                60 * 1000 * cal.get(Calendar.MINUTE) + // Minutes into the current hour
                60 * 60 * 1000 * cal.get(Calendar.HOUR_OF_DAY); // Hours into the current day

        // The last millisecond of the current day is always rounded to the first millisecond
        // of the next day because DATETIME is only accurate to 1/300th of a second.
        if (millisSinceMidnight >= 1000 * 60 * 60 * 24 - 1) {
            ++daysSinceSQLBaseDate;
            millisSinceMidnight = 0;
        }

        if (JDBCType.SMALLDATETIME == jdbcType) {

            int secondsSinceMidnight = (millisSinceMidnight / 1000);
            int minutesSinceMidnight = (secondsSinceMidnight / 60);

            // Values that are 29.998 seconds or less are rounded down to the nearest minute
            minutesSinceMidnight = ((secondsSinceMidnight % 60) > 29.998) ? minutesSinceMidnight + 1 : minutesSinceMidnight;

            // minutesSinceMidnight for (23:59:30)
            int maxMinutesSinceMidnight_SmallDateTime = 1440;
            // Verification for smalldatetime to be within valid range of (1900.01.01) to (2079.06.06)
            // smalldatetime for unencrypted does not allow insertion of 2079.06.06 23:59:59 and it is rounded up
            // to 2079.06.07 00:00:00, therefore, we are checking minutesSinceMidnight for that condition. If it's not within valid range, then
            // throw an exception now so that statement execution is safely canceled.
            // 157 is the calculated day of year from 06-06 , 1440 is minutesince midnight for (23:59:30)
            if ((daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1900, 1, TDS.BASE_YEAR_1900)
                    || daysSinceSQLBaseDate > DDC.daysSinceBaseDate(2079, 157, TDS.BASE_YEAR_1900))
                    || (daysSinceSQLBaseDate == DDC.daysSinceBaseDate(2079, 157, TDS.BASE_YEAR_1900)
                            && minutesSinceMidnight >= maxMinutesSinceMidnight_SmallDateTime)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                Object[] msgArgs = {SSType.SMALLDATETIME};
                throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW, DriverError.NOT_SET, null);
            }

            ByteBuffer days = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
            days.putShort((short) daysSinceSQLBaseDate);
            ByteBuffer seconds = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
            seconds.putShort((short) minutesSinceMidnight);

            byte[] value = new byte[4];
            System.arraycopy(days.array(), 0, value, 0, 2);
            System.arraycopy(seconds.array(), 0, value, 2, 2);
            return SQLServerSecurityUtility.encryptWithKey(value, cryptoMeta, con);
        }
        else if (JDBCType.DATETIME == jdbcType) {
            // Last-ditch verification that the value is in the valid range for the
            // DATETIMEN TDS data type (1/1/1753 to 12/31/9999). If it's not, then
            // throw an exception now so that statement execution is safely canceled.
            // Attempting to put an invalid value on the wire would result in a TDS
            // exception, which would close the connection.
            // These are based on SQL Server algorithms
            // And put it all on the wire...
            if (daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1753, 1, TDS.BASE_YEAR_1900)
                    || daysSinceSQLBaseDate >= DDC.daysSinceBaseDate(10000, 1, TDS.BASE_YEAR_1900)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                Object[] msgArgs = {SSType.DATETIME};
                throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW, DriverError.NOT_SET, null);
            }

            // Number of days since the SQL Server Base Date (January 1, 1900)
            ByteBuffer days = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            days.putInt(daysSinceSQLBaseDate);
            ByteBuffer seconds = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            seconds.putInt((3 * millisSinceMidnight + 5) / 10);

            byte[] value = new byte[8];
            System.arraycopy(days.array(), 0, value, 0, 4);
            System.arraycopy(seconds.array(), 0, value, 4, 4);
            return SQLServerSecurityUtility.encryptWithKey(value, cryptoMeta, con);
        }

        assert false : "Unexpected JDBCType type " + jdbcType;
        return null;
    }

    void writeEncryptedRPCDateTime2(String sName,
            GregorianCalendar localCalendar,
            int subSecondNanos,
            int scale,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

        if (null == localCalendar)
            writeEncryptedRPCByteArray(null);
        else
            writeEncryptedRPCByteArray(writeEncryptedScaledTemporal(localCalendar, subSecondNanos, scale, SSType.DATETIME2, (short) 0));

        writeByte(TDSType.DATETIME2N.byteValue());
        writeByte((byte) (scale));
        writeCryptoMetaData();
    }

    void writeEncryptedRPCDateTimeOffset(String sName,
            GregorianCalendar utcCalendar,
            int minutesOffset,
            int subSecondNanos,
            int scale,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

        if (null == utcCalendar)
            writeEncryptedRPCByteArray(null);
        else {
            assert 0 == utcCalendar.get(Calendar.ZONE_OFFSET);
            writeEncryptedRPCByteArray(
                    writeEncryptedScaledTemporal(utcCalendar, subSecondNanos, scale, SSType.DATETIMEOFFSET, (short) minutesOffset));
        }

        writeByte(TDSType.DATETIMEOFFSETN.byteValue());
        writeByte((byte) (scale));
        writeCryptoMetaData();

    }

    void writeRPCDateTime2(String sName,
            GregorianCalendar localCalendar,
            int subSecondNanos,
            int scale,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.DATETIME2N);
        writeByte((byte) scale);

        if (null == localCalendar) {
            writeByte((byte) 0);
            return;
        }

        writeByte((byte) TDS.datetime2ValueLength(scale));
        writeScaledTemporal(localCalendar, subSecondNanos, scale, SSType.DATETIME2);
    }

    void writeRPCDateTimeOffset(String sName,
            GregorianCalendar utcCalendar,
            int minutesOffset,
            int subSecondNanos,
            int scale,
            boolean bOut) throws SQLServerException {
        writeRPCNameValType(sName, bOut, TDSType.DATETIMEOFFSETN);
        writeByte((byte) scale);

        if (null == utcCalendar) {
            writeByte((byte) 0);
            return;
        }

        assert 0 == utcCalendar.get(Calendar.ZONE_OFFSET);

        writeByte((byte) TDS.datetimeoffsetValueLength(scale));
        writeScaledTemporal(utcCalendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);

        writeShort((short) minutesOffset);
    }


    /**
     * Returns subSecondNanos rounded to the maximum precision supported. The maximum fractional scale is MAX_FRACTIONAL_SECONDS_SCALE(7). Eg1: if you
     * pass 456,790,123 the function would return 456,790,100 Eg2: if you pass 456,790,150 the function would return 456,790,200 Eg3: if you pass
     * 999,999,951 the function would return 1,000,000,000 This is done to ensure that we have consistent rounding behaviour in setters and getters.
     * Bug #507919
     */
    private int getRoundedSubSecondNanos(int subSecondNanos) {
        int roundedNanos = ((subSecondNanos + (Nanos.PER_MAX_SCALE_INTERVAL / 2)) / Nanos.PER_MAX_SCALE_INTERVAL) * Nanos.PER_MAX_SCALE_INTERVAL;
        return roundedNanos;
    }

    /**
     * Writes to the TDS channel a temporal value as an instance instance of one of the scaled temporal SQL types: DATE, TIME, DATETIME2, or
     * DATETIMEOFFSET.
     *
     * @param cal
     *            Calendar representing the value to write, except for any sub-second nanoseconds
     * @param subSecondNanos
     *            the sub-second nanoseconds (0 - 999,999,999)
     * @param scale
     *            the scale (in digits: 0 - 7) to use for the sub-second nanos component
     * @param ssType
     *            the SQL Server data type (DATE, TIME, DATETIME2, or DATETIMEOFFSET)
     *
     * @throws SQLServerException
     *             if an I/O error occurs or if the value is not in the valid range
     */
    private void writeScaledTemporal(GregorianCalendar cal,
            int subSecondNanos,
            int scale,
            SSType ssType) throws SQLServerException {

        assert con.isKatmaiOrLater();

        assert SSType.DATE == ssType || SSType.TIME == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: "
                + ssType;

        // First, for types with a time component, write the scaled nanos since midnight
        if (SSType.TIME == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
            assert subSecondNanos >= 0;
            assert subSecondNanos < Nanos.PER_SECOND;
            assert scale >= 0;
            assert scale <= TDS.MAX_FRACTIONAL_SECONDS_SCALE;

            int secondsSinceMidnight = cal.get(Calendar.SECOND) + 60 * cal.get(Calendar.MINUTE) + 60 * 60 * cal.get(Calendar.HOUR_OF_DAY);

            // Scale nanos since midnight to the desired scale, rounding the value as necessary
            long divisor = Nanos.PER_MAX_SCALE_INTERVAL * (long) Math.pow(10, TDS.MAX_FRACTIONAL_SECONDS_SCALE - scale);

            // The scaledNanos variable represents the fractional seconds of the value at the scale
            // indicated by the scale variable. So, for example, scaledNanos = 3 means 300 nanoseconds
            // at scale TDS.MAX_FRACTIONAL_SECONDS_SCALE, but 3000 nanoseconds at
            // TDS.MAX_FRACTIONAL_SECONDS_SCALE - 1
            long scaledNanos = ((long) Nanos.PER_SECOND * secondsSinceMidnight + getRoundedSubSecondNanos(subSecondNanos) + divisor / 2) / divisor;

            // SQL Server rounding behavior indicates that it always rounds up unless
            // we are at the max value of the type(NOT every day), in which case it truncates.
            // Side effect on Calendar date:
            // If rounding nanos to the specified scale rolls the value to the next day ...
            if (Nanos.PER_DAY / divisor == scaledNanos) {

                // If the type is time, always truncate
                if (SSType.TIME == ssType) {
                    --scaledNanos;
                }
                // If the type is datetime2 or datetimeoffset, truncate only if its the max value supported
                else {
                    assert SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: " + ssType;

                    // ... then bump the date, provided that the resulting date is still within
                    // the valid date range.
                    //
                    // Extreme edge case (literally, the VERY edge...):
                    // If nanos overflow rolls the date value out of range (that is, we have a value
                    // a few nanoseconds later than 9999-12-31 23:59:59) then truncate the nanos
                    // instead of rolling.
                    //
                    // This case is very likely never hit by "real world" applications, but exists
                    // here as a security measure to ensure that such values don't result in a
                    // connection-closing TDS exception.
                    cal.add(Calendar.SECOND, 1);

                    if (cal.get(Calendar.YEAR) <= 9999) {
                        scaledNanos = 0;
                    }
                    else {
                        cal.add(Calendar.SECOND, -1);
                        --scaledNanos;
                    }
                }
            }

            // Encode the scaled nanos to TDS
            int encodedLength = TDS.nanosSinceMidnightLength(scale);
            byte[] encodedBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

            writeBytes(encodedBytes);
        }

        // Second, for types with a date component, write the days into the Common Era
        if (SSType.DATE == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
            // Computation of the number of days into the Common Era assumes that
            // the DAY_OF_YEAR field reflects a pure Gregorian calendar - one that
            // uses Gregorian leap year rules across the entire range of dates.
            //
            // For the DAY_OF_YEAR field to accurately reflect pure Gregorian behavior,
            // we need to use a pure Gregorian calendar for dates that are Julian dates
            // under a standard Gregorian calendar and for (Gregorian) dates later than
            // the cutover date in the cutover year.
            if (cal.getTimeInMillis() < GregorianChange.STANDARD_CHANGE_DATE.getTime()
                    || cal.getActualMaximum(Calendar.DAY_OF_YEAR) < TDS.DAYS_PER_YEAR) {
                int year = cal.get(Calendar.YEAR);
                int month = cal.get(Calendar.MONTH);
                int date = cal.get(Calendar.DATE);

                // Set the cutover as early as possible (pure Gregorian behavior)
                cal.setGregorianChange(GregorianChange.PURE_CHANGE_DATE);

                // Initialize the date field by field (preserving the "wall calendar" value)
                cal.set(year, month, date);
            }

            int daysIntoCE = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR), 1);

            // Last-ditch verification that the value is in the valid range for the
            // DATE/DATETIME2/DATETIMEOFFSET TDS data type (1/1/0001 to 12/31/9999).
            // If it's not, then throw an exception now so that statement execution
            // is safely canceled. Attempting to put an invalid value on the wire
            // would result in a TDS exception, which would close the connection.
            if (daysIntoCE < 0 || daysIntoCE >= DDC.daysSinceBaseDate(10000, 1, 1)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                Object[] msgArgs = {ssType};
                throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW, DriverError.NOT_SET, null);
            }

            byte encodedBytes[] = new byte[3];
            encodedBytes[0] = (byte) ((daysIntoCE >> 0) & 0xFF);
            encodedBytes[1] = (byte) ((daysIntoCE >> 8) & 0xFF);
            encodedBytes[2] = (byte) ((daysIntoCE >> 16) & 0xFF);
            writeBytes(encodedBytes);
        }
    }

    /**
     * Writes to the TDS channel a temporal value as an instance instance of one of the scaled temporal SQL types: DATE, TIME, DATETIME2, or
     * DATETIMEOFFSET.
     *
     * @param cal
     *            Calendar representing the value to write, except for any sub-second nanoseconds
     * @param subSecondNanos
     *            the sub-second nanoseconds (0 - 999,999,999)
     * @param scale
     *            the scale (in digits: 0 - 7) to use for the sub-second nanos component
     * @param ssType
     *            the SQL Server data type (DATE, TIME, DATETIME2, or DATETIMEOFFSET)
     * @param minutesOffset
     *            the offset value for DATETIMEOFFSET
     * @throws SQLServerException
     *             if an I/O error occurs or if the value is not in the valid range
     */
    byte[] writeEncryptedScaledTemporal(GregorianCalendar cal,
            int subSecondNanos,
            int scale,
            SSType ssType,
            short minutesOffset) throws SQLServerException {
        assert con.isKatmaiOrLater();

        assert SSType.DATE == ssType || SSType.TIME == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: "
                + ssType;

        // store the time and minutesOffset portion of DATETIME2 and DATETIMEOFFSET to be used with date portion
        byte encodedBytesForEncryption[] = null;

        int secondsSinceMidnight = 0;
        long divisor = 0;
        long scaledNanos = 0;

        // First, for types with a time component, write the scaled nanos since midnight
        if (SSType.TIME == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
            assert subSecondNanos >= 0;
            assert subSecondNanos < Nanos.PER_SECOND;
            assert scale >= 0;
            assert scale <= TDS.MAX_FRACTIONAL_SECONDS_SCALE;

            secondsSinceMidnight = cal.get(Calendar.SECOND) + 60 * cal.get(Calendar.MINUTE) + 60 * 60 * cal.get(Calendar.HOUR_OF_DAY);

            // Scale nanos since midnight to the desired scale, rounding the value as necessary
            divisor = Nanos.PER_MAX_SCALE_INTERVAL * (long) Math.pow(10, TDS.MAX_FRACTIONAL_SECONDS_SCALE - scale);

            // The scaledNanos variable represents the fractional seconds of the value at the scale
            // indicated by the scale variable. So, for example, scaledNanos = 3 means 300 nanoseconds
            // at scale TDS.MAX_FRACTIONAL_SECONDS_SCALE, but 3000 nanoseconds at
            // TDS.MAX_FRACTIONAL_SECONDS_SCALE - 1
            scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight + getRoundedSubSecondNanos(subSecondNanos) + divisor / 2) / divisor)
                    * divisor / 100;

            // for encrypted time value, SQL server cannot do rounding or casting,
            // So, driver needs to cast it before encryption.
            if (SSType.TIME == ssType && 864000000000L <= scaledNanos) {
                scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight + getRoundedSubSecondNanos(subSecondNanos)) / divisor) * divisor / 100;
            }

            // SQL Server rounding behavior indicates that it always rounds up unless
            // we are at the max value of the type(NOT every day), in which case it truncates.
            // Side effect on Calendar date:
            // If rounding nanos to the specified scale rolls the value to the next day ...
            if (Nanos.PER_DAY / divisor == scaledNanos) {

                // If the type is time, always truncate
                if (SSType.TIME == ssType) {
                    --scaledNanos;
                }
                // If the type is datetime2 or datetimeoffset, truncate only if its the max value supported
                else {
                    assert SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: " + ssType;

                    // ... then bump the date, provided that the resulting date is still within
                    // the valid date range.
                    //
                    // Extreme edge case (literally, the VERY edge...):
                    // If nanos overflow rolls the date value out of range (that is, we have a value
                    // a few nanoseconds later than 9999-12-31 23:59:59) then truncate the nanos
                    // instead of rolling.
                    //
                    // This case is very likely never hit by "real world" applications, but exists
                    // here as a security measure to ensure that such values don't result in a
                    // connection-closing TDS exception.
                    cal.add(Calendar.SECOND, 1);

                    if (cal.get(Calendar.YEAR) <= 9999) {
                        scaledNanos = 0;
                    }
                    else {
                        cal.add(Calendar.SECOND, -1);
                        --scaledNanos;
                    }
                }
            }

            // Encode the scaled nanos to TDS
            int encodedLength = TDS.nanosSinceMidnightLength(TDS.MAX_FRACTIONAL_SECONDS_SCALE);
            byte[] encodedBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

            if (SSType.TIME == ssType) {
                byte[] cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytes, cryptoMeta, con);
                return cipherText;
            }
            else if (SSType.DATETIME2 == ssType) {
                // for DATETIME2 sends both date and time part together for encryption
                encodedBytesForEncryption = new byte[encodedLength + 3];
                System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, 0, encodedBytes.length);
            }
            else if (SSType.DATETIMEOFFSET == ssType) {
                // for DATETIMEOFFSET sends date, time and offset part together for encryption
                encodedBytesForEncryption = new byte[encodedLength + 5];
                System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, 0, encodedBytes.length);
            }
        }

        // Second, for types with a date component, write the days into the Common Era
        if (SSType.DATE == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
            // Computation of the number of days into the Common Era assumes that
            // the DAY_OF_YEAR field reflects a pure Gregorian calendar - one that
            // uses Gregorian leap year rules across the entire range of dates.
            //
            // For the DAY_OF_YEAR field to accurately reflect pure Gregorian behavior,
            // we need to use a pure Gregorian calendar for dates that are Julian dates
            // under a standard Gregorian calendar and for (Gregorian) dates later than
            // the cutover date in the cutover year.
            if (cal.getTimeInMillis() < GregorianChange.STANDARD_CHANGE_DATE.getTime()
                    || cal.getActualMaximum(Calendar.DAY_OF_YEAR) < TDS.DAYS_PER_YEAR) {
                int year = cal.get(Calendar.YEAR);
                int month = cal.get(Calendar.MONTH);
                int date = cal.get(Calendar.DATE);

                // Set the cutover as early as possible (pure Gregorian behavior)
                cal.setGregorianChange(GregorianChange.PURE_CHANGE_DATE);

                // Initialize the date field by field (preserving the "wall calendar" value)
                cal.set(year, month, date);
            }

            int daysIntoCE = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR), 1);

            // Last-ditch verification that the value is in the valid range for the
            // DATE/DATETIME2/DATETIMEOFFSET TDS data type (1/1/0001 to 12/31/9999).
            // If it's not, then throw an exception now so that statement execution
            // is safely canceled. Attempting to put an invalid value on the wire
            // would result in a TDS exception, which would close the connection.
            if (daysIntoCE < 0 || daysIntoCE >= DDC.daysSinceBaseDate(10000, 1, 1)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                Object[] msgArgs = {ssType};
                throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW, DriverError.NOT_SET, null);
            }

            byte encodedBytes[] = new byte[3];
            encodedBytes[0] = (byte) ((daysIntoCE >> 0) & 0xFF);
            encodedBytes[1] = (byte) ((daysIntoCE >> 8) & 0xFF);
            encodedBytes[2] = (byte) ((daysIntoCE >> 16) & 0xFF);

            byte[] cipherText;
            if (SSType.DATE == ssType) {
                cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytes, cryptoMeta, con);
            }
            else if (SSType.DATETIME2 == ssType) {
                // for Max value, does not round up, do casting instead.
                if (3652058 == daysIntoCE) {	// 9999-12-31
                    if (864000000000L == scaledNanos) {	// 24:00:00 in nanoseconds
                        // does not round up
                        scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight + getRoundedSubSecondNanos(subSecondNanos)) / divisor)
                                * divisor / 100;

                        int encodedLength = TDS.nanosSinceMidnightLength(TDS.MAX_FRACTIONAL_SECONDS_SCALE);
                        byte[] encodedNanoBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

                        // for DATETIME2 sends both date and time part together for encryption
                        encodedBytesForEncryption = new byte[encodedLength + 3];
                        System.arraycopy(encodedNanoBytes, 0, encodedBytesForEncryption, 0, encodedNanoBytes.length);
                    }
                }
                // Copy the 3 byte date value
                System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, (encodedBytesForEncryption.length - 3), 3);

                cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytesForEncryption, cryptoMeta, con);
            }
            else {
                // for Max value, does not round up, do casting instead.
                if (3652058 == daysIntoCE) {	// 9999-12-31
                    if (864000000000L == scaledNanos) {	// 24:00:00 in nanoseconds
                        // does not round up
                        scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight + getRoundedSubSecondNanos(subSecondNanos)) / divisor)
                                * divisor / 100;

                        int encodedLength = TDS.nanosSinceMidnightLength(TDS.MAX_FRACTIONAL_SECONDS_SCALE);
                        byte[] encodedNanoBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

                        // for DATETIMEOFFSET sends date, time and offset part together for encryption
                        encodedBytesForEncryption = new byte[encodedLength + 5];
                        System.arraycopy(encodedNanoBytes, 0, encodedBytesForEncryption, 0, encodedNanoBytes.length);
                    }
                }

                // Copy the 3 byte date value
                System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, (encodedBytesForEncryption.length - 5), 3);
                // Copy the 2 byte minutesOffset value
                System.arraycopy(ByteBuffer.allocate(Short.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putShort(minutesOffset).array(), 0,
                        encodedBytesForEncryption, (encodedBytesForEncryption.length - 2), 2);

                cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytesForEncryption, cryptoMeta, con);
            }
            return cipherText;
        }

        // Invalid type ssType. This condition should never happen.
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownSSType"));
        Object[] msgArgs = {ssType};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);

        return null;
    }

    private byte[] scaledNanosToEncodedBytes(long scaledNanos,
            int encodedLength) {
        byte encodedBytes[] = new byte[encodedLength];
        for (int i = 0; i < encodedLength; i++)
            encodedBytes[i] = (byte) ((scaledNanos >> (8 * i)) & 0xFF);
        return encodedBytes;
    }

    /**
     * Append the data in a stream in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param stream
     *            is the stream
     * @param streamLength
     *            length of the stream (may be unknown)
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     * @param jdbcType
     *            The JDBC type used to determine whether the value is textual or non-textual.
     * @param collation
     *            The SQL collation associated with the value. Null for non-textual SQL Server types.
     * @throws SQLServerException
     */
    void writeRPCInputStream(String sName,
            InputStream stream,
            long streamLength,
            boolean bOut,
            JDBCType jdbcType,
            SQLCollation collation) throws SQLServerException {
        assert null != stream;
        assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength >= 0;

        // Send long values and values with unknown length
        // using PLP chunking on Yukon and later.
        boolean usePLP = (DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength > DataTypes.SHORT_VARTYPE_MAX_BYTES);
        if (usePLP) {
            assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength <= DataTypes.MAX_VARTYPE_MAX_BYTES;

            writeRPCNameValType(sName, bOut, jdbcType.isTextual() ? TDSType.BIGVARCHAR : TDSType.BIGVARBINARY);

            // Handle Yukon v*max type header here.
            writeVMaxHeader(streamLength, false, jdbcType.isTextual() ? collation : null);
        }

        // Send non-PLP in all other cases
        else {
            // If the length of the InputStream is unknown then we need to buffer the entire stream
            // in memory so that we can determine its length and send that length to the server
            // before the stream data itself.
            if (DataTypes.UNKNOWN_STREAM_LENGTH == streamLength) {
                // Create ByteArrayOutputStream with initial buffer size of 8K to handle typical
                // binary field sizes more efficiently. Note we can grow beyond 8000 bytes.
                ByteArrayOutputStream baos = new ByteArrayOutputStream(8000);
                streamLength = 0L;

                // Since Shiloh is limited to 64K TDS packets, that's a good upper bound on the maximum
                // length of InputStream we should try to handle before throwing an exception.
                long maxStreamLength = 65535L * con.getTDSPacketSize();

                try {
                    byte buff[] = new byte[8000];
                    int bytesRead;

                    while (streamLength < maxStreamLength && -1 != (bytesRead = stream.read(buff, 0, buff.length))) {
                        baos.write(buff);
                        streamLength += bytesRead;
                    }
                }
                catch (IOException e) {
                    throw new SQLServerException(e.getMessage(), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET, e);
                }

                if (streamLength >= maxStreamLength) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
                    Object[] msgArgs = {streamLength};
                    SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
                }

                assert streamLength <= Integer.MAX_VALUE;
                stream = new ByteArrayInputStream(baos.toByteArray(), 0, (int) streamLength);
            }

            assert 0 <= streamLength && streamLength <= DataTypes.IMAGE_TEXT_MAX_BYTES;

            boolean useVarType = streamLength <= DataTypes.SHORT_VARTYPE_MAX_BYTES;

            writeRPCNameValType(sName, bOut,
                    jdbcType.isTextual() ? (useVarType ? TDSType.BIGVARCHAR : TDSType.TEXT) : (useVarType ? TDSType.BIGVARBINARY : TDSType.IMAGE));

            // Write maximum length, optional collation, and actual length
            if (useVarType) {
                writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
                if (jdbcType.isTextual())
                    collation.writeCollation(this);
                writeShort((short) streamLength);
            }
            else {
                writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
                if (jdbcType.isTextual())
                    collation.writeCollation(this);
                writeInt((int) streamLength);
            }
        }

        // Write the data
        writeStream(stream, streamLength, usePLP);
    }

    /**
     * Append the XML data in a stream in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param stream
     *            is the stream
     * @param streamLength
     *            length of the stream (may be unknown)
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     * @throws SQLServerException
     */
    void writeRPCXML(String sName,
            InputStream stream,
            long streamLength,
            boolean bOut) throws SQLServerException {
        assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength >= 0;
        assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength <= DataTypes.MAX_VARTYPE_MAX_BYTES;

        writeRPCNameValType(sName, bOut, TDSType.XML);
        writeByte((byte) 0); // No schema
        // Handle null here and return, we're done here if it's null.
        if (null == stream) {
            // Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
            writeLong(0xFFFFFFFFFFFFFFFFL);
        }
        else if (DataTypes.UNKNOWN_STREAM_LENGTH == streamLength) {
            // Append v*max length.
            // UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
            writeLong(0xFFFFFFFFFFFFFFFEL);

            // NOTE: Don't send the first chunk length, this will be calculated by caller.
        }
        else {
            // For v*max types with known length, length is <totallength8><chunklength4>
            // We're sending same total length as chunk length (as we're sending 1 chunk).
            writeLong(streamLength);
        }
        if (null != stream)
            // Write the data
            writeStream(stream, streamLength, true);
    }

    /**
     * Append the data in a character reader in RPC transmission format.
     * 
     * @param sName
     *            the optional parameter name
     * @param re
     *            the reader
     * @param reLength
     *            the reader data length (in characters)
     * @param bOut
     *            boolean true if the data value is being registered as an ouput parameter
     * @param collation
     *            The SQL collation associated with the value. Null for non-textual SQL Server types.
     * @throws SQLServerException
     */
    void writeRPCReaderUnicode(String sName,
            Reader re,
            long reLength,
            boolean bOut,
            SQLCollation collation) throws SQLServerException {
        assert null != re;
        assert DataTypes.UNKNOWN_STREAM_LENGTH == reLength || reLength >= 0;

        // Textual RPC requires a collation. If none is provided, as is the case when
        // the SSType is non-textual, then use the database collation by default.
        if (null == collation)
            collation = con.getDatabaseCollation();

        // Send long values and values with unknown length
        // using PLP chunking on Yukon and later.
        boolean usePLP = (DataTypes.UNKNOWN_STREAM_LENGTH == reLength || reLength > DataTypes.SHORT_VARTYPE_MAX_CHARS);
        if (usePLP) {
            assert DataTypes.UNKNOWN_STREAM_LENGTH == reLength || reLength <= DataTypes.MAX_VARTYPE_MAX_CHARS;

            writeRPCNameValType(sName, bOut, TDSType.NVARCHAR);

            // Handle Yukon v*max type header here.
            writeVMaxHeader((DataTypes.UNKNOWN_STREAM_LENGTH == reLength) ? DataTypes.UNKNOWN_STREAM_LENGTH : 2 * reLength,	// Length (in bytes)
                    false, collation);
        }

        // Send non-PLP in all other cases
        else {
            // Length must be known if we're not sending PLP-chunked data. Yukon is handled above.
            // For Shiloh, this is enforced in DTV by converting the Reader to some other length-
            // prefixed value in the setter.
            assert 0 <= reLength && reLength <= DataTypes.NTEXT_MAX_CHARS;

            // For non-PLP types, use the long TEXT type rather than the short VARCHAR
            // type if the stream is too long to fit in the latter or if we don't know the length up
            // front so we have to assume that it might be too long.
            boolean useVarType = reLength <= DataTypes.SHORT_VARTYPE_MAX_CHARS;

            writeRPCNameValType(sName, bOut, useVarType ? TDSType.NVARCHAR : TDSType.NTEXT);

            // Write maximum length, collation, and actual length of the data
            if (useVarType) {
                writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
                collation.writeCollation(this);
                writeShort((short) (2 * reLength));
            }
            else {
                writeInt(DataTypes.NTEXT_MAX_CHARS);
                collation.writeCollation(this);
                writeInt((int) (2 * reLength));
            }
        }

        // Write the data
        writeReader(re, reLength, usePLP);
    }
}

/**
 * TDSPacket provides a mechanism for chaining TDS response packets together in a singly-linked list.
 *
 * Having both the link and the data in the same class allows TDSReader marks (see below) to automatically hold onto exactly as much response data as
 * they need, and no more. Java reference semantics ensure that a mark holds onto its referenced packet and subsequent packets (through next
 * references). When all marked references to a packet go away, the packet, and any linked unmarked packets, can be reclaimed by GC.
 */
final class TDSPacket {
    final byte[] header = new byte[TDS.PACKET_HEADER_SIZE];
    final byte[] payload;
    int payloadLength;
    volatile TDSPacket next;

    final public String toString() {
        return "TDSPacket(SPID:" + Util.readUnsignedShortBigEndian(header, TDS.PACKET_HEADER_SPID) + " Seq:" + header[TDS.PACKET_HEADER_SEQUENCE_NUM]
                + ")";
    }

    TDSPacket(int size) {
        payload = new byte[size];
        payloadLength = 0;
        next = null;
    }

    final boolean isEOM() {
        return TDS.STATUS_BIT_EOM == (header[TDS.PACKET_HEADER_MESSAGE_STATUS] & TDS.STATUS_BIT_EOM);
    }
};

/**
 * TDSReaderMark encapsulates a fixed position in the response data stream.
 *
 * Response data is quantized into a linked chain of packets. A mark refers to a specific location in a specific packet and relies on Java's reference
 * semantics to automatically keep all subsequent packets accessible until the mark is destroyed.
 */
final class TDSReaderMark {
    final TDSPacket packet;
    final int payloadOffset;

    TDSReaderMark(TDSPacket packet,
            int payloadOffset) {
        this.packet = packet;
        this.payloadOffset = payloadOffset;
    }
}

/**
 * TDSReader encapsulates the TDS response data stream.
 *
 * Bytes are read from SQL Server into a FIFO of packets. Reader methods traverse the packets to access the data.
 */
final class TDSReader {
    private final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Reader");
    final private String traceID;

    final public String toString() {
        return traceID;
    }

    private final TDSChannel tdsChannel;
    private final SQLServerConnection con;

    private final TDSCommand command;

    final TDSCommand getCommand() {
        assert null != command;
        return command;
    }

    final SQLServerConnection getConnection() {
        return con;
    }

    private TDSPacket currentPacket = new TDSPacket(0);
    private TDSPacket lastPacket = currentPacket;
    private int payloadOffset = 0;
    private int packetNum = 0;

    private boolean isStreaming = true;
    private boolean useColumnEncryption = false;
    private boolean serverSupportsColumnEncryption = false;

    private final byte valueBytes[] = new byte[256];
    private static final AtomicInteger lastReaderID = new AtomicInteger(0);

    private static int nextReaderID() {
        return lastReaderID.incrementAndGet();
    }

    TDSReader(TDSChannel tdsChannel,
            SQLServerConnection con,
            TDSCommand command) {
        this.tdsChannel = tdsChannel;
        this.con = con;
        this.command = command; // may be null
        // if the logging level is not detailed than fine or more we will not have proper readerids.
        if (logger.isLoggable(Level.FINE))
            traceID = "TDSReader@" + nextReaderID() + " (" + con.toString() + ")";
        else
            traceID = con.toString();
        if (con.isColumnEncryptionSettingEnabled()) {
            useColumnEncryption = true;
        }
        serverSupportsColumnEncryption = con.getServerSupportsColumnEncryption();
    }

    final boolean isColumnEncryptionSettingEnabled() {
        return useColumnEncryption;
    }

    final boolean getServerSupportsColumnEncryption() {
        return serverSupportsColumnEncryption;
    }

    final void throwInvalidTDS() throws SQLServerException {
        if (logger.isLoggable(Level.SEVERE))
            logger.severe(toString() + " got unexpected value in TDS response at offset:" + payloadOffset);
        con.throwInvalidTDS();
    }

    final void throwInvalidTDSToken(String tokenName) throws SQLServerException {
        if (logger.isLoggable(Level.SEVERE))
            logger.severe(toString() + " got unexpected value in TDS response at offset:" + payloadOffset);
        con.throwInvalidTDSToken(tokenName);
    }

    /**
     * Ensures that payload data is available to be read, automatically advancing to (and possibly reading) the next packet.
     *
     * @return true if additional data is available to be read false if no more data is available
     */
    private boolean ensurePayload() throws SQLServerException {
        if (payloadOffset == currentPacket.payloadLength)
            if (!nextPacket())
                return false;
        assert payloadOffset < currentPacket.payloadLength;
        return true;
    }

    /**
     * Advance (and possibly read) the next packet.
     *
     * @return true if additional data is available to be read false if no more data is available
     */
    private boolean nextPacket() throws SQLServerException {
        assert null != currentPacket;

        // Shouldn't call this function unless we're at the end of the current packet...
        TDSPacket consumedPacket = currentPacket;
        assert payloadOffset == consumedPacket.payloadLength;

        // If no buffered packets are left then maybe we can read one...
        // This action must be synchronized against against another thread calling
        // readAllPackets() to read in ALL of the remaining packets of the current response.
        if (null == consumedPacket.next) {
            readPacket();

            if (null == consumedPacket.next)
                return false;
        }

        // Advance to that packet. If we are streaming through the
        // response, then unlink the current packet from the next
        // before moving to allow the packet to be reclaimed.
        TDSPacket nextPacket = consumedPacket.next;
        if (isStreaming) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Moving to next packet -- unlinking consumed packet");

            consumedPacket.next = null;
        }
        currentPacket = nextPacket;
        payloadOffset = 0;
        return true;
    }

    /**
     * Reads the next packet of the TDS channel.
     *
     * This method is synchronized to guard against simultaneously reading packets from one thread that is processing the response and another thread
     * that is trying to buffer it with TDSCommand.detach().
     */
    synchronized final boolean readPacket() throws SQLServerException {
        if (null != command && !command.readingResponse())
            return false;

        // Number of packets in should always be less than number of packets out.
        // If the server has been notified for an interrupt, it may be less by
        // more than one packet.
        assert tdsChannel.numMsgsRcvd < tdsChannel.numMsgsSent : "numMsgsRcvd:" + tdsChannel.numMsgsRcvd + " should be less than numMsgsSent:"
                + tdsChannel.numMsgsSent;

        TDSPacket newPacket = new TDSPacket(con.getTDSPacketSize());

        // First, read the packet header.
        for (int headerBytesRead = 0; headerBytesRead < TDS.PACKET_HEADER_SIZE;) {
            int bytesRead = tdsChannel.read(newPacket.header, headerBytesRead, TDS.PACKET_HEADER_SIZE - headerBytesRead);
            if (bytesRead < 0) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " Premature EOS in response. packetNum:" + packetNum + " headerBytesRead:" + headerBytesRead);

                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, ((0 == packetNum && 0 == headerBytesRead)
                        ? SQLServerException.getErrString("R_noServerResponse") : SQLServerException.getErrString("R_truncatedServerResponse")));
            }

            headerBytesRead += bytesRead;
        }

        // Header size is a 2 byte unsigned short integer in big-endian order.
        int packetLength = Util.readUnsignedShortBigEndian(newPacket.header, TDS.PACKET_HEADER_MESSAGE_LENGTH);

        // Make header size is properly bounded and compute length of the packet payload.
        if (packetLength < TDS.PACKET_HEADER_SIZE || packetLength > con.getTDSPacketSize()) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(
                        toString() + " TDS header contained invalid packet length:" + packetLength + "; packet size:" + con.getTDSPacketSize());
            }
            throwInvalidTDS();
        }

        newPacket.payloadLength = packetLength - TDS.PACKET_HEADER_SIZE;

        // Just grab the SPID for logging (another big-endian unsigned short).
        tdsChannel.setSPID(Util.readUnsignedShortBigEndian(newPacket.header, TDS.PACKET_HEADER_SPID));

        // Packet header looks good enough.
        // When logging, copy the packet header to the log buffer.
        byte[] logBuffer = null;
        if (tdsChannel.isLoggingPackets()) {
            logBuffer = new byte[packetLength];
            System.arraycopy(newPacket.header, 0, logBuffer, 0, TDS.PACKET_HEADER_SIZE);
        }

        // Now for the payload...
        for (int payloadBytesRead = 0; payloadBytesRead < newPacket.payloadLength;) {
            int bytesRead = tdsChannel.read(newPacket.payload, payloadBytesRead, newPacket.payloadLength - payloadBytesRead);
            if (bytesRead < 0)
                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, SQLServerException.getErrString("R_truncatedServerResponse"));

            payloadBytesRead += bytesRead;
        }

        ++packetNum;

        lastPacket.next = newPacket;
        lastPacket = newPacket;

        // When logging, append the payload to the log buffer and write out the whole thing.
        if (tdsChannel.isLoggingPackets()) {
            System.arraycopy(newPacket.payload, 0, logBuffer, TDS.PACKET_HEADER_SIZE, newPacket.payloadLength);
            tdsChannel.logPacket(logBuffer, 0, packetLength,
                    this.toString() + " received Packet:" + packetNum + " (" + newPacket.payloadLength + " bytes)");
        }

        // If end of message, then bump the count of messages received and disable
        // interrupts. If an interrupt happened prior to disabling, then expect
        // to read the attention ack packet as well.
        if (newPacket.isEOM()) {
            ++tdsChannel.numMsgsRcvd;

            // Notify the command (if any) that we've reached the end of the response.
            if (null != command)
                command.onResponseEOM();
        }

        return true;
    }

    final TDSReaderMark mark() {
        TDSReaderMark mark = new TDSReaderMark(currentPacket, payloadOffset);
        isStreaming = false;

        if (logger.isLoggable(Level.FINEST))
            logger.finest(this.toString() + ": Buffering from: " + mark.toString());

        return mark;
    }

    final void reset(TDSReaderMark mark) {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(this.toString() + ": Resetting to: " + mark.toString());

        currentPacket = mark.packet;
        payloadOffset = mark.payloadOffset;
    }

    final void stream() {
        isStreaming = true;
    }

    /**
     * Returns the number of bytes that can be read (or skipped over) from this TDSReader without blocking by the next caller of a method for this
     * TDSReader.
     *
     * @return the actual number of bytes available.
     */
    final int available() {
        // The number of bytes that can be read without blocking is just the number
        // of bytes that are currently buffered. That is the number of bytes left
        // in the current packet plus the number of bytes in the remaining packets.
        int available = currentPacket.payloadLength - payloadOffset;
        for (TDSPacket packet = currentPacket.next; null != packet; packet = packet.next)
            available += packet.payloadLength;
        return available;
    }

    /**
     * 
     * @return number of bytes available in the current packet
     */
    final int availableCurrentPacket() {
        /*
         * The number of bytes that can be read from the current chunk, without including the next chunk that is buffered. This is so the driver can
         * confirm if the next chunk sent is new packet or just continuation
         */
        int available = currentPacket.payloadLength - payloadOffset;
        return available;
    }

    final int peekTokenType() throws SQLServerException {
        // Check whether we're at EOF
        if (!ensurePayload())
            return -1;

        // Peek at the current byte (don't increment payloadOffset!)
        return currentPacket.payload[payloadOffset] & 0xFF;
    }

    final short peekStatusFlag() throws SQLServerException {
        // skip the current packet(i.e, TDS packet type) and peek into the status flag (USHORT)
        if (payloadOffset + 3 <= currentPacket.payloadLength) {
            short value = Util.readShort(currentPacket.payload, payloadOffset + 1);
            return value;
        }

        return 0;
    }

    final int readUnsignedByte() throws SQLServerException {
        // Ensure that we have a packet to read from.
        if (!ensurePayload())
            throwInvalidTDS();

        return currentPacket.payload[payloadOffset++] & 0xFF;
    }

    final short readShort() throws SQLServerException {
        if (payloadOffset + 2 <= currentPacket.payloadLength) {
            short value = Util.readShort(currentPacket.payload, payloadOffset);
            payloadOffset += 2;
            return value;
        }

        return Util.readShort(readWrappedBytes(2), 0);
    }

    final int readUnsignedShort() throws SQLServerException {
        if (payloadOffset + 2 <= currentPacket.payloadLength) {
            int value = Util.readUnsignedShort(currentPacket.payload, payloadOffset);
            payloadOffset += 2;
            return value;
        }

        return Util.readUnsignedShort(readWrappedBytes(2), 0);
    }

    final String readUnicodeString(int length) throws SQLServerException {
        int byteLength = 2 * length;
        byte bytes[] = new byte[byteLength];
        readBytes(bytes, 0, byteLength);
        return Util.readUnicodeString(bytes, 0, byteLength, con);

    }

    final char readChar() throws SQLServerException {
        return (char) readShort();
    }

    final int readInt() throws SQLServerException {
        if (payloadOffset + 4 <= currentPacket.payloadLength) {
            int value = Util.readInt(currentPacket.payload, payloadOffset);
            payloadOffset += 4;
            return value;
        }

        return Util.readInt(readWrappedBytes(4), 0);
    }

    final int readIntBigEndian() throws SQLServerException {
        if (payloadOffset + 4 <= currentPacket.payloadLength) {
            int value = Util.readIntBigEndian(currentPacket.payload, payloadOffset);
            payloadOffset += 4;
            return value;
        }

        return Util.readIntBigEndian(readWrappedBytes(4), 0);
    }

    final long readUnsignedInt() throws SQLServerException {
        return readInt() & 0xFFFFFFFFL;
    }

    final long readLong() throws SQLServerException {
        if (payloadOffset + 8 <= currentPacket.payloadLength) {
            long value = Util.readLong(currentPacket.payload, payloadOffset);
            payloadOffset += 8;
            return value;
        }

        return Util.readLong(readWrappedBytes(8), 0);
    }

    final void readBytes(byte[] value,
            int valueOffset,
            int valueLength) throws SQLServerException {
        for (int bytesRead = 0; bytesRead < valueLength;) {
            // Ensure that we have a packet to read from.
            if (!ensurePayload())
                throwInvalidTDS();

            // Figure out how many bytes to copy from the current packet
            // (the lesser of the remaining value bytes and the bytes left in the packet).
            int bytesToCopy = valueLength - bytesRead;
            if (bytesToCopy > currentPacket.payloadLength - payloadOffset)
                bytesToCopy = currentPacket.payloadLength - payloadOffset;

            // Copy some bytes from the current packet to the destination value.
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Reading " + bytesToCopy + " bytes from offset " + payloadOffset);

            System.arraycopy(currentPacket.payload, payloadOffset, value, valueOffset + bytesRead, bytesToCopy);
            bytesRead += bytesToCopy;
            payloadOffset += bytesToCopy;
        }
    }

    final byte[] readWrappedBytes(int valueLength) throws SQLServerException {
        assert valueLength <= valueBytes.length;
        readBytes(valueBytes, 0, valueLength);
        return valueBytes;
    }

    final Object readDecimal(int valueLength,
            TypeInfo typeInfo,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        if (valueLength > valueBytes.length) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(toString() + " Invalid value length:" + valueLength);
            }
            throwInvalidTDS();
        }

        readBytes(valueBytes, 0, valueLength);
        return DDC.convertBigDecimalToObject(Util.readBigDecimal(valueBytes, valueLength, typeInfo.getScale()), jdbcType, streamType);
    }

    final Object readMoney(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        BigInteger bi;
        switch (valueLength) {
            case 8: // money
            {
                int intBitsHi = readInt();
                int intBitsLo = readInt();

                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[8];
                    Util.writeIntBigEndian(intBitsHi, value, 0);
                    Util.writeIntBigEndian(intBitsLo, value, 4);
                    return value;
                }

                bi = BigInteger.valueOf(((long) intBitsHi << 32) | (intBitsLo & 0xFFFFFFFFL));
                break;
            }

            case 4: // smallmoney
                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[4];
                    Util.writeIntBigEndian(readInt(), value, 0);
                    return value;
                }

                bi = BigInteger.valueOf(readInt());
                break;

            default:
                throwInvalidTDS();
                return null;
        }

        return DDC.convertBigDecimalToObject(new BigDecimal(bi, 4), jdbcType, streamType);
    }

    final Object readReal(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        if (4 != valueLength)
            throwInvalidTDS();

        return DDC.convertFloatToObject(Float.intBitsToFloat(readInt()), jdbcType, streamType);
    }

    final Object readFloat(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        if (8 != valueLength)
            throwInvalidTDS();

        return DDC.convertDoubleToObject(Double.longBitsToDouble(readLong()), jdbcType, streamType);
    }

    final Object readDateTime(int valueLength,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        // Build and return the right kind of temporal object.
        int daysSinceSQLBaseDate;
        int ticksSinceMidnight;
        int msecSinceMidnight;

        switch (valueLength) {
            case 8:
                // SQL datetime is 4 bytes for days since SQL Base Date
                // (January 1, 1900 00:00:00 GMT) and 4 bytes for
                // the number of three hundredths (1/300) of a second
                // since midnight.
                daysSinceSQLBaseDate = readInt();
                ticksSinceMidnight = readInt();

                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[8];
                    Util.writeIntBigEndian(daysSinceSQLBaseDate, value, 0);
                    Util.writeIntBigEndian(ticksSinceMidnight, value, 4);
                    return value;
                }

                msecSinceMidnight = (ticksSinceMidnight * 10 + 1) / 3; // Convert to msec (1 tick = 1 300th of a sec = 3 msec)
                break;

            case 4:
                // SQL smalldatetime has less precision. It stores 2 bytes
                // for the days since SQL Base Date and 2 bytes for minutes
                // after midnight.
                daysSinceSQLBaseDate = readUnsignedShort();
                ticksSinceMidnight = readUnsignedShort();

                if (JDBCType.BINARY == jdbcType) {
                    byte value[] = new byte[4];
                    Util.writeShortBigEndian((short) daysSinceSQLBaseDate, value, 0);
                    Util.writeShortBigEndian((short) ticksSinceMidnight, value, 2);
                    return value;
                }

                msecSinceMidnight = ticksSinceMidnight * 60 * 1000; // Convert to msec (1 tick = 1 min = 60,000 msec)
                break;

            default:
                throwInvalidTDS();
                return null;
        }

        // Convert the DATETIME/SMALLDATETIME value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME, appTimeZoneCalendar, daysSinceSQLBaseDate, msecSinceMidnight, 0); // scale
                                                                                                                                        // (ignored
                                                                                                                                        // for
                                                                                                                                        // fixed-scale
                                                                                                                                        // DATETIME/SMALLDATETIME
                                                                                                                                        // types)
    }

    final Object readDate(int valueLength,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.DAYS_INTO_CE_LENGTH != valueLength)
            throwInvalidTDS();

        // Initialize the date fields to their appropriate values.
        int localDaysIntoCE = readDaysIntoCE();

        // Convert the DATE value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATE, appTimeZoneCalendar, localDaysIntoCE, 0,  // midnight local to app time zone
                0); // scale (ignored for DATE)
    }

    final Object readTime(int valueLength,
            TypeInfo typeInfo,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.timeValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // Read the value from the server
        long localNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());

        // Convert the TIME value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.TIME, appTimeZoneCalendar, 0, localNanosSinceMidnight, typeInfo.getScale());
    }

    final Object readDateTime2(int valueLength,
            TypeInfo typeInfo,
            Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.datetime2ValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // Read the value's constituent components
        long localNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());
        int localDaysIntoCE = readDaysIntoCE();

        // Convert the DATETIME2 value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME2, appTimeZoneCalendar, localDaysIntoCE, localNanosSinceMidnight,
                typeInfo.getScale());
    }

    final Object readDateTimeOffset(int valueLength,
            TypeInfo typeInfo,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.datetimeoffsetValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // The nanos since midnight and days into Common Era parts of DATETIMEOFFSET values
        // are in UTC. Use the minutes offset part to convert to local.
        long utcNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());
        int utcDaysIntoCE = readDaysIntoCE();
        int localMinutesOffset = readShort();

        // Convert the DATETIMEOFFSET value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIMEOFFSET,
                new GregorianCalendar(new SimpleTimeZone(localMinutesOffset * 60 * 1000, ""), Locale.US), utcDaysIntoCE, utcNanosSinceMidnight,
                typeInfo.getScale());
    }

    private int readDaysIntoCE() throws SQLServerException {
        byte value[] = new byte[TDS.DAYS_INTO_CE_LENGTH];
        readBytes(value, 0, value.length);

        int daysIntoCE = 0;
        for (int i = 0; i < value.length; i++)
            daysIntoCE |= ((value[i] & 0xFF) << (8 * i));

        // Theoretically should never encounter a value that is outside of the valid date range
        if (daysIntoCE < 0)
            throwInvalidTDS();

        return daysIntoCE;
    }

    // Scale multipliers used to convert variable-scaled temporal values to a fixed 100ns scale.
    //
    // Using this array is measurably faster than using Math.pow(10, ...)
    private final static int[] SCALED_MULTIPLIERS = {10000000, 1000000, 100000, 10000, 1000, 100, 10, 1};

    private long readNanosSinceMidnight(int scale) throws SQLServerException {
        assert 0 <= scale && scale <= TDS.MAX_FRACTIONAL_SECONDS_SCALE;

        byte value[] = new byte[TDS.nanosSinceMidnightLength(scale)];
        readBytes(value, 0, value.length);

        long hundredNanosSinceMidnight = 0;
        for (int i = 0; i < value.length; i++)
            hundredNanosSinceMidnight |= (value[i] & 0xFFL) << (8 * i);

        hundredNanosSinceMidnight *= SCALED_MULTIPLIERS[scale];

        if (!(0 <= hundredNanosSinceMidnight && hundredNanosSinceMidnight < Nanos.PER_DAY / 100))
            throwInvalidTDS();

        return 100 * hundredNanosSinceMidnight;
    }

    final static String guidTemplate = "NNNNNNNN-NNNN-NNNN-NNNN-NNNNNNNNNNNN";

    final Object readGUID(int valueLength,
            JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        // GUIDs must be exactly 16 bytes
        if (16 != valueLength)
            throwInvalidTDS();

        // Read in the GUID's binary value
        byte guid[] = new byte[16];
        readBytes(guid, 0, 16);

        switch (jdbcType) {
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case GUID: {
                StringBuilder sb = new StringBuilder(guidTemplate.length());
                for (int i = 0; i < 4; i++) {
                    sb.append(Util.hexChars[(guid[3 - i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[3 - i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 2; i++) {
                    sb.append(Util.hexChars[(guid[5 - i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[5 - i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 2; i++) {
                    sb.append(Util.hexChars[(guid[7 - i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[7 - i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 2; i++) {
                    sb.append(Util.hexChars[(guid[8 + i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[8 + i] & 0x0F]);
                }
                sb.append('-');
                for (int i = 0; i < 6; i++) {
                    sb.append(Util.hexChars[(guid[10 + i] & 0xF0) >> 4]);
                    sb.append(Util.hexChars[guid[10 + i] & 0x0F]);
                }

                try {
                    return DDC.convertStringToObject(sb.toString(), Encoding.UNICODE.charset(), jdbcType, streamType);
                }
                catch (UnsupportedEncodingException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
                    throw new SQLServerException(form.format(new Object[] {"UNIQUEIDENTIFIER", jdbcType}), null, 0, e);
                }
            }

            default: {
                if (StreamType.BINARY == streamType || StreamType.ASCII == streamType)
                    return new ByteArrayInputStream(guid);

                return guid;
            }
        }
    }

    /**
     * Reads a multi-part table name from TDS and returns it as an array of Strings.
     */
    final SQLIdentifier readSQLIdentifier() throws SQLServerException {
        // Multi-part names should have between 1 and 4 parts
        int numParts = readUnsignedByte();
        if (!(1 <= numParts && numParts <= 4))
            throwInvalidTDS();

        // Each part is a length-prefixed Unicode string
        String[] nameParts = new String[numParts];
        for (int i = 0; i < numParts; i++)
            nameParts[i] = readUnicodeString(readUnsignedShort());

        // Build the identifier from the name parts
        SQLIdentifier identifier = new SQLIdentifier();
        identifier.setObjectName(nameParts[numParts - 1]);
        if (numParts >= 2)
            identifier.setSchemaName(nameParts[numParts - 2]);
        if (numParts >= 3)
            identifier.setDatabaseName(nameParts[numParts - 3]);
        if (4 == numParts)
            identifier.setServerName(nameParts[numParts - 4]);

        return identifier;
    }

    final SQLCollation readCollation() throws SQLServerException {
        SQLCollation collation = null;

        try {
            collation = new SQLCollation(this);
        }
        catch (UnsupportedEncodingException e) {
            con.terminate(SQLServerException.DRIVER_ERROR_INVALID_TDS, e.getMessage(), e);
            // not reached
        }

        return collation;
    }

    final void skip(int bytesToSkip) throws SQLServerException {
        assert bytesToSkip >= 0;

        while (bytesToSkip > 0) {
            // Ensure that we have a packet to read from.
            if (!ensurePayload())
                throwInvalidTDS();

            int bytesSkipped = bytesToSkip;
            if (bytesSkipped > currentPacket.payloadLength - payloadOffset)
                bytesSkipped = currentPacket.payloadLength - payloadOffset;

            bytesToSkip -= bytesSkipped;
            payloadOffset += bytesSkipped;
        }
    }

    final void TryProcessFeatureExtAck(boolean featureExtAckReceived) throws SQLServerException {
        // in case of redirection, do not check if TDS_FEATURE_EXTENSION_ACK is received or not.
        if (null != this.con.getRoutingInfo()) {
            return;
        }

        if (isColumnEncryptionSettingEnabled() && !featureExtAckReceived)
            throw new SQLServerException(this, SQLServerException.getErrString("R_AE_NotSupportedByServer"), null, 0, false);
    }
}

/**
 * Timer for use with Commands that support a timeout.
 *
 * Once started, the timer runs for the prescribed number of seconds unless stopped. If the timer runs out, it interrupts its associated Command with
 * a reason like "timed out".
 */
final class TimeoutTimer implements Runnable {
    private static final String threadGroupName = "mssql-jdbc-TimeoutTimer";
    private final int timeoutSeconds;
    private final TDSCommand command;
    private volatile Future<?> task;

    private static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicReference<ThreadGroup> tgr = new AtomicReference<>();
        private final AtomicInteger threadNumber = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r)
        {
            ThreadGroup tg = tgr.get();

            if (tg == null || tg.isDestroyed())
            {
                tg = new ThreadGroup(threadGroupName);
                tgr.set(tg);
            }

            Thread t = new Thread(tg, r, tg.getName() + "-" + threadNumber.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    });

    private volatile boolean canceled = false;

    TimeoutTimer(int timeoutSeconds,
            TDSCommand command) {
        assert timeoutSeconds > 0;
        assert null != command;

        this.timeoutSeconds = timeoutSeconds;
        this.command = command;
    }

    final void start() {
        task = executor.submit(this);
    }

    final void stop() {
        task.cancel(true);
        canceled = true;
    }

    public void run() {
        int secondsRemaining = timeoutSeconds;
        try {
            // Poll every second while time is left on the timer.
            // Return if/when the timer is canceled.
            do {
                if (canceled)
                    return;

                Thread.sleep(1000);
            }
            while (--secondsRemaining > 0);
        }
        catch (InterruptedException e) {
            // re-interrupt the current thread, in order to restore the thread's interrupt status.
            Thread.currentThread().interrupt();
            return;
        }

        // If the timer wasn't canceled before it ran out of
        // time then interrupt the registered command.
        try {
            command.interrupt(SQLServerException.getErrString("R_queryTimedOut"));
        }
        catch (SQLServerException e) {
            // Unfortunately, there's nothing we can do if we
            // fail to time out the request. There is no way
            // to report back what happened.
            command.log(Level.FINE, "Command could not be timed out. Reason: " + e.getMessage());
        }
    }
}

/**
 * TDSCommand encapsulates an interruptable TDS conversation.
 *
 * A conversation may consist of one or more TDS request and response messages. A command may be interrupted at any point, from any thread, and for
 * any reason. Acknowledgement and handling of an interrupt is fully encapsulated by this class.
 *
 * Commands may be created with an optional timeout (in seconds). Timeouts are implemented as a form of interrupt, where the interrupt event occurs
 * when the timeout period expires. Currently, only the time to receive the response from the channel counts against the timeout period.
 */
abstract class TDSCommand {
    abstract boolean doExecute() throws SQLServerException;

    final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Command");
    private final String logContext;

    final String getLogContext() {
        return logContext;
    }

    private String traceID;

    final public String toString() {
        if (traceID == null)
            traceID = "TDSCommand@" + Integer.toHexString(hashCode()) + " (" + logContext + ")";
        return traceID;
    }

    final void log(Level level,
            String message) {
        logger.log(level, toString() + ": " + message);
    }

    // Optional timer that is set if the command was created with a non-zero timeout period.
    // When the timer expires, the command is interrupted.
    private final TimeoutTimer timeoutTimer;

    // TDS channel accessors
    // These are set/reset at command execution time.
    // Volatile ensures visibility to execution thread and interrupt thread
    private volatile TDSWriter tdsWriter;
    private volatile TDSReader tdsReader;
    
    protected TDSWriter getTDSWriter(){
        return tdsWriter;
    }

    // Lock to ensure atomicity when manipulating more than one of the following
    // shared interrupt state variables below.
    private final Object interruptLock = new Object();

    // Flag set when this command starts execution, indicating that it is
    // ready to respond to interrupts; and cleared when its last response packet is
    // received, indicating that it is no longer able to respond to interrupts.
    // If the command is interrupted after interrupts have been disabled, then the
    // interrupt is ignored.
    private volatile boolean interruptsEnabled = false;

    protected boolean getInterruptsEnabled() {
        return interruptsEnabled;
    }

    protected void setInterruptsEnabled(boolean interruptsEnabled) {
        synchronized (interruptLock) {
            this.interruptsEnabled = interruptsEnabled;
        }
    }

    // Flag set to indicate that an interrupt has happened.
    private volatile boolean wasInterrupted = false;

    private boolean wasInterrupted() {
        return wasInterrupted;
    }

    // The reason for the interrupt.
    private volatile String interruptReason = null;

    // Flag set when this command's request to the server is complete.
    // If a command is interrupted before its request is complete, it is the executing
    // thread's responsibility to send the attention signal to the server if necessary.
    // After the request is complete, the interrupting thread must send the attention signal.
    private volatile boolean requestComplete;

    protected boolean getRequestComplete() {
        return requestComplete;
    }

    protected void setRequestComplete(boolean requestComplete) {
        synchronized (interruptLock) {
            this.requestComplete = requestComplete;
        }
    }

    // Flag set when an attention signal has been sent to the server, indicating that a
    // TDS packet containing the attention ack message is to be expected in the response.
    // This flag is cleared after the attention ack message has been received and processed.
    private volatile boolean attentionPending = false;

    boolean attentionPending() {
        return attentionPending;
    }

    // Flag set when this command's response has been processed. Until this flag is set,
    // there may be unprocessed information left in the response, such as transaction
    // ENVCHANGE notifications.
    private volatile boolean processedResponse;

    protected boolean getProcessedResponse() {
        return processedResponse;
    }

    protected void setProcessedResponse(boolean processedResponse) {
        synchronized (interruptLock) {
            this.processedResponse = processedResponse;
        }
    }

    // Flag set when this command's response is ready to be read from the server and cleared
    // after its response has been received, but not necessarily processed, up to and including
    // any attention ack. The command's response is read either on demand as it is processed,
    // or by detaching.
    private volatile boolean readingResponse;

    final boolean readingResponse() {
        return readingResponse;
    }

    /**
     * Creates this command with an optional timeout.
     *
     * @param logContext
     *            the string describing the context for this command.
     * @param timeoutSeconds
     *            (optional) the time before which the command must complete before it is interrupted. A value of 0 means no timeout.
     */
    TDSCommand(String logContext,
            int timeoutSeconds) {
        this.logContext = logContext;
        this.timeoutTimer = (timeoutSeconds > 0) ? (new TimeoutTimer(timeoutSeconds, this)) : null;
    }

    /**
     * Executes this command.
     *
     * @param tdsWriter
     * @param tdsReader
     * @throws SQLServerException
     *             on any error executing the command, including cancel or timeout.
     */

    boolean execute(TDSWriter tdsWriter,
            TDSReader tdsReader) throws SQLServerException {
        this.tdsWriter = tdsWriter;
        this.tdsReader = tdsReader;
        assert null != tdsReader;
        try {
            return doExecute(); // Derived classes implement the execution details
        }
        catch (SQLServerException e) {
            try {
                // If command execution threw an exception for any reason before the request
                // was complete then interrupt the command (it may already be interrupted)
                // and close it out to ensure that any response to the error/interrupt
                // is processed.
                // no point in trying to cancel on a closed connection.
                if (!requestComplete && !tdsReader.getConnection().isClosed()) {
                    interrupt(e.getMessage());
                    onRequestComplete();
                    close();
                }
            }
            catch (SQLServerException interruptException) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(this.toString() + ": Ignoring error in sending attention: " + interruptException.getMessage());
            }
            // throw the original exception even if trying to interrupt fails even in the case
            // of trying to send a cancel to the server.
            throw e;
        }
    }

    /**
     * Provides sane default response handling.
     *
     * This default implementation just consumes everything in the response message.
     */
    void processResponse(TDSReader tdsReader) throws SQLServerException {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(this.toString() + ": Processing response");
        try {
            TDSParser.parse(tdsReader, getLogContext());
        }
        catch (SQLServerException e) {
            if (SQLServerException.DRIVER_ERROR_FROM_DATABASE != e.getDriverErrorCode())
                throw e;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Ignoring error from database: " + e.getMessage());
        }
    }

    /**
     * Clears this command from the TDS channel so that another command can execute.
     *
     * This method does not process the response. It just buffers it in memory, including any attention ack that may be present.
     */
    final void detach() throws SQLServerException {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(this + ": detaching...");

        // Read any remaining response packets from the server.
        // This operation may be timed out or cancelled from another thread.
        while (tdsReader.readPacket())
            ;

        // Postcondition: the entire response has been read
        assert !readingResponse;
    }

    final void close() {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(this + ": closing...");

        if (logger.isLoggable(Level.FINEST))
            logger.finest(this + ": processing response...");

        while (!processedResponse) {
            try {
                processResponse(tdsReader);
            }
            catch (SQLServerException e) {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(this + ": close ignoring error processing response: " + e.getMessage());

                if (tdsReader.getConnection().isSessionUnAvailable()) {
                    processedResponse = true;
                    attentionPending = false;
                }
            }
        }

        if (attentionPending) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this + ": processing attention ack...");

            try {
                TDSParser.parse(tdsReader, "attention ack");
            }
            catch (SQLServerException e) {
                if (tdsReader.getConnection().isSessionUnAvailable()) {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(this + ": giving up on attention ack after connection closed by exception: " + e);
                    attentionPending = false;
                }
                else {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(this + ": ignored exception: " + e);
                }
            }

            // If the parser returns to us without processing the expected attention ack,
            // then assume that no attention ack is forthcoming from the server and
            // terminate the connection to prevent any other command from executing.
            if (attentionPending) {
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.severe(this.toString() + ": expected attn ack missing or not processed; terminating connection...");
                }

                try {
                    tdsReader.throwInvalidTDS();
                }
                catch (SQLServerException e) {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(this + ": ignored expected invalid TDS exception: " + e);

                    assert tdsReader.getConnection().isSessionUnAvailable();
                    attentionPending = false;
                }
            }
        }

        // Postcondition:
        // Response has been processed and there is no attention pending -- the command is closed.
        // Of course the connection may be closed too, but the command is done regardless...
        assert processedResponse && !attentionPending;
    }

    /**
     * Interrupts execution of this command, typically from another thread.
     *
     * Only the first interrupt has any effect. Subsequent interrupts are ignored. Interrupts are also ignored until enabled. If interrupting the
     * command requires an attention signal to be sent to the server, then this method sends that signal if the command's request is already complete.
     *
     * Signalling mechanism is "fire and forget". It is up to either the execution thread or, possibly, a detaching thread, to ensure that any pending
     * attention ack later will be received and processed.
     *
     * @param reason
     *            the reason for the interrupt, typically cancel or timeout.
     * @throws SQLServerException
     *             if interrupting fails for some reason. This call does not throw the reason for the interrupt.
     */
    void interrupt(String reason) throws SQLServerException {
        // Multiple, possibly simultaneous, interrupts may occur.
        // Only the first one should be recognized and acted upon.
        synchronized (interruptLock) {
            if (interruptsEnabled && !wasInterrupted()) {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(this + ": Raising interrupt for reason:" + reason);

                wasInterrupted = true;
                interruptReason = reason;
                if (requestComplete)
                    attentionPending = tdsWriter.sendAttention();

            }
        }
    }

    private boolean interruptChecked = false;

    /**
     * Checks once whether an interrupt has occurred, and, if it has, throws an exception indicating that fact.
     *
     * Any calls after the first to check for interrupts are no-ops. This method is called periodically from this command's execution thread to notify
     * the app when an interrupt has happened.
     *
     * It should only be called from places where consistent behavior can be ensured after the exception is thrown. For example, it should not be
     * called at arbitrary times while processing the response, as doing so could leave the response token stream in an inconsistent state. Currently,
     * response processing only checks for interrupts after every result or OUT parameter.
     *
     * Request processing checks for interrupts before writing each packet.
     *
     * @throws SQLServerException
     *             if this command was interrupted, throws the reason for the interrupt.
     */
    final void checkForInterrupt() throws SQLServerException {
        // Throw an exception with the interrupt reason if this command was interrupted.
        // Note that the interrupt reason may be null. Checking whether the
        // command was interrupted does not require the interrupt lock since only one
        // of the shared state variables is being manipulated; interruptChecked is not
        // shared with the interrupt thread.
        if (wasInterrupted() && !interruptChecked) {
            interruptChecked = true;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(this + ": throwing interrupt exception, reason: " + interruptReason);

            throw new SQLServerException(interruptReason, SQLState.STATEMENT_CANCELED, DriverError.NOT_SET, null);
        }
    }

    /**
     * Notifies this command when no more request packets are to be sent to the server.
     *
     * After the last packet has been sent, the only way to interrupt the request is to send an attention signal from the interrupt() method.
     *
     * Note that this method is called when the request completes normally (last packet sent with EOM bit) or when it completes after being
     * interrupted (0 or more packets sent with no EOM bit).
     */
    final void onRequestComplete() throws SQLServerException {
        synchronized (interruptLock) {
	        assert !requestComplete;
	
	        if (logger.isLoggable(Level.FINEST))
	            logger.finest(this + ": request complete");

            requestComplete = true;

            // If this command was interrupted before its request was complete then
            // we need to send the attention signal if necessary. Note that if no
            // attention signal is sent (i.e. no packets were sent to the server before
            // the interrupt happened), then don't expect an attention ack or any
            // other response.
            if (!interruptsEnabled) {
                assert !attentionPending;
                assert !processedResponse;
                assert !readingResponse;
                processedResponse = true;
            }
            else if (wasInterrupted()) {

                if (tdsWriter.isEOMSent()) {
                    attentionPending = tdsWriter.sendAttention();
                    readingResponse = attentionPending;
                }
                else {
                    assert !attentionPending;
                    readingResponse = tdsWriter.ignoreMessage();
                }

                processedResponse = !readingResponse;
            }
            else {
                assert !attentionPending;
                assert !processedResponse;
                readingResponse = true;
            }
        }
    }

    /**
     * Notifies this command when the last packet of the response has been read.
     *
     * When the last packet is read, interrupts are disabled. If an interrupt occurred prior to disabling that caused an attention signal to be sent
     * to the server, then an extra packet containing the attention ack is read.
     *
     * This ensures that on return from this method, the TDS channel is clear of all response packets for this command.
     *
     * Note that this method is called for the attention ack message itself as well, so we need to be sure not to expect more than one attention
     * ack...
     */
    final void onResponseEOM() throws SQLServerException {
        boolean readAttentionAck = false;

        // Atomically disable interrupts and check for a previous interrupt requiring
        // an attention ack to be read.
        synchronized (interruptLock) {
            if (interruptsEnabled) {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(this + ": disabling interrupts");

                // Determine whether we still need to read the attention ack packet.
                //
                // When a command is interrupted, Yukon (and later) always sends a response
                // containing at least a DONE(ERROR) token before it sends the attention ack,
                // even if the command's request was not complete.
                readAttentionAck = attentionPending;

                interruptsEnabled = false;
            }
        }

        // If an attention packet needs to be read then read it. This should
        // be done outside of the interrupt lock to avoid unnecessarily blocking
        // interrupting threads. Note that it is remotely possible that the call
        // to readPacket won't actually read anything if the attention ack was
        // already read by TDSCommand.detach(), in which case this method could
        // be called from multiple threads, leading to a benign followup process
        // to clear the readingResponse flag.
        if (readAttentionAck)
            tdsReader.readPacket();

        readingResponse = false;
    }

    /**
     * Notifies this command when the end of its response token stream has been reached.
     *
     * After this call, we are guaranteed that tokens in the response have been processed.
     */
    final void onTokenEOF() {
        processedResponse = true;
    }

    /**
     * Notifies this command when the attention ack (a DONE token with a special flag) has been processed.
     *
     * After this call, the attention ack should no longer be expected.
     */
    final void onAttentionAck() {
        assert attentionPending;
        attentionPending = false;
    }

    /**
     * Starts sending this command's TDS request to the server.
     *
     * @param tdsMessageType
     *            the type of the TDS message (RPC, QUERY, etc.)
     * @return the TDS writer used to write the request.
     * @throws SQLServerException
     *             on any error, including acknowledgement of an interrupt.
     */
    final TDSWriter startRequest(byte tdsMessageType) throws SQLServerException {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(this + ": starting request...");

        // Start this command's request message
        try {
            tdsWriter.startMessage(this, tdsMessageType);
        }
        catch (SQLServerException e) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this + ": starting request: exception: " + e.getMessage());

            throw e;
        }

        // (Re)initialize this command's interrupt state for its current execution.
        // To ensure atomically consistent behavior, do not leave the interrupt lock
        // until interrupts have been (re)enabled.
        synchronized (interruptLock) {
            requestComplete = false;
            readingResponse = false;
            processedResponse = false;
            attentionPending = false;
            wasInterrupted = false;
            interruptReason = null;
            interruptsEnabled = true;
        }

        return tdsWriter;
    }

    /**
     * Finishes the TDS request and then starts reading the TDS response from the server.
     *
     * @return the TDS reader used to read the response.
     * @throws SQLServerException
     *             if there is any kind of error.
     */
    final TDSReader startResponse() throws SQLServerException {
        return startResponse(false);
    }

    final TDSReader startResponse(boolean isAdaptive) throws SQLServerException {
        // Finish sending the request message. If this command was interrupted
        // at any point before endMessage() returns, then endMessage() throws an
        // exception with the reason for the interrupt. Request interrupts
        // are disabled by the time endMessage() returns.
        if (logger.isLoggable(Level.FINEST))
            logger.finest(this + ": finishing request");

        try {
            tdsWriter.endMessage();
        }
        catch (SQLServerException e) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this + ": finishing request: endMessage threw exception: " + e.getMessage());

            throw e;
        }

        // If command execution is subject to timeout then start timing until
        // the server returns the first response packet.
        if (null != timeoutTimer) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Starting timer...");

            timeoutTimer.start();
        }

        if (logger.isLoggable(Level.FINEST))
            logger.finest(this.toString() + ": Reading response...");

        try {
            // Wait for the server to execute the request and read the first packet
            // (responseBuffering=adaptive) or all packets (responseBuffering=full)
            // of the response.
            if (isAdaptive) {
                tdsReader.readPacket();
            }
            else {
                while (tdsReader.readPacket())
                    ;
            }
        }
        catch (SQLServerException e) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Exception reading response: " + e.getMessage());

            throw e;
        }
        finally {
            // If command execution was subject to timeout then stop timing as soon
            // as the server returns the first response packet or errors out.
            if (null != timeoutTimer) {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(this.toString() + ": Stopping timer...");

                timeoutTimer.stop();
            }
        }

        return tdsReader;
    }
}

/**
 * UninterruptableTDSCommand encapsulates an uninterruptable TDS conversation.
 *
 * TDSCommands have interruptability built in. However, some TDSCommands such as DTC commands, connection commands, cursor close and prepared
 * statement handle close shouldn't be interruptable. This class provides a base implementation for such commands.
 */
abstract class UninterruptableTDSCommand extends TDSCommand {
    UninterruptableTDSCommand(String logContext) {
        super(logContext, 0);
    }

    final void interrupt(String reason) throws SQLServerException {
        // Interrupting an uninterruptable command is a no-op. That is,
        // it can happen, but it should have no effect.
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(toString() + " Ignoring interrupt of uninterruptable TDS command; Reason:" + reason);
        }
    }
}
