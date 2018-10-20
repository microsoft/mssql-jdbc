/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.Buffer;
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
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.microsoft.sqlserver.jdbc.dataclassification.SensitivityClassification;
import com.microsoft.sqlserver.jdbc.timeouts.TimeoutCommand;
import com.microsoft.sqlserver.jdbc.timeouts.TimeoutPoller;


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
    static final int TDS_SQLRESCOLSRCS = 0xa2;
    static final int TDS_SQLDATACLASSIFICATION = 0xa3;

    // FedAuth
    static final byte TDS_FEATURE_EXT_FEDAUTH = 0x02;
    static final int TDS_FEDAUTH_LIBRARY_SECURITYTOKEN = 0x01;
    static final int TDS_FEDAUTH_LIBRARY_ADAL = 0x02;
    static final int TDS_FEDAUTH_LIBRARY_RESERVED = 0x7F;
    static final byte ADALWORKFLOW_ACTIVEDIRECTORYPASSWORD = 0x01;
    static final byte ADALWORKFLOW_ACTIVEDIRECTORYINTEGRATED = 0x02;
    static final byte FEDAUTH_INFO_ID_STSURL = 0x01; // FedAuthInfoData is token endpoint URL from which to acquire fed
                                                     // auth token
    static final byte FEDAUTH_INFO_ID_SPN = 0x02; // FedAuthInfoData is the SPN to use for acquiring fed auth token

    // AE constants
    // 0x03 is for x_eFeatureExtensionId_Rcs
    static final byte TDS_FEATURE_EXT_AE = 0x04;
    static final byte MAX_SUPPORTED_TCE_VERSION = 0x01; // max version
    static final int CUSTOM_CIPHER_ALGORITHM_ID = 0; // max version
    // 0x06 is for x_eFeatureExtensionId_LoginToken
    // 0x07 is for x_eFeatureExtensionId_ClientSideTelemetry
    // Data Classification constants
    static final byte TDS_FEATURE_EXT_DATACLASSIFICATION = 0x09;
    static final byte DATA_CLASSIFICATION_NOT_ENABLED = 0x00;
    static final byte MAX_SUPPORTED_DATA_CLASSIFICATION_VERSION = 0x01;

    static final int AES_256_CBC = 1;
    static final int AEAD_AES_256_CBC_HMAC_SHA256 = 2;
    static final int AE_METADATA = 0x08;

    static final byte TDS_FEATURE_EXT_UTF8SUPPORT = 0x0A;

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
            case TDS_FEATURE_EXT_DATACLASSIFICATION:
                return "TDS_FEATURE_EXT_DATACLASSIFICATION (0x09)";
            case TDS_FEATURE_EXT_UTF8SUPPORT:
                return "TDS_FEATURE_EXT_UTF8SUPPORT (0x0A)";
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
    static final byte PKT_FEDAUTH_TOKEN_MESSAGE = 8; // Authentication token for federated authentication

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
    static final int TRACE_HEADER_LENGTH = 26; // header length (4) + header type (2) + guid (16) + Sequence number size
                                               // (4)

    static final short HEADERTYPE_TRACE = 3; // trace header type

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
    private TDS() {}
}


class Nanos {
    static final int PER_SECOND = 1000000000;
    static final int PER_MAX_SCALE_INTERVAL = PER_SECOND / (int) Math.pow(10, TDS.MAX_FRACTIONAL_SECONDS_SCALE);
    static final int PER_MILLISECOND = PER_SECOND / 1000;
    static final long PER_DAY = 24 * 60 * 60 * (long) PER_SECOND;

    private Nanos() {}
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
        cal.set(1, Calendar.FEBRUARY, 577738, 0, 0, 0);// 577738 = 1+577737(no of days since epoch that brings us to oct
                                                       // 15th 1582)
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
        } else
            EXTRA_DAYS_TO_BE_ADDED = 0;
    }

    private GregorianChange() {}
}


final class UTC {

    // UTC/GMT time zone singleton.
    static final TimeZone timeZone = new SimpleTimeZone(0, "UTC");

    private UTC() {}
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
    final void open(String host, int port, int timeoutMillis, boolean useParallel, boolean useTnir,
            boolean isTnirFirstAttempt, int timeoutMillisForFullTimeout) throws SQLServerException {
        if (logger.isLoggable(Level.FINER))
            logger.finer(this.toString() + ": Opening TCP socket...");

        SocketFinder socketFinder = new SocketFinder(traceID, con);
        channelSocket = tcpSocket = socketFinder.findSocket(host, port, timeoutMillis, useParallel, useTnir,
                isTnirFirstAttempt, timeoutMillisForFullTimeout);

        try {

            // Set socket options
            tcpSocket.setTcpNoDelay(true);
            tcpSocket.setKeepAlive(true);

            // set SO_TIMEOUT
            int socketTimeout = con.getSocketTimeoutMilliseconds();
            tcpSocket.setSoTimeout(socketTimeout);

            inputStream = tcpInputStream = tcpSocket.getInputStream();
            outputStream = tcpOutputStream = tcpSocket.getOutputStream();
        } catch (IOException ex) {
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
         * The mission: To close the SSLSocket and release everything that it is holding onto other than the TCP/IP
         * socket and streams. The challenge: Simply closing the SSLSocket tries to do additional, unnecessary shutdown
         * I/O over the TCP/IP streams that are bound to the socket proxy, resulting in a not responding and confusing
         * SQL Server. Solution: Rewire the ProxySocket's input and output streams (one more time) to closed streams.
         * SSLSocket sees that the streams are already closed and does not attempt to do any further I/O on them before
         * closing itself.
         */

        // Create a couple of cheap closed streams
        InputStream is = new ByteArrayInputStream(new byte[0]);
        try {
            is.close();
        } catch (IOException e) {
            // No reason to expect a brand new ByteArrayInputStream not to close,
            // but just in case...
            logger.fine("Ignored error closing InputStream: " + e.getMessage());
        }

        OutputStream os = new ByteArrayOutputStream();
        try {
            os.close();
        } catch (IOException e) {
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
        } catch (IOException e) {
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
     * Used during SSL handshake, this class implements an InputStream that reads SSL handshake response data (framed in
     * TDS messages) from the TDS channel.
     */
    private class SSLHandshakeInputStream extends InputStream {
        private final TDSReader tdsReader;
        private final SSLHandshakeOutputStream sslHandshakeOutputStream;

        private final Logger logger;
        private final String logContext;

        SSLHandshakeInputStream(TDSChannel tdsChannel, SSLHandshakeOutputStream sslHandshakeOutputStream) {
            this.tdsReader = tdsChannel.getReader(null);
            this.sslHandshakeOutputStream = sslHandshakeOutputStream;
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (SSLHandshakeInputStream):";
        }

        /**
         * If there is no handshake response data available to be read from existing packets then this method ensures
         * that the SSL handshake output stream has been flushed to the server, and reads another packet (starting the
         * next TDS response message).
         *
         * Note that simply using TDSReader.ensurePayload isn't sufficient as it does not automatically start the new
         * response message.
         */
        private void ensureSSLPayload() throws IOException {
            if (0 == tdsReader.available()) {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext
                            + " No handshake response bytes available. Flushing SSL handshake output stream.");

                try {
                    sslHandshakeOutputStream.endMessage();
                } catch (SQLServerException e) {
                    logger.finer(logContext + " Ending TDS message threw exception:" + e.getMessage());
                    throw new IOException(e.getMessage());
                }

                if (logger.isLoggable(Level.FINEST))
                    logger.finest(logContext + " Reading first packet of SSL handshake response");

                try {
                    tdsReader.readPacket();
                } catch (SQLServerException e) {
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
            } catch (SQLServerException e) {
                logger.finer(logContext + " Skipping bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }

            return n;
        }

        private final byte oneByte[] = new byte[1];

        public int read() throws IOException {
            int bytesRead;

            while (0 == (bytesRead = readInternal(oneByte, 0, oneByte.length)));

            assert 1 == bytesRead || -1 == bytesRead;
            return 1 == bytesRead ? oneByte[0] : -1;
        }

        public int read(byte[] b) throws IOException {
            return readInternal(b, 0, b.length);
        }

        public int read(byte b[], int offset, int maxBytes) throws IOException {
            return readInternal(b, offset, maxBytes);
        }

        private int readInternal(byte b[], int offset, int maxBytes) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Reading " + maxBytes + " bytes...");

            ensureSSLPayload();

            try {
                tdsReader.readBytes(b, offset, maxBytes);
            } catch (SQLServerException e) {
                logger.finer(logContext + " Reading bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }

            return maxBytes;
        }
    }

    /**
     * Used during SSL handshake, this class implements an OutputStream that writes SSL handshake request data (framed
     * in TDS messages) to the TDS channel.
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

        public void write(byte[] b, int off, int len) throws IOException {
            writeInternal(b, off, len);
        }

        private void writeInternal(byte[] b, int off, int len) throws IOException {
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
            } catch (SQLServerException e) {
                logger.finer(logContext + " Writing bytes threw exception:" + e.getMessage());
                throw new IOException(e.getMessage());
            }
        }
    }

    /**
     * This class implements an InputStream that just forwards all of its methods to an underlying InputStream.
     *
     * It is more predictable than FilteredInputStream which forwards some of its read methods directly to the
     * underlying stream, but not others.
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

            while (0 == (bytesRead = readInternal(oneByte, 0, oneByte.length)));

            assert 1 == bytesRead || -1 == bytesRead;
            return 1 == bytesRead ? oneByte[0] : -1;
        }

        public int read(byte[] b) throws IOException {
            return readInternal(b, 0, b.length);
        }

        public int read(byte b[], int offset, int maxBytes) throws IOException {
            return readInternal(b, offset, maxBytes);
        }

        private int readInternal(byte b[], int offset, int maxBytes) throws IOException {
            int bytesRead;

            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Reading " + maxBytes + " bytes");

            try {
                bytesRead = filteredStream.read(b, offset, maxBytes);
            } catch (IOException e) {
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
     * This class essentially does what FilteredOutputStream does, but is more efficient for our usage.
     * FilteredOutputStream transforms block writes to sequences of single-byte writes.
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

        public void write(byte[] b, int off, int len) throws IOException {
            writeInternal(b, off, len);
        }

        private void writeInternal(byte[] b, int off, int len) throws IOException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(toString() + " Writing " + len + " bytes");

            filteredStream.write(b, off, len);
        }
    }

    /**
     * This class implements a Socket whose I/O streams can be switched from using a TDSChannel for I/O to using its
     * underlying TCP/IP socket.
     *
     * The SSL socket binds to a ProxySocket. The initial SSL handshake is done over TDSChannel I/O streams so that the
     * handshake payload is framed in TDS packets. The I/O streams are then switched to TCP/IP I/O streams using
     * setStreams, and SSL communications continue directly over the TCP/IP I/O streams.
     *
     * Most methods other than those for getting the I/O streams are simply forwarded to the TDSChannel's underlying
     * TCP/IP socket. Methods that change the socket binding or provide direct channel access are disallowed.
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
            SSLHandshakeInputStream sslHandshakeInputStream = new SSLHandshakeInputStream(tdsChannel,
                    sslHandshakeOutputStream);
            this.proxyOutputStream = new ProxyOutputStream(sslHandshakeOutputStream);
            this.proxyInputStream = new ProxyInputStream(sslHandshakeInputStream);
        }

        void setStreams(InputStream is, OutputStream os) {
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

        public void connect(SocketAddress endpoint, int timeout) throws IOException {
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

        public void setSoLinger(boolean on, int linger) throws SocketException {
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
     * A PermissiveX509TrustManager is used to "verify" the authenticity of the server when the trustServerCertificate
     * connection property is set to true.
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

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " Trusting client certificate (!)");
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
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

        HostNameOverrideX509TrustManager(TDSChannel tdsChannel, X509TrustManager tm, String hostName) {
            this.logger = tdsChannel.getLogger();
            this.logContext = tdsChannel.toString() + " (HostNameOverrideX509TrustManager):";
            defaultTrustManager = tm;
            // canonical name is in lower case so convert this to lowercase too.
            this.hostName = hostName.toLowerCase(Locale.ENGLISH);;
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
            // Note the parser will handle gracefully (essentially will return empty string) , inside the quotes (e.g
            // cn="Foo, bar") however
            // RFC 952 says that the hostName cant have commas however the parser should not (and will not) crash if it
            // sees a , within quotes.
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
                    logger.finer(logContext + " The name in certificate " + nameInCert
                            + " does not match with the server name " + hostName + ".");
                return false;
            }

            if (logger.isLoggable(Level.FINER))
                logger.finer(logContext + " The name in certificate:" + nameInCert + " validated against server name "
                        + hostName + ".");

            return true;
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(logContext + " Forwarding ClientTrusted.");
            defaultTrustManager.checkClientTrusted(chain, authType);
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
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
                                logger.finer(logContext + "Key: " + key + "; KeyClass:"
                                        + (key != null ? key.getClass() : null) + ";value: " + value + "; valueClass:"
                                        + (value != null ? value.getClass() : null));

                            }

                            // From
                            // Documentation(http://download.oracle.com/javase/6/docs/api/java/security/cert/X509Certificate.html):
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
                                            logger.finer(logContext + " found a valid name in certificate: "
                                                    + dnsNameInSANCert);
                                        }
                                        break;
                                    }
                                }

                                if (logger.isLoggable(Level.FINER)) {
                                    logger.finer(logContext
                                            + " the following name in certificate does not match the serverName: "
                                            + value);
                                }
                            }

                        } else {
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
     *        Server Host Name for SSL Handshake
     * @param port
     *        Server Port for SSL Handshake
     * @throws SQLServerException
     */
    void enableSSL(String host, int port) throws SQLServerException {
        // If enabling SSL fails, which it can for a number of reasons, the following items
        // are used in logging information to the TDS channel logger to help diagnose the problem.
        Provider tmfProvider = null; // TrustManagerFactory provider
        Provider sslContextProvider = null; // SSLContext provider
        Provider ksProvider = null; // KeyStore provider
        String tmfDefaultAlgorithm = null; // Default algorithm (typically X.509) used by the TrustManagerFactory
        SSLHandhsakeState handshakeState = SSLHandhsakeState.SSL_HANDHSAKE_NOT_STARTED;

        boolean isFips = false;
        String trustStoreType = null;
        String sslProtocol = null;

        // If anything in here fails, terminate the connection and throw an exception
        try {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " Enabling SSL...");

            String trustStoreFileName = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.TRUST_STORE.toString());
            String trustStorePassword = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString());
            String hostNameInCertificate = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString());

            trustStoreType = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString());

            if (StringUtils.isEmpty(trustStoreType)) {
                trustStoreType = SQLServerDriverStringProperty.TRUST_STORE_TYPE.getDefaultValue();
            }

            isFips = Boolean.valueOf(
                    con.activeConnectionProperties.getProperty(SQLServerDriverBooleanProperty.FIPS.toString()));
            sslProtocol = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.SSL_PROTOCOL.toString());

            if (isFips) {
                validateFips(trustStoreType, trustStoreFileName);
            }

            assert TDS.ENCRYPT_OFF == con.getRequestedEncryptionLevel() || // Login only SSL
                    TDS.ENCRYPT_ON == con.getRequestedEncryptionLevel(); // Full SSL

            assert TDS.ENCRYPT_OFF == con.getNegotiatedEncryptionLevel() || // Login only SSL
                    TDS.ENCRYPT_ON == con.getNegotiatedEncryptionLevel() || // Full SSL
                    TDS.ENCRYPT_REQ == con.getNegotiatedEncryptionLevel(); // Full SSL

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
                } else {
                    tm = new TrustManager[] {
                            (TrustManager) tmClass.getDeclaredConstructor(String.class).newInstance(constructorArg)};
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
                    } finally {
                        // We are done with the trustStorePassword (if set). Clear it for better security.
                        con.activeConnectionProperties
                                .remove(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString());

                        // We are also done with the trust store input stream.
                        if (null != is) {
                            try {
                                is.close();
                            } catch (IOException e) {
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
                        tm = new TrustManager[] {new HostNameOverrideX509TrustManager(this, (X509TrustManager) tm[0],
                                hostNameInCertificate)};
                    } else {
                        tm = new TrustManager[] {
                                new HostNameOverrideX509TrustManager(this, (X509TrustManager) tm[0], host)};
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

            // don't close proxy when SSL socket is closed
            sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket(proxySocket, host, port, false);

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
        } catch (Exception e) {
            // Log the original exception and its source at FINER level
            if (logger.isLoggable(Level.FINER))
                logger.log(Level.FINER, e.getMessage(), e);

            // If enabling SSL fails, the following information may help diagnose the problem.
            // Do not use Level INFO or above which is sent to standard output/error streams.
            // This is because due to an intermittent TLS 1.2 connection issue, we will be retrying the connection and
            // do not want to print this message in console.
            if (logger.isLoggable(Level.FINER))
                logger.log(Level.FINER, "java.security path: " + JAVA_SECURITY + "\n" + "Security providers: "
                        + Arrays.asList(Security.getProviders()) + "\n"
                        + ((null != sslContextProvider) ? ("SSLContext provider info: " + sslContextProvider.getInfo()
                                + "\n" + "SSLContext provider services:\n" + sslContextProvider.getServices() + "\n")
                                                        : "")
                        + ((null != tmfProvider) ? ("TrustManagerFactory provider info: " + tmfProvider.getInfo()
                                + "\n") : "")
                        + ((null != tmfDefaultAlgorithm) ? ("TrustManagerFactory default algorithm: "
                                + tmfDefaultAlgorithm + "\n") : "")
                        + ((null != ksProvider) ? ("KeyStore provider info: " + ksProvider.getInfo() + "\n") : "")
                        + "java.ext.dirs: " + System.getProperty("java.ext.dirs"));

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_sslFailed"));
            Object[] msgArgs = {e.getMessage()};

            // It is important to get the localized message here, otherwise error messages won't match for different
            // locales.
            String errMsg = e.getLocalizedMessage();
            // If the message is null replace it with the non-localized message or a dummy string. This can happen if a
            // custom
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
            } else {
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
    private void validateFips(final String trustStoreType, final String trustStoreFileName) throws SQLServerException {
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
     * When a location to a trust store is specified, this method attempts to load that store. Otherwise, it looks for
     * and attempts to load the default trust store using essentially the same logic (outlined in the JSSE Reference
     * Guide) as the default X.509 TrustManagerFactory.
     *
     * @return an InputStream containing the contents of the loaded trust store
     * @return null if the trust store cannot be loaded.
     *
     *         Note: It is by design that this function returns null when the trust store cannot be loaded rather than
     *         throwing an exception. The reason is that KeyStore.load, which uses the returned InputStream, interprets
     *         a null InputStream to mean that there are no trusted certificates, which mirrors the behavior of the
     *         default (no trust store, no password specified) path.
     */
    final InputStream loadTrustStore(String trustStoreFileName) {
        FileInputStream is = null;

        // First case: Trust store filename was specified
        if (null != trustStoreFileName) {
            try {
                if (logger.isLoggable(Level.FINEST))
                    logger.finest(toString() + " Opening specified trust store: " + trustStoreFileName);

                is = new FileInputStream(trustStoreFileName);
            } catch (FileNotFoundException e) {
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
                    logger.finest(toString() + " Opening default trust store (from javax.net.ssl.trustStore): "
                            + trustStoreFileName);

                is = new FileInputStream(trustStoreFileName);
            } catch (FileNotFoundException e) {
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
            } catch (FileNotFoundException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(toString() + " Trust store not found: " + e.getMessage());
            }

            // No jssecerts. Try again with cacerts...
            if (null == is) {
                try {
                    if (logger.isLoggable(Level.FINEST))
                        logger.finest(toString() + " Opening default trust store: " + CACERTS);

                    is = new FileInputStream(CACERTS);
                } catch (FileNotFoundException e) {
                    if (logger.isLoggable(Level.FINE))
                        logger.fine(toString() + " Trust store not found: " + e.getMessage());

                    // No jssecerts or cacerts. Treat it as if the trust store is empty so that
                    // the TrustManager reports that no certificate is found.
                }
            }
        }

        return is;
    }

    final int read(byte[] data, int offset, int length) throws SQLServerException {
        try {
            return inputStream.read(data, offset, length);
        } catch (IOException e) {
            if (logger.isLoggable(Level.FINE))
                logger.fine(toString() + " read failed:" + e.getMessage());

            if (e instanceof SocketTimeoutException) {
                con.terminate(SQLServerException.ERROR_SOCKET_TIMEOUT, e.getMessage(), e);
            } else {
                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
            }

            return 0; // Keep the compiler happy.
        }
    }

    final void write(byte[] data, int offset, int length) throws SQLServerException {
        try {
            outputStream.write(data, offset, length);
        } catch (IOException e) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(toString() + " write failed:" + e.getMessage());

            con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage(), e);
        }
    }

    final void flush() throws SQLServerException {
        try {
            outputStream.flush();
        } catch (IOException e) {
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
            } catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing inputStream", e);
            }
        }

        if (null != outputStream) {
            if (logger.isLoggable(Level.FINEST))
                logger.finest(this.toString() + ": Closing outputStream...");

            try {
                outputStream.close();
            } catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing outputStream", e);
            }
        }

        if (null != tcpSocket) {
            if (logger.isLoggable(Level.FINER))
                logger.finer(this.toString() + ": Closing TCP socket...");

            try {
                tcpSocket.close();
            } catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + ": Ignored error closing socket", e);
            }
        }
    }

    /**
     * Logs TDS packet data to the com.microsoft.sqlserver.jdbc.TDS.DATA logger
     *
     * @param data
     *        the buffer containing the TDS packet payload data to log
     * @param nStartOffset
     *        offset into the above buffer from where to start logging
     * @param nLength
     *        length (in bytes) of payload
     * @param messageDetail
     *        other loggable details about the payload
     */
    /* L0 */ void logPacket(byte data[], int nStartOffset, int nLength, String messageDetail) {
        assert 0 <= nLength && nLength <= data.length;
        assert 0 <= nStartOffset && nStartOffset <= data.length;

        final char hexChars[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

        final char printableChars[] = {'.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', ' ', '!', '\"', '#',
                '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', '0', '1', '2', '3', '4', '5', '6', '7',
                '8', '9', ':', ';', '<', '=', '>', '?', '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '[', '\\', ']', '^', '_', '`',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
                'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.',
                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'};

        // Log message body lines have this form:
        //
        // "XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX XX ................"
        // 012345678911111111112222222222333333333344444444445555555555666666
        // 01234567890123456789012345678901234567890123456789012345
        //
        final char lineTemplate[] = {' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
                ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',

                ' ', ' ',

                '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.', '.'};

        char logLine[] = new char[lineTemplate.length];
        System.arraycopy(lineTemplate, 0, logLine, 0, lineTemplate.length);

        // Logging builds up a string buffer for the entire log trace
        // before writing it out. So use an initial size large enough
        // that the buffer doesn't have to resize itself.
        StringBuilder logMsg = new StringBuilder(messageDetail.length() + // Message detail
                4 * nLength + // 2-digit hex + space + ASCII, per byte
                4 * (1 + nLength / 16) + // 2 extra spaces + CR/LF, per line (16 bytes per line)
                80); // Extra fluff: IP:Port, Connection #, SPID, ...

        // Format the headline like so:
        // /157.55.121.182:2983 Connection 1, SPID 53, Message info here ...
        //
        // Note: the log formatter itself timestamps what we write so we don't have
        // to do it again here.
        logMsg.append(tcpSocket.getLocalAddress().toString() + ":" + tcpSocket.getLocalPort() + " SPID:" + spid + " "
                + messageDetail + "\r\n");

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
     * @throws IOException
     *         thrown if the socket timeout cannot be read
     */
    final int getNetworkTimeout() throws IOException {
        return tcpSocket.getSoTimeout();
    }

    /**
     * Set the socket SO_TIMEOUT value.
     *
     * @param timeout
     *        the socket timeout in milliseconds
     * @throws IOException
     *         thrown if the socket timeout cannot be set
     */
    final void setNetworkTimeout(int timeout) throws IOException {
        tcpSocket.setSoTimeout(timeout);
    }
}


/**
 * SocketFinder is used to find a server socket to which a connection can be made. This class abstracts the logic of
 * finding a socket from TDSChannel class.
 * 
 * In the case when useParallel is set to true, this is achieved by trying to make parallel connections to multiple IP
 * addresses. This class is responsible for spawning multiple threads and keeping track of the search result and the
 * connected socket or exception to be thrown.
 * 
 * In the case where multiSubnetFailover is false, we try our old logic of trying to connect to the first ip address
 * 
 * Typical usage of this class is SocketFinder sf = new SocketFinder(traceId, conn); Socket = sf.getSocket(hostName,
 * port, timeout);
 */
final class SocketFinder {
    /**
     * Indicates the result of a search
     */
    enum Result {
        UNKNOWN, // search is still in progress
        SUCCESS, // found a socket
        FAILURE// failed in finding a socket
    }

    // Thread pool - the values in the constructor are chosen based on the
    // explanation given in design_connection_director_multisubnet.doc
    private static final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5,
            TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

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
     *        traceID of the caller
     * @param sqlServerConnection
     *        the SQLServer connection
     */
    SocketFinder(String callerTraceID, SQLServerConnection sqlServerConnection) {
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
    Socket findSocket(String hostName, int portNumber, int timeoutInMilliSeconds, boolean useParallel, boolean useTnir,
            boolean isTnirFirstAttempt, int timeoutInMilliSecondsForFullTimeout) throws SQLServerException {
        assert timeoutInMilliSeconds != 0 : "The driver does not allow a time out of 0";

        try {
            InetAddress[] inetAddrs = null;

            // inetAddrs is only used if useParallel is true or TNIR is true. Skip resolving address if that's not the
            // case.
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
                } else if (!useTnir) {
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
                MessageFormat form = new MessageFormat(
                        SQLServerException.getErrString("R_ipAddressLimitWithMultiSubnetFailover"));
                Object[] msgArgs = {Integer.toString(ipAddressLimit)};
                String errorStr = form.format(msgArgs);
                // we do not want any retry to happen here. So, terminate the connection
                // as the config is unsupported.
                conn.terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, errorStr);
            }

            if (inetAddrs.length == 1) {
                // Single address so do not start any threads
                return getConnectedSocket(inetAddrs[0], portNumber, timeoutInMilliSeconds);
            }
            timeoutInMilliSeconds = Math.max(timeoutInMilliSeconds, minTimeoutForParallelConnections);
            if (Util.isIBM()) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(this.toString() + "Using Java NIO with timeout:" + timeoutInMilliSeconds);
                }
                findSocketUsingJavaNIO(inetAddrs, portNumber, timeoutInMilliSeconds);
            } else {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(this.toString() + "Using Threading with timeout:" + timeoutInMilliSeconds);
                }
                findSocketUsingThreading(inetAddrs, portNumber, timeoutInMilliSeconds);
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

        } catch (InterruptedException ex) {
            // re-interrupt the current thread, in order to restore the thread's interrupt status.
            Thread.currentThread().interrupt();

            close(selectedSocket);
            SQLServerException.ConvertConnectExceptionToSQLServerException(hostName, portNumber, conn, ex);
        } catch (IOException ex) {
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
     * This function uses java NIO to connect to all the addresses in inetAddrs with in a specified timeout. If it
     * succeeds in connecting, it closes all the other open sockets and updates the result to success.
     * 
     * @param inetAddrs
     *        the array of inetAddress to which connection should be made
     * @param portNumber
     *        the port number at which connection should be made
     * @param timeoutInMilliSeconds
     * @throws IOException
     */
    private void findSocketUsingJavaNIO(InetAddress[] inetAddrs, int portNumber,
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
                    logger.finer(this.toString() + " initiated connection to address: " + inetAddr + ", portNumber: "
                            + portNumber);
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
                            logger.finer(this.toString() + " processing the channel :" + ch);// this traces the IP by
                                                                                             // default

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
                        } catch (IOException ex) {
                            if (logger.isLoggable(Level.FINER))
                                logger.finer(this.toString() + " the exception: " + ex.getClass() + " with message: "
                                        + ex.getMessage() + " occured while processing the channel: " + ch);
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
        } catch (IOException ex) {
            // in case of an exception, close the selected channel.
            // All other channels will be closed in the finally block,
            // as they need to be closed irrespective of a success/failure
            close(selectedChannel);
            throw ex;
        } finally {
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
    private Socket getDefaultSocket(String hostName, int portNumber, int timeoutInMilliSeconds) throws IOException {
        // Open the socket, with or without a timeout, throwing an UnknownHostException
        // if there is a failure to resolve the host name to an InetSocketAddress.
        //
        // Note that Socket(host, port) throws an UnknownHostException if the host name
        // cannot be resolved, but that InetSocketAddress(host, port) does not - it sets
        // the returned InetSocketAddress as unresolved.
        InetSocketAddress addr = new InetSocketAddress(hostName, portNumber);
        return getConnectedSocket(addr, timeoutInMilliSeconds);
    }

    private Socket getConnectedSocket(InetAddress inetAddr, int portNumber,
            int timeoutInMilliSeconds) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(inetAddr, portNumber);
        return getConnectedSocket(addr, timeoutInMilliSeconds);
    }

    private Socket getConnectedSocket(InetSocketAddress addr, int timeoutInMilliSeconds) throws IOException {
        assert timeoutInMilliSeconds != 0 : "timeout cannot be zero";
        if (addr.isUnresolved())
            throw new java.net.UnknownHostException();
        selectedSocket = new Socket();
        selectedSocket.connect(addr, timeoutInMilliSeconds);
        return selectedSocket;
    }

    private void findSocketUsingThreading(InetAddress[] inetAddrs, int portNumber,
            int timeoutInMilliSeconds) throws IOException, InterruptedException {
        assert timeoutInMilliSeconds != 0 : "The timeout cannot be zero";

        assert inetAddrs.length != 0 : "Number of inetAddresses should not be zero in this function";

        LinkedList<Socket> sockets = new LinkedList<>();
        LinkedList<SocketConnector> socketConnectors = new LinkedList<>();

        try {

            // create a socket, inetSocketAddress and a corresponding socketConnector per inetAddress
            noOfSpawnedThreads = inetAddrs.length;
            for (InetAddress inetAddress : inetAddrs) {
                Socket s = new Socket();
                sockets.add(s);

                InetSocketAddress inetSocketAddress = new InetSocketAddress(inetAddress, portNumber);

                SocketConnector socketConnector = new SocketConnector(s, inetSocketAddress, timeoutInMilliSeconds,
                        this);
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
                        logger.finer(this.toString() + " TimeRemaining:" + timeRemaining + "; Result:" + result
                                + "; Max. open thread count: " + threadPoolExecutor.getLargestPoolSize()
                                + "; Current open thread count:" + threadPoolExecutor.getActiveCount());
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

        } finally {
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
            } catch (IOException e) {
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
            } catch (IOException e) {
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
            } catch (IOException e) {
                if (logger.isLoggable(Level.FINE))
                    logger.log(Level.FINE, this.toString() + "Ignored the following error while closing socketChannel",
                            e);
            }
        }
    }

    /**
     * Used by socketConnector threads to notify the socketFinder of their connection attempt result(a connected socket
     * or exception). It updates the result, socket and exception variables of socketFinder object. This method notifies
     * the parent thread if a socket is found or if all the spawned threads have notified. It also closes a socket if it
     * is not selected for use by socketFinder.
     * 
     * @param socket
     *        the SocketConnector's socket
     * @param exception
     *        Exception that occurred in socket connector thread
     * @param threadId
     *        Id of the calling Thread for diagnosis
     */
    void updateResult(Socket socket, IOException exception, String threadId) {
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
                        logger.finer(
                                "The following child thread released parentThreadLock and notified the parent thread:"
                                        + threadId);
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
     * If there are multiple exceptions, that are not related to socketTimeout the first non-socketTimeout exception is
     * picked. If all exceptions are related to socketTimeout, the first exception is picked. Note: This method is not
     * thread safe. The caller should ensure thread safety.
     * 
     * @param ex
     *        the IOException
     * @param traceId
     *        the traceId of the thread
     */
    public void updateSelectedException(IOException ex, String traceId) {
        boolean updatedException = false;
        if (selectedException == null
                || (!(ex instanceof SocketTimeoutException)) && (selectedException instanceof SocketTimeoutException)) {
            selectedException = ex;
            updatedException = true;
        }

        if (updatedException) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("The selected exception is updated to the following: ExceptionType:" + ex.getClass()
                        + "; ExceptionMessage:" + ex.getMessage() + "; by the following thread:" + traceId);
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
    SocketConnector(Socket socket, InetSocketAddress inetSocketAddress, int timeOutInMilliSeconds,
            SocketFinder socketFinder) {
        this.socket = socket;
        this.inetSocketAddress = inetSocketAddress;
        this.timeoutInMilliseconds = timeOutInMilliSeconds;
        this.socketFinder = socketFinder;
        this.threadID = Long.toString(nextThreadID());
        this.traceID = "SocketConnector:" + this.threadID + "(" + socketFinder.toString() + ")";
    }

    /**
     * If search for socket has not finished, this function tries to connect a socket(with a timeout) synchronously. It
     * further notifies the socketFinder the result of the connection attempt
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
                    logger.finer(this.toString() + " connecting to InetSocketAddress:" + inetSocketAddress
                            + " with timeout:" + timeoutInMilliseconds);
                }

                socket.connect(inetSocketAddress, timeoutInMilliseconds);
            } catch (IOException ex) {
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
        } else {
            lastThreadID++;
        }
        return lastThreadID;
    }
}

/**
 * TDSPacket provides a mechanism for chaining TDS response packets together in a singly-linked list.
 *
 * Having both the link and the data in the same class allows TDSReader marks (see below) to automatically hold onto
 * exactly as much response data as they need, and no more. Java reference semantics ensure that a mark holds onto its
 * referenced packet and subsequent packets (through next references). When all marked references to a packet go away,
 * the packet, and any linked unmarked packets, can be reclaimed by GC.
 */
final class TDSPacket {
    final byte[] header = new byte[TDS.PACKET_HEADER_SIZE];
    final byte[] payload;
    int payloadLength;
    volatile TDSPacket next;

    final public String toString() {
        return "TDSPacket(SPID:" + Util.readUnsignedShortBigEndian(header, TDS.PACKET_HEADER_SPID) + " Seq:"
                + header[TDS.PACKET_HEADER_SEQUENCE_NUM] + ")";
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
 * Response data is quantized into a linked chain of packets. A mark refers to a specific location in a specific packet
 * and relies on Java's reference semantics to automatically keep all subsequent packets accessible until the mark is
 * destroyed.
 */
final class TDSReaderMark {
    final TDSPacket packet;
    final int payloadOffset;

    TDSReaderMark(TDSPacket packet, int payloadOffset) {
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
    private TdsTimeoutCommand timeoutCommand;

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
    private boolean serverSupportsDataClassification = false;

    private final byte valueBytes[] = new byte[256];

    protected SensitivityClassification sensitivityClassification;

    private static final AtomicInteger lastReaderID = new AtomicInteger(0);

    private static int nextReaderID() {
        return lastReaderID.incrementAndGet();
    }

    TDSReader(TDSChannel tdsChannel, SQLServerConnection con, TDSCommand command) {
        this.tdsChannel = tdsChannel;
        this.con = con;
        this.command = command; // may be null
        // if the logging level is not detailed than fine or more we will not have proper reader IDs.
        if (logger.isLoggable(Level.FINE))
            traceID = "TDSReader@" + nextReaderID() + " (" + con.toString() + ")";
        else
            traceID = con.toString();
        if (con.isColumnEncryptionSettingEnabled()) {
            useColumnEncryption = true;
        }
        serverSupportsColumnEncryption = con.getServerSupportsColumnEncryption();
        serverSupportsDataClassification = con.getServerSupportsDataClassification();
    }

    final boolean isColumnEncryptionSettingEnabled() {
        return useColumnEncryption;
    }

    final boolean getServerSupportsColumnEncryption() {
        return serverSupportsColumnEncryption;
    }

    final boolean getServerSupportsDataClassification() {
        return serverSupportsDataClassification;
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
     * Ensures that payload data is available to be read, automatically advancing to (and possibly reading) the next
     * packet.
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
     * This method is synchronized to guard against simultaneously reading packets from one thread that is processing
     * the response and another thread that is trying to buffer it with TDSCommand.detach().
     */
    synchronized final boolean readPacket() throws SQLServerException {
        if (null != command && !command.readingResponse())
            return false;

        // Number of packets in should always be less than number of packets out.
        // If the server has been notified for an interrupt, it may be less by
        // more than one packet.
        assert tdsChannel.numMsgsRcvd < tdsChannel.numMsgsSent : "numMsgsRcvd:" + tdsChannel.numMsgsRcvd
                + " should be less than numMsgsSent:" + tdsChannel.numMsgsSent;

        TDSPacket newPacket = new TDSPacket(con.getTDSPacketSize());

        if (null != command) {
            // if cancelQueryTimeout is set, we should wait for the total amount of queryTimeout + cancelQueryTimeout to
            // terminate the connection.
            if ((command.getCancelQueryTimeoutSeconds() > 0
                    && command.getQueryTimeoutSeconds() > 0)) {
                //if a timeout is configured with this object, add it to the timeout poller
                int timeout = command.getCancelQueryTimeoutSeconds() + command.getQueryTimeoutSeconds();
                this.timeoutCommand = new TdsTimeoutCommand(timeout, this.command, this.con);
                TimeoutPoller.getTimeoutPoller().addTimeoutCommand(this.timeoutCommand);
            }
        }
        // First, read the packet header.
        for (int headerBytesRead = 0; headerBytesRead < TDS.PACKET_HEADER_SIZE;) {
            int bytesRead = tdsChannel.read(newPacket.header, headerBytesRead,
                    TDS.PACKET_HEADER_SIZE - headerBytesRead);
            if (bytesRead < 0) {
                if (logger.isLoggable(Level.FINER))
                    logger.finer(toString() + " Premature EOS in response. packetNum:" + packetNum + " headerBytesRead:"
                            + headerBytesRead);

                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED,
                        ((0 == packetNum && 0 == headerBytesRead) ? SQLServerException.getErrString(
                                "R_noServerResponse") : SQLServerException.getErrString("R_truncatedServerResponse")));
            }

            headerBytesRead += bytesRead;
        }

        // if execution was subject to timeout then stop timing
        if (this.timeoutCommand != null) {
            TimeoutPoller.getTimeoutPoller().remove(this.timeoutCommand);
        }
        // Header size is a 2 byte unsigned short integer in big-endian order.
        int packetLength = Util.readUnsignedShortBigEndian(newPacket.header, TDS.PACKET_HEADER_MESSAGE_LENGTH);

        // Make header size is properly bounded and compute length of the packet payload.
        if (packetLength < TDS.PACKET_HEADER_SIZE || packetLength > con.getTDSPacketSize()) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(toString() + " TDS header contained invalid packet length:" + packetLength
                        + "; packet size:" + con.getTDSPacketSize());
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
            int bytesRead = tdsChannel.read(newPacket.payload, payloadBytesRead,
                    newPacket.payloadLength - payloadBytesRead);
            if (bytesRead < 0)
                con.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED,
                        SQLServerException.getErrString("R_truncatedServerResponse"));

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
     * Returns the number of bytes that can be read (or skipped over) from this TDSReader without blocking by the next
     * caller of a method for this TDSReader.
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
         * The number of bytes that can be read from the current chunk, without including the next chunk that is
         * buffered. This is so the driver can confirm if the next chunk sent is new packet or just continuation
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

    final void readBytes(byte[] value, int valueOffset, int valueLength) throws SQLServerException {
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

    final Object readDecimal(int valueLength, TypeInfo typeInfo, JDBCType jdbcType,
            StreamType streamType) throws SQLServerException {
        if (valueLength > valueBytes.length) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(toString() + " Invalid value length:" + valueLength);
            }
            throwInvalidTDS();
        }

        readBytes(valueBytes, 0, valueLength);
        return DDC.convertBigDecimalToObject(Util.readBigDecimal(valueBytes, valueLength, typeInfo.getScale()),
                jdbcType, streamType);
    }

    final Object readMoney(int valueLength, JDBCType jdbcType, StreamType streamType) throws SQLServerException {
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

    final Object readReal(int valueLength, JDBCType jdbcType, StreamType streamType) throws SQLServerException {
        if (4 != valueLength)
            throwInvalidTDS();

        return DDC.convertFloatToObject(Float.intBitsToFloat(readInt()), jdbcType, streamType);
    }

    final Object readFloat(int valueLength, JDBCType jdbcType, StreamType streamType) throws SQLServerException {
        if (8 != valueLength)
            throwInvalidTDS();

        return DDC.convertDoubleToObject(Double.longBitsToDouble(readLong()), jdbcType, streamType);
    }

    final Object readDateTime(int valueLength, Calendar appTimeZoneCalendar, JDBCType jdbcType,
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

                msecSinceMidnight = (ticksSinceMidnight * 10 + 1) / 3; // Convert to msec (1 tick = 1 300th of a sec = 3
                                                                       // msec)
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
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME, appTimeZoneCalendar, daysSinceSQLBaseDate,
                msecSinceMidnight, 0); // scale
                                       // (ignored
                                       // for
                                       // fixed-scale
                                       // DATETIME/SMALLDATETIME
                                       // types)
    }

    final Object readDate(int valueLength, Calendar appTimeZoneCalendar, JDBCType jdbcType) throws SQLServerException {
        if (TDS.DAYS_INTO_CE_LENGTH != valueLength)
            throwInvalidTDS();

        // Initialize the date fields to their appropriate values.
        int localDaysIntoCE = readDaysIntoCE();

        // Convert the DATE value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATE, appTimeZoneCalendar, localDaysIntoCE, 0, // midnight
                                                                                                           // local to
                                                                                                           // app time
                                                                                                           // zone
                0); // scale (ignored for DATE)
    }

    final Object readTime(int valueLength, TypeInfo typeInfo, Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.timeValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // Read the value from the server
        long localNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());

        // Convert the TIME value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.TIME, appTimeZoneCalendar, 0, localNanosSinceMidnight,
                typeInfo.getScale());
    }

    final Object readDateTime2(int valueLength, TypeInfo typeInfo, Calendar appTimeZoneCalendar,
            JDBCType jdbcType) throws SQLServerException {
        if (TDS.datetime2ValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // Read the value's constituent components
        long localNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());
        int localDaysIntoCE = readDaysIntoCE();

        // Convert the DATETIME2 value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME2, appTimeZoneCalendar, localDaysIntoCE,
                localNanosSinceMidnight, typeInfo.getScale());
    }

    final Object readDateTimeOffset(int valueLength, TypeInfo typeInfo, JDBCType jdbcType) throws SQLServerException {
        if (TDS.datetimeoffsetValueLength(typeInfo.getScale()) != valueLength)
            throwInvalidTDS();

        // The nanos since midnight and days into Common Era parts of DATETIMEOFFSET values
        // are in UTC. Use the minutes offset part to convert to local.
        long utcNanosSinceMidnight = readNanosSinceMidnight(typeInfo.getScale());
        int utcDaysIntoCE = readDaysIntoCE();
        int localMinutesOffset = readShort();

        // Convert the DATETIMEOFFSET value to the desired Java type.
        return DDC.convertTemporalToObject(jdbcType, SSType.DATETIMEOFFSET,
                new GregorianCalendar(new SimpleTimeZone(localMinutesOffset * 60 * 1000, ""), Locale.US), utcDaysIntoCE,
                utcNanosSinceMidnight, typeInfo.getScale());
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

    final Object readGUID(int valueLength, JDBCType jdbcType, StreamType streamType) throws SQLServerException {
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
                } catch (UnsupportedEncodingException e) {
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
        } catch (UnsupportedEncodingException e) {
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

    final void tryProcessFeatureExtAck(boolean featureExtAckReceived) throws SQLServerException {
        // in case of redirection, do not check if TDS_FEATURE_EXTENSION_ACK is received or not.
        if (null != this.con.getRoutingInfo()) {
            return;
        }

        if (isColumnEncryptionSettingEnabled() && !featureExtAckReceived)
            throw new SQLServerException(this, SQLServerException.getErrString("R_AE_NotSupportedByServer"), null, 0,
                    false);
    }

    final void trySetSensitivityClassification(SensitivityClassification sensitivityClassification) {
        this.sensitivityClassification = sensitivityClassification;
    }
}

/**
 * The tds default implementation of a timeout command
 */
class TdsTimeoutCommand extends TimeoutCommand<TDSCommand> {
    public TdsTimeoutCommand(int timeout, TDSCommand command, SQLServerConnection sqlServerConnection) {
        super(timeout, command, sqlServerConnection);
    }

    @Override
    public void interrupt() {
        TDSCommand command = getCommand();
        SQLServerConnection sqlServerConnection = getSqlServerConnection();
        try {
            // If TCP Connection to server is silently dropped, exceeding the query timeout on the same connection does
            // not throw SQLTimeoutException
            // The application stops responding instead until SocketTimeoutException is thrown. In this case, we must
            // manually terminate the connection.
            if (null == command && null != sqlServerConnection) {
                sqlServerConnection.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED,
                        SQLServerException.getErrString(ErrorConstants.CONNECTION_CLOSED));
            } else {
                // If the timer wasn't canceled before it ran out of
                // time then interrupt the registered command.
                command.interrupt(SQLServerException.getErrString(ErrorConstants.TIMEOUT_ERROR));
            }
        } catch (SQLServerException e) {
            // Unfortunately, there's nothing we can do if we
            // fail to time out the request. There is no way
            // to report back what happened.
            assert null != command;
            command.log(Level.FINE, "Command could not be timed out. Reason: " + e.getMessage());
        }
    }
}


/**
 * UninterruptableTDSCommand encapsulates an uninterruptable TDS conversation.
 *
 * TDSCommands have interruptability built in. However, some TDSCommands such as DTC commands, connection commands,
 * cursor close and prepared statement handle close shouldn't be interruptable. This class provides a base
 * implementation for such commands.
 */
abstract class UninterruptableTDSCommand extends TDSCommand {
    UninterruptableTDSCommand(String logContext) {
        super(logContext, 0, 0);
    }

    final void interrupt(String reason) throws SQLServerException {
        // Interrupting an uninterruptable command is a no-op. That is,
        // it can happen, but it should have no effect.
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(toString() + " Ignoring interrupt of uninterruptable TDS command; Reason:" + reason);
        }
    }
}
