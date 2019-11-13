/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import mssql.security.provider.MD4;


/**
 * Provides an implementation of NTLMv2 authentication
 * 
 * @see <a
 *      href=https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/b38c36ed-2804-4868-a9ff-8dd3182128e4">MS-NLMP:NTLMAuthentication
 *      Protocol</a>
 */
final class NTLMAuthentication extends SSPIAuthentication {
    private final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.NTLMAuthentication");

    /**
     * Section 2.2.1.1 NEGOTIATE_MESSAGE
     *
     * Section 2.2.1.2 CHALLENGE_MESSAGE
     *
     * Section 2.2.1.3 AUTHENCIATE_MESSAGE
     *
     * NTLM signature for all 3 messages - "NTLMSSP\0"
     */
    private static final byte[] NTLM_HEADER_SIGNATURE = {0x4e, 0x54, 0x4c, 0x4d, 0x53, 0x53, 0x50, 0x00};

    // NTLM messages type
    private static final int NTLM_MESSAGE_TYPE_NEGOTIATE = 0x00000001;
    private static final int NTLM_MESSAGE_TYPE_CHALLENGE = 0x00000002;
    private static final int NTLM_MESSAGE_TYPE_AUTHENTICATE = 0x00000003;

    /**
     * Section 2.2.2.7 NTLM v2: NTLMv2_CLIENT_CHALLENGE
     * 
     * <pre>
     * RespType   current version(aka Responseversion) of the challenge response type. This field MUST be 0x01
     * HiRespType max supported version (aka HiResponserversion) of the client response type.
     * </pre>
     */
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE = {0x01, 0x01};

    /**
     * Section 2.2.2.7 NTLM v2: NTLMv2_CLIENT_CHALLENGE
     *
     * <pre>
     * Reserved1  SHOULD be 0x0000
     * Reserved2  SHOULD be 0x00000000
     * Reserved3  SHOULD be 0x00000000
     * </pre>
     */
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED1 = {0x00, 0x00};
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED2 = {0x00, 0x00, 0x00, 0x00};
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED3 = {0x00, 0x00, 0x00, 0x00};

    /**
     * Section 3.1.5.1.1 Client Initiates the NEGOTIATE_MESSAGE
     * 
     * If NTLM v2 authentication is used and the CHALLENGE_MESSAGE TargetInfo field (section 2.2.1.2) has an
     * MsvAvTimestamp present, the client SHOULD NOT send the LmChallengeResponse and SHOULD send Z(24) instead.
     *
     * <pre>
     * Section 2.2.2.1 AV_PAIR
     * MsvAvTimestamp is always sent in the CHALLENGE_MESSAGE.
     * Section 7 Appendix B: Product Behavior
     * </pre>
     *
     * MsvAvTimestamp AV_PAIR type is not supported in Windows NT, Windows 2000, Windows XP, and Windows Server 2003.
     * 
     * This defined here for completeness as it's necessary to 0 the correct number of bytes in the message.
     */
    private static final byte[] NTLM_LMCHALLENAGERESPONSE = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    /**
     * Section 2.2.2.10 VERSION
     *
     * used for debugging purposes only and its value does not affect NTLM message processing
     * 
     * This defined here for completeness as it's necessary to 0 the correct number of bytes in the message.
     * 
     */
    private static final byte[] NTLMSSP_VERSION = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

    /**
     * Section 2.2.5 NEGOTIATE
     *
     * <pre>
     * NTLM negotiate flags
     * NTLMSSP_NEGOTIATE_UNICODE                  A bit requests unicode character set encoding.
     * NTLMSSP_REQUEST_TARGET                     C bit TargetName field of the CHALLENGE_MESSAGE.
     * NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED      K bit indicates whether the domain name is provided.
     * NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED L bit indicates whether the Workstation field is present.
     * NTLMSSP_NEGOTIATE_TARGET_INFO              S bit indicates whether the TargetInfo fields are populated.
     * NTLMSSP_NEGOTIATE_ALWAYS_SIGN              M bit requests the presence of a signature block on all messages.
     * NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY P bit requests usage of the NTLM v2 session security.
     *
     * Note - This is not specified in spec but NTLMSSP_NEGOTIATE_ALWAYS_SIGN is required for server to verify MIC!!
     *        If not set, server ignores MIC field even tho MSVAVFLAGS is set!!
     * </pre>
     */
    private static final long NTLMSSP_NEGOTIATE_UNICODE = 0x00000001;
    private static final long NTLMSSP_REQUEST_TARGET = 0x00000004;
    private static final long NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED = 0x00001000;
    private static final long NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED = 0x00002000;
    private static final long NTLMSSP_NEGOTIATE_TARGET_INFO = 0x00800000;
    private static final long NTLMSSP_NEGOTIATE_ALWAYS_SIGN = 0x00008000;
    private static final long NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY = 0x00080000;

    /**
     * Section 2.2.2.1 AV_PAIR
     *
     * <pre>
     * AAvId (2bytes)
     * A 16-bit unsigned integer that defines the information type in the Value field. The contents of this field MUST
     * be a value from the following table. The corresponding Value field in this AV_PAIR MUST contain the information
     * specified in the description of that AvId
     * NTLM_AVID_MSVAVEOL             indicates that this is the last AV_PAIR in the list
     * NTLM_AVID_MSVAVNBCOMPUTERNAME  the server's NetBIOS computer name in unicode
     * NTLM_AVID_MSVAVNBDOMAINNAME    the server's NetBIOS domain name in unicode
     * NTLM_AVID_MSVAVDNSCOMPUTERNAME the FQDN of the computer in unicode
     * NTLM_AVID_MSVAVDNSDOMAINNAME   the FQDN of the domain in unicode
     * NTLM_AVID_MSVAVDNSTREENAME     the FQDN of the forest in unicode (not currently used)
     * NTLM_AVID_MSVAVFLAGS           indicates server or client configuration (0x00000002 indicates MIC provided)
     * NTLM_AVID_MSVAVTIMESTAMP       FILETIME structure that contains the server local time (ALWAYS present in CHALLENGE_MESSAGE
     * NTLM_AVID_MSVAVSINGLEHOST      Single Host Data structure  (not currently used)
     * NTLM_AVID_MSVAVTARGETNAME      SPN of the target server in unicode (not currently used)
     * </pre>
     */
    private static final short NTLM_AVID_MSVAVEOL = 0x0000;
    private static final short NTLM_AVID_MSVAVNBCOMPUTERNAME = 0x0001;
    private static final short NTLM_AVID_MSVAVNBDOMAINNAME = 0x0002;
    private static final short NTLM_AVID_MSVAVDNSCOMPUTERNAME = 0x0003;
    private static final short NTLM_AVID_MSVAVDNSDOMAINNAME = 0x0004;
    private static final short NTLM_AVID_MSVAVDNSTREENAME = 0x0005;
    private static final short NTLM_AVID_MSVAVFLAGS = 0x0006; // value in NTLM_AVID_VALUE_MIC
    private static final short NTLM_AVID_MSVAVTIMESTAMP = 0x0007;
    private static final short NTLM_AVID_MSVAVSINGLEHOST = 0x0008;
    private static final short NTLM_AVID_MSVAVTARGETNAME = 0x0009;

    /**
     * Section 2.2.2.1 AV_PAIR
     *
     * value of NTLM_AVID_MSVAVFLAGS that indicates a MIC is provided
     * 
     * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server
     * 
     * If the CHALLENGE_MESSAGE TargetInfo field has an MsvAvTimestamp present, the client SHOULD provide a MIC
     */
    private static final int NTLM_AVID_LENGTH = 2; // length of an AvId
    private static final int NTLM_AVLEN_LENGTH = 2; // length of an AVLen

    private static final int NTLM_AVFLAG_VALUE_MIC = 0x00000002; // indicates MIC is provided
    private static final int NTLM_MIC_LENGTH = 16; // length of MIC field

    private static final int NTLM_AVID_MSVAVFLAGS_LEN = 4; // length of MSVAVFLAG

    /**
     * Section 2.2.1.1 NEGOTIATE_MESSAGE
     *
     * <pre>
     * offset to payload in Negotiate message
     *     8 (signature) + 
     *     4 (message type) + 
     *     8 (domain name) + 
     *     8 (workstation) + 
     *     4 (negotiate flags) +
     *     0 (version)
     * </pre>
     */
    private static final int NTLM_NEGOTIATE_PAYLOAD_OFFSET = 32;

    /**
     * Section 2.2.1.3 AUTHENTICATE_MESSAGE
     *
     * <pre>
     * offsets in Authenticate message
     *     8 (signature) + 
     *     4 (message type) + 
     *     8 (lmChallengeResp) + 
     *     8 (ntChallengeResp) + 
     *     8 (domain name) + 
     *     8 (user name) +
     *     8 (workstation) + 
     *     8 (encrypted random session key) + 
     *     4 (negotiate flags) + 
     *     8 (version) + 
     *     16 (MIC)
     * </pre>
     */
    private static final int NTLM_AUTHENTICATE_PAYLOAD_OFFSET = 88;

    // client challenge length
    private static final int NTLM_CLIENT_NONCE_LENGTH = 8;

    // server challenge length
    private static final int NTLM_SERVER_CHALLENGE_LENGTH = 8;

    // Windows Filetime timestamp length
    private static final int NTLM_TIMESTAMP_LENGTH = 8;

    // Windows epoch time difference from Unix epoch time in secs
    private static final long WINDOWS_EPOCH_DIFF = 11644473600L;

    /**
     * NTLM Client Context
     */
    private class NTLMContext {
        // domain name to authenticate to
        private final String domainName;

        // unicode bytes of domain name
        private final byte[] domainUbytes;

        // user credentials
        private final String upperUserName;
        private final byte[] userNameUbytes;
        private final byte[] passwordHash;

        // workstation
        private String workstation;

        // unicode bytes of server SPN
        private final byte[] spnUbytes;

        // message authentication code
        private Mac mac = null;

        // negotiate flags from Challenge msg
        private long negotiateFlags = 0x00000000;

        // session key calculated from password
        private byte[] sessionBaseKey = null;

        // Windows FileTime timestamp - number of 100 nanosecond ticks since Windows Epoch time
        private byte[] timestamp = null;

        // target info field from Challenge msg
        private byte[] targetInfo = null;

        // server challenge from Challenge msg
        private byte[] serverChallenge = new byte[NTLM_SERVER_CHALLENGE_LENGTH];

        /**
         * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server
         * 
         * Save messages for calculating MIC
         */
        private byte[] negotiateMsg = null;
        private byte[] challengeMsg = null;

        /**
         * Creates an NTLM client context
         *
         * @param con
         *        connection to SQL server
         * @param domainName
         *        domain name to authentication in using NTLM
         * @param userName
         *        user name
         * @param password
         *        password
         * @param workstation
         *        hostname of the workstation
         * @throws SQLServerException
         *         if error occurs
         */
        NTLMContext(final SQLServerConnection con, final String domainName, final String userName,
                final byte[] passwordHash, final String workstation) throws SQLServerException {

            this.domainName = domainName.toUpperCase();
            this.domainUbytes = unicode(this.domainName);

            this.userNameUbytes = null != userName ? unicode(userName) : null;
            this.upperUserName = null != userName ? userName.toUpperCase() : null;

            this.passwordHash = passwordHash;

            this.workstation = workstation;

            String spn = null != con ? getSpn(con) : null;
            this.spnUbytes = null != spn ? unicode(spn) : null;

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(toString() + " SPN detected: " + spn);
            }

            try {
                mac = Mac.getInstance("HmacMD5");
            } catch (NoSuchAlgorithmException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmHmacMD5Error"));
                Object[] msgArgs = {domainName, e.getMessage()};
                throw new SQLServerException(form.format(msgArgs), e);
            }
        }
    };

    // Handle to NTLM context
    private NTLMContext context = null;

    /**
     * Creates an instance of the NTLM authentication
     *
     * @param con
     *        connection to SQL server
     * @param domainName
     *        domain name used for NTLM authentication
     * @param userName
     *        domain user
     * @param password
     *        domain password
     * @param workstation
     *        hostname of the workstation
     * @throws SQLServerException
     *         if error occurs
     */
    NTLMAuthentication(final SQLServerConnection con, final String domainName, final String userName,
            final byte[] passwordHash, final String workstation) throws SQLServerException {
        if (null == context) {
            this.context = new NTLMContext(con, domainName, userName, passwordHash, workstation);
        }
    }

    /**
     * Generates the NTLM client context
     *
     * @param inToken
     *        SSPI input blob
     * @param done
     *        indicates processing is done
     * @return NTLM client context
     * @throws SQLServerException
     *         if error occurs
     */
    @Override
    byte[] generateClientContext(final byte[] inToken, final boolean[] done) throws SQLServerException {
        return initializeSecurityContext(inToken, done);
    }

    /**
     * Releases the NTLM client context
     *
     * @throws SQLServerException
     *         if error occurs
     */
    @Override
    void releaseClientContext() {
        context = null;
    }

    /**
     * Parses the Type 2 NTLM Challenge message from server
     *
     * Section 2.2.1.2 CHALLENGE_MESSAGE
     *
     * @param inToken
     *        SSPI input blob
     * @throws SQLServerException
     *         if error occurs
     */
    private void parseNtlmChallenge(final byte[] inToken) throws SQLServerException {
        // get token buffer
        ByteBuffer token = ByteBuffer.wrap(inToken).order(ByteOrder.LITTLE_ENDIAN);

        // verify signature
        byte[] signature = new byte[NTLM_HEADER_SIGNATURE.length];
        token.get(signature);
        if (!Arrays.equals(signature, NTLM_HEADER_SIGNATURE)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmSignatureError"));
            Object[] msgArgs = {signature};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        // verify message type
        int messageType = token.getInt();
        if (messageType != NTLM_MESSAGE_TYPE_CHALLENGE) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmMessageTypeError"));
            Object[] msgArgs = {messageType};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        // target name fields
        int targetNameLen = token.getShort();
        token.getShort(); // targetNameMaxLen
        token.getInt(); // targetNameOffset

        // negotiate flags
        token.getInt();

        // get server challenge
        token.get(context.serverChallenge);

        // 8 bytes reserved 32-40: all 0's
        token.getLong();

        // target info fields
        int targetInfoLen = token.getShort();
        token.getShort(); // targetInfoMaxLen
        token.getInt(); // targetInfoOffset

        token.getLong(); // version - not used, for debug only

        // get requested target name
        byte[] targetName = new byte[targetNameLen];
        token.get(targetName);

        // verify targetInfo - was requested so should always be sent
        context.targetInfo = new byte[targetInfoLen];
        token.get(context.targetInfo);
        if (0 == context.targetInfo.length) {
            throw new SQLServerException(SQLServerException.getErrString("R_ntlmNoTargetInfo"), null);
        }

        // parse target info AV pairs
        ByteBuffer targetInfoBuf = ByteBuffer.wrap(context.targetInfo).order(ByteOrder.LITTLE_ENDIAN);
        boolean done = false;
        for (int i = 0; i < context.targetInfo.length && !done;) {
            int id = targetInfoBuf.getShort();
            byte[] value = new byte[targetInfoBuf.getShort()];
            targetInfoBuf.get(value);
            switch (id) {
                case NTLM_AVID_MSVAVTIMESTAMP:
                    if (value.length > 0) {
                        context.timestamp = new byte[NTLM_TIMESTAMP_LENGTH];
                        System.arraycopy(value, 0, context.timestamp, 0, NTLM_TIMESTAMP_LENGTH);
                    }
                    break;
                case NTLM_AVID_MSVAVEOL:
                    done = true;
                    break;
                case NTLM_AVID_MSVAVDNSDOMAINNAME:
                case NTLM_AVID_MSVAVDNSCOMPUTERNAME:
                case NTLM_AVID_MSVAVFLAGS:
                case NTLM_AVID_MSVAVNBCOMPUTERNAME:
                case NTLM_AVID_MSVAVNBDOMAINNAME:
                case NTLM_AVID_MSVAVDNSTREENAME:
                case NTLM_AVID_MSVAVSINGLEHOST:
                case NTLM_AVID_MSVAVTARGETNAME:
                    break;
                default:
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmUnknownValue"));
                    Object[] msgArgs = {value};
                    throw new SQLServerException(form.format(msgArgs), null);
            }

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(toString() + " NTLM Challenge Message target info: AvId " + id);
            }
        }

        /**
         * Section 2.2.2.1 AV_PAIR
         *
         * Section 7 Appendix B Product Behavior
         * 
         * This structure should always be sent in the CHALLENGE_MESSAGE not supported in Windows NT, Windows 2000,
         * Windows XP, and Windows Server 2003
         */
        if (null == context.timestamp || 0 >= context.timestamp.length) {
            // this SHOULD always be present but for some reason occasionally this had seen to be missing
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(toString() + " NTLM Challenge Message target info error: Missing timestamp.");
            }
        } else {
            // save msg for calculating MIC in Authenticate msg
            context.challengeMsg = new byte[inToken.length];
            System.arraycopy(inToken, 0, context.challengeMsg, 0, inToken.length);
        }
    }

    /**
     * Initializes the NTLM client security context
     *
     * @param inToken
     *        SSPI input blob
     * @param done
     *        indicates processing is done
     * @return outbound security context
     * @throws SQLServerException
     *         if error occurs
     */
    private byte[] initializeSecurityContext(final byte[] inToken, final boolean[] done) throws SQLServerException {
        if (null == inToken || 0 == inToken.length) {
            return generateNtlmNegotiate();
        } else {
            // get challenge msg from server
            parseNtlmChallenge(inToken);

            // get authenticate msg
            done[0] = true;
            return generateNtlmAuthenticate();
        }
    }

    /**
     * Generates NTLMv2 Client Challenge blob
     *
     * <pre>
     * Section 2.2.2.7 NTLM v2: NTLMv2_CLIENT_CHALLENGE:
     *
     * clientChallenge blob consists of:
     *     Responserversion,
     *     HiResponserversion,    NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE,
     *     Z(6),                  NTLM_CLIENT_CHALLENGE_RESERVED1, NTLM_CLIENT_CHALLENGE_RESERVED2
     *     Time,                  current timestamp
     *     ClientChallenge,       clientNonce
     *     Z(4),                  NTLM_CLIENT_CHALLENGE_RESERVED2
     *     ServerName,            CHALLENGE_MESSAGE.TargetInfo (from 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server)
     *     Z(4))                  EOL
     * </pre>
     *
     * @param clientNonce
     *        client challenge nonce
     * @return client challenge blob
     */
    private byte[] generateClientChallengeBlob(final byte[] clientNonce) {
        // timestamp is number of 100 nanosecond ticks since Windows Epoch
        ByteBuffer time = ByteBuffer.allocate(NTLM_TIMESTAMP_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        time.putLong((TimeUnit.SECONDS.toNanos(Instant.now().getEpochSecond() + WINDOWS_EPOCH_DIFF)) / 100);
        byte[] currentTime = time.array();

        // allocate token buffer
        ByteBuffer token = ByteBuffer
                .allocate(NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE.length + NTLM_CLIENT_CHALLENGE_RESERVED1.length
                        + NTLM_CLIENT_CHALLENGE_RESERVED2.length + currentTime.length + NTLM_CLIENT_NONCE_LENGTH
                        + NTLM_CLIENT_CHALLENGE_RESERVED3.length + context.targetInfo.length
                        + /* add MIC */ NTLM_AVID_LENGTH + NTLM_AVLEN_LENGTH + NTLM_AVID_MSVAVFLAGS_LEN
                        + /* add SPN */ NTLM_AVID_LENGTH + NTLM_AVLEN_LENGTH + context.spnUbytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        token.put(NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE);
        token.put(NTLM_CLIENT_CHALLENGE_RESERVED1);
        token.put(NTLM_CLIENT_CHALLENGE_RESERVED2);

        token.put(currentTime, 0, NTLM_TIMESTAMP_LENGTH);
        token.put(clientNonce, 0, NTLM_CLIENT_NONCE_LENGTH);
        token.put(NTLM_CLIENT_CHALLENGE_RESERVED3);

        /**
         * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server
         *
         * If the CHALLENGE_MESSAGE TargetInfo field has an MsvAvTimestamp present, the client SHOULD provide a MIC
         */
        if (null == context.timestamp || 0 >= context.timestamp.length) {
            token.put(context.targetInfo, 0, context.targetInfo.length);
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(toString()
                        + " MsvAvTimestamp not recieved from SQL Server in Challenge Message. MIC field will not be set.");
            }
        } else {
            // copy targetInfo up to NTLM_AVID_MSVAVEOL
            token.put(context.targetInfo, 0, context.targetInfo.length - NTLM_AVID_LENGTH - NTLM_AVLEN_LENGTH);

            // MIC
            token.putShort(NTLM_AVID_MSVAVFLAGS);
            token.putShort((short) NTLM_AVID_MSVAVFLAGS_LEN);
            token.putInt((int) NTLM_AVFLAG_VALUE_MIC);
        }

        // SPN
        token.putShort(NTLM_AVID_MSVAVTARGETNAME);
        token.putShort((short) context.spnUbytes.length);
        token.put(context.spnUbytes, 0, context.spnUbytes.length);

        // EOL
        token.putShort(NTLM_AVID_MSVAVEOL);
        token.putShort((short) 0);

        return token.array();
    }

    /**
     * Gets the HMAC MD5 hash
     *
     * @see <a href="https://www.ietf.org/rfc/rfc2104.txt">https://www.ietf.org/rfc/rfc2104.txt</a>
     * 
     * @param key
     *        key used for hash
     * @param data
     *        input data
     * @return HMAC MD5 hash
     * @throws InvalidKeyException
     *         if key is invalid
     */
    private byte[] hmacMD5(final byte[] key, final byte[] data) throws InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacMD5");
        context.mac.init(keySpec);
        return context.mac.doFinal(data);
    }

    /**
     * Gets the MD4 hash of input string
     *
     * @param str
     *        input string
     * @return MD4 hash
     */
    private static byte[] md4(final byte[] str) {
        MD4 md = new MD4();
        md.reset();
        md.update(str);
        return md.digest();
    }

    /**
     * Gets the unicode of a string
     *
     * @param str
     *        string to convert to unicode
     * @return unicode of string
     */
    private static byte[] unicode(final String str) {
        return (null != str) ? str.getBytes(java.nio.charset.StandardCharsets.UTF_16LE) : null;
    }

    /**
     * Concatenates 2 byte arrays
     *
     * @param arr1
     *        array 1
     * @param arr2
     *        array 2
     * @return concatenated array of arr1 and and arr2
     */
    private byte[] concat(final byte[] arr1, final byte[] arr2) {
        if (null == arr1 || null == arr2) {
            return null;
        }

        byte[] temp = new byte[arr1.length + arr2.length];
        System.arraycopy(arr1, 0, temp, 0, arr1.length);
        System.arraycopy(arr2, 0, temp, arr1.length, arr2.length);
        return temp;
    }

    /**
     * Get length of byte array
     * 
     * @param arr
     *        array to get length of
     * @return array length or 0 if null array
     */
    private int getByteArrayLength(byte[] arr) {
        return null == arr ? 0 : arr.length;
    }

    /**
     * Generates a key from password to get NTLMv2 hash Section 3.3.2 NTLM v2 Authentication
     *
     * @return NT response key
     * @throws InvalidKeyException
     *         if error getting hash due to invalid key
     */
    private byte[] ntowfv2() throws InvalidKeyException {
        return hmacMD5(context.passwordHash,
                (null != context.upperUserName) ? unicode(context.upperUserName + context.domainName)
                                                : unicode(context.domainName));
    }

    /**
     * Computes the NT response and keys from the Challenge message
     *
     * Section 3.3.2 NTLM v2 Authentication
     *
     * <pre>
     * Set temp to ConcatenationOf(Responserversion, HiResponserversion, 
     * Z(6), Time, ClientChallenge, Z(4), ServerName, Z(4)) 
     * Set NTProofStr to HMAC_MD5(ResponseKeyNT,  
     * ConcatenationOf(CHALLENGE_MESSAGE.ServerChallenge,temp)) 
     * Set NtChallengeResponse to ConcatenationOf(NTProofStr, temp) 
     * Set LmChallengeResponse to ConcatenationOf(HMAC_MD5(ResponseKeyLM,  
     * ConcatenationOf(CHALLENGE_MESSAGE.ServerChallenge, ClientChallenge)), 
     * ClientChallenge )
     * </pre>
     *
     * @param responseKeyNT
     *        NT response hash key
     * @return computed response
     * @throws InvalidKeyException
     *         if error getting hash due to invalid key
     */
    private byte[] computeResponse(final byte[] responseKeyNT) throws InvalidKeyException {
        // get random client challenge nonce
        byte[] clientNonce = new byte[NTLM_CLIENT_NONCE_LENGTH];
        ThreadLocalRandom.current().nextBytes(clientNonce);

        // get client challenge blob
        byte[] temp = generateClientChallengeBlob(clientNonce);

        byte[] ntProofStr = hmacMD5(responseKeyNT, concat(context.serverChallenge, temp));

        context.sessionBaseKey = hmacMD5(responseKeyNT, ntProofStr);

        return concat(ntProofStr, temp);
    }

    /**
     * Generates the NT Challenge response to server Challenge
     *
     * Section 3.3.2 NTLM v2 Authentication
     *
     * @return NT challenge response
     * @throws InvalidKeyException
     *         if error getting hash due to invalid key
     */
    private byte[] getNtChallengeResp() throws InvalidKeyException {
        byte[] responseKeyNT = ntowfv2();
        return computeResponse(responseKeyNT);
    }

    /**
     * Generates the Type 3 NTLM Authentication message
     *
     * Section 2.2.1.3 AUTHENTICATE_MESSAGE
     *
     * @return NTLM Authenticate message
     * @throws SQLServerException
     *         if error occurs
     */
    private byte[] generateNtlmAuthenticate() throws SQLServerException {
        int domainNameLen = getByteArrayLength(context.domainUbytes);
        int userNameLen = getByteArrayLength(context.userNameUbytes);
        byte[] workstationBytes = unicode(context.workstation);
        int workstationLen = getByteArrayLength(workstationBytes);
        byte[] msg = null;

        try {
            // get NT challenge response
            byte[] ntChallengeResp = getNtChallengeResp();
            int ntChallengeLen = getByteArrayLength(ntChallengeResp);

            // allocate token buffer
            ByteBuffer token = ByteBuffer.allocate(NTLM_AUTHENTICATE_PAYLOAD_OFFSET + NTLM_LMCHALLENAGERESPONSE.length
                    + ntChallengeLen + domainNameLen + userNameLen + workstationLen).order(ByteOrder.LITTLE_ENDIAN);

            // set NTLM signature and message type
            token.put(NTLM_HEADER_SIGNATURE, 0, NTLM_HEADER_SIGNATURE.length);
            token.putInt(NTLM_MESSAGE_TYPE_AUTHENTICATE);

            // start of payload data
            int offset = NTLM_AUTHENTICATE_PAYLOAD_OFFSET;

            // LM challenge response
            token.putShort((short) 0);
            token.putShort((short) 0);
            token.putInt(offset);
            offset += NTLM_LMCHALLENAGERESPONSE.length;

            // NT challenge response
            token.putShort((short) ntChallengeLen);
            token.putShort((short) ntChallengeLen);
            token.putInt(offset);
            offset += ntChallengeLen;

            // domain name fields
            token.putShort((short) domainNameLen);
            token.putShort((short) domainNameLen);
            token.putInt(offset);
            offset += domainNameLen;

            // user name fields
            token.putShort((short) userNameLen);
            token.putShort((short) userNameLen);
            token.putInt(offset);
            offset += userNameLen;

            // workstation fields
            token.putShort((short) workstationLen);
            token.putShort((short) workstationLen);
            token.putInt(offset);
            offset += workstationLen;

            // not used = encrypted random session key fields
            token.putShort((short) 0);
            token.putShort((short) 0);
            token.putInt(offset);

            // same negotiate flags sent in negotiate message
            token.putInt((int) context.negotiateFlags);

            // version - not used but need to send blank to separate from MIC otherwise server confuses this with MIC
            token.put(NTLMSSP_VERSION, 0, NTLMSSP_VERSION.length);

            // 0 the MIC field first for calculation
            byte[] mic = new byte[NTLM_MIC_LENGTH];
            int micPosition = token.position(); // save position
            token.put(mic, 0, NTLM_MIC_LENGTH);

            // payload data
            token.put(NTLM_LMCHALLENAGERESPONSE, 0, NTLM_LMCHALLENAGERESPONSE.length);
            token.put(ntChallengeResp, 0, ntChallengeLen);
            token.put(context.domainUbytes, 0, domainNameLen);
            token.put(context.userNameUbytes, 0, userNameLen);
            token.put(workstationBytes, 0, workstationLen);

            msg = token.array();

            /**
             * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server
             * 
             * MIC is calculated by using a 0 MIC then hmacMD5 of session key and concat of the 3 msgs
             */
            if (null != context.timestamp && 0 < context.timestamp.length) {
                SecretKeySpec keySpec = new SecretKeySpec(context.sessionBaseKey, "HmacMD5");
                context.mac.init(keySpec);
                context.mac.update(context.negotiateMsg);
                context.mac.update(context.challengeMsg);
                mic = context.mac.doFinal(msg);

                // put calculated MIC into Authenticate msg
                System.arraycopy(mic, 0, msg, micPosition, NTLM_MIC_LENGTH);
            }
        } catch (InvalidKeyException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmAuthenticateError"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(form.format(msgArgs), e);
        }

        return msg;
    }

    /**
     * Generates the Type 1 NTLM Negotiate message
     *
     * Section 2.2.1.1 NEGOTIATE_MESSAGE
     *
     * @return NTLM Negotiate message
     */
    private byte[] generateNtlmNegotiate() {
        int domainNameLen = getByteArrayLength(context.domainUbytes);
        int workstationLen = getByteArrayLength(context.workstation.getBytes());

        ByteBuffer token = null;
        token = ByteBuffer.allocate(NTLM_NEGOTIATE_PAYLOAD_OFFSET + domainNameLen + workstationLen)
                .order(ByteOrder.LITTLE_ENDIAN);

        // signature and message type
        token.put(NTLM_HEADER_SIGNATURE, 0, NTLM_HEADER_SIGNATURE.length);
        token.putInt(NTLM_MESSAGE_TYPE_NEGOTIATE);

        // NTLM negotiate flags - only NTLMV2 supported
        context.negotiateFlags = NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED | NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED
                | NTLMSSP_REQUEST_TARGET | NTLMSSP_NEGOTIATE_TARGET_INFO | NTLMSSP_NEGOTIATE_UNICODE
                | NTLMSSP_NEGOTIATE_ALWAYS_SIGN | NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY;
        token.putInt((int) context.negotiateFlags);

        int offset = NTLM_NEGOTIATE_PAYLOAD_OFFSET;

        // domain name fields
        token.putShort((short) domainNameLen);
        token.putShort((short) domainNameLen);
        token.putInt(offset);
        offset += domainNameLen;

        // workstation field
        token.putShort((short) workstationLen);
        token.putShort((short) workstationLen);
        token.putInt(offset);
        offset += workstationLen;

        // version - not used, for debug only

        // payload
        token.put(context.domainUbytes, 0, domainNameLen);
        token.put(context.workstation.getBytes(), 0, workstationLen);

        // save msg for calculating MIC in Authenticate msg
        byte[] msg = token.array();
        context.negotiateMsg = new byte[msg.length];
        System.arraycopy(msg, 0, context.negotiateMsg, 0, msg.length);

        return msg;
    }

    public static byte[] getNtlmPasswordHash(String password) throws SQLServerException {
        if (null == password) {
            throw new SQLServerException(SQLServerException.getErrString("R_NtlmNoUserPasswordDomain"), null);
        }

        return md4(unicode(password));
    }
}
