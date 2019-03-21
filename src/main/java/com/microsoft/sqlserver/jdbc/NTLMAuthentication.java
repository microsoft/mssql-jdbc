/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import mssql.security.provider.MD4;


/**
 * Provides an implementation of NTLM authentication
 * 
 * <pre>
 * See <a href=https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/b38c36ed-2804-4868-a9ff-8dd3182128e4">MS-NLMP:NTLM Authentication Protocol</a>
 * </pre>
 */
final class NTLMAuthentication extends SSPIAuthentication {

    /**
     * <pre>
     * Section 2.2.1.1 NEGOTIATE_MESSAGE
     * Section 2.2.1.2 CHALLENGE_MESSAGE
     * Section 2.2.1.3 AUTHENCIATE_MESSAGE
     * 
     * NTLM signature for all 3 messages - "NTLMSSP\0"
     * </pre>
     */
    private static final byte[] NTLM_HEADER_SIGNATURE = {0x4e, 0x54, 0x4c, 0x4d, 0x53, 0x53, 0x50, 0x00};

    // NTLM messages type
    private static final int NTLM_MESSAGE_TYPE_NEGOTIATE = 0x00000001;
    private static final int NTLM_MESSAGE_TYPE_CHALLENGE = 0x00000002;
    private static final int NTLM_MESSAGE_TYPE_AUTHENTICATE = 0x00000003;

    /**
     * <pre>
     * Section 2.2.2.7 NTLM v2: NTLMv2_CLIENT_CHALLENGE
     * 
     * RespType - current version of the challenge response type. This field MUST be 0x01
     * HiRespType - max supported version (HiRespType) of the client response type.
     * </pre>
     */
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE = {0x01, 0x01};

    /**
     * <pre>
     * Section 2.2.2.7 NTLM v2: NTLMv2_CLIENT_CHALLENGE
     * 
     * Reserved1  SHOULD be 0x0000
     * Reserved2  SHOULD be 0x00000000  
     * Reserved3  SHOULD be 0x00000000
     * </pre>
     */
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED1 = {0x00, 0x00};
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED2 = {0x00, 0x00, 0x00, 0x00};;
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED3 = {0x00, 0x00, 0x00, 0x00};;

    /**
     * <pre>
     * Section 2.2.5 NEGOTIATE
     * 
     * NTLM negotiate flags
     * NTLMSSP_NEGOTIATE_UNICODE                  A bit requests Unicode character set encoding
     * NTLMSSP_REQUEST_TARGET                     C bit TargetName field of the CHALLENGE_MESSAGE
     * NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED      K bit indicates whether the domain name is provided
     * NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED L bit indicates whether the Workstation field is present
     * NTLMSSP_NEGOTIATE_TARGET_INFO              S bit indicates whether the TargetInfo fields are populated
     * NTLMSSP_NEGOTIATE_ALWAYS_SIGN              M bit requests the presence of a signature block on all messages.
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
    private static final long NTLMSSP_NEGOTIATE_SIGN = 0x00000010;
    private static final long NTLMSSP_NEGOTIATE_ALWAYS_SIGN = 0x00008000;

    /**
     * <pre>
     * Section 2.2.2.1 AV_PAIR
     * 
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
     * <pre>
     * Section 2.2.2.1 AV_PAIR
     * value of NTLM_AVID_MSVAVFLAGS that indicates a MIC is provided
     * 
     * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server
     * If the CHALLENGE_MESSAGE TargetInfo field has an MsvAvTimestamp present, the client SHOULD provide a MIC
     * </pre>
     */
    private static final short NTLM_AVID_VALUE_MIC = 0x00000002;

    // length of MIC field
    private static final int NTLM_MIC_LENGTH = 16;

    /**
     * <pre>
     * Section 2.2.1.1 NEGOTIATE_MESSAGE
     *
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
     * <pre>
     * Section 2.2.1.3 AUTHENTICATE_MESSAGE
     * 
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
     *     0 (version) + 
     *     16 (mic)
     * </pre>
     */
    private static final int NTLM_AUTHENTICATE_MIC_OFFSET = 64;
    private static final int NTLM_AUTHENTICATE_PAYLOAD_OFFSET = 80;

    // challenge lengths
    private static final int NTLM_CLIENT_NONCE_LENGTH = 8;
    private static final int NTLM_SERVER_CHALLENGE_LENGTH = 8;

    // Windows Filetime timestamp length
    private static final int NTLM_TIMESTAMP_LENGTH = 8;

    // Windows epoch time difference from Unix epoch time in secs
    private static final long WINDOWS_EPOCH_DIFF = 11644473600L;

    /*
     * NTLM Context
     */
    private class NTLMContext {
        // domain name to connect to
        private final String domainName;
        private final byte[] domainBytes;

        // user credentials
        private final String userName;
        private final String password;
        private final byte[] userBytes;

        private final String serverName;
        private final String workstation;
        private final byte[] workstationBytes;

        // message authentication code
        private Mac mac = null;

        // negotiate flags
        long negotiateFlags = 0x00000000;

        // session key calculated from user's password
        byte[] sessionBaseKey = null;

        // Windows FileTime timestamp - number of 100 nanosecond ticks since Windows Epoch time
        byte[] timestamp = new byte[NTLM_TIMESTAMP_LENGTH];

        // output token
        private ByteBuffer token = null;

        // target info field from server
        private byte[] targetInfo = null;

        // server challenge
        private byte[] serverChallenge = new byte[NTLM_SERVER_CHALLENGE_LENGTH];

        /**
         * </pre>
         * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server saved messages for calculatling MIC
         */
        private byte[] negotiateMsg = null;
        private byte[] challengeMsg = null;

        NTLMContext(SQLServerConnection con, String serverName, String domainName,
                String workstation) throws NoSuchAlgorithmException {
            this.domainName = null != domainName ? domainName.toUpperCase() : "\0";
            this.domainBytes = this.domainName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            this.userName = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString());
            this.userBytes = userName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            this.password = con.activeConnectionProperties
                    .getProperty(SQLServerDriverStringProperty.PASSWORD.toString());
            this.serverName = serverName;
            this.workstation = workstation;
            this.workstationBytes = workstation.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            mac = Mac.getInstance("HmacMD5");
        }
    };

    private NTLMContext context = null;

    /*
     * Creates an instance of the NTLM authentication
     * @param con - connection
     * @param domainName - domain name to connect to
     */
    NTLMAuthentication(SQLServerConnection con, String serverName, String domainName,
            String workstation) throws SQLServerException {
        try {
            if (context == null) {
                this.context = new NTLMContext(con, serverName, domainName, workstation);
            }
        } catch (NoSuchAlgorithmException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmInitError"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(form.format(msgArgs), e);
        }
    }

    /*
     * Generate SSPI client context
     * @see com.microsoft.sqlserver.jdbc.SSPIAuthentication#generateClientContext(byte[], boolean[])
     * @param inToken -SSPI input blob
     * @param done - indicates processing is done
     */
    @Override
    byte[] generateClientContext(byte[] inToken, boolean[] done) throws SQLServerException {
        return initializeSecurityContext(inToken, done);
    }

    /*
     * Release SSPI client context
     * @see com.microsoft.sqlserver.jdbc.SSPIAuthentication#releaseClientContext()
     */
    @Override
    int releaseClientContext() throws SQLServerException {
        context = null;
        return 0;
    }

    /*
     * Get the NTLM Challenge message from server
     * @param inToken - SSPI input blob
     */
    private void getNtlmChallenge(byte[] inToken) throws SQLServerException, IOException {

        context.token = ByteBuffer.wrap(inToken).order(ByteOrder.LITTLE_ENDIAN);

        // verify signature
        byte[] signature = new byte[NTLM_HEADER_SIGNATURE.length];
        context.token.get(signature);
        if (!Arrays.equals(signature, NTLM_HEADER_SIGNATURE)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmSignatureError"));
            Object[] msgArgs = {signature};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        // verify message type
        int messageType = context.token.getInt();
        if (messageType != NTLM_MESSAGE_TYPE_CHALLENGE) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmMessageTypeError"));
            Object[] msgArgs = {messageType};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        // target name fields
        int targetNameLen = context.token.getShort();
        context.token.getShort(); // targetNameMaxLen
        context.token.getInt(); // targetNameOffset

        // negotiate flags
        int flags = context.token.getInt();

        // get server challenge
        context.token.get(context.serverChallenge);

        // 8 bytes reserved 32-40: all 0's
        context.token.getLong();

        // target info fields
        int targetInfoLen = context.token.getShort();
        context.token.getShort(); // targetInfoMaxLen
        context.token.getInt(); // targetInfoOffset

        context.token.getLong(); // version - not used

        // get requested target name
        // the target name is not verified since this is also in the target info av pairs
        byte[] targetName = new byte[targetNameLen];
        context.token.get(targetName);

        // verfy targetInfo - was requested so should always be sent
        context.targetInfo = new byte[targetInfoLen];
        context.token.get(context.targetInfo);
        if (context.targetInfo.length == 0) {
            throw new SQLServerException(SQLServerException.getErrString("R_ntlmNoTargetInfo"), null);
        }

        ByteBuffer targetInfoBuf = ByteBuffer.wrap(context.targetInfo).order(ByteOrder.LITTLE_ENDIAN);
        boolean done = false;
        for (int i = 0; i < context.targetInfo.length && !done;) {
            int id = targetInfoBuf.getShort();
            int len = targetInfoBuf.getShort();
            byte[] value = new byte[len];
            targetInfoBuf.get(value);

            switch (id) {
                // verify domain name
                case NTLM_AVID_MSVAVDNSDOMAINNAME:
                    if (context.domainName.equalsIgnoreCase(new String(value))) {
                        throw new SQLServerException(SQLServerException.getErrString("R_ntlmNoTargetInfo"), null);
                    }
                    break;
                case NTLM_AVID_MSVAVDNSCOMPUTERNAME:
                    // verify server name
                    if (context.serverName.equalsIgnoreCase(new String(value))) {
                        throw new SQLServerException(SQLServerException.getErrString("R_ntlmNoServer"), null);
                    }
                    break;
                case NTLM_AVID_MSVAVTIMESTAMP:
                    System.arraycopy(value, 0, context.timestamp, 0, NTLM_TIMESTAMP_LENGTH);
                    break;
                case NTLM_AVID_MSVAVFLAGS:
                    targetInfoBuf.putInt(NTLM_AVID_VALUE_MIC);
                case NTLM_AVID_MSVAVEOL:
                    done = true;
                    break;
                case NTLM_AVID_MSVAVNBCOMPUTERNAME:
                case NTLM_AVID_MSVAVNBDOMAINNAME:
                case NTLM_AVID_MSVAVDNSTREENAME:
                case NTLM_AVID_MSVAVSINGLEHOST:
                case NTLM_AVID_MSVAVTARGETNAME:
                    // ignore others
                    break;
                default:
                    throw new SQLServerException(SQLServerException.getErrString("R_ntlmUnknownValue"), null);
            }
        }

        /*
         * Section 2.2.2.1 AV_PAIR, Section 7 Appendix B: Product Behavior This structure is always sent in the
         * CHALLENGE_MESSAGE not supported in Windows NT, Windows 2000, Windows XP, and Windows Server 2003
         */
        if (null == context.timestamp || context.timestamp[0] == '\0') {
            throw new SQLServerException(SQLServerException.getErrString("R_ntlmNoTimestamp"), null);
        }

        // save msg for calculating MIC in Authenticate msg
        context.challengeMsg = new byte[inToken.length];
        System.arraycopy(inToken, 0, context.challengeMsg, 0, inToken.length);
    }

    /*
     * Initializes a security context
     * @param inToken - SSPI input blob
     * @param done - indicates processing is done
     */
    private byte[] initializeSecurityContext(byte[] inToken, boolean[] done) throws SQLServerException {

        try {
            if (inToken.length == 0) {
                return getNtlmNegotiateMsg();
            } else {
                // server challenge msg
                getNtlmChallenge(inToken);

                // get authenticate msg
                done[0] = true;
                return getNtlmAuthenticateMsg();
            }
        } catch (IOException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmNegotiateError"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(form.format(msgArgs), e);
        }
    }

    /*
     * Get NTLMv2 Client Challenge
     * @param clientNonce - client challenge nonce
     */
    private byte[] getClientChallenge(byte[] clientNonce) throws IOException {

        // timestamp is number of 100 nanosecond ticks since Windows Epoch
        ByteBuffer time = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        time.putLong((TimeUnit.SECONDS.toNanos(Instant.now().getEpochSecond() + WINDOWS_EPOCH_DIFF)) / 100);
        byte[] currentTime = time.array();

        context.token = ByteBuffer
                .allocate(NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE.length + NTLM_CLIENT_CHALLENGE_RESERVED1.length
                        + NTLM_CLIENT_CHALLENGE_RESERVED2.length + currentTime.length + NTLM_CLIENT_NONCE_LENGTH
                        + NTLM_CLIENT_CHALLENGE_RESERVED3.length + context.targetInfo.length + 8)
                .order(ByteOrder.LITTLE_ENDIAN);

        context.token.put(NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE);
        context.token.put(NTLM_CLIENT_CHALLENGE_RESERVED1);
        context.token.put(NTLM_CLIENT_CHALLENGE_RESERVED2);

        context.token.put(currentTime, 0, NTLM_TIMESTAMP_LENGTH);
        context.token.put(clientNonce, 0, NTLM_CLIENT_NONCE_LENGTH);
        context.token.put(NTLM_CLIENT_CHALLENGE_RESERVED3);

        /**
         * <pre>
         * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server
         * If the CHALLENGE_MESSAGE TargetInfo field has an MsvAvTimestamp present, the client SHOULD provide a MIC
         * </pre>
         */
        ByteBuffer newTargetInfo = ByteBuffer.allocate(context.targetInfo.length + 8).order(ByteOrder.LITTLE_ENDIAN);

        // copy targetInfo up to NTLM_AVID_MSVAVEOL
        newTargetInfo.put(context.targetInfo, 0, context.targetInfo.length - 4);

        // MIC flag
        // TODO: add to include MIC
        // newTargetInfo.putShort(NTLM_AVID_MSVAVFLAGS);
        // newTargetInfo.putShort((short) 4);
        // newTargetInfo.putInt((int) NTLM_AVID_VALUE_MIC);

        // EOL
        newTargetInfo.putShort(NTLM_AVID_MSVAVEOL);
        newTargetInfo.putShort((short) 0);

        context.token.put(newTargetInfo.array(), 0, newTargetInfo.array().length);

        return context.token.array();
    }

    /*
     * Get HMAC MD5 hash https://www.ietf.org/rfc/rfc2104.txt
     * @param key - key
     * @param data - data
     */
    private byte[] hmacMD5(byte[] key, byte[] data) throws InvalidKeyException {
        SecretKeySpec keySpec = new SecretKeySpec(key, "HmacMD5");
        context.mac.init(keySpec);
        return context.mac.doFinal(data);
    }

    /*
     * Get MD4 hash of input string
     * @param str - input string
     */
    private static byte[] MD4(byte[] str) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        MD4 md = new MD4();
        md.reset();
        md.update(str);
        return md.digest();
    }

    /*
     * Get unicode of string
     * @param str - string to convert to unicode
     */
    private byte[] unicode(String str) {
        return str.getBytes(UTF_16LE);
    }

    /*
     * Generate key from password to get NTLMv2 hash
     */
    private byte[] ntowfv2() throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {

        return hmacMD5(MD4(unicode(context.password)), unicode(context.userName.toUpperCase() + context.domainName));
    }

    /*
     * Compute challenge response
     * @param responseKeyNT - response hash key
     * @param clientChallenge - client challenge
     */
    private byte[] computeResponse(byte[] responseKeyNT,
            byte[] clientChallenge) throws NoSuchAlgorithmException, InvalidKeyException {

        // concatenate client and server challenge
        int clientChallengeLen = clientChallenge.length;
        byte[] temp = new byte[NTLM_SERVER_CHALLENGE_LENGTH + clientChallengeLen];
        System.arraycopy(context.serverChallenge, 0, temp, 0, NTLM_SERVER_CHALLENGE_LENGTH);
        System.arraycopy(clientChallenge, 0, temp, NTLM_SERVER_CHALLENGE_LENGTH, clientChallengeLen);

        byte[] ntProofStr = hmacMD5(responseKeyNT, temp);
        int ntProofStrLen = ntProofStr.length;

        context.sessionBaseKey = hmacMD5(responseKeyNT, ntProofStr);

        byte[] ntlmv2resp = new byte[ntProofStr.length + clientChallengeLen];
        System.arraycopy(ntProofStr, 0, ntlmv2resp, 0, ntProofStrLen);
        System.arraycopy(clientChallenge, 0, ntlmv2resp, ntProofStrLen, clientChallengeLen);
        return ntlmv2resp;
    }

    /*
     * Get NT Challenge response to server challenge
     * @param clientNonce - client challenge nonce
     */
    private byte[] getNtChallengeResp(
            byte[] clientNonce) throws NoSuchAlgorithmException, InvalidKeyException, IOException {
        byte[] responseKeyNT = ntowfv2();
        return computeResponse(responseKeyNT, getClientChallenge(clientNonce));
    }

    /*
     * Get NTLM Authentication message
     */
    private byte[] getNtlmAuthenticateMsg() throws SQLServerException {
        int domainNameLen = context.domainBytes.length * 2;
        int userNameLen = context.userBytes.length * 2;
        int workstationLen = context.workstationBytes.length * 2;
        byte[] clientNonce = new byte[NTLM_CLIENT_NONCE_LENGTH];
        byte[] msg = null;

        try {
            // get random client challenge nonce
            SecureRandom.getInstanceStrong().nextBytes(clientNonce);

            // LM Challenge response is only sent if no MsAvTimestamp

            byte[] ntChallengeResp = getNtChallengeResp(clientNonce);

            context.token = ByteBuffer.allocate(NTLM_AUTHENTICATE_PAYLOAD_OFFSET + domainNameLen + userNameLen
                    + workstationLen + ntChallengeResp.length).order(ByteOrder.LITTLE_ENDIAN);

            // set NTLM signature and message type
            context.token.put(NTLM_HEADER_SIGNATURE, 0, NTLM_HEADER_SIGNATURE.length);
            context.token.putInt(NTLM_MESSAGE_TYPE_AUTHENTICATE);

            int offset = NTLM_AUTHENTICATE_PAYLOAD_OFFSET + domainNameLen + userNameLen + workstationLen;

            /**
             * <pre>
             * Section 3.1.5.1.1 Client Initiates the NEGOTIATE_MESSAGE
             * If NTLM v2 authentication is used and the CHALLENGE_MESSAGE TargetInfo field (section 2.2.1.2) has an
             * MsvAvTimestamp present, the client SHOULD NOT send the LmChallengeResponse and SHOULD send Z(24)
             * instead.
             * </pre>
             */
            context.token.putShort((short) 0);
            context.token.putShort((short) 0);
            context.token.putInt(offset);

            // NT challenge response
            int len = ntChallengeResp.length;
            context.token.putShort((short) len);
            context.token.putShort((short) len);
            context.token.putInt(offset);

            // start of payload data
            offset = NTLM_AUTHENTICATE_PAYLOAD_OFFSET;

            // domain name fields
            len = domainNameLen;
            context.token.putShort((short) len);
            context.token.putShort((short) len);
            context.token.putInt(offset);
            offset += len;

            // user name fields
            len = userNameLen;
            context.token.putShort((short) len);
            context.token.putShort((short) len);
            context.token.putInt(offset);
            offset += len;

            // workstation fields
            len = workstationLen;
            context.token.putShort((short) len);
            context.token.putShort((short) len);
            context.token.putInt(offset);
            offset += len;

            // not used = encrypted random session key fields
            len = 0; // do not send
            context.token.putShort((short) len);
            context.token.putShort((short) len);
            context.token.putInt(offset);

            // same negotiate flags sent in negotiate message
            context.token.putInt((int) context.negotiateFlags);

            // version not used - for debug only

            // 0 the MIC field first for calculation
            byte[] mic = new byte[NTLM_MIC_LENGTH];
            context.token.put(mic, 0, NTLM_MIC_LENGTH);

            // payload
            context.token.put(unicode(context.domainName), 0, domainNameLen);
            context.token.put(unicode(context.userName), 0, userNameLen);
            context.token.put(unicode(context.workstation), 0, workstationLen);
            context.token.put(ntChallengeResp, 0, ntChallengeResp.length);

            /**
             * <pre>
             * Section 3.1.5.1.2 Client Receives a CHALLENGE_MESSAGE from the Server
             * MIC is calculated by concatenating of all 3 msgs with 0 MIC then hmacMD5 of session key and concat of the
             * 3 msgs
             * </pre>
             */
            // TODO: verify
            msg = context.token.array();
            SecretKeySpec keySpec = new SecretKeySpec(context.sessionBaseKey, "HmacMD5");
            context.mac.init(keySpec);
            context.mac.update(context.negotiateMsg);
            context.mac.update(context.challengeMsg);
            // context.mac.update(msg);
            mic = context.mac.doFinal(msg);

            // put calculated MIC into Authenticate msg
            System.arraycopy(mic, 0, msg, NTLM_AUTHENTICATE_MIC_OFFSET, NTLM_MIC_LENGTH);
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmAuthError"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(form.format(msgArgs), e);
        }
        return msg;
    }

    /*
     * Get NTLM Negotiate message
     */
    private byte[] getNtlmNegotiateMsg() throws SQLServerException, IOException {
        context.token = ByteBuffer
                .allocate(NTLM_NEGOTIATE_PAYLOAD_OFFSET + context.domainBytes.length + context.workstationBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        // signature and message type
        context.token.put(NTLM_HEADER_SIGNATURE, 0, NTLM_HEADER_SIGNATURE.length);
        context.token.putInt(NTLM_MESSAGE_TYPE_NEGOTIATE);

        // NTLM negotiate flags - only NTLMV2 supported
        context.negotiateFlags = NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED | NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED
                | NTLMSSP_REQUEST_TARGET | NTLMSSP_NEGOTIATE_TARGET_INFO | NTLMSSP_NEGOTIATE_UNICODE
                | NTLMSSP_NEGOTIATE_ALWAYS_SIGN | NTLMSSP_NEGOTIATE_SIGN;
        context.token.putInt((int) context.negotiateFlags);

        // domain name fields
        int len = context.domainBytes.length;
        int offset = NTLM_NEGOTIATE_PAYLOAD_OFFSET;
        context.token.putShort((short) len);
        context.token.putShort((short) len);
        context.token.putInt(offset);
        offset += len;

        // workstation field
        len = context.workstationBytes.length;
        context.token.putShort((short) len);
        context.token.putShort((short) len);
        context.token.putInt(offset);
        offset += len;

        // version not used - for debug only

        // payload
        context.token.put(context.domainBytes, 0, context.domainBytes.length);
        context.token.put(context.workstationBytes, 0, context.workstationBytes.length);

        // save msg for calculating MIC in Authenticate msg
        byte[] msg = context.token.array();
        context.negotiateMsg = new byte[msg.length];
        System.arraycopy(msg, 0, context.negotiateMsg, 0, msg.length);

        return msg;
    }
}
