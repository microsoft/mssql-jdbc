/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

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

import mssql.java.security.MD4;


/**
 * Provides an implementation of NTLM authentication
 * 
 * <pre>
 * See <a href=https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/b38c36ed-2804-4868-a9ff-8dd3182128e4">MS-NLMP:NTLM Authentication Protocol</a>
 * </pre>
 */
final class NTLMAuthentication extends SSPIAuthentication {
    // NTLM signature "NTLMSSP\0"
    private static final byte[] NTLM_HEADER_SIGNATURE = {0x4e, 0x54, 0x4c, 0x4d, 0x53, 0x53, 0x50, 0x00};

    // NTLM messages type
    private static final int NTLM_MESSAGE_TYPE_NEGOTIATE = 0x00000001;
    private static final int NTLM_MESSAGE_TYPE_CHALLENGE = 0x00000002;
    private static final int NTLM_MESSAGE_TYPE_AUTHENTICATE = 0x00000003;

    // current and max supported version of the client challenge response type
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE = {0x01, 0x01};

    // client challenge reserved fields
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED1 = {0x00, 0x00};
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED2 = {0x00, 0x00, 0x00, 0x00};;
    private static final byte[] NTLM_CLIENT_CHALLENGE_RESERVED3 = {0x00, 0x00, 0x00, 0x00};;

    // NTLM negotiate flags
    private static final long NTLMSSP_NEGOTIATE_UNICODE = 0x00000001;
    private static final long NTLMSSP_REQUEST_TARGET = 0x00000004;
    private static final long NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED = 0x00001000;
    private static final long NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED = 0x00002000;
    private static final long NTLMSSP_NEGOTIATE_ALWAYS_SIGN = 0x00008000;

    // NTLM target name types
    // TODO: use for verification
    private static final long NTLMSSP_TARGET_TYPE_DOMAIN = 0x00010000; // sent from server only
    private static final long NTLMSSP_TARGET_TYPE_SERVER = 0x00020000; // sent from server only
    private static final long NTLM_NEGOTIATE_TARGET_INFO = 0x00800000; // sent from server only

    // offsets to payload
    // 8 (signature) + 4 (message type) + 8 (domain name) + 8 (workstation) + 4 (negotiate flags) + 0 (version)
    private static final int NTLM_NEGOTIATE_PAYLOAD_OFFSET = 32;

    // 8 (signature) + 4 (message type) + 8 (lmChallengeResp) + 8 (ntChallengeResp) + 8 (domain name) + 8 (user name) +
    // 8 (workstation) + 8 (encrypted random session key) + 4 (negotiate flags) + 0 (version) + 0 (mic)
    private static final int NTLM_AUTHENTICATE_PAYLOAD_OFFSET = 64;

    // challenge lengths
    private static final int NTLM_CLIENT_NONCE_LENGTH = 8;
    private static final int NTLM_SERVER_CHALLENGE_LENGTH = 8;

    private class NtlmContext {
        // domain name to connect to
        private final String domainName;
        private final byte[] domainBytes;

        // user credentials
        private final String userName;
        private final String password;
        private final byte[] userBytes;
        private final String workstation;
        private final byte[] workstationBytes;

        // message authentication code
        private Mac mac = null;

        // output token
        private ByteBuffer token = null;

        // target info field from server
        private byte[] targetInfo = null;

        // server challenge
        private byte[] serverChallenge = new byte[NTLM_SERVER_CHALLENGE_LENGTH];

        NtlmContext(SQLServerConnection con, String domainName, String workstation) throws NoSuchAlgorithmException {
            this.domainName = domainName.toUpperCase();
            this.domainBytes = domainName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            this.userName = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString());
            this.userBytes = userName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            password = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString());
            this.workstation = workstation;
            this.workstationBytes = workstation.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            mac = Mac.getInstance("HmacMD5");
        }
    };

    private NtlmContext context = null;

    /*
     * Creates an instance of the NTLM authentication
     * @param con - connection
     * @param domainName - domain name to connect to
     */
    NTLMAuthentication(SQLServerConnection con, String domainName, String workstation) throws SQLServerException {
        try {
            if (context == null) {
                this.context = new NtlmContext(con, domainName, workstation);
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
    private void getNtlmChallenge(byte[] inToken) throws SQLServerException {

        // TODO: verify Challenge message fields

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

        context.token.getLong(); /// version - not used

        // get requested target name
        // TODO: verify
        byte[] targetName = new byte[targetNameLen];
        context.token.get(targetName);

        // if targetInfo was requested, should always be sent
        // TODO: verify targetInfo av pairs
        // TODO: check MsAvTimeStamp and add MIC
        context.targetInfo = new byte[targetInfoLen];
        context.token.get(context.targetInfo);
    }

    /*
     * Initializes a security context
     * @param inToken - SSPI input blob
     * @param done - indicates processing is done
     */
    private byte[] initializeSecurityContext(byte[] inToken, boolean[] done) throws SQLServerException {
        if (inToken.length == 0) {
            return getNtlmNegotiateMsg();
        } else {
            getNtlmChallenge(inToken);
            done[0] = true;
            return getNtlmAuthenticateMsg();
        }
    }

    /*
     * Get LM Challenge response to server challenge
     * @param clientNonce - client challenge nonce
     */
    private byte[] getLmChallengeResp(
            byte[] clientNonce) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {

        byte[] respKey = lmowfv2(context.password);
        return computeResponse(respKey, clientNonce);
    }

    /*
     * Get NTLMv2 Client Challenge
     * @param clientNonce - client challenge nonce
     */
    private byte[] getClientChallenge(byte[] clientNonce) {

        // timestamp is number of 100 nanosecond ticks since Epoch
        ByteBuffer time = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        time.putLong(TimeUnit.SECONDS.toNanos(Instant.now().getEpochSecond()) / 100);
        byte[] timestamp = time.array();

        context.token = ByteBuffer
                .allocate(NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE.length + NTLM_CLIENT_CHALLENGE_RESERVED1.length
                        + NTLM_CLIENT_CHALLENGE_RESERVED2.length + timestamp.length + NTLM_CLIENT_NONCE_LENGTH
                        + NTLM_CLIENT_CHALLENGE_RESERVED3.length + context.targetInfo.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        context.token.put(NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE);
        context.token.put(NTLM_CLIENT_CHALLENGE_RESERVED1);
        context.token.put(NTLM_CLIENT_CHALLENGE_RESERVED2);

        context.token.put(timestamp, 0, timestamp.length);
        context.token.put(clientNonce, 0, NTLM_CLIENT_NONCE_LENGTH);
        context.token.put(NTLM_CLIENT_CHALLENGE_RESERVED3);

        context.token.put(context.targetInfo, 0, context.targetInfo.length);

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
     * @param password - password to obtain key of
     */
    private byte[] ntowfv2(
            String password) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {

        return hmacMD5(MD4(unicode(password)), unicode(context.userName.toUpperCase() + context.domainName));
    }

    /*
     * Generate key from password to get LM hash
     * @param password - password to obtain hash of
     */
    private byte[] lmowfv2(
            String password) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {

        return ntowfv2(password);
    }

    /*
     * Compute challenge response
     * @param key - hash key
     * @param clientChallenge - client challenge
     */
    private byte[] computeResponse(byte[] key,
            byte[] clientChallenge) throws NoSuchAlgorithmException, InvalidKeyException {

        int len = clientChallenge.length;

        // concatenate client and server challenge
        byte[] temp = new byte[NTLM_SERVER_CHALLENGE_LENGTH + len];
        System.arraycopy(context.serverChallenge, 0, temp, 0, NTLM_SERVER_CHALLENGE_LENGTH);
        System.arraycopy(clientChallenge, 0, temp, NTLM_SERVER_CHALLENGE_LENGTH, len);

        byte[] ntProofStr = hmacMD5(key, temp);
        int ntProofStrLen = ntProofStr.length;

        byte[] ntlmv2resp = new byte[ntProofStr.length + len];
        System.arraycopy(ntProofStr, 0, ntlmv2resp, 0, ntProofStrLen);
        System.arraycopy(clientChallenge, 0, ntlmv2resp, ntProofStrLen, len);
        return ntlmv2resp;
    }

    /*
     * Get NT Challenge response to server challenge
     * @param clientNonce - client challenge nonce
     */
    private byte[] getNtChallengeResp(
            byte[] clientNonce) throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
        return computeResponse(ntowfv2(context.password), getClientChallenge(clientNonce));
    }

    /*
     * Get NTLM Authentication message
     */
    private byte[] getNtlmAuthenticateMsg() throws SQLServerException {
        int domainNameLen = context.domainBytes.length * 2;
        int userNameLen = context.userBytes.length * 2;
        int workstationLen = context.workstationBytes.length * 2;
        byte[] clientNonce = new byte[NTLM_CLIENT_NONCE_LENGTH];

        try {
            // get random client challenge nonce
            SecureRandom.getInstanceStrong().nextBytes(clientNonce);

            byte[] lmChallengeResp = getLmChallengeResp(clientNonce);
            byte[] ntChallengeResp = getNtChallengeResp(clientNonce);

            context.token = ByteBuffer.allocate(NTLM_AUTHENTICATE_PAYLOAD_OFFSET + domainNameLen + userNameLen
                    + workstationLen + lmChallengeResp.length + ntChallengeResp.length).order(ByteOrder.LITTLE_ENDIAN);

            // set NTLM signature and message type
            context.token.put(NTLM_HEADER_SIGNATURE, 0, NTLM_HEADER_SIGNATURE.length);
            context.token.putInt(NTLM_MESSAGE_TYPE_AUTHENTICATE);

            // LM challenge response
            int len = lmChallengeResp.length;
            int offset = NTLM_AUTHENTICATE_PAYLOAD_OFFSET + domainNameLen + userNameLen + workstationLen;
            context.token.putShort((short) len);
            context.token.putShort((short) len);
            context.token.putInt(offset);
            offset += lmChallengeResp.length;

            // NT challenge response
            len = ntChallengeResp.length;
            context.token.putShort((short) len);
            context.token.putShort((short) len);
            context.token.putInt(offset);

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

            // same negotiate flags sent before
            context.token.putInt((int) (NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED
                    | NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED | NTLMSSP_REQUEST_TARGET | NTLMSSP_NEGOTIATE_UNICODE));

            // version not requested

            // TODO - add MIC - 16 bytes

            // payload
            context.token.put(unicode(context.domainName), 0, domainNameLen);
            context.token.put(unicode(context.userName), 0, userNameLen);
            context.token.put(unicode(context.workstation), 0, workstationLen);

            context.token.put(lmChallengeResp, 0, lmChallengeResp.length);
            context.token.put(ntChallengeResp, 0, ntChallengeResp.length);

        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ntlmAuthError"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(form.format(msgArgs), e);
        }
        return context.token.array();
    }

    /*
     * Get NTLM Negotiate message
     */
    private byte[] getNtlmNegotiateMsg() throws SQLServerException {
        context.token = ByteBuffer
                .allocate(NTLM_NEGOTIATE_PAYLOAD_OFFSET + context.domainBytes.length + context.workstationBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        // signature and message type
        context.token.put(NTLM_HEADER_SIGNATURE, 0, NTLM_HEADER_SIGNATURE.length);
        context.token.putInt(NTLM_MESSAGE_TYPE_NEGOTIATE);

        // NTLM negotiate flags - only NTLMV2 supported
        context.token.putInt((int) (NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED | NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED
                | NTLMSSP_REQUEST_TARGET | NTLMSSP_NEGOTIATE_UNICODE));

        // domain name fields
        int len = context.domainBytes.length;
        int offset = NTLM_NEGOTIATE_PAYLOAD_OFFSET;
        context.token.putShort((short) len);
        context.token.putShort((short) len);
        context.token.putInt(offset);
        // offset += len;

        // workstation field
        len = context.workstationBytes.length;
        context.token.putShort((short) len);
        context.token.putShort((short) len);
        context.token.putInt(offset);
        offset += len;

        // version - not used

        // payload
        context.token.put(context.domainBytes, 0, context.domainBytes.length);
        context.token.put(context.workstationBytes, 0, context.workstationBytes.length);

        return context.token.array();
    }
}
