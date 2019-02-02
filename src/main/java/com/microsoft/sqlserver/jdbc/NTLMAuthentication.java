/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


final class NTLMAuthentication extends SSPIAuthentication {
    private final static java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.NTLMAuthentication");

    private final SQLServerConnection con;
    private final String domainName;
    private final byte[] domainBytes;
    private final String userName;
    private final String password;
    private final byte[] userBytes;

    private boolean ntlmNegotiated = false;
    private static byte[] targetInfo = null;
    private byte[] nonce = new byte[TDS.NTLM_SERVER_CHALLENGE_LENGTH]; // server challenge

    // get Ntlm Challenge message from server
    private void getNtlmChallenge(byte[] inToken) {
        ByteBuffer buf = ByteBuffer.wrap(inToken).order(ByteOrder.LITTLE_ENDIAN);
       // ByteBuffer buf = ByteBuffer.wrap(inToken);
        
        byte[] signature = new byte[TDS.NTLM_HEADER_SIGNATURE.length];
        buf.get(signature);
        assert (signature.equals(TDS.NTLM_HEADER_SIGNATURE));

        int messageType = buf.getInt();
        assert (messageType == TDS.NTLM_MESSAGE_TYPE_CHALLENGE);

        int targetNameLen = buf.getShort();
        buf.getShort(); // targetNameMaxLen
        buf.getInt(); // targetNameOffset
        
        buf.getInt();
        // negotiate flags
     //  byte[] flags = new byte[4];
     //  buf.get(flags, 0, 4);
        /*
byte flag1 = buf.get();
byte flag2 = buf.get();
byte flag3 = buf.get();
byte flag4 = buf.get();

// 1 2 3 4
// 3 4 1 2
// -119 -94 37-126

if (((flag4 & 0xff) & (TDS.NTLM_NEGOTIATE_EXTENDED_SESSIONSECURITY & 0xff)) == 1) {
    System.out.println("die NTLM v2 not supported");
}

// 1110 0111
// 0010 0000
int f = flag3 & 0xff;
int s = (int)(TDS.NTLM_NEGOTIATE_128 >> 24);

if (((flag3 & 0xff) & (int)(TDS.NTLM_NEGOTIATE_128 >> 24)) == 0) {
    System.out.println("128 bit encryption not supported");
}
if (((flag3 & 0xff) & (int)(TDS.NTLM_NEGOTIATE_56 >> 24)) == 0) {
    System.out.println("56 bit encryption not supported");
}

       /*
        
       // P
        if ((flags[1] & TDS.NTLM_NEGOTIATE_EXTENDED_SESSIONSECURITY) == 1) {
            System.out.println("die NTLM v2 not supported");
        }
        // U
        if ((flags[0] & TDS.NTLM_NEGOTIATE_128) != 1) {
            System.out.println("128 bit encryption not supported");
        }
        // W
        if ((flags[0] & TDS.NTLM_NEGOTIATE_56) != 1) {
            System.out.println("56 bit encryption not supported");
        }
        // E
        if ((flags[3] & TDS.NTLM_NEGOTIATE_SEAL) != 1) {
            System.out.println("seal not supported");
        }
        */
        // check flags!
        // 0010 1000 1001 1000 0010 0000 0101
/*
        // ntlm v2 not supported
        if ((flags[0] & TDS.NTLM_NEGOTIATE_EXTENDED_SESSIONSECURITY) == 1) {
            System.out.println("die NTLM v2 not supported");
        }
        
        if ((flags[0] & TDS.NTLM_NEGOTIATE_128) != 1) {
            System.out.println("128 bit encryption not supported");
        }
        if ((flags[0] & TDS.NTLM_NEGOTIATE_56) != 1) {
            System.out.println("56 bit encryption not supported");
        }
        if ((flags[3] & TDS.NTLM_NEGOTIATE_SEAL) != 1) {
            System.out.println("seal not supported");
        }
        /*
        byte[] array1 = buf.order(ByteOrder.LITTLE_ENDIAN).array();
        byte[] array2 = buf.order(ByteOrder.BIG_ENDIAN).array();
        int flags = buf.order(ByteOrder.LITTLE_ENDIAN).getInt();
        if ((flags & TDS.NTLM_NEGOTIATE_128) != 1) {
            System.out.println("128 bit encryption not supported");
        }
        if ((flags & TDS.NTLM_NEGOTIATE_56) != 1) {
            System.out.println("56 bit encryption not supported");
        }
        if ((flags & TDS.NTLM_NEGOTIATE_SEAL) != 1) {
            System.out.println("seal not supported");
        }
        */
        
        // server challenge
        buf.get(nonce);

        // 8 bytes reserved 32-40: all 0's
        buf.getLong();

        int targetInfoLen = buf.getShort();
        buf.getShort(); // targetInfoMaxLen
        buf.getInt(); // targetInfoOffset

        buf.getLong(); /// version - not used

        // payload
        byte[] targetName = new byte[targetNameLen];
        buf.get(targetName);
        if (targetNameLen > 0) {
            assert (targetName.length > 0);
        }

        targetInfo = new byte[targetInfoLen];
        buf.get(targetInfo);
        // targetInfo was requested, should always be sent
        assert (targetInfoLen > 0 && targetInfo.length > 0);

        // need to verify targetInfo fields? av_pair
        // check Avid MsvAvTimeStamp and MsvAvFlags?

    }

    NTLMAuthentication(SQLServerConnection con, String domainName) throws SQLServerException {
        this.con = con;
        this.domainName = domainName.toUpperCase();
        domainBytes = domainName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        userName = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString());
        userBytes = userName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        password = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString());
    }

    @Override
    byte[] GenerateClientContext(byte[] pin, boolean[] done) throws SQLServerException {
        if (!ntlmNegotiated) {
            return sendNtlmNegotiateMsg();
        } else {
            getNtlmChallenge(pin);
            return sendNtlmAuthenticateMsg();
        }
    }

    byte[] getLmChallengeResp(byte[] clientNonce) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        /*
         * byte[] resp = new byte[0]; Arrays.fill(resp, (byte) 0); return resp;
         */
        byte[] respKey = lmowfv2(password);
        return computeResponse(respKey, clientNonce);
    }

    private static byte[] getClientChallenge(byte[] clientNonce) {
        // 100 nanoseconds
        long now = TimeUnit.SECONDS.toNanos(Instant.now().getEpochSecond()) / 100;
        // little-endian time
        ByteBuffer time = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        time.putLong(now);
        byte[] timestamp = time.array();

        // byte[] unknown2 = new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};

        ByteBuffer buf = ByteBuffer.allocate(TDS.NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE.length
                + TDS.NTLM_CLIENT_CHALLENGE_RESERVED1.length + TDS.NTLM_CLIENT_CHALLENGE_RESERVED2.length
                + timestamp.length + TDS.NTLM_CLIENT_CHALLENGE_LENGTH + TDS.NTLM_CLIENT_CHALLENGE_RESERVED3.length
                + targetInfo.length).order(ByteOrder.LITTLE_ENDIAN);

        buf.put(TDS.NTLM_CLIENT_CHALLENGE_RESPONSE_TYPE);
        buf.put(TDS.NTLM_CLIENT_CHALLENGE_RESERVED1);
        buf.put(TDS.NTLM_CLIENT_CHALLENGE_RESERVED2);

        buf.put(timestamp, 0, timestamp.length);
        buf.put(clientNonce, 0, TDS.NTLM_CLIENT_CHALLENGE_LENGTH);
        buf.put(TDS.NTLM_CLIENT_CHALLENGE_RESERVED3);

        buf.put(targetInfo, 0, targetInfo.length);
        // buf.put(unknown2, 0, unknown2.length); // what is this?? don't need it!
        return buf.array();
    }

    // hmacMD5
    // https://www.ietf.org/rfc/rfc2104.txt
    private byte[] hmacMD5(byte[] key, byte[] data) {
        Mac mac = null;
        try {
            mac = Mac.getInstance("HmacMD5");
            SecretKeySpec keySpec = new SecretKeySpec(key, "HmacMD5");
            mac.init(keySpec);
        } catch (Exception e) {
            System.out.println("mac exception: " + e.getMessage());
        }
        return mac.doFinal(data);
    }

    // private static byte[] MD4(String str) throws UnsupportedEncodingException, NoSuchAlgorithmException {
    // byte[] array = str.getBytes(UTF_16LE);
    private static byte[] MD4(byte[] array) throws UnsupportedEncodingException, NoSuchAlgorithmException {

        MD4 md = new MD4();
        md.reset();
        md.update(array);
        return md.digest();
    }

    private byte[] unicode(String str) {
        return str.getBytes(UTF_16LE);
    }
    
    // generate key from user password
    private byte[] ntowfv2(String password) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        /*
         * byte[] passwordHash = null; try { passwordHash = MD4(password); } catch (Exception e) {
         * System.out.println("MD5 exception: " + e.getMessage()); } return hmacMD5(passwordHash,
         * (userName.toUpperCase() + domainName).getBytes(UTF_16LE));
         */
        return hmacMD5(MD4(unicode(password)), unicode(userName.toUpperCase() + domainName));
    }
    // generate key from user password
    private byte[] lmowfv2(String password) throws UnsupportedEncodingException, NoSuchAlgorithmException {
       
        return ntowfv2(password);
    }

    private byte[] computeResponse(byte[] key, byte[] clientChallenge) throws NoSuchAlgorithmException {

        int clientChallengeLen = clientChallenge.length;

        // concatenate client and server challenge
        byte[] temp = new byte[TDS.NTLM_SERVER_CHALLENGE_LENGTH + clientChallengeLen];
        System.arraycopy(nonce, 0, temp, 0, TDS.NTLM_SERVER_CHALLENGE_LENGTH);
        System.arraycopy(clientChallenge, 0, temp, TDS.NTLM_SERVER_CHALLENGE_LENGTH, clientChallengeLen);

        byte[] ntProofStr = hmacMD5(key, temp);
        int ntProofStrLen = ntProofStr.length;

        byte[] ntlmv2resp = new byte[ntProofStr.length + clientChallengeLen];
        System.arraycopy(ntProofStr, 0, ntlmv2resp, 0, ntProofStrLen);
        System.arraycopy(clientChallenge, 0, ntlmv2resp, ntProofStrLen, clientChallengeLen);
        return ntlmv2resp;
    }

    private byte[] getNtChallengeResp(byte[] clientNonce) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        byte[] respKey = ntowfv2(password);
        
        byte[] clientChallenge = getClientChallenge(clientNonce);
        return computeResponse(respKey, clientChallenge);
    }

    private byte[] sendNtlmAuthenticateMsg() throws SQLServerException {
        int domainNameLen = domainBytes.length * 2;
        int userNameLen = userBytes.length * 2;
        ByteBuffer buf = null;

        // get challenge resps
        try {
            // need to support anonymous authentication? userName = "" and password = ""
            
            // random client challenge nonce
            byte[] clientNonce = new byte[8];
           // new Random().nextBytes(clientNonce);
            SecureRandom.getInstanceStrong().nextBytes(clientNonce);
            
            byte[] lmChallengeResp = getLmChallengeResp(clientNonce);
            byte[] ntChallengeResp = getNtChallengeResp(clientNonce);

            buf = ByteBuffer.allocate(TDS.NTLM_AUTHENTICATE_MESSAGE_BUFFER_OFFSET + domainNameLen + userNameLen
                    + lmChallengeResp.length + ntChallengeResp.length).order(ByteOrder.LITTLE_ENDIAN);

            int len;
            int bufferOffset = 0;
            // System.out.println("offset: "+ TDS.NTLM_HEADER_SIGNATURE.length+4)
            // signature
            buf.put(TDS.NTLM_HEADER_SIGNATURE, 0, TDS.NTLM_HEADER_SIGNATURE.length);

            // message type
            buf.putInt(TDS.NTLM_MESSAGE_TYPE_AUTHENTICATE);

            // Lm challenge response
            len = lmChallengeResp.length;
            bufferOffset = TDS.NTLM_AUTHENTICATE_MESSAGE_BUFFER_OFFSET + domainNameLen + userNameLen + 0; // host name 0
            buf.putShort((short) len);
            buf.putShort((short) len);
            buf.putInt(bufferOffset);
            bufferOffset += lmChallengeResp.length;

            // Nt challenge response
            len = ntChallengeResp.length;
            buf.putShort((short) len);
            buf.putShort((short) len);
            buf.putInt(bufferOffset);

            // 8 (signature) + 4 (message type)+ 8 (lmChallengeResp) + 8 (ntChallengeResp) + 8 (domain name) + 8 (user
            // name) + 0 (workstation) + 0 (encrypted random session key) + 4 (negotiate flags) + 0 (version) + 16 (mic)

            bufferOffset = TDS.NTLM_AUTHENTICATE_MESSAGE_BUFFER_OFFSET;

            // domain name fields
            len = domainBytes.length * 2;
            buf.putShort((short) len);
            buf.putShort((short) len);
            buf.putInt(bufferOffset);
            bufferOffset += domainNameLen;

            // user name fields
            len = userBytes.length * 2;
            buf.putShort((short) len);
            buf.putShort((short) len);
            buf.putInt(bufferOffset);
            bufferOffset += userNameLen;

            // workstation fields
            len = 0; // do not send
            buf.putShort((short) len);
            buf.putShort((short) len);
            buf.putInt(bufferOffset);
            // bufferOffset += 0;

            // encrypted random session key fields
            len = 0; // do not send
            buf.putShort((short) len);
            buf.putShort((short) len);
            buf.putInt(bufferOffset);
            // bufferOffset += 0;

            // negotiate flags
       //      buf.putInt(0x88201); // 1000 1000 0010 0000 0001 jtds

            buf.putInt((int) (TDS.NTLM_NEGOTIATE_EXTENDED_SESSIONSECURITY | TDS.NTLM_NEGOTIATE_ALWAYS_SIGN
                    | TDS.NTLM_NEGOTIATE_OEM_DOMAIN_SUPPLIED | TDS.NTLM_NEGOTIATE_UNICODE));

            // version not requested

            // MIC - 16 bytes "omitted"

            // payload
            buf.put(unicode(domainName), 0, domainNameLen);
            buf.put(unicode(userName), 0, userNameLen);
            buf.put(lmChallengeResp, 0, lmChallengeResp.length);
            buf.put(ntChallengeResp, 0, ntChallengeResp.length);

        } catch (Exception e) {
            System.out.println("caught exception: " + e.getMessage());
        }
        return buf.array();
    }

    private byte[] sendNtlmNegotiateMsg() throws SQLServerException {
        int len = TDS.NTLM_NEGOTIATE_MESSAGE_BUFFER_OFFSET + domainBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(TDS.NTLM_NEGOTIATE_MESSAGE_BUFFER_OFFSET + domainBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        // signature
        buf.put(TDS.NTLM_HEADER_SIGNATURE, 0, TDS.NTLM_HEADER_SIGNATURE.length);

        // message type
        buf.putInt(TDS.NTLM_MESSAGE_TYPE_NEGOTIATE);

        // negotiate flags - only NTLMV2 supported
        buf.putInt((int) (TDS.NTLM_NEGOTIATE_128 | TDS.NTLM_NEGOTIATE_56 |TDS.NTLM_NEGOTIATE_SEAL | TDS.NTLM_NEGOTIATE_EXTENDED_SESSIONSECURITY | TDS.NTLM_NEGOTIATE_ALWAYS_SIGN
                | TDS.NTLM_NEGOTIATE_OEM_WORKSTATION_SUPPLIED | TDS.NTLM_NEGOTIATE_OEM_DOMAIN_SUPPLIED
                | TDS.NTLM_REQUEST_TARGET | TDS.NTLM_NEGOTIATE_UNICODE));

        // 0x89005 1000 1001 0000 0000 0101
        // jtds: 0x8b205 1000 1011 0010 0101

        // domain name fields
        len = domainBytes.length;
        buf.putShort((short) len);
        buf.putShort((short) len);
        buf.putInt((short) TDS.NTLM_NEGOTIATE_MESSAGE_BUFFER_OFFSET);

        // workstation fields
        len = 0;
        buf.putShort((short) len);
        buf.putShort((short) len);
        buf.putInt((short) TDS.NTLM_NEGOTIATE_MESSAGE_BUFFER_OFFSET);

        // payload
        buf.put(domainBytes, 0, domainBytes.length);

        ntlmNegotiated = true;
        return buf.array();
    }

    @Override
    int ReleaseClientContext() throws SQLServerException {
        // TODO Auto-generated method stub
        return 0;
    }
}
