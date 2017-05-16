/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;

/**
 * 
 * This class is used to handle exceptions that may be recieved from sqljdbc_auth.dll and sqljdbc_xa.dll
 *
 */
class DLLException extends Exception {
    private static final long serialVersionUID = -4498171382218222079L;
    // category status and state are always either -1 or a positive number
    // Internal Adal error category used in retry logic and building error message in managed code
    private int category = -9;
    // Public facing failing status returned from Adal APIs in SNISecADALGetAccessToken
    private int status = -9;
    // Internal last Adal API called in SNISecADALGetAccessToken for troubleshooting
    private int state = -9;

    // Internal error code used to choose which error message to print
    private int errCode = -1; // any value that is not assigned to an error
    // Parameters used to build error messages from auth dll
    private String param1 = "";
    private String param2 = "";
    private String param3 = "";

    DLLException(String message,
            int category,
            int status,
            int state) {
        super(message);
        this.category = category;
        this.status = status;
        this.state = state;
    }

    DLLException(String param1,
            String param2,
            String param3,
            int errCode) {
        this.errCode = errCode;
        this.param1 = param1;
        this.param2 = param2;
        this.param3 = param3;
    }

    int GetCategory() {
        return this.category;
    }

    int GetStatus() {
        return this.status;
    }

    int GetState() {
        return this.state;
    }

    int GetErrCode() {
        return this.errCode;
    }

    String GetParam1() {
        return this.param1;
    }

    String GetParam2() {
        return this.param2;
    }

    String GetParam3() {
        return this.param3;
    }

    static void buildException(int errCode,
            String param1,
            String param2,
            String param3) throws SQLServerException {

        String errMessage = getErrMessage(errCode);
        MessageFormat form = new MessageFormat(SQLServerException.getErrString(errMessage));

        String[] msgArgs = buildMsgParams(errMessage, param1, param2, param3);

        throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
    }

    private static String[] buildMsgParams(String errMessage,
            String parameter1,
            String parameter2,
            String parameter3) {

        String[] msgArgs = new String[3];

        if ("R_AECertLocBad".equalsIgnoreCase(errMessage)) {
            msgArgs[0] = parameter1;
            msgArgs[1] = parameter1 + "/" + parameter2 + "/" + parameter3;
        }
        else if ("R_AECertStoreBad".equalsIgnoreCase(errMessage)) {
            msgArgs[0] = parameter2;
            msgArgs[1] = parameter1 + "/" + parameter2 + "/" + parameter3;
        }
        else if ("R_AECertHashEmpty".equalsIgnoreCase(errMessage)) {
            msgArgs[0] = parameter1 + "/" + parameter2 + "/" + parameter3;
        }
        else {
            msgArgs[0] = parameter1;
            msgArgs[1] = parameter2;
            msgArgs[2] = parameter3;
        }

        return msgArgs;
    }

    private static String getErrMessage(int errCode) {
        String message;
        switch (errCode) {
            case 1:
                message = "R_AEKeypathEmpty";
                break;
            case 2:
                message = "R_EncryptedCEKNull";
                break;
            case 3:
                message = "R_NullKeyEncryptionAlgorithm";
                break;
            case 4:
                message = "R_AEWinApiErr";
                break;
            case 5:
                message = "R_AECertpathBad";
                break;
            case 6:
                message = "R_AECertLocBad";
                break;
            case 7:
                message = "R_AECertStoreBad";
                break;
            case 8:
                message = "R_AECertHashEmpty";
                break;
            case 9:
                message = "R_AECertNotFound";
                break;
            case 10:
                message = "R_AEMaloc";
                break;
            case 11:
                message = "R_EmptyEncryptedCEK";
                break;
            case 12:
                message = "R_InvalidKeyEncryptionAlgorithm";
                break;
            case 13:
                message = "R_AEKeypathLong";
                break;
            case 14:
                message = "R_InvalidEcryptionAlgorithmVersion";
                break;
            case 15:
                message = "R_AEECEKLenBad";
                break;
            case 16:
                message = "R_AEECEKSigLenBad";
                break;
            case 17:
                message = "R_InvalidCertificateSignature";
                break;
            default:
                message = "R_AEWinApiErr";
                break;

        }
        return message;
    }
}
