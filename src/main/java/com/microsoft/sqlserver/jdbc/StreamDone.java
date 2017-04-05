/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamDone/StreamDoneInProc/StreamDoneProc stores a TDS Done packet that denotes the completion of a database operation.
 */

class StreamDone extends StreamPacket {
    /** the done completion status */
    private short status;
    /** the row update count */
    private long rowCount;

    /**
     * the current command (See Appendix A of TDS spec)
     */
    static final short CMD_SELECT = 0xC1;
    static final short CMD_SELECTINTO = 0xC2;
    static final short CMD_INSERT = 0xC3;
    static final short CMD_DELETE = 0xC4;
    static final short CMD_UPDATE = 0xC5;
    static final short CMD_EXECUTE = 0xE0;
    static final short CMD_BULKINSERT = 0xF0;
    static final short CMD_MERGE = 0x117;

    // DDL commands
    static final short CMD_CNST_CREATE = 0x2e;
    static final short CMD_DENY = 0x99;
    static final short CMD_DROPSCHEMA = 0xb0;
    static final short CMD_FUNCCREATE = 0xb2;
    static final short CMD_FUNCDESTROY = 0xb3;
    static final short CMD_ASMCREATE = 0xb5;
    static final short CMD_CMD = 0xb6;
    static final short CMD_TABCREATE = 0xc6;
    static final short CMD_TABDESTROY = 0xc7;
    static final short CMD_INDCREATE = 0xc8;
    static final short CMD_INDDESTROY = 0xc9;
    static final short CMD_DBCREATE = 0xcb;
    static final short CMD_DBDESTROY = 0xcc;
    static final short CMD_GRANT = 0xcd;
    static final short CMD_REVOKE = 0xce;
    static final short CMD_VIEWCREATE = 0xcf;
    static final short CMD_VIEWDESTROY = 0xd0;
    static final short CMD_DBEXTEND = 0xd7;
    static final short CMD_ALTERTAB = 0xd8;
    static final short CMD_TRIGCREATE = 0xdd;
    static final short CMD_PROCCREATE = 0xde;
    static final short CMD_PROCDESTROY = 0xdf;
    static final short CMD_TRIGDESTROY = 0xe1;
    static final short CMD_DBCC_CMD = 0xe6;
    static final short CMD_DEFAULTCREATE = 0xe9;
    static final short CMD_RULECREATE = 0xec;
    static final short CMD_RULEDESTROY = 0xed;
    static final short CMD_DEFAULTDESTROY = 0xee;
    static final short CMD_STATSDESTROY = 0x100;
    static final short CMD_ASMDESTROY = 0x10e;
    static final short CMD_ASMALTER = 0x10f;
    static final short CMD_TYPEDESTROY = 0x110;
    static final short CMD_TYPECREATE = 0x111;
    static final short CMD_CLRPROCEDURECREATE = 0x112;
    static final short CMD_CLRFUNCTIONCREATE = 0x113;
    static final short CMD_SERVICEALTER = 0x114;
    static final short CMD_MSGTYPECREATE = 0x115;
    static final short CMD_MSGTYPEDESTROY = 0x116;
    static final short CMD_CONTRACTCREATE = 0x119;
    static final short CMD_CONTRACTDESTROY = 0x11a;
    static final short CMD_SERVICECREATE = 0x11b;
    static final short CMD_SERVICEDESTROY = 0x11c;
    static final short CMD_QUEUECREATE = 0x11d;
    static final short CMD_QUEUEDESTROY = 0x11e;
    static final short CMD_QUEUEALTER = 0x11f;
    static final short CMD_FTXTINDEX_CREATE = 0x126;
    static final short CMD_FTXTINDEX_ALTER = 0x127;
    static final short CMD_FTXTINDEX_DROP = 0x128;
    static final short CMD_PRTFUNCTIONCREATE = 0x129;
    static final short CMD_PRTFUNCTIONDROP = 0x12a;
    static final short CMD_PRTSCHEMECREATE = 0x12b;
    static final short CMD_PRTSCHEMEDROP = 0x12c;
    static final short CMD_FTXTCATALOG_CREATE = 0x130;
    static final short CMD_FTXTCATALOG_ALTER = 0x131;
    static final short CMD_FTXTCATALOG_DROP = 0x132;
    static final short CMD_XMLSCHEMACREATE = 0x135;
    static final short CMD_XMLSCHEMAALTER = 0x136;
    static final short CMD_XMLSCHEMADROP = 0x137;
    static final short CMD_ENDPOINTCREATE = 0x138;
    static final short CMD_ENDPOINTALTER = 0x139;
    static final short CMD_ENDPOINTDROP = 0x13a;
    static final short CMD_USERCREATE = 0x13b;
    static final short CMD_USERALTER = 0x13c;
    static final short CMD_USERDROP = 0x13d;
    static final short CMD_ROLECREATE = 0x13f;
    static final short CMD_ROLEALTER = 0x140;
    static final short CMD_ROLEDROP = 0x141;
    static final short CMD_APPROLECREATE = 0x142;
    static final short CMD_APPROLEALTER = 0x143;
    static final short CMD_APPROLEDROP = 0x144;
    static final short CMD_LOGINCREATE = 0x145;
    static final short CMD_LOGINALTER = 0x146;
    static final short CMD_LOGINDROP = 0x147;
    static final short CMD_SYNONYMCREATE = 0x148;
    static final short CMD_SYNONYMDROP = 0x149;
    static final short CMD_CREATESCHEMA = 0x14a;
    static final short CMD_ALTERSCHEMA = 0x14b;
    static final short CMD_AGGCREATE = 0x14c;
    static final short CMD_AGGDESTROY = 0x14d;
    static final short CMD_CLRTRIGGERCREATE = 0x14e;
    static final short CMD_PRTFUNCTIONALTER = 0x14f;
    static final short CMD_PRTSCHEMEALTER = 0x150;
    static final short CMD_INDALTER = 0x151;
    static final short CMD_ROUTECREATE = 0x157;
    static final short CMD_ROUTEALTER = 0x158;
    static final short CMD_ROUTEDESTROY = 0x15a;
    static final short CMD_EVENTNOTIFICATIONCREATE = 0x160;
    static final short CMD_EVENTNOTIFICATIONDROP = 0x161;
    static final short CMD_XMLINDEXCREATE = 0x162;
    static final short CMD_BINDINGCREATE = 0x166;
    static final short CMD_BINDINGALTER = 0x167;
    static final short CMD_BINDINGDESTROY = 0x168;
    static final short CMD_MSGTYPEALTER = 0x16e;
    static final short CMD_CERTCREATE = 0x170;
    static final short CMD_CERTDROP = 0x171;
    static final short CMD_CERTALTER = 0x172;
    static final short CMD_SECDESCCREATE = 0x17d;
    static final short CMD_SECDESCDROP = 0x17e;
    static final short CMD_SECDESCALTER = 0x17f;
    static final short CMD_OBFUSKEYCREATE = 0x182;
    static final short CMD_OBFUSKEYALTER = 0x183;
    static final short CMD_OBFUSKEYDROP = 0x184;
    static final short CMD_ALTERAUTHORIZATION = 0x18c;
    static final short CMD_CREDENTIALCREATE = 0x198;
    static final short CMD_CREDENTIALALTER = 0x199;
    static final short CMD_CREDENTIALDROP = 0x19a;
    static final short CMD_MASTERKEYCREATE = 0x19b;
    static final short CMD_MASTERKEYDROP = 0x19c;
    static final short CMD_MASTERKEYALTER = 0x1a1;
    static final short CMD_ASYMKEYCREATE = 0x1a3;
    static final short CMD_ASYMKEYDROP = 0x1a4;
    static final short CMD_ASYMKEYALTER = 0x1a9;

    private short curCmd;

    /**
     * Set the packet contents
     */
    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        packetType = tdsReader.readUnsignedByte(); // token type
        assert TDS.TDS_DONE == packetType || TDS.TDS_DONEPROC == packetType || TDS.TDS_DONEINPROC == packetType;

        status = tdsReader.readShort();
        curCmd = tdsReader.readShort();
        rowCount = tdsReader.readLong();

        if (isAttnAck())
            tdsReader.getCommand().onAttentionAck();
    }

    /**
     * Return the packet's current command
     */
    /* L0 */ final short getCurCmd() {
        return curCmd;
    }

    /**
     * Check if this done packet is the final done packet (top nesting)
     * 
     * @return true if final
     */
    /* L0 */ final boolean isFinal() {
        return (status & 0x0001) == 0;
    }

    /**
     * Check if a error state was indicated.
     * 
     * @return true if error
     */
    /* L0 */ final boolean isError() {
        return (status & 0x0002) != 0;
    }

    /**
     * Determine if a done packet has a row count
     * 
     * @return true if the row count is present
     */
    /* L0 */ final boolean updateCountIsValid() {
        return (status & 0x0010) != 0;
    }

    /**
     * Check if a cancelled state was indicated.
     * 
     * @return true if cancelled
     */
    /* L0 */ final boolean isAttnAck() {
        return (status & 0x0020) != 0;
    }

    /**
     * Check if a RPC in batch was indicated
     * 
     * @return true if RPC in batch
     */
    /* L0 */ final boolean wasRPCInBatch() {
        return (status & 0x0080) != 0;
    }

    /**
     * Return the update count
     */
    final long getUpdateCount() {
        assert cmdIsDMLOrDDL();

        switch (curCmd) {
            case CMD_INSERT:
            case CMD_BULKINSERT:
            case CMD_DELETE:
            case CMD_UPDATE:
            case CMD_MERGE:
            case CMD_SELECTINTO:
                return updateCountIsValid() ? rowCount : -1;

            default: // DDL assumed
                return 0;
        }
    }

    final boolean cmdIsDMLOrDDL() {
        switch (curCmd) {
            case CMD_INSERT:
            case CMD_BULKINSERT:
            case CMD_DELETE:
            case CMD_UPDATE:
            case CMD_MERGE:
            case CMD_SELECTINTO:

                // DDL
                // Lifted from SQL Server 2005 Books Online at:
                // http://msdn2.microsoft.com/en-us/library/ms180824.aspx
            case CMD_CNST_CREATE:
            case CMD_DENY:
            case CMD_DROPSCHEMA:
            case CMD_FUNCCREATE:
            case CMD_FUNCDESTROY:
            case CMD_ASMCREATE:
            case CMD_CMD:
            case CMD_TABCREATE:
            case CMD_TABDESTROY:
            case CMD_INDCREATE:
            case CMD_INDDESTROY:
            case CMD_DBCREATE:
            case CMD_DBDESTROY:
            case CMD_GRANT:
            case CMD_REVOKE:
            case CMD_VIEWCREATE:
            case CMD_VIEWDESTROY:
            case CMD_DBEXTEND:
            case CMD_ALTERTAB:
            case CMD_TRIGCREATE:
            case CMD_PROCCREATE:
            case CMD_PROCDESTROY:
            case CMD_TRIGDESTROY:
            case CMD_DBCC_CMD:
            case CMD_DEFAULTCREATE:
            case CMD_RULECREATE:
            case CMD_RULEDESTROY:
            case CMD_DEFAULTDESTROY:
            case CMD_STATSDESTROY:
            case CMD_ASMDESTROY:
            case CMD_ASMALTER:
            case CMD_TYPEDESTROY:
            case CMD_TYPECREATE:
            case CMD_CLRPROCEDURECREATE:
            case CMD_CLRFUNCTIONCREATE:
            case CMD_SERVICEALTER:
            case CMD_MSGTYPECREATE:
            case CMD_MSGTYPEDESTROY:
            case CMD_CONTRACTCREATE:
            case CMD_CONTRACTDESTROY:
            case CMD_SERVICECREATE:
            case CMD_SERVICEDESTROY:
            case CMD_QUEUECREATE:
            case CMD_QUEUEDESTROY:
            case CMD_QUEUEALTER:
            case CMD_FTXTINDEX_CREATE:
            case CMD_FTXTINDEX_ALTER:
            case CMD_FTXTINDEX_DROP:
            case CMD_PRTFUNCTIONCREATE:
            case CMD_PRTFUNCTIONDROP:
            case CMD_PRTSCHEMECREATE:
            case CMD_PRTSCHEMEDROP:
            case CMD_FTXTCATALOG_CREATE:
            case CMD_FTXTCATALOG_ALTER:
            case CMD_FTXTCATALOG_DROP:
            case CMD_XMLSCHEMACREATE:
            case CMD_XMLSCHEMAALTER:
            case CMD_XMLSCHEMADROP:
            case CMD_ENDPOINTCREATE:
            case CMD_ENDPOINTALTER:
            case CMD_ENDPOINTDROP:
            case CMD_USERCREATE:
            case CMD_USERALTER:
            case CMD_USERDROP:
            case CMD_ROLECREATE:
            case CMD_ROLEALTER:
            case CMD_ROLEDROP:
            case CMD_APPROLECREATE:
            case CMD_APPROLEALTER:
            case CMD_APPROLEDROP:
            case CMD_LOGINCREATE:
            case CMD_LOGINALTER:
            case CMD_LOGINDROP:
            case CMD_SYNONYMCREATE:
            case CMD_SYNONYMDROP:
            case CMD_CREATESCHEMA:
            case CMD_ALTERSCHEMA:
            case CMD_AGGCREATE:
            case CMD_AGGDESTROY:
            case CMD_CLRTRIGGERCREATE:
            case CMD_PRTFUNCTIONALTER:
            case CMD_PRTSCHEMEALTER:
            case CMD_INDALTER:
            case CMD_ROUTECREATE:
            case CMD_ROUTEALTER:
            case CMD_ROUTEDESTROY:
            case CMD_EVENTNOTIFICATIONCREATE:
            case CMD_EVENTNOTIFICATIONDROP:
            case CMD_XMLINDEXCREATE:
            case CMD_BINDINGCREATE:
            case CMD_BINDINGALTER:
            case CMD_BINDINGDESTROY:
            case CMD_MSGTYPEALTER:
            case CMD_CERTCREATE:
            case CMD_CERTDROP:
            case CMD_CERTALTER:
            case CMD_SECDESCCREATE:
            case CMD_SECDESCDROP:
            case CMD_SECDESCALTER:
            case CMD_OBFUSKEYCREATE:
            case CMD_OBFUSKEYALTER:
            case CMD_OBFUSKEYDROP:
            case CMD_ALTERAUTHORIZATION:
            case CMD_CREDENTIALCREATE:
            case CMD_CREDENTIALALTER:
            case CMD_CREDENTIALDROP:
            case CMD_MASTERKEYCREATE:
            case CMD_MASTERKEYDROP:
            case CMD_MASTERKEYALTER:
            case CMD_ASYMKEYCREATE:
            case CMD_ASYMKEYDROP:
            case CMD_ASYMKEYALTER:
                return true;

            default:
                return false;
        }
    }
}
