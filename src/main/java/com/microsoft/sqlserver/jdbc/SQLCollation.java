/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;


/**
 * SQLCollation is helper class used to read TDS collation from a TDS stream.
 * Collation is in the following BNF format (see TDS spec for full details):
 * 
 * LCID			:=	20 * BIT;
 * fIgnoreCase	:=	BIT;
 * fIgnoreAccent	:=	BIT;
 * fIgnoreWidth	:=	BIT;
 * fIgnoreKana	:=	BIT;
 * fBinary		:=	BIT;
 * ColFlags		:=	fIgnoreCase, fIgnoreAccent, fIgnoreWidth, fIgnoreKana, fBinary, FRESERVEDBIT, FRESERVEDBIT, FRESERVEDBIT;
 * Version		:=	4 * BIT;
 * SortId		:=	BYTE;
 * 
 * COLLATION	:=	LCID, ColFlags, Version, SortId;
 * 
 */
final class SQLCollation implements java.io.Serializable
{
    private final int info;     // First 4 bytes of TDS collation.
    private int langID() { return info & 0x0000FFFF; }
    private final int sortId;   // 5th byte of TDS collation.
    private final Encoding encoding;

    // Utility methods for getting details of this collation's encoding
    final Charset getCharset() throws SQLServerException { return encoding.charset(); }
    final boolean supportsAsciiConversion() { return encoding.supportsAsciiConversion(); }
    final boolean hasAsciiCompatibleSBCS() { return encoding.hasAsciiCompatibleSBCS(); }

    static final int tdsLength() { return 5; } // Length of collation in TDS (in bytes)

    /**
     * Returns the collation info
     * 
     * @return
     */
    int getCollationInfo() {
        return this.info;
    }
    
    /**
     * return sort ID
     * 
     * @return
     */
    int getCollationSortID() {
        return this.sortId;
    }
    
    /**
     * Reads TDS collation from TDS buffer into SQLCollation class.
     * @param tdsReader
     */
    SQLCollation(TDSReader tdsReader) throws UnsupportedEncodingException, SQLServerException
    {
    	/*
    	 * TDS rule for collation:
    	 * COLLATION = LCID ColFlags Version SortId
    	 */
        info = tdsReader.readInt(); // 4 bytes, contains: LCID ColFlags Version 
        sortId = tdsReader.readUnsignedByte(); // 1 byte, contains: SortId
        // For a SortId==0 collation, the LCID bits correspond to a LocaleId
        encoding = (0 == sortId) ? encodingFromLCID() : encodingFromSortId();
    }

    /**
     * Writes TDS collation from SQLCollation class into TDS buffer at offset.
     * @param tdsWriter TDS writer to write collation to.
     */
    void writeCollation(TDSWriter tdsWriter) throws SQLServerException
    {
      tdsWriter.writeInt(info);
      tdsWriter.writeByte((byte) (sortId & 0xFF));
    }

    /**
     * Enumeration of Windows locales recognized by SQL Server.
     *
     * For our purposes in the driver, locales are only described by their LangID and character encodings.
     *
     * The set of locales is derived from the following resources:
     *
     * http://download.microsoft.com/download/9/5/e/95ef66af-9026-4bb0-a41d-a4f81802d92c/[MS-LCID].pdf
     * Lists LCID values and their corresponding meanings (in RFC 3066 format).  Used to derive the names
     * for the various enumeration constants.
     *
     * x_rgLocaleMap and x_rgLcidOrdMap in sql\common\include\localemap.h in Katmai source tree
     * Collectively, these two tables provide a mapping of collation-version specific encodings
     * for every locale supported by SQL Server.  Lang IDs are derived from locales' LCIDs.
     */
    enum WindowsLocale
    {
        ar_SA (0x0401, Encoding.CP1256),
        bg_BG (0x0402, Encoding.CP1251),
        ca_ES (0x0403, Encoding.CP1252),
        zh_TW (0x0404, Encoding.CP950),
        cs_CZ (0x0405, Encoding.CP1250),
        da_DK (0x0406, Encoding.CP1252),
        de_DE (0x0407, Encoding.CP1252),
        el_GR (0x0408, Encoding.CP1253),
        en_US (0x0409, Encoding.CP1252),
        es_ES_tradnl (0x040a, Encoding.CP1252),
        fi_FI (0x040b, Encoding.CP1252),
        fr_FR (0x040c, Encoding.CP1252),
        he_IL (0x040d, Encoding.CP1255),
        hu_HU (0x040e, Encoding.CP1250),
        is_IS (0x040f, Encoding.CP1252),
        it_IT (0x0410, Encoding.CP1252),
        ja_JP (0x0411, Encoding.CP932),
        ko_KR (0x0412, Encoding.CP949),
        nl_NL (0x0413, Encoding.CP1252),
        nb_NO (0x0414, Encoding.CP1252),
        pl_PL (0x0415, Encoding.CP1250),
        pt_BR (0x0416, Encoding.CP1252),
        rm_CH (0x0417, Encoding.CP1252),
        ro_RO (0x0418, Encoding.CP1250),
        ru_RU (0x0419, Encoding.CP1251),
        hr_HR (0x041a, Encoding.CP1250),
        sk_SK (0x041b, Encoding.CP1250),
        sq_AL (0x041c, Encoding.CP1250),
        sv_SE (0x041d, Encoding.CP1252),
        th_TH (0x041e, Encoding.CP874),
        tr_TR (0x041f, Encoding.CP1254),
        ur_PK (0x0420, Encoding.CP1256),
        id_ID (0x0421, Encoding.CP1252),
        uk_UA (0x0422, Encoding.CP1251),
        be_BY (0x0423, Encoding.CP1251),
        sl_SI (0x0424, Encoding.CP1250),
        et_EE (0x0425, Encoding.CP1257),
        lv_LV (0x0426, Encoding.CP1257),
        lt_LT (0x0427, Encoding.CP1257),
        tg_Cyrl_TJ (0x0428, Encoding.CP1251),
        fa_IR (0x0429, Encoding.CP1256),
        vi_VN (0x042a, Encoding.CP1258),
        hy_AM (0x042b, Encoding.CP1252),
        az_Latn_AZ (0x042c, Encoding.CP1254),
        eu_ES (0x042d, Encoding.CP1252),
        wen_DE (0x042e, Encoding.CP1252),
        mk_MK (0x042f, Encoding.CP1251),
        tn_ZA (0x0432, Encoding.CP1252),
        xh_ZA (0x0434, Encoding.CP1252),
        zu_ZA (0x0435, Encoding.CP1252),
        Af_ZA (0x0436, Encoding.CP1252),
        ka_GE (0x0437, Encoding.CP1252),
        fo_FO (0x0438, Encoding.CP1252),
        hi_IN (0x0439, Encoding.UNICODE),
        mt_MT (0x043a, Encoding.UNICODE),
        se_NO (0x043b, Encoding.CP1252),
        ms_MY (0x043e, Encoding.CP1252),
        kk_KZ (0x043f, Encoding.CP1251),
        ky_KG (0x0440, Encoding.CP1251),
        sw_KE (0x0441, Encoding.CP1252),
        tk_TM (0x0442, Encoding.CP1250),
        uz_Latn_UZ (0x0443, Encoding.CP1254),
        tt_RU (0x0444, Encoding.CP1251),
        bn_IN (0x0445, Encoding.UNICODE),
        pa_IN (0x0446, Encoding.UNICODE),
        gu_IN (0x0447, Encoding.UNICODE),
        or_IN (0x0448, Encoding.UNICODE),
        ta_IN (0x0449, Encoding.UNICODE),
        te_IN (0x044a, Encoding.UNICODE),
        kn_IN (0x044b, Encoding.UNICODE),
        ml_IN (0x044c, Encoding.UNICODE),
        as_IN (0x044d, Encoding.UNICODE),
        mr_IN (0x044e, Encoding.UNICODE),
        sa_IN (0x044f, Encoding.UNICODE),
        mn_MN (0x0450, Encoding.CP1251),
        bo_CN (0x0451, Encoding.UNICODE),
        cy_GB (0x0452, Encoding.CP1252),
        km_KH (0x0453, Encoding.UNICODE),
        lo_LA (0x0454, Encoding.UNICODE),
        gl_ES (0x0456, Encoding.CP1252),
        kok_IN (0x0457, Encoding.UNICODE),
        syr_SY (0x045a, Encoding.UNICODE),
        si_LK (0x045b, Encoding.UNICODE),
        iu_Cans_CA (0x045d, Encoding.CP1252),
        am_ET (0x045e, Encoding.CP1252),
        ne_NP (0x0461, Encoding.UNICODE),
        fy_NL (0x0462, Encoding.CP1252),
        ps_AF (0x0463, Encoding.UNICODE),
        fil_PH (0x0464, Encoding.CP1252),
        dv_MV (0x0465, Encoding.UNICODE),
        ha_Latn_NG (0x0468, Encoding.CP1252),
        yo_NG (0x046a, Encoding.CP1252),
        quz_BO (0x046b, Encoding.CP1252),
        nso_ZA (0x046c, Encoding.CP1252),
        ba_RU (0x046d, Encoding.CP1251),
        lb_LU (0x046e, Encoding.CP1252),
        kl_GL (0x046f, Encoding.CP1252),
        ig_NG (0x0470, Encoding.CP1252),
        ii_CN (0x0478, Encoding.CP1252),
        arn_CL (0x047a, Encoding.CP1252),
        moh_CA (0x047c, Encoding.CP1252),
        br_FR (0x047e, Encoding.CP1252),
        ug_CN (0x0480, Encoding.CP1256),
        mi_NZ (0x0481, Encoding.UNICODE),
        oc_FR (0x0482, Encoding.CP1252),
        co_FR (0x0483, Encoding.CP1252),
        gsw_FR (0x0484, Encoding.CP1252),
        sah_RU (0x0485, Encoding.CP1251),
        qut_GT (0x0486, Encoding.CP1252),
        rw_RW (0x0487, Encoding.CP1252),
        wo_SN (0x0488, Encoding.CP1252),
        prs_AF (0x048c, Encoding.CP1256),
        ar_IQ (0x0801, Encoding.CP1256),
        zh_CN (0x0804, Encoding.CP936),
        de_CH (0x0807, Encoding.CP1252),
        en_GB (0x0809, Encoding.CP1252),
        es_MX (0x080a, Encoding.CP1252),
        fr_BE (0x080c, Encoding.CP1252),
        it_CH (0x0810, Encoding.CP1252),
        nl_BE (0x0813, Encoding.CP1252),
        nn_NO (0x0814, Encoding.CP1252),
        pt_PT (0x0816, Encoding.CP1252),
        sr_Latn_CS (0x081a, Encoding.CP1250),
        sv_FI (0x081d, Encoding.CP1252),
        Lithuanian_Classic (0x0827, Encoding.CP1257),
        az_Cyrl_AZ (0x082c, Encoding.CP1251),
        dsb_DE (0x082e, Encoding.CP1252),
        se_SE (0x083b, Encoding.CP1252),
        ga_IE (0x083c, Encoding.CP1252),
        ms_BN (0x083e, Encoding.CP1252),
        uz_Cyrl_UZ (0x0843, Encoding.CP1251),
        bn_BD (0x0845, Encoding.UNICODE),
        mn_Mong_CN (0x0850, Encoding.CP1251),
        iu_Latn_CA (0x085d, Encoding.CP1252),
        tzm_Latn_DZ (0x085f, Encoding.CP1252),
        quz_EC (0x086b, Encoding.CP1252),
        ar_EG (0x0c01, Encoding.CP1256),
        zh_HK (0x0c04, Encoding.CP950),
        de_AT (0x0c07, Encoding.CP1252),
        en_AU (0x0c09, Encoding.CP1252),
        es_ES (0x0c0a, Encoding.CP1252),
        fr_CA (0x0c0c, Encoding.CP1252),
        sr_Cyrl_CS (0x0c1a, Encoding.CP1251),
        se_FI (0x0c3b, Encoding.CP1252),
        quz_PE (0x0c6b, Encoding.CP1252),
        ar_LY (0x1001, Encoding.CP1256),
        zh_SG (0x1004, Encoding.CP936),
        de_LU (0x1007, Encoding.CP1252),
        en_CA (0x1009, Encoding.CP1252),
        es_GT (0x100a, Encoding.CP1252),
        fr_CH (0x100c, Encoding.CP1252),
        hr_BA (0x101a, Encoding.CP1250),
        smj_NO (0x103b, Encoding.CP1252),
        ar_DZ (0x1401, Encoding.CP1256),
        zh_MO (0x1404, Encoding.CP950),
        de_LI (0x1407, Encoding.CP1252),
        en_NZ (0x1409, Encoding.CP1252),
        es_CR (0x140a, Encoding.CP1252),
        fr_LU (0x140c, Encoding.CP1252),
        bs_Latn_BA (0x141a, Encoding.CP1250),
        smj_SE (0x143b, Encoding.CP1252),
        ar_MA (0x1801, Encoding.CP1256),
        en_IE (0x1809, Encoding.CP1252),
        es_PA (0x180a, Encoding.CP1252),
        fr_MC (0x180c, Encoding.CP1252),
        sr_Latn_BA (0x181a, Encoding.CP1250),
        sma_NO (0x183b, Encoding.CP1252),
        ar_TN (0x1c01, Encoding.CP1256),
        en_ZA (0x1c09, Encoding.CP1252),
        es_DO (0x1c0a, Encoding.CP1252),
        sr_Cyrl_BA (0x1c1a, Encoding.CP1251),
        sma_SB (0x1c3b, Encoding.CP1252),
        ar_OM (0x2001, Encoding.CP1256),
        en_JM (0x2009, Encoding.CP1252),
        es_VE (0x200a, Encoding.CP1252),
        bs_Cyrl_BA (0x201a, Encoding.CP1251),
        sms_FI (0x203b, Encoding.CP1252),
        ar_YE (0x2401, Encoding.CP1256),
        en_CB (0x2409, Encoding.CP1252),
        es_CO (0x240a, Encoding.CP1252),
        smn_FI (0x243b, Encoding.CP1252),
        ar_SY (0x2801, Encoding.CP1256),
        en_BZ (0x2809, Encoding.CP1252),
        es_PE (0x280a, Encoding.CP1252),
        ar_JO (0x2c01, Encoding.CP1256),
        en_TT (0x2c09, Encoding.CP1252),
        es_AR (0x2c0a, Encoding.CP1252),
        ar_LB (0x3001, Encoding.CP1256),
        en_ZW (0x3009, Encoding.CP1252),
        es_EC (0x300a, Encoding.CP1252),
        ar_KW (0x3401, Encoding.CP1256),
        en_PH (0x3409, Encoding.CP1252),
        es_CL (0x340a, Encoding.CP1252),
        ar_AE (0x3801, Encoding.CP1256),
        es_UY (0x380a, Encoding.CP1252),
        ar_BH (0x3c01, Encoding.CP1256),
        es_PY (0x3c0a, Encoding.CP1252),
        ar_QA (0x4001, Encoding.CP1256),
        en_IN (0x4009, Encoding.CP1252),
        es_BO (0x400a, Encoding.CP1252),
        en_MY (0x4409, Encoding.CP1252),
        es_SV (0x440a, Encoding.CP1252),
        en_SG (0x4809, Encoding.CP1252),
        es_HN (0x480a, Encoding.CP1252),
        es_NI (0x4c0a, Encoding.CP1252),
        es_PR (0x500a, Encoding.CP1252),
        es_US (0x540a, Encoding.CP1252);

        private final int langID;
        private final Encoding encoding;

        WindowsLocale(int langID,
                Encoding encoding) {
            this.langID = langID;
            this.encoding = encoding;
        }

        final Encoding getEncoding() throws UnsupportedEncodingException {
            return encoding.checkSupported();
        }
    }

    // Index from of windows locales by their LangIDs for fast lookup
    // of encodings associated with various SQL collations
    private static final Map<Integer, WindowsLocale> localeIndex;

    private Encoding encodingFromLCID() throws UnsupportedEncodingException {
        WindowsLocale locale = localeIndex.get(langID());

        if (null == locale) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownLCID"));
            Object[] msgArgs = {Integer.toHexString(langID()).toUpperCase()};
            throw new UnsupportedEncodingException(form.format(msgArgs));
        }

        try {
            return locale.getEncoding();
        }
        catch (UnsupportedEncodingException inner) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownLCID"));
            Object[] msgArgs = {locale};
            UnsupportedEncodingException e = new UnsupportedEncodingException(form.format(msgArgs));
            e.initCause(inner);
            throw e;
        }
    }

    /**
     * Enumeration of original SQL Server sort orders recognized by SQL Server.
     *
     * If SQL collation has a non-zero sortId, then use this enum to determine the encoding. From sql_main\sql\common\src\sqlscol.cpp (SQLServer code
     * base).
     */
    enum SortOrder
    {
        BIN_CP437               (30, "SQL_Latin1_General_CP437_BIN",        Encoding.CP437),
        DICTIONARY_437          (31, "SQL_Latin1_General_CP437_CS_AS",      Encoding.CP437),
        NOCASE_437              (32, "SQL_Latin1_General_CP437_CI_AS",      Encoding.CP437),
        NOCASEPREF_437          (33, "SQL_Latin1_General_Pref_CP437_CI_AS", Encoding.CP437),
        NOACCENTS_437           (34, "SQL_Latin1_General_CP437_CI_AI",      Encoding.CP437),
        BIN2_CP437              (35, "SQL_Latin1_General_CP437_BIN2",       Encoding.CP437),

        BIN_CP850               (40, "SQL_Latin1_General_CP850_BIN",        Encoding.CP850),
        DICTIONARY_850          (41, "SQL_Latin1_General_CP850_CS_AS",      Encoding.CP850),
        NOCASE_850              (42, "SQL_Latin1_General_CP850_CI_AS",      Encoding.CP850),
        NOCASEPREF_850          (43, "SQL_Latin1_General_Pref_CP850_CI_AS", Encoding.CP850),
        NOACCENTS_850           (44, "SQL_Latin1_General_CP850_CI_AI",      Encoding.CP850),
        BIN2_CP850              (45, "SQL_Latin1_General_CP850_BIN2",       Encoding.CP850),

        CASELESS_34             (49, "SQL_1xCompat_CP850_CI_AS",            Encoding.CP850),
        BIN_ISO_1               (50, "bin_iso_1",                           Encoding.CP1252),
        DICTIONARY_ISO          (51, "SQL_Latin1_General_CP1_CS_AS",        Encoding.CP1252),
        NOCASE_ISO              (52, "SQL_Latin1_General_CP1_CI_AS",        Encoding.CP1252),
        NOCASEPREF_ISO          (53, "SQL_Latin1_General_Pref_CP1_CI_AS",   Encoding.CP1252),
        NOACCENTS_ISO           (54, "SQL_Latin1_General_CP1_CI_AI",        Encoding.CP1252),
        ALT_DICTIONARY          (55, "SQL_AltDiction_CP850_CS_AS",          Encoding.CP850),
        ALT_NOCASEPREF          (56, "SQL_AltDiction_Pref_CP850_CI_AS",     Encoding.CP850),
        ALT_NOACCENTS           (57, "SQL_AltDiction_CP850_CI_AI",          Encoding.CP850),
        SCAND_NOCASEPREF        (58, "SQL_Scandinavian_Pref_CP850_CI_AS",   Encoding.CP850),
        SCAND_DICTIONARY        (59, "SQL_Scandinavian_CP850_CS_AS",        Encoding.CP850),
        SCAND_NOCASE            (60, "SQL_Scandinavian_CP850_CI_AS",        Encoding.CP850),
        ALT_NOCASE              (61, "SQL_AltDiction_CP850_CI_AS",          Encoding.CP850),

        DICTIONARY_1252         (71, "dictionary_1252",                     Encoding.CP1252),
        NOCASE_1252             (72, "nocase_1252",                         Encoding.CP1252),
        DNK_NOR_DICTIONARY      (73, "dnk_nor_dictionary",                  Encoding.CP1252),
        FIN_SWE_DICTIONARY      (74, "fin_swe_dictionary",                  Encoding.CP1252),
        ISL_DICTIONARY          (75, "isl_dictionary",                      Encoding.CP1252),

        BIN_CP1250              (80, "bin_cp1250",                          Encoding.CP1250),
        DICTIONARY_1250         (81, "SQL_Latin1_General_CP1250_CS_AS",     Encoding.CP1250),
        NOCASE_1250             (82, "SQL_Latin1_General_CP1250_CI_AS",     Encoding.CP1250),
        CSYDIC                  (83, "SQL_Czech_CP1250_CS_AS",              Encoding.CP1250),
        CSYNC                   (84, "SQL_Czech_CP1250_CI_AS",              Encoding.CP1250),
        HUNDIC                  (85, "SQL_Hungarian_CP1250_CS_AS",          Encoding.CP1250),
        HUNNC                   (86, "SQL_Hungarian_CP1250_CI_AS",          Encoding.CP1250),
        PLKDIC                  (87, "SQL_Polish_CP1250_CS_AS",             Encoding.CP1250),
        PLKNC                   (88, "SQL_Polish_CP1250_CI_AS",             Encoding.CP1250),
        ROMDIC                  (89, "SQL_Romanian_CP1250_CS_AS",           Encoding.CP1250),
        ROMNC                   (90, "SQL_Romanian_CP1250_CI_AS",           Encoding.CP1250),
        SHLDIC                  (91, "SQL_Croatian_CP1250_CS_AS",           Encoding.CP1250),
        SHLNC                   (92, "SQL_Croatian_CP1250_CI_AS",           Encoding.CP1250),
        SKYDIC                  (93, "SQL_Slovak_CP1250_CS_AS",             Encoding.CP1250),
        SKYNC                   (94, "SQL_Slovak_CP1250_CI_AS",             Encoding.CP1250),
        SLVDIC                  (95, "SQL_Slovenian_CP1250_CS_AS",          Encoding.CP1250),
        SLVNC                   (96, "SQL_Slovenian_CP1250_CI_AS",          Encoding.CP1250),
        POLISH_CS               (97, "polish_cs",                           Encoding.CP1250),
        POLISH_CI               (98, "polish_ci",                           Encoding.CP1250),

        BIN_CP1251              (104, "bin_cp1251",                         Encoding.CP1251),
        DICTIONARY_1251         (105, "SQL_Latin1_General_CP1251_CS_AS",    Encoding.CP1251),
        NOCASE_1251             (106, "SQL_Latin1_General_CP1251_CI_AS",    Encoding.CP1251),
        UKRDIC                  (107, "SQL_Ukrainian_CP1251_CS_AS",         Encoding.CP1251),
        UKRNC                   (108, "SQL_Ukrainian_CP1251_CI_AS",         Encoding.CP1251),

        BIN_CP1253              (112, "bin_cp1253",                         Encoding.CP1253),
        DICTIONARY_1253         (113, "SQL_Latin1_General_CP1253_CS_AS",    Encoding.CP1253),
        NOCASE_1253             (114, "SQL_Latin1_General_CP1253_CI_AS",    Encoding.CP1253),

        GREEK_MIXEDDICTIONARY   (120, "SQL_MixDiction_CP1253_CS_AS",        Encoding.CP1253),
        GREEK_ALTDICTIONARY     (121, "SQL_AltDiction_CP1253_CS_AS",        Encoding.CP1253),
        GREEK_ALTDICTIONARY2    (122, "SQL_AltDiction2_CP1253_CS_AS",       Encoding.CP1253),
        GREEK_NOCASEDICT        (124, "SQL_Latin1_General_CP1253_CI_AI",    Encoding.CP1253), 
        BIN_CP1254              (128, "bin_cp1254",                         Encoding.CP1254),
        DICTIONARY_1254         (129, "SQL_Latin1_General_CP1254_CS_AS",    Encoding.CP1254),
        NOCASE_1254             (130, "SQL_Latin1_General_CP1254_CI_AS",    Encoding.CP1254),

        BIN_CP1255              (136, "bin_cp1255",                         Encoding.CP1255),
        DICTIONARY_1255         (137, "SQL_Latin1_General_CP1255_CS_AS",    Encoding.CP1255),
        NOCASE_1255             (138, "SQL_Latin1_General_CP1255_CI_AS",    Encoding.CP1255),

        BIN_CP1256              (144, "bin_cp1256",                         Encoding.CP1256),
        DICTIONARY_1256         (145, "SQL_Latin1_General_CP1256_CS_AS",    Encoding.CP1256),
        NOCASE_1256             (146, "SQL_Latin1_General_CP1256_CI_AS",    Encoding.CP1256),

        BIN_CP1257              (152, "bin_cp1257",                         Encoding.CP1257),
        DICTIONARY_1257         (153, "SQL_Latin1_General_CP1257_CS_AS",    Encoding.CP1257),
        NOCASE_1257             (154, "SQL_Latin1_General_CP1257_CI_AS",    Encoding.CP1257),
        ETIDIC                  (155, "SQL_Estonian_CP1257_CS_AS",          Encoding.CP1257),
        ETINC                   (156, "SQL_Estonian_CP1257_CI_AS",          Encoding.CP1257),
        LVIDIC                  (157, "SQL_Latvian_CP1257_CS_AS",           Encoding.CP1257),
        LVINC                   (158, "SQL_Latvian_CP1257_CI_AS",           Encoding.CP1257),
        LTHDIC                  (159, "SQL_Lithuanian_CP1257_CS_AS",        Encoding.CP1257),
        LTHNC                   (160, "SQL_Lithuanian_CP1257_CI_AS",        Encoding.CP1257),

        DANNO_NOCASEPREF        (183, "SQL_Danish_Pref_CP1_CI_AS",          Encoding.CP1252),
        SVFI1_NOCASEPREF        (184, "SQL_SwedishPhone_Pref_CP1_CI_AS",    Encoding.CP1252),
        SVFI2_NOCASEPREF        (185, "SQL_SwedishStd_Pref_CP1_CI_AS",      Encoding.CP1252),
        ISLAN_NOCASEPREF        (186, "SQL_Icelandic_Pref_CP1_CI_AS",       Encoding.CP1252),

        BIN_CP932               (192, "bin_cp932",                          Encoding.CP932),
        NLS_CP932               (193, "nls_cp932",                          Encoding.CP932),
        BIN_CP949               (194, "bin_cp949",                          Encoding.CP949),
        NLS_CP949               (195, "nls_cp949",                          Encoding.CP949),
        BIN_CP950               (196, "bin_cp950",                          Encoding.CP950),
        NLS_CP950               (197, "nls_cp950",                          Encoding.CP950),
        BIN_CP936               (198, "bin_cp936",                          Encoding.CP936),
        NLS_CP936               (199, "nls_cp936",                          Encoding.CP936),
        NLS_CP932_CS            (200, "nls_cp932_cs",                       Encoding.CP932),
        NLS_CP949_CS            (201, "nls_cp949_cs",                       Encoding.CP949),
        NLS_CP950_CS            (202, "nls_cp950_cs",                       Encoding.CP950),
        NLS_CP936_CS            (203, "nls_cp936_cs",                       Encoding.CP936),
        BIN_CP874               (204, "bin_cp874",                          Encoding.CP874),
        NLS_CP874               (205, "nls_cp874",                          Encoding.CP874),
        NLS_CP874_CS            (206, "nls_cp874_cs",                       Encoding.CP874),

        EBCDIC_037              (210, "SQL_EBCDIC037_CP1_CS_AS",            Encoding.CP1252),
        EBCDIC_273              (211, "SQL_EBCDIC273_CP1_CS_AS",            Encoding.CP1252),
        EBCDIC_277              (212, "SQL_EBCDIC277_CP1_CS_AS",            Encoding.CP1252),
        EBCDIC_278              (213, "SQL_EBCDIC278_CP1_CS_AS",            Encoding.CP1252),
        EBCDIC_280              (214, "SQL_EBCDIC280_CP1_CS_AS",            Encoding.CP1252),
        EBCDIC_284              (215, "SQL_EBCDIC284_CP1_CS_AS",            Encoding.CP1252),
        EBCDIC_285              (216, "SQL_EBCDIC285_CP1_CS_AS",            Encoding.CP1252),
        EBCDIC_297              (217, "SQL_EBCDIC297_CP1_CS_AS",            Encoding.CP1252);

        private final int sortId;
        private final String name;
        private final Encoding encoding;

        final Encoding getEncoding() throws UnsupportedEncodingException {
            return encoding.checkSupported();
        }

        SortOrder(int sortId,
                String name,
                Encoding encoding) {
            this.sortId = sortId;
            this.name = name;
            this.encoding = encoding;
        }

        public final String toString() {
            return name;
        }
    }

    private static final HashMap<Integer, SortOrder> sortOrderIndex;

    private Encoding encodingFromSortId() throws UnsupportedEncodingException {
        SortOrder sortOrder = sortOrderIndex.get(sortId);

        if (null == sortOrder) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownSortId"));
            Object[] msgArgs = {sortId};
            throw new UnsupportedEncodingException(form.format(msgArgs));
        }

        try {
            return sortOrder.getEncoding();
        }
        catch (UnsupportedEncodingException inner) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownSortId"));
            Object[] msgArgs = {sortOrder};
            UnsupportedEncodingException e = new UnsupportedEncodingException(form.format(msgArgs));
            e.initCause(inner);
            throw e;
        }
    }

    static {
        // Populate the windows locale and sort order indices

        localeIndex = new HashMap<>();
        for (WindowsLocale locale : EnumSet.allOf(WindowsLocale.class))
            localeIndex.put(locale.langID, locale);

        sortOrderIndex = new HashMap<>();
        for (SortOrder sortOrder : EnumSet.allOf(SortOrder.class))
            sortOrderIndex.put(sortOrder.sortId, sortOrder);
    }
}

/**
 * Enumeration of encodings that are supported by SQL Server (and hopefully the JVM).
 *
 * See, for example, https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html for a complete list of supported encodings with
 * their canonical names.
 */
enum Encoding
{
    UNICODE ("UTF-16LE", true, false),
    CP437   ("Cp437", false, false),
    CP850   ("Cp850", false, false),
    CP874   ("MS874", true, true),
    CP932   ("MS932", true, false),
    CP936   ("MS936", true, false),
    CP949   ("MS949", true, false),
    CP950   ("MS950", true, false),
    CP1250  ("Cp1250", true, true),
    CP1251  ("Cp1251", true, true),
    CP1252  ("Cp1252", true, true),
    CP1253  ("Cp1253", true, true),
    CP1254  ("Cp1254", true, true),
    CP1255  ("Cp1255", true, true),
    CP1256  ("Cp1256", true, true),
    CP1257  ("Cp1257", true, true),
    CP1258  ("Cp1258", true, true);

    private final String charsetName;
    private final boolean supportsAsciiConversion;
    private final boolean hasAsciiCompatibleSBCS;
    private boolean jvmSupportConfirmed = false;
    private Charset charset;

    private Encoding(String charsetName,
            boolean supportsAsciiConversion,
            boolean hasAsciiCompatibleSBCS) {
        this.charsetName = charsetName;
        this.supportsAsciiConversion = supportsAsciiConversion;
        this.hasAsciiCompatibleSBCS = hasAsciiCompatibleSBCS;
    }

    final Encoding checkSupported() throws UnsupportedEncodingException {
        if (!jvmSupportConfirmed) {
            // Checks for support by converting a java.lang.String
            // This works for all of the code pages above in SE 5 and later.
            if (!Charset.isSupported(charsetName)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_codePageNotSupported"));
                Object[] msgArgs = {charsetName};
                throw new UnsupportedEncodingException(form.format(msgArgs));
            }

            jvmSupportConfirmed = true;
        }

        return this;
    }

    final Charset charset() throws SQLServerException {
        try {
            checkSupported();
            if (charset == null) {
                charset = Charset.forName(charsetName);
            }
        }
        catch (UnsupportedEncodingException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_codePageNotSupported"));
            Object[] msgArgs = {charsetName};
            throw new SQLServerException(form.format(msgArgs), e);
        }
        return charset;
    }

    /**
     * Returns true if the collation supports conversion to ascii.
     *
     * Per discussions with richards and michkap on UNICODE alias -> ASCII range is 0x00 to 0x7F. The range of 0x00 to 0x7F of 1250-1258, 874, 932,
     * 936, 949, and 950 are identical to ASCII. See also -> http://blogs.msdn.com/michkap/archive/2005/11/23/495193.aspx
     */
    boolean supportsAsciiConversion() {
        return supportsAsciiConversion;
    }

    /**
     * Returns true if the collation supports conversion to ascii AND it uses a single-byte character set.
     *
     * Per discussions with richards and michkap on UNICODE alias -> ASCII range is 0x00 to 0x7F. The range of 0x00 to 0x7F of 1250-1258 and 874 are
     * identical to ASCII for these SBCS character sets. See also -> http://blogs.msdn.com/michkap/archive/2005/11/23/495193.aspx
     */
    boolean hasAsciiCompatibleSBCS() {
        return hasAsciiCompatibleSBCS;
    }
}
