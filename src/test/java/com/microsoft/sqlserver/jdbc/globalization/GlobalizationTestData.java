/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.globalization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

/**
 * Shared test data for globalization tests. Replaces FX
 * fxLanguages/fxCollations/fxEnglishLanguageUTF16.
 */
public final class GlobalizationTestData {

    private GlobalizationTestData() {
    }

    /**
     * Representative sample strings per language (FX
     * TCCollations._testedLanguages[]).
     */
    public static final Map<String, String> LANGUAGE_SAMPLES;

    static {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("English", "Hello World 12345");
        m.put("German", "Stra\u00DFe \u00C4pfel \u00D6l \u00DCber f\u00FCnf");
        m.put("Breton", "Degemer mat d'an Breizh");
        m.put("Turkish", "\u0130stanbul \u0131lk g\u00FCn\u00FC \u011F\u00F6t\u00FCr");
        m.put("Russian", "\u041F\u0440\u0438\u0432\u0435\u0442 \u043C\u0438\u0440 \u0442\u0435\u0441\u044212");
        m.put("Arabic", "\u0645\u0631\u062D\u0628\u0627 \u0628\u0627\u0644\u0639\u0627\u0644\u0645");
        m.put("Japanese", "\u3053\u3093\u306B\u3061\u306F\u4E16\u754C");
        m.put("Chinese (China)", "\u4F60\u597D\u4E16\u754C\u6D4B\u8BD5");
        m.put("Chinese (Taiwan)", "\u4F60\u597D\u4E16\u754C\u6E2C\u8A66");
        m.put("ChineseHongKong", "\u4F60\u597D\u4E16\u754C\u6E2C\u8A66");
        LANGUAGE_SAMPLES = Collections.unmodifiableMap(m);
    }

    /** Uppercase/lowercase char pairs per language (FX TCCaseSensitivity). */
    public static final Map<String, char[]> UPPERCASE_CHARS;
    public static final Map<String, char[]> LOWERCASE_CHARS;

    static {
        Map<String, char[]> upper = new LinkedHashMap<>();
        Map<String, char[]> lower = new LinkedHashMap<>();
        upper.put("English", new char[] { 'A', 'B', 'C', 'D', 'E' });
        lower.put("English", new char[] { 'a', 'b', 'c', 'd', 'e' });
        upper.put("German", new char[] { '\u00C4', '\u00D6', '\u00DC' });
        lower.put("German", new char[] { '\u00E4', '\u00F6', '\u00FC' });
        upper.put("Russian", new char[] { '\u0410', '\u0411', '\u0412', '\u0413', '\u0414' });
        lower.put("Russian", new char[] { '\u0430', '\u0431', '\u0432', '\u0433', '\u0434' });
        UPPERCASE_CHARS = Collections.unmodifiableMap(upper);
        LOWERCASE_CHARS = Collections.unmodifiableMap(lower);
    }

    /**
     * Language → SQL Server collation (FX TCCollations + fxLanguage.collations()).
     */
    public static final Map<String, String> LANGUAGE_COLLATIONS;

    static {
        Map<String, String> c = new LinkedHashMap<>();
        c.put("English", "Latin1_General_CI_AS");
        c.put("German", "German_PhoneBook_CI_AS");
        c.put("Breton", "Breton_100_CI_AS");
        c.put("Turkish", "Turkish_CI_AS");
        c.put("Russian", "Cyrillic_General_CI_AS");
        c.put("Arabic", "Arabic_CI_AS");
        c.put("Japanese", "Japanese_CI_AS");
        c.put("Chinese (China)", "Chinese_PRC_CI_AS");
        c.put("Chinese (Taiwan)", "Chinese_Taiwan_Stroke_CI_AS");
        c.put("ChineseHongKong", "Chinese_Hong_Kong_Stroke_90_CI_AS");
        LANGUAGE_COLLATIONS = Collections.unmodifiableMap(c);
    }

    /**
     * Character types for UTF-16 testing (FX fxEnglishLanguageUTF16.CharacterType).
     */
    public enum CharacterType {
        ASCII("ASCII", "Hello World 12345"),
        BMP("BMP", "\u00C4\u00D6\u00DC\u00DF\u03B1\u03B2\u03B3\u0410\u0411\u0412"),
        SURROGATE("SURROGATE", "\uD800\uDC00\uD801\uDC01\uD835\uDD38\uD83D\uDE00"),
        COMBINING("COMBINING", "a\u0301e\u0308o\u0302n\u0303u\u0327");

        private final String name;
        private final String sampleChars;

        CharacterType(String name, String sampleChars) {
            this.name = name;
            this.sampleChars = sampleChars;
        }

        public String getName() {
            return name;
        }

        public String getSampleChars() {
            return sampleChars;
        }
    }

    /** Where special characters appear in the string (FX CharacterPosition). */
    public enum CharacterPosition {
        MIDDLE("MIDDLE"),
        END("END");

        private final String name;

        CharacterPosition(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * Generates test string with special chars at the given position (FX
     * createSelectedData).
     */
    public static String generateTestString(CharacterType type, CharacterPosition position) {
        String special = type.getSampleChars();
        String padding = "TestPadding";

        switch (position) {
            case MIDDLE:
                return padding + special + padding;
            case END:
                return padding + padding + special;
            default:
                return special;
        }
    }

    /**
     * JDBC methods for sending character data (FX TCDenaliUTF16 CSendXxx classes,
     * 22 variants).
     */
    public enum SendMethod {
        LITERAL("Literal", false),
        PS_SET_STRING("PS.setString", false),
        PS_SET_NSTRING("PS.setNString", false),
        PS_SET_CHARACTER_STREAM("PS.setCharacterStream", true),
        PS_SET_NCHARACTER_STREAM("PS.setNCharacterStream", true),
        PS_SET_CLOB("PS.setClob", true),
        PS_SET_NCLOB("PS.setNClob", true),
        CS_SET_STRING("CS.setString", false),
        CS_SET_NSTRING("CS.setNString", false),
        RS_UPDATE_STRING("RS.updateString", false),
        RS_UPDATE_NSTRING("RS.updateNString", false),
        RS_UPDATE_CHARACTER_STREAM("RS.updateCharacterStream", true),
        RS_UPDATE_NCHARACTER_STREAM("RS.updateNCharacterStream", true),
        RS_UPDATE_CLOB("RS.updateClob", true),
        RS_UPDATE_NCLOB("RS.updateNClob", true);

        private final String displayName;
        private final boolean needsLob;

        SendMethod(String displayName, boolean needsLob) {
            this.displayName = displayName;
            this.needsLob = needsLob;
        }

        public String getDisplayName() {
            return displayName;
        }

        public boolean needsLob() {
            return needsLob;
        }
    }

    /**
     * JDBC methods for receiving character data (FX TCDenaliUTF16 CReceiveXxx
     * classes, 19 variants).
     */
    public enum ReceiveMethod {
        RS_GET_STRING("RS.getString", false),
        RS_GET_NSTRING("RS.getNString", false),
        RS_GET_CHARACTER_STREAM("RS.getCharacterStream", true),
        RS_GET_NCHARACTER_STREAM("RS.getNCharacterStream", true),
        RS_GET_CLOB("RS.getClob", true),
        RS_GET_NCLOB("RS.getNClob", true),
        CS_OUT_GET_STRING("CS.out.getString", false),
        CS_OUT_GET_NSTRING("CS.out.getNString", false),
        CS_OUT_GET_CHARACTER_STREAM("CS.out.getCharacterStream", true),
        CS_OUT_GET_NCHARACTER_STREAM("CS.out.getNCharacterStream", true),
        CS_OUT_GET_CLOB("CS.out.getClob", true),
        CS_OUT_GET_NCLOB("CS.out.getNClob", true),
        CS_INOUT_GET_STRING("CS.inout.getString", false),
        CS_INOUT_GET_NSTRING("CS.inout.getNString", false),
        CS_INOUT_GET_CHARACTER_STREAM("CS.inout.getCharacterStream", true),
        CS_INOUT_GET_NCHARACTER_STREAM("CS.inout.getNCharacterStream", true),
        CS_INOUT_GET_CLOB("CS.inout.getClob", true),
        CS_INOUT_GET_NCLOB("CS.inout.getNClob", true);

        private final String displayName;
        private final boolean needsLob;

        ReceiveMethod(String displayName, boolean needsLob) {
            this.displayName = displayName;
            this.needsLob = needsLob;
        }

        public String getDisplayName() {
            return displayName;
        }

        public boolean needsLob() {
            return needsLob;
        }
    }

    // ---- Parameterized Test Data Providers ----

    /** Provides (language, collation, sampleData) for collation tests. */
    public static Stream<Arguments> collationLanguageProvider() {
        List<Arguments> args = new ArrayList<>();
        for (Map.Entry<String, String> entry : LANGUAGE_COLLATIONS.entrySet()) {
            String language = entry.getKey();
            String collation = entry.getValue();
            String sampleData = LANGUAGE_SAMPLES.get(language);
            if (sampleData != null) {
                args.add(Arguments.of(language, collation, sampleData));
            }
        }
        return args.stream();
    }

    /** Provides (language, collation, sampleData, sspauValue) for SSPAU tests. */
    public static Stream<Arguments> collationSSPAUProvider() {
        List<Arguments> args = new ArrayList<>();
        for (Map.Entry<String, String> entry : LANGUAGE_COLLATIONS.entrySet()) {
            String language = entry.getKey();
            String collation = entry.getValue();
            String sampleData = LANGUAGE_SAMPLES.get(language);
            if (sampleData != null) {
                // Both SSPAU values, just like FX TCCollations
                args.add(Arguments.of(language, collation, sampleData, true));
                args.add(Arguments.of(language, collation, sampleData, false));
            }
        }
        return args.stream();
    }

    /** Provides (language, upperChars, lowerChars) for case sensitivity tests. */
    public static Stream<Arguments> caseSensitivityProvider() {
        List<Arguments> args = new ArrayList<>();
        for (Map.Entry<String, char[]> entry : UPPERCASE_CHARS.entrySet()) {
            String language = entry.getKey();
            char[] upper = entry.getValue();
            char[] lower = LOWERCASE_CHARS.get(language);
            if (lower != null) {
                args.add(Arguments.of(language, upper, lower));
            }
        }
        return args.stream();
    }

    /**
     * Provides pairwise (sendMethod, receiveMethod, charType, charPosition) for
     * UTF-16 tests.
     */
    public static Stream<Arguments> utf16RoundTripProvider() {
        List<Arguments> args = new ArrayList<>();
        SendMethod[] sends = SendMethod.values();
        ReceiveMethod[] receives = ReceiveMethod.values();
        CharacterType[] charTypes = CharacterType.values();
        CharacterPosition[] charPositions = CharacterPosition.values();

        // Pairwise coverage: cycle through all dimensions
        int maxLen = Math.max(sends.length, Math.max(receives.length,
                Math.max(charTypes.length, charPositions.length)));

        for (int i = 0; i < maxLen; i++) {
            SendMethod sm = sends[i % sends.length];
            ReceiveMethod rm = receives[i % receives.length];
            CharacterType ct = charTypes[i % charTypes.length];
            CharacterPosition cp = charPositions[i % charPositions.length];
            args.add(Arguments.of(sm, rm, ct, cp));
        }

        // Extra combinations for each CharacterType
        for (CharacterType ct : charTypes) {
            for (int i = 0; i < 3; i++) {
                SendMethod sm = sends[(ct.ordinal() * 3 + i) % sends.length];
                ReceiveMethod rm = receives[(ct.ordinal() * 3 + i + 1) % receives.length];
                CharacterPosition cp = charPositions[(ct.ordinal() + i) % charPositions.length];
                args.add(Arguments.of(sm, rm, ct, cp));
            }
        }

        return args.stream();
    }

    /** Provides all available locales for JVM locale testing (FX TCLocales). */
    public static Stream<Arguments> localeProvider() {
        return Arrays.stream(Locale.getAvailableLocales())
                .map(Arguments::of);
    }

    /** Provides (Locale, sqlType) for calendar locale testing (FX TCCalendar). */
    public static Stream<Arguments> calendarLocaleProvider() {
        // Representative locale coverage
        Locale[] locales = {
                Locale.US, Locale.GERMANY, Locale.FRANCE, Locale.JAPAN, Locale.CHINA,
                Locale.KOREA, Locale.forLanguageTag("ar-SA"), Locale.forLanguageTag("th-TH"),
                Locale.forLanguageTag("ru-RU"), Locale.forLanguageTag("tr-TR")
        };
        String[] sqlTypes = { "TIME", "DATETIME2" };

        List<Arguments> args = new ArrayList<>();
        for (Locale locale : locales) {
            for (String sqlType : sqlTypes) {
                args.add(Arguments.of(locale, sqlType));
            }
        }
        return args.stream();
    }
}
