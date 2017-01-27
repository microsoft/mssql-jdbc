/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fips;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.security.Provider;
import java.security.Security;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.Utils;

/**
 * Class which will useful for checking if FIPS env. set or not.
 * 
 * @since 6.1.2
 */
@RunWith(JUnitPlatform.class)
public class FipsEnvTest {

    protected static Logger logger = Logger.getLogger("FipsEnvTest");

    protected static Properties p = null;

    protected static final String ORACLE_JVM = "ORACLE_JVM";

    protected static final String IBM_JVM = "IBM_JVM";

    protected static final String SAP_JVM = "SAP_JVM";

    protected static String currentJVM = ORACLE_JVM;

    /**
     * Before class init method.
     */
    @BeforeAll
    public static void populateProperties() {
        p = System.getProperties();

        if (p.getProperty("java.vendor").startsWith("IBM")) {
            currentJVM = IBM_JVM;
        }

        // TODO: Need to check this.
        if (p.getProperty("java.vendor").startsWith("SAP")) {
            currentJVM = SAP_JVM;
        }
    }

    /**
     * After stabilizing parameterized test case TODO: Enable FIPS can be done in two ways.
     * <LI>JVM Level - Done.
     * <LI>Program Level - Not Done. We need to test both on different environments.
     * 
     * @since 6.1.2
     */
    @Test
    public void testFIPSOnOracle() throws Exception {
        assumeTrue(ORACLE_JVM.equals(currentJVM), "Aborting test: As this is not Oracle Env. ");

        assumeTrue("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), "Aborting test case as FIPS_ENV property is not set. ");

        assertTrue(isFIPS("SunJSSE"), "FIPS Should be enabled");

        // As JDK 1.7 is not supporting lambda for time being commenting.
        /*
         * assumingThat("NSSFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), () -> assertAll("All FIPS", () -> assertTrue(isFIPS("SunJSSE"),
         * "FIPS should be Enabled."), () -> assertTrue(isFIPS("SunPKCS11-NSS"), "Testing")));
         * 
         * assumingThat("BCFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), () -> assertAll("All FIPS", () -> assertTrue(isFIPS("SunJSSE"),
         * "FIPS should be Enabled."), () -> assertTrue(isFIPS("BCFIPS"), "Testing")));
         * 
         * assumingThat("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), ()-> assertTrue(isFIPS("SunJSSE"), "FIPS Should be enabled"));
         */
    }

    /**
     * It will test FIPS on IBM Env. If JVM is not IBM test will not fail. It will simply skipped.
     * 
     * @since 6.1.2
     */
    @Test
    public void testFIPSOnIBM() throws Exception {
        assumeTrue(IBM_JVM.equals(currentJVM), "Aborting test: As this is not IBM Env. ");

        assumeTrue("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), "Aborting test case as FIPS is not enabled. ");

        assertTrue(isFIPS("IBMJCEFIP"), "FIPS Should be enabled");

        // As JDK 1.7 is not supporting lambda for time being commenting.
        /*
         * assumingThat("NSSFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), () -> assertAll("All FIPS", () -> assertTrue(isFIPS("IBMJCEFIP"),
         * "FIPS should be Enabled."), () -> assertTrue(isFIPS("SunPKCS11-NSS"), "Testing")));
         * 
         * assumingThat("BCFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), () -> assertAll("All FIPS", () -> assertTrue(isFIPS("IBMJCEFIPS"),
         * "FIPS should be Enabled."), () -> assertTrue(isFIPS("BCFIPS"), "Testing")));
         * 
         * assumingThat("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), ()-> assertTrue(isFIPS("IBMJCEFIPS"), "FIPS Should be enabled"));
         */
    }

    /**
     * In case of FIPs enabled this test method will call {@link #isFIPS(String)} with appropriate FIPS provider. May be useful only for JDK 1.8
     */
    @Test
    @Disabled
    public void testFIPSEnv() {
        assumeTrue("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), "Aborting test: This is FIPS Enabled JVM");

        // As JDK 1.7 is not supporting lambda for time being commenting.
        /*
         * assumingThat(System.getProperty("java.vendor").startsWith("IBM"), () -> assertTrue(isFIPS("IBMJCEFIP"), "FIPS should be Enabled."));
         * 
         * assumingThat(System.getProperty("java.vendor").startsWith("Oracle"), () -> assertTrue(isFIPS("SunJSSE"), "FIPS should be Enabled."));
         */
    }

    /**
     * Just simple method to check if JVM is configured for FIPS or not. CAUTION: We observed that <code>SSLContext.getDefault().getProvider</code>
     * fails because it could not find any algorithm.
     * 
     * @param provider
     *            FIPS Provider
     * @return boolean
     * @throws Exception
     */
    public static boolean isFIPS(String provider) throws Exception {
        Provider jsse = Security.getProvider(provider);
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(jsse.toString());
            logger.fine(jsse.getInfo());
        }
        return jsse != null && jsse.getInfo().contains("FIPS");
    }

    @Test
    @Disabled
    public void printJVMInfo() {
        Enumeration<Object> keys = p.keys();

        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            String value = (String) p.get(key);

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(key + ": " + value);
            }
        }
    }

}
