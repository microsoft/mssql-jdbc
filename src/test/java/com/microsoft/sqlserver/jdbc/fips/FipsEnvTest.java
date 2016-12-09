/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fips;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assumptions.assumingThat;

import java.security.Provider;
import java.security.Security;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.Utils;

/**
 * Class which will useful for checking if FIPS env. set or not. 
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
		
		if(p.getProperty("java.vendor").startsWith("IBM")) {
			currentJVM = IBM_JVM;
		}
		
		//TODO: Need to check this. 
		if(p.getProperty("java.vendor").startsWith("SAP")) {
			currentJVM = SAP_JVM;
		}
	}

	

	/**
	 * After stabilizing parameterized test case {@link #testFIPSOnOracle(String)} we can delete this test case.
	 * TODO: Enable FIPS can be done in two ways. 
	 * <LI> JVM Level
	 * <LI> Program Level
	 * We need to test both... 
	 * @since 6.1.2
	 */
	@Test
	public void testFIPSOnOracle() {
		assumeTrue(ORACLE_JVM.equals(currentJVM), "Aborting test: As this is not Oracle Env. "); 

		assumingThat("NSSFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")),
				() -> assertAll("All FIPS", () -> assertTrue(isFIPS("SunJSSE"), "FIPS should be Enabled."), () -> assertTrue(isFIPS("SunPKCS11-NSS"), "Testing")));

		assumingThat("BCFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")),
				() -> assertAll("All FIPS", () -> assertTrue(isFIPS("SunJSSE"), "FIPS should be Enabled."), () -> assertTrue(isFIPS("BCFIPS"), "Testing")));
		
		assumingThat("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), ()-> assertTrue(isFIPS("SunJSSE"), "FIPS Should be enabled"));
	}

	/**
	 * It will test 
	 */
	@Test
	public void testFIPSOnIBM() {
		assumeTrue(IBM_JVM.equals(currentJVM), "Aborting test: As this is not IBM Env. "); 

		assumingThat("NSSFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")),
				() -> assertAll("All FIPS", () -> assertTrue(isFIPS("IBMJCEFIP"), "FIPS should be Enabled."), () -> assertTrue(isFIPS("SunPKCS11-NSS"), "Testing")));

		assumingThat("BCFIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")),
				() -> assertAll("All FIPS", () -> assertTrue(isFIPS("IBMJCEFIPS"), "FIPS should be Enabled."), () -> assertTrue(isFIPS("BCFIPS"), "Testing")));
		
		assumingThat("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), ()-> assertTrue(isFIPS("IBMJCEFIPS"), "FIPS Should be enabled"));
	}


	/**
	 * In case of FIPs enabled this test method will call {@link #isFIPS(String)} with appropriate FIPS provider. 
	 */
	@Test
	public void testFIPSEnv() {
		assumeTrue("FIPS".equals(Utils.getConfiguredProperty("FIPS_ENV")), "Aborting test: This is FIPS Enabled JVM");

		assumingThat(System.getProperty("java.vendor").startsWith("IBM"), () -> assertTrue(isFIPS("IBMJCEFIP"), "FIPS should be Enabled."));

		assumingThat(System.getProperty("java.vendor").startsWith("Oracle"), () -> assertTrue(isFIPS("SunJSSE"), "FIPS should be Enabled."));

	}
	
	/**
	 * Just simple method to check if JVM is configured for FIPS or not. 
	 * @param provider
	 * @return
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

	
	@Deprecated
	private boolean isFIPS() throws Exception {
		// We observed that SSLContext.getDefault().getProvider fails because it could not able to find any algorithm.
		Provider jsse = SSLContext.getDefault().getProvider();
		if (logger.isLoggable(Level.FINE)) {
			logger.fine(jsse.toString());
			logger.fine(jsse.getInfo());
		}
		return jsse != null && jsse.getInfo().contains("FIPS");
	}
	
//		@Test
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
