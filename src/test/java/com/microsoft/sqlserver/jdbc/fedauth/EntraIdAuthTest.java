package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * It is added only for understanding the PR pipelines run.
 */
@RunWith(JUnitPlatform.class)
public class EntraIdAuthTest {
	
	@Test
	public void testEntraIdAuth() {
		System.out.println("Testing entraIdAuth. It is dummy test, need to be deleted after testing");
		assertTrue(true);
	}

}
