/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import com.microsoft.sqlserver.jdbc.*;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Test the timeout in SQLServerBulkCopyOptions. Source table is created with
 * large row count so skip data validation.
 */
@RunWith(JUnitPlatform.class)
@DisplayName("BulkCopy Timeout Test")
public class BulkCopyTimeoutTest extends BulkCopyTestSetUp {

	/**
	 * This tests no timeout is triggered with a zero timeout, since we only
	 * configure timeout threads for timeout values above 0
	 */
	@Test
	@DisplayName("BulkCopy:test zero timeout")
	public void testZeroTimeOut() throws SQLException {
		testBulkCopyWithTimeout(0);
	}

	/**
	 * This tests a small timeout value of 1 second. Specifically after we already
	 * sent a bulk load in the copy operation we sleep for 1 seconds which should
	 * trigger a timeout (keep in mind this entire bulk copy is fairly fast, 100-150
	 * ms, so the mocked sleep, lets us simulate a timeout nicely without a large
	 * refactor)
	 */
	@Test
	@DisplayName("BulkCopy:test real timeout")
	public void testRealTimeout() {
		new MockUp<SQLServerBulkCopy>() {
			@Mock
			private boolean writeBatchData(TDSWriter tdsWriter, TDSCommand command, boolean insertRowByRow)
					throws SQLServerException {
				// this method is one of the core methods used in sending bulk copies, so its
				// ideal for timeout simulation
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new RuntimeException("Unexpected interrupt", e);
				}
				return true;
			}
		};
		assertThrows(SQLException.class, () -> testBulkCopyWithTimeout(1), "The query has timed out.");
	}

	/**
	 * To verify SQLException: The timeout argument cannot be negative.
	 * 
	 * @throws SQLException
	 */
	@Test
	@DisplayName("BulkCopy:test negative timeout")
	public void testNegativeTimeOut() {
		assertThrows(SQLException.class, () -> testBulkCopyWithTimeout(-1), "The timeout argument cannot be negative.");
	}

	private void testBulkCopyWithTimeout(int timeout) throws SQLException {
		BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
		bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
		SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
		option.setBulkCopyTimeout(timeout);
		bulkWrapper.useBulkCopyOptions(true);
		bulkWrapper.setBulkOptions(option);
		BulkCopyTestUtil.performBulkCopyWithoutErrorHandling(bulkWrapper, sourceTable, false);
	}
}
