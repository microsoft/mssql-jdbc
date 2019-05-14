/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.dataclassification.InformationType;
import com.microsoft.sqlserver.jdbc.dataclassification.Label;
import com.microsoft.sqlserver.jdbc.dataclassification.SensitivityProperty;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
public class DataClassificationTest extends AbstractTest {
    private static final String tableName = RandomUtil.getIdentifier("DataClassification");

    /**
     * Tests data classification metadata information from SQL Server
     * 
     * @throws Exception
     */
    @Test
    public void testDataClassificationMetadata() throws Exception {
        try (Statement stmt = connection.createStatement();) {
            assert (TestUtils.serverSupportsDataClassification(stmt));
            createTable(connection, stmt);
            runTestsForServer(stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
        }
    }

    /**
     * Creates a new table in database with data classification column tags Inserts rows of data in the table
     * 
     * @param connection
     * @param stmt
     * @throws SQLException
     */
    private void createTable(Connection connection, Statement stmt) throws SQLException {
        String createQuery = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " ("
                + "[Id] [int] IDENTITY(1,1) NOT NULL," + "[CompanyName] [nvarchar](40) NOT NULL,"
                + "[ContactName] [nvarchar](50) NULL," + "[ContactTitle] [nvarchar](40) NULL,"
                + "[City] [nvarchar](40) NULL," + "[CountryName] [nvarchar](40) NULL,"
                + "[Phone] [nvarchar](30) MASKED WITH (FUNCTION = 'default()') NULL,"
                + "[Fax] [nvarchar](30) MASKED WITH (FUNCTION = 'default()') NULL)";
        stmt.execute(createQuery);

        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + ".CompanyName WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Company name', INFORMATION_TYPE_ID='COMPANY')");
        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + ".ContactName WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Person name', INFORMATION_TYPE_ID='NAME')");
        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + ".Phone WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Contact Information', INFORMATION_TYPE_ID='CONTACT')");
        stmt.execute("ADD SENSITIVITY CLASSIFICATION TO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + ".Fax WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='Contact Information', INFORMATION_TYPE_ID='CONTACT')");

        // INSERT ROWS OF DATA
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (?,?,?,?,?,?,?)")) {

            ps.setString(1, "Exotic Liquids");
            ps.setString(2, "Charlotte Cooper");
            ps.setObject(3, null);
            ps.setObject(4, "London");
            ps.setString(5, "UK");
            ps.setString(6, "(171) 555-2222");
            ps.setString(7, null);
            ps.execute();

            ps.setString(1, "New Orleans");
            ps.setString(2, "Cajun Delights");
            ps.setObject(3, null);
            ps.setObject(4, "New Orleans");
            ps.setString(5, "USA");
            ps.setString(6, "(100) 555-4822");
            ps.setString(7, null);
            ps.execute();

            ps.setString(1, "Grandma Kelly's Homestead");
            ps.setString(2, "Regina Murphy");
            ps.setObject(3, null);
            ps.setObject(4, "Ann Arbor");
            ps.setString(5, "USA");
            ps.setString(6, "(313) 555-5735");
            ps.setString(7, "(313) 555-3349");
            ps.execute();
        }
    }

    /**
     * Selects data from the table and triggers verifySensitivityClassification method
     * 
     * @param stmt
     * @throws Exception
     */
    private void runTestsForServer(Statement stmt) throws Exception {
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            verifySensitivityClassification(rs);
        }
    }

    /**
     * Verifies ResultSet received to contain data classification information as set.
     * 
     * @param rs
     * @throws SQLException
     */
    private void verifySensitivityClassification(SQLServerResultSet rs) throws SQLException {
        if (null != rs.getSensitivityClassification()) {
            for (int columnPos = 0; columnPos < rs.getSensitivityClassification().getColumnSensitivities().size();
                    columnPos++) {
                for (SensitivityProperty sp : rs.getSensitivityClassification().getColumnSensitivities().get(columnPos)
                        .getSensitivityProperties()) {

                    List<InformationType> infoTypes = rs.getSensitivityClassification().getInformationTypes();
                    assert (infoTypes.size() == 3);
                    for (int i = 0; i < infoTypes.size(); i++) {
                        verifyInfoType(infoTypes.get(i), i + 1);
                    }

                    List<Label> labels = rs.getSensitivityClassification().getLabels();
                    assert (labels.size() == 1);
                    verifyLabel(labels.get(0));

                    if (columnPos == 1 || columnPos == 2 || columnPos == 6 || columnPos == 7) {
                        verifyLabel(sp.getLabel());
                        verifyInfoType(sp.getInformationType(), columnPos);
                    }
                }
            }
        }
    }

    private void verifyInfoType(InformationType informationType, int i) {
        assert (informationType != null);
        assert (informationType.getId().equalsIgnoreCase(i == 1 ? "COMPANY" : (i == 2 ? "NAME" : "CONTACT")));
        assert (informationType.getName()
                .equalsIgnoreCase(i == 1 ? "Company name" : (i == 2 ? "Person Name" : "Contact Information")));
    }

    private void verifyLabel(Label label) {
        assert (label != null);
        assert (label.getId().equalsIgnoreCase("L1"));
        assert (label.getName().equalsIgnoreCase("PII"));
    }
}
