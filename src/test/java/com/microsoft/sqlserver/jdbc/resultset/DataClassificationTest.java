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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.dataclassification.InformationType;
import com.microsoft.sqlserver.jdbc.dataclassification.Label;
import com.microsoft.sqlserver.jdbc.dataclassification.SensitivityProperty;
import com.microsoft.sqlserver.jdbc.dataclassification.SensitivityClassification.SensitivityRank;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class DataClassificationTest extends AbstractTest {
    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("DataClassification"));

    private static final String addSensitivitySql = "ADD SENSITIVITY CLASSIFICATION TO %s.%s WITH (LABEL='PII', LABEL_ID='L1', INFORMATION_TYPE='%s', INFORMATION_TYPE_ID='%s'%s)";
    private static final String sensitivityRankSql = ", RANK=%s";

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /**
     * Tests data classification metadata information from SQL Server
     * 
     * TODO: remove xAzureSQLDW tag once issue on server is fixed (currently DW not returning rank info) VSO issue 12931
     * 
     * @throws Exception
     */
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xAzureSQLDW)
    @Test
    public void testDataClassificationMetadata() throws Exception {
        try (Statement stmt = connection.createStatement();) {
            if (!TestUtils.serverSupportsDataClassification(stmt)) {
                fail(TestResource.getResource("R_dataClassificationNotSupported"));
            }

            for (SensitivityRank i : SensitivityRank.values()) {
                if (SensitivityRank.NOT_DEFINED != i) {
                    createTable(connection, stmt);
                    addSensitivity(connection, stmt, i.toString());
                    insertData(connection, stmt);
                    try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName
                            + "ORDER BY [CompanyName], [ContactTitle], [CountryName], [Phone], [Fax]")) {
                        verifySensitivityClassification(rs, i.getValue());
                    }
                    dropTable();
                }
            }
        }
    }

    /**
     * Tests data classification not supported
     * 
     * @throws SQLException
     * @throws SQLServerException
     * 
     * @throws Exception
     */
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xSQLv15)
    @Test
    public void testDataClassificationNotSupported() throws Exception {
        try (Statement stmt = connection.createStatement();) {
            if (TestUtils.serverSupportsDataClassification(stmt)) {
                fail(TestResource.getResource("R_expectedFailPassed"));
            }
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
        String createQuery = "CREATE TABLE " + tableName + " (" + "[Id] [int] IDENTITY(1,1) NOT NULL,"
                + "[CompanyName] [nvarchar](40) NOT NULL," + "[ContactName] [nvarchar](50) NULL,"
                + "[ContactTitle] [nvarchar](40) NULL," + "[City] [nvarchar](40) NULL,"
                + "[CountryName] [nvarchar](40) NULL,"
                + "[Phone] [nvarchar](30) MASKED WITH (FUNCTION = 'default()') NULL,"
                + "[Fax] [nvarchar](30) MASKED WITH (FUNCTION = 'default()') NULL)";
        stmt.execute(createQuery);
    }

    private void addSensitivity(Connection connection, Statement stmt, String rank) throws SQLException {
        String addRankSql = String.format(sensitivityRankSql, rank);
        stmt.execute(String.format(addSensitivitySql, tableName, "CompanyName", "Company name", "COMPANY", addRankSql));
        stmt.execute(String.format(addSensitivitySql, tableName, "ContactName", "Person name", "NAME", addRankSql));
        stmt.execute(
                String.format(addSensitivitySql, tableName, "Phone", "Contact Information", "CONTACT", addRankSql));
        stmt.execute(String.format(addSensitivitySql, tableName, "Fax", "Contact Information", "CONTACT", addRankSql));
    }

    private void insertData(Connection connection, Statement stmt) throws SQLException {
        try (PreparedStatement ps = connection
                .prepareStatement("INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?)")) {

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
     * Verifies ResultSet received to contain data classification information as set.
     * 
     * @param rs
     * @param rank
     * @throws SQLException
     */
    private void verifySensitivityClassification(SQLServerResultSet rs, int rank) throws SQLException {
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

                    verifyLabel(sp.getLabel());
                    verifyInfoType(sp.getInformationType(), columnPos);
                    assertEquals(rank, sp.getSensitivityRank(), TestResource.getResource("R_valuesAreDifferent"));

                    int sensitivityRank = rs.getSensitivityClassification().getSensitivityRank();
                    assertEquals(rank, sensitivityRank, TestResource.getResource("R_valuesAreDifferent"));
                }
            }
        }
    }

    private void verifyInfoType(InformationType informationType, int i) {
        assertTrue(informationType != null);
        assertTrue(informationType.getId().equalsIgnoreCase(i == 1 ? "COMPANY" : (i == 2 ? "NAME" : "CONTACT")));
        assertTrue(informationType.getName()
                .equalsIgnoreCase(i == 1 ? "Company name" : (i == 2 ? "Person Name" : "Contact Information")));
    }

    private void verifyLabel(Label label) {
        assertTrue(label != null);
        assertTrue(label.getId().equalsIgnoreCase("L1"));
        assertTrue(label.getName().equalsIgnoreCase("PII"));
    }

    @AfterAll
    public static void dropTable() throws Exception {
        try (Statement stmt = connection.createStatement();) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }
}
