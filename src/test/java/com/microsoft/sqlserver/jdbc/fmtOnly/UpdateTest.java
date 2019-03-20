package com.microsoft.sqlserver.jdbc.fmtOnly;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ParserUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class UpdateTest extends AbstractTest {

    public void basicInsertTest() {
        ArrayList<Pair<String, String>> valuePair = new ArrayList<>();

        valuePair.forEach(p -> assertEquals(p.getRight(), ParserUtils.getTableName(p.getLeft()).trim()));
    }

    /*
     * https://docs.microsoft.com/en-us/sql/t-sql/queries/update-transact-sql?view=sql-server-2017#UpdateExamples
     */
    @Test
    public void updateExamplesTest() {
        ArrayList<Pair<String, String>> valuePair = new ArrayList<>();

        // A. Using DELETE with no WHERE clause
        valuePair.add(Pair.of("UPDATE Person.Address  \r\n" + "SET ModifiedDate = GETDATE();  ", "Person . Address"));

        // B. Updating multiple columns
        valuePair.add(
                Pair.of("UPDATE Sales.SalesPerson  \r\n" + "SET Bonus = 6000, CommissionPct = .10, SalesQuota = NULL;",
                        "Sales . SalesPerson"));

        // C. Using the WHERE clause
        valuePair.add(Pair.of("UPDATE Production.Product  \r\n" + "SET Color = N'Metallic Red'  \r\n"
                + "WHERE Name LIKE N'Road-250%' AND Color = N'Red';  ", "Production . Product"));

        // D. Using the TOP clause
        valuePair.add(
                Pair.of("UPDATE TOP (10) HumanResources.Employee\r\n" + "SET VacationHours = VacationHours * 1.25 ;",
                        "HumanResources . Employee"));
        valuePair.add(Pair.of(
                "UPDATE HumanResources.Employee  \r\n" + "SET VacationHours = VacationHours + 8  \r\n"
                        + "FROM (SELECT TOP 10 BusinessEntityID FROM HumanResources.Employee  \r\n"
                        + "     ORDER BY HireDate ASC) AS th  \r\n"
                        + "WHERE HumanResources.Employee.BusinessEntityID = th.BusinessEntityID;  ",
                "HumanResources . Employee ,(SELECT TOP 10 BusinessEntityID FROM HumanResources . Employee ORDER BY HireDate ASC ) AS th"));

        // E. Using the WITH common_table_expression clause
        valuePair.add(Pair.of("WITH Parts(AssemblyID, ComponentID, PerAssemblyQty, EndDate, ComponentLevel) AS  \r\n"
                + "(  \r\n" + "    SELECT b.ProductAssemblyID, b.ComponentID, b.PerAssemblyQty,  \r\n"
                + "        b.EndDate, 0 AS ComponentLevel  \r\n" + "    FROM Production.BillOfMaterials AS b  \r\n"
                + "    WHERE b.ProductAssemblyID = 800  \r\n" + "          AND b.EndDate IS NULL  \r\n"
                + "    UNION ALL  \r\n" + "    SELECT bom.ProductAssemblyID, bom.ComponentID, p.PerAssemblyQty,  \r\n"
                + "        bom.EndDate, ComponentLevel + 1  \r\n" + "    FROM Production.BillOfMaterials AS bom   \r\n"
                + "        INNER JOIN Parts AS p  \r\n" + "        ON bom.ProductAssemblyID = p.ComponentID  \r\n"
                + "        AND bom.EndDate IS NULL  \r\n" + ")  \r\n" + "UPDATE Production.BillOfMaterials  \r\n"
                + "SET PerAssemblyQty = c.PerAssemblyQty * 2  \r\n" + "FROM Production.BillOfMaterials AS c  \r\n"
                + "JOIN Parts AS d ON c.ProductAssemblyID = d.AssemblyID  \r\n" + "WHERE d.ComponentLevel = 0;",
                "Production . BillOfMaterials ,Production . BillOfMaterials AS c JOIN Parts AS d ON c . ProductAssemblyID = d . AssemblyID"));

//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
//        valuePair.add(Pair.of("", ""));
        valuePair.forEach(p -> assertEquals(p.getRight(), ParserUtils.getTableName(p.getLeft()).trim()));
    }
}
