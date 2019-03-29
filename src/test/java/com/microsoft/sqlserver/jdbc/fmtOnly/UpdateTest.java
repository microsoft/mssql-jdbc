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
                "HumanResources . Employee,(SELECT TOP 10 BusinessEntityID FROM HumanResources . Employee ORDER BY HireDate ASC ) AS th"));

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
                "Production . BillOfMaterials,Production . BillOfMaterials AS c JOIN Parts AS d ON c . ProductAssemblyID = d . AssemblyID"));

        // F. Using the WHERE CURRENT OF clause
        valuePair.add(Pair.of(
                "UPDATE HumanResources.EmployeePayHistory SET PayFrequency = 2 " + "WHERE CURRENT OF complex_cursor;",
                "HumanResources . EmployeePayHistory"));

        // G. Specifying a computed value
        valuePair.add(Pair.of("UPDATE Production.Product SET ListPrice = ListPrice * 2;  ", "Production . Product"));

        // H. Specifying a compound operator
        valuePair.add(Pair.of("UPDATE Production.Product SET ListPrice += @NewPrice WHERE Color = N'Red';  ",
                "Production . Product"));

        valuePair.add(Pair.of(
                "UPDATE Production.ScrapReason SET Name += ' - tool malfunction' WHERE ScrapReasonID BETWEEN 10 and 12;  ",
                "Production . ScrapReason"));
        
        // I. Specifying a subquery in the SET clause
        valuePair.add(Pair.of("UPDATE Sales.SalesPerson  \r\n" + 
                "SET SalesYTD = SalesYTD +   \r\n" + 
                "    (SELECT SUM(so.SubTotal)   \r\n" + 
                "     FROM Sales.SalesOrderHeader AS so  \r\n" + 
                "     WHERE so.OrderDate = (SELECT MAX(OrderDate)  \r\n" + 
                "                           FROM Sales.SalesOrderHeader AS so2  \r\n" + 
                "                           WHERE so2.SalesPersonID = so.SalesPersonID)  \r\n" + 
                "     AND Sales.SalesPerson.BusinessEntityID = so.SalesPersonID  \r\n" + 
                "     GROUP BY so.SalesPersonID);", "Sales . SalesPerson"));

        // J. Updating rows using DEFAULT values
        valuePair.add(Pair.of(
                "UPDATE Production.Location  \r\n" + "SET CostRate = DEFAULT  \r\n" + "WHERE CostRate > 20.00; ",
                "Production . Location"));

        // K. Specifying a view as the target object
        valuePair.add(Pair.of("UPDATE Person.vStateProvinceCountryRegion  \r\n"
                + "SET CountryRegionName = 'United States of America'  \r\n"
                + "WHERE CountryRegionName = 'United States';  ", "Person . vStateProvinceCountryRegion"));

        // L. Specifying a table alias as the target object
        // Possibly not supported
        // valuePair.add(Pair.of(
        // "UPDATE sr \r\n" + "SET sr.Name += ' - tool malfunction' \r\n"
        // + "FROM Production.ScrapReason AS sr \r\n" + "JOIN Production.WorkOrder AS wo \r\n"
        // + " ON sr.ScrapReasonID = wo.ScrapReasonID \r\n" + " AND wo.ScrappedQty > 300; ",
        // "sr, Production.ScrapReason AS sr JOIN Production.WorkOrder AS wo ON sr.ScrapReasonID = wo.ScrapReasonID"));

        // M. Specifying a table variable as the target object
        // Can't support temp tables
        // valuePair.add(Pair.of("INSERT INTO @MyTableVar (EmpID) \r\n" +
        // " SELECT BusinessEntityID FROM HumanResources.Employee;", "@MyTableVar"));

        // N. Using the UPDATE statement with information from another table
        valuePair.add(Pair.of(
                "UPDATE Sales.SalesPerson SET SalesYTD = SalesYTD + SubTotal " + "FROM Sales.SalesPerson AS sp  \r\n"
                        + "JOIN Sales.SalesOrderHeader AS so ON sp.BusinessEntityID = so.SalesPersonID "
                        + "     AND so.OrderDate = (SELECT MAX(OrderDate)  \r\n"
                        + "             FROM Sales.SalesOrderHeader  \r\n"
                        + "             WHERE SalesPersonID = sp.BusinessEntityID);  ",
                "Sales . SalesPerson, Sales . SalesPerson AS sp JOIN Sales . SalesOrderHeader AS so ON sp . BusinessEntityID = so.SalesPersonID,Sales . SalesOrderHeader"));
        valuePair.add(Pair.of("UPDATE Sales.SalesPerson  \r\n" + "SET SalesYTD = SalesYTD +   \r\n"
                + "    (SELECT SUM(so.SubTotal)   \r\n" + "     FROM Sales.SalesOrderHeader AS so  \r\n"
                + "     WHERE so.OrderDate = (SELECT MAX(OrderDate)  \r\n"
                + "                           FROM Sales.SalesOrderHeader AS so2  \r\n"
                + "                           WHERE so2.SalesPersonID = so.SalesPersonID)  \r\n"
                + "     AND Sales.SalesPerson.BusinessEntityID = so.SalesPersonID  \r\n"
                + "     GROUP BY so.SalesPersonID);", "s"));

        // O. Updating data in a remote table by using a linked server
        valuePair.add(Pair.of(
                "UPDATE MyLinkedServer.AdventureWorks2012.HumanResources.Department  \r\n"
                        + "SET GroupName = N'Public Relations'  \r\n" + "WHERE DepartmentID = 4;  ",
                "MyLinkedServer . AdventureWorks2012 . HumanResources . Department"));

        // P. Updating data in a remote table by using the OPENQUERY function
        valuePair.add(Pair.of(
                "UPDATE OPENQUERY (MyLinkedServer, 'SELECT GroupName FROM HumanResources.Department WHERE DepartmentID = 4')   \r\n"
                        + "SET GroupName = 'Sales and Marketing';  ",
                "OPENQUERY (MyLinkedServer, 'SELECT GroupName FROM HumanResources.Department WHERE DepartmentID = 4')"));

        // Q. Updating data in a remote table by using the OPENDATASOURCE function
        valuePair.add(Pair.of(
                "UPDATE OPENDATASOURCE('SQLNCLI', 'Data Source=<server name>;Integrated Security=SSPI').AdventureWorks2012.HumanResources.Department\r\n"
                        + "SET GroupName = 'Sales and Marketing' WHERE DepartmentID = 4;  ",
                "OPENDATASOURCE('SQLNCLI', 'Data Source=<server name>;Integrated Security=SSPI') . AdventureWorks2012 . HumanResources . Department"));

        // R. Using UPDATE with .WRITE to modify data in an nvarchar(max) column

        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        // valuePair.add(Pair.of("", ""));
        valuePair.forEach(p -> assertEquals(p.getRight(), ParserUtils.getTableName(p.getLeft()).trim()));
    }
}
