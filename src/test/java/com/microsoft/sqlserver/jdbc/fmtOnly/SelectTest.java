package com.microsoft.sqlserver.jdbc.fmtOnly;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ParserUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class SelectTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @Test
    public void basicSelectTest() {
        // minor case sensitivity checking
        ParserUtils.compareTableName("select * from jdbctest", "jdbctest");
        ParserUtils.compareTableName("SelECt * fRom jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT c1, c2 FROM jdbctest, jdbctest2;", "jdbctest , jdbctest2");

        // escape sequence
        ParserUtils.compareTableName("select * from [jdbctest]]]", "[jdbctest]]]");
        ParserUtils.compareTableName("select * from [jdb]]ctest]", "[jdb]]ctest]");
        ParserUtils.compareTableName("select * from [j[db]]ctest]", "[j[db]]ctest]");

        // basic cases
        ParserUtils.compareTableName("SELECT * FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT * FROM jdbctest;", "jdbctest");
        ParserUtils.compareTableName("SELECT * FROM /*hello this is a comment*/jdbctest;", "jdbctest");
        ParserUtils.compareTableName("SELECT * FROM jdbctest ORDER BY blah...", "jdbctest");
        ParserUtils.compareTableName("SELECT * FROM jdbctest WHERE blah...", "jdbctest");
        ParserUtils.compareTableName("SELECT * FROM jdbctest HAVING blah...", "jdbctest");
        ParserUtils.compareTableName("SELECT * FROM jdbctest OPTION blah...", "jdbctest");
        ParserUtils.compareTableName("SELECT * FROM jdbctest GROUP BY blah...", "jdbctest");

        // double quote literal
        ParserUtils.compareTableName("SELECT * FROM \"jdbc test\"", "\"jdbc test\"");
        ParserUtils.compareTableName("SELECT * FROM \"jdbc /*test*/\"", "\"jdbc /*test*/\"");
        ParserUtils.compareTableName("SELECT * FROM \"jdbc //test\"", "\"jdbc //test\"");
        ParserUtils.compareTableName("SELECT * FROM \"dbo\".\"jdbcDB\".\"jdbctest\"",
                "\"dbo\" . \"jdbcDB\" . \"jdbctest\"");
        ParserUtils.compareTableName("SELECT * FROM \"jdbctest\"", "\"jdbctest\"");

        // square bracket literal
        ParserUtils.compareTableName("SELECT * FROM [jdbctest]", "[jdbctest]");
        ParserUtils.compareTableName("SELECT * FROM [dbo].[jdbcDB].[jdbctest]", "[dbo] . [jdbcDB] . [jdbctest]");
        ParserUtils.compareTableName("SELECT * FROM [dbo].\"jdbcDB\".\"jdbctest\"",
                "[dbo] . \"jdbcDB\" . \"jdbctest\"");
        ParserUtils.compareTableName("SELECT * FROM [jdbc test]", "[jdbc test]");
        ParserUtils.compareTableName("SELECT * FROM [jdbc /*test*/]", "[jdbc /*test*/]");
        ParserUtils.compareTableName("SELECT * FROM [jdbc //test]", "[jdbc //test]");

        // with parameters
        ParserUtils.compareTableName("SELECT c1,c2,c3 FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT (c1,c2,c3) FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT ?,?,? FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT (c1,?,c3) FROM jdbctest", "jdbctest");

        // with special parameters
        ParserUtils.compareTableName("SELECT [c1],\"c2\" FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT [c1]]],\"c2\" FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT [c]]1],\"c2\" FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT [c1],\"[c2]\" FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT [\"c1],\"FROM\" FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT [\"c\"1\"],\"c2\" FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT ['FROM'1)],\"c2\" FROM jdbctest", "jdbctest");
        ParserUtils.compareTableName("SELECT [((c)1{}],\"{{c}2)(\" FROM jdbctest", "jdbctest");

        // with sub queries
        ParserUtils.compareTableName(
                "SELECT t.*, a+b AS total_sum FROM (SELECT SUM(col1) as a, SUM(col2) AS b FROM table ORDER BY a ASC) t ORDER BY total_sum DSC",
                "(SELECT SUM (col1 )as a , SUM (col2 )AS b FROM table ORDER BY a ASC ) t");
        ParserUtils.compareTableName("SELECT col1 FROM myTestInts UNION "
                + "SELECT top 1 (select top 1 CONVERT(char(10), max(col1),121) a FROM myTestInts Order by a) FROM myTestInts Order by col1",
                "myTestInts UNION SELECT top 1 (select top 1 CONVERT (char (10 ), max (col1 ), 121 )a FROM myTestInts Order by a ) FROM myTestInts");

        // Multiple Selects
        ParserUtils.compareTableName("SELECT * FROM table1;SELECT * FROM table2", "table1,table2");
        ParserUtils.compareTableName("SELECT * FROM table1;SELECT * FROM table1", "table1");
    }

    /*
     * A collection of SELECT T-SQL statements from the Microsoft docs.
     * @link https://docs.microsoft.com/en-us/sql/t-sql/queries/select-examples-transact-sql?view=sql-server-2017
     */
    @Test
    public void selectExamplesTest() {
        // A. Using SELECT to retrieve rows and columns
        ParserUtils.compareTableName("SELECT * FROM Production.Product ORDER BY Name ASC;", "Production . Product");
        ParserUtils.compareTableName("SELECT p.* FROM Production.Product AS p ORDER BY Name ASC;",
                "Production . Product AS p");
        ParserUtils.compareTableName(
                "SELECT Name, ProductNumber, ListPrice AS Price FROM Production.Product ORDER BY Name ASC;",
                "Production . Product");
        ParserUtils.compareTableName(
                "SELECT Name, ProductNumber, ListPrice AS Price FROM Production.Product WHERE ProductLine = 'R' AND DaysToManufacture < 4 ORDER BY Name ASC;",
                "Production . Product");
        // B. Using SELECT with column headings and calculations
        ParserUtils.compareTableName(
                "SELECT p.Name AS ProductName, NonDiscountSales = (OrderQty * UnitPrice), Discounts = ((OrderQty * UnitPrice) * UnitPriceDiscount) FROM Production.Product AS p INNER JOIN Sales.SalesOrderDetail AS sod ON p.ProductID = sod.ProductID ORDER BY ProductName DESC;",
                "Production . Product AS p INNER JOIN Sales . SalesOrderDetail AS sod ON p . ProductID = sod . ProductID");
        ParserUtils.compareTableName(
                "SELECT 'Total income is', ((OrderQty * UnitPrice) * (1.0 - UnitPriceDiscount)), ' for ', p.Name AS ProductName FROM Production.Product AS p INNER JOIN Sales.SalesOrderDetail AS sod ON p.ProductID = sod.ProductID ORDER BY ProductName ASC;",
                "Production . Product AS p INNER JOIN Sales . SalesOrderDetail AS sod ON p . ProductID = sod . ProductID");
        // C. Using DISTINCT with SELECT
        ParserUtils.compareTableName("SELECT DISTINCT JobTitle FROM HumanResources.Employee ORDER BY JobTitle;",
                "HumanResources . Employee");
        // D. Creating tables with SELECT INTO
        ParserUtils.compareTableName("SELECT * INTO #Bicycles FROM AdventureWorks2012.Production.Product "
                + "WHERE ProductNumber LIKE 'BK%';", "AdventureWorks2012 . Production . Product");
        ParserUtils.compareTableName("SELECT * INTO dbo.NewProducts FROM Production.Product "
                + "WHERE ListPrice > $25 AND ListPrice < $100;", "Production . Product");
        // E. Using correlated subqueries
        ParserUtils.compareTableName(
                "SELECT DISTINCT Name\r\n" + "FROM Production.Product AS p WHERE EXISTS "
                        + "    (SELECT * FROM Production.ProductModel AS pm "
                        + "     WHERE p.ProductModelID = pm.ProductModelID "
                        + "           AND pm.Name LIKE 'Long-Sleeve Logo Jersey%');",
                "Production . Product AS p,Production . ProductModel AS pm");
        ParserUtils.compareTableName(
                "SELECT DISTINCT Name FROM Production.Product WHERE ProductModelID IN "
                        + "    (SELECT ProductModelID FROM Production.ProductModel "
                        + "     WHERE Name LIKE 'Long-Sleeve Logo Jersey%');",
                "Production . Product,Production . ProductModel");
        ParserUtils.compareTableName(
                "SELECT DISTINCT p.LastName, p.FirstName FROM Person.Person AS p "
                        + "JOIN HumanResources.Employee AS e "
                        + "ON e.BusinessEntityID = p.BusinessEntityID WHERE 5000.00 IN (SELECT Bonus "
                        + "FROM Sales.SalesPerson AS sp WHERE e.BusinessEntityID = sp.BusinessEntityID);",
                "Person . Person AS p JOIN HumanResources . Employee AS e ON e . BusinessEntityID = p . BusinessEntityID,Sales . SalesPerson AS sp");
        // F. Using GROUP BY
        ParserUtils.compareTableName("SELECT SalesOrderID, SUM(LineTotal) AS SubTotal FROM Sales.SalesOrderDetail "
                + "GROUP BY SalesOrderID ORDER BY SalesOrderID;", "Sales . SalesOrderDetail");
        // G. Using GROUP BY with multiple groups
        ParserUtils.compareTableName("SELECT ProductID, SpecialOfferID, AVG(UnitPrice) AS [Average Price], "
                + "SUM(LineTotal) AS SubTotal FROM Sales.SalesOrderDetail " + "GROUP BY ProductID, SpecialOfferID"
                + "ORDER BY ProductID;", "Sales . SalesOrderDetail");
        // H. Using GROUP BY and WHERE
        ParserUtils.compareTableName(
                "SELECT ProductModelID, AVG(ListPrice) AS [Average List Price] FROM Production.Product "
                        + "WHERE ListPrice > $1000 GROUP BY ProductModelID ORDER BY ProductModelID;",
                "Production . Product");
        // I. Using GROUP BY with an expression
        ParserUtils.compareTableName(
                "SELECT AVG(OrderQty) AS [Average Quantity], "
                        + "NonDiscountSales = (OrderQty * UnitPrice) FROM Sales.SalesOrderDetail "
                        + "GROUP BY (OrderQty * UnitPrice) ORDER BY (OrderQty * UnitPrice) DESC;",
                "Sales . SalesOrderDetail");
        // J. Using GROUP BY with ORDER BY
        ParserUtils.compareTableName(
                "SELECT ProductID, AVG(UnitPrice) AS [Average Price] FROM Sales.SalesOrderDetail "
                        + "WHERE OrderQty > 10 GROUP BY ProductID ORDER BY AVG(UnitPrice);",
                "Sales . SalesOrderDetail");
        // K. Using the HAVING clause
        ParserUtils.compareTableName(
                "SELECT ProductID FROM Sales.SalesOrderDetail GROUP BY ProductID HAVING AVG(OrderQty) > 5 ORDER BY ProductID",
                "Sales . SalesOrderDetail");
        ParserUtils.compareTableName(
                "SELECT SalesOrderID, CarrierTrackingNumber FROM Sales.SalesOrderDetail "
                        + "GROUP BY SalesOrderID, CarrierTrackingNumber "
                        + "HAVING CarrierTrackingNumber LIKE '4BD%' ORDER BY SalesOrderID ;  ",
                "Sales . SalesOrderDetail");
        // L. Using HAVING and GROUP BY
        ParserUtils.compareTableName(
                "SELECT ProductID FROM Sales.SalesOrderDetail WHERE UnitPrice < 25.00 "
                        + "GROUP BY ProductID HAVING AVG(OrderQty) > 5 ORDER BY ProductID;",
                "Sales . SalesOrderDetail");
        // M. Using HAVING with SUM and AVG
        ParserUtils.compareTableName(
                "SELECT ProductID, AVG(OrderQty) AS AverageQuantity, SUM(LineTotal) AS Total\r\n"
                        + "FROM Sales.SalesOrderDetail\r\n" + "GROUP BY ProductID\r\n"
                        + "HAVING SUM(LineTotal) > $1000000.00\r\n" + "AND AVG(OrderQty) < 3;",
                "Sales . SalesOrderDetail");
        ParserUtils.compareTableName(
                "SELECT ProductID, Total = SUM(LineTotal)\r\n" + "FROM Sales.SalesOrderDetail\r\n"
                        + "GROUP BY ProductID\r\n" + "HAVING SUM(LineTotal) > $2000000.00;",
                "Sales . SalesOrderDetail");
        ParserUtils.compareTableName("SELECT ProductID, SUM(LineTotal) AS Total\r\n" + "FROM Sales.SalesOrderDetail\r\n"
                + "GROUP BY ProductID\r\n" + "HAVING COUNT(*) > 1500;", "Sales . SalesOrderDetail");
        // N. Using the INDEX optimizer hint
        ParserUtils.compareTableName(
                "SELECT pp.FirstName, pp.LastName, e.NationalIDNumber "
                        + "FROM HumanResources.Employee AS e WITH (INDEX(AK_Employee_NationalIDNumber)) "
                        + "JOIN Person.Person AS pp on e.BusinessEntityID = pp.BusinessEntityID "
                        + "WHERE LastName = 'Johnson';",
                "HumanResources . Employee AS e WITH (INDEX (AK_Employee_NationalIDNumber )) JOIN Person . Person AS pp on e . BusinessEntityID = pp . BusinessEntityID");
        ParserUtils.compareTableName(
                "SELECT pp.LastName, pp.FirstName, e.JobTitle "
                        + "FROM HumanResources.Employee AS e WITH (INDEX = 0) JOIN Person.Person AS pp "
                        + "ON e.BusinessEntityID = pp.BusinessEntityID WHERE LastName = 'Johnson';",
                "HumanResources . Employee AS e WITH (INDEX = 0 ) JOIN Person . Person AS pp ON e . BusinessEntityID = pp . BusinessEntityID");
        // M. Using OPTION and the GROUP hints
        ParserUtils.compareTableName("SELECT ProductID, OrderQty, SUM(LineTotal) AS Total FROM Sales.SalesOrderDetail "
                + "WHERE UnitPrice < $5.00 GROUP BY ProductID, OrderQty "
                + "ORDER BY ProductID, OrderQty OPTION (HASH GROUP, FAST 10);", "Sales . SalesOrderDetail");
        // O. Using the UNION query hint
        ParserUtils.compareTableName(
                "SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours "
                        + "FROM HumanResources.Employee AS e1 UNION "
                        + "SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours "
                        + "FROM HumanResources.Employee AS e2 OPTION (MERGE UNION);",
                "HumanResources . Employee AS e1 UNION SELECT BusinessEntityID , JobTitle , HireDate , VacationHours , SickLeaveHours FROM HumanResources . Employee AS e2");
        // P. Using a simple UNION
        ParserUtils.compareTableName("SELECT ProductModelID, Name FROM Production.ProductModel "
                + "WHERE ProductModelID NOT IN (3, 4) UNION SELECT ProductModelID, Name "
                + "FROM dbo.Gloves ORDER BY Name;", "Production . ProductModel,dbo . Gloves");

        // Q. Using SELECT INTO with UNION
        ParserUtils.compareTableName("SELECT ProductModelID, Name INTO dbo.ProductResults "
                + "FROM Production.ProductModel WHERE ProductModelID NOT IN (3, 4) UNION "
                + "SELECT ProductModelID, Name FROM dbo.Gloves;", "Production . ProductModel,dbo . Gloves");
        // R. Using UNION of two SELECT statements with ORDER BY*
        ParserUtils.compareTableName("SELECT ProductModelID, Name FROM Production.ProductModel "
                + "WHERE ProductModelID NOT IN (3, 4) UNION SELECT ProductModelID, Name "
                + "FROM dbo.Gloves ORDER BY Name;", "Production . ProductModel,dbo . Gloves");
        // S. Using UNION of three SELECT statements to show the effects of ALL and parentheses
        ParserUtils.compareTableName(
                "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeOne UNION ALL "
                        + "SELECT LastName, FirstName ,JobTitle FROM dbo.EmployeeTwo UNION ALL "
                        + "SELECT LastName, FirstName,JobTitle FROM dbo.EmployeeThree;",
                "dbo . EmployeeOne UNION ALL SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeTwo UNION ALL SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeThree");
        ParserUtils.compareTableName(
                "SELECT LastName, FirstName,JobTitle FROM dbo.EmployeeOne UNION "
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeTwo UNION "
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeThree;",
                "dbo . EmployeeOne UNION SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeTwo UNION SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeThree");
        ParserUtils.compareTableName(
                "SELECT LastName, FirstName,JobTitle FROM [dbo].[EmployeeOne] UNION ALL ("
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeTwo UNION "
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeThree);",
                "[dbo] . [EmployeeOne] UNION ALL (SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeTwo UNION SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeThree )");
    }
}
