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
public class SelectTest extends AbstractTest {

    @Test
    public void basicSelectTest() {
        ArrayList<Pair<String, String>> valuePair = new ArrayList<>();
        // minor case sensitivity checking
        valuePair.add(Pair.of("select * from jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SelECt * fRom jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT c1, c2 FROM jdbctest, jdbctest2;", "jdbctest , jdbctest2"));

        // escape sequence
        valuePair.add(Pair.of("select * from [jdbctest]]]", "[jdbctest]]]"));
        valuePair.add(Pair.of("select * from [jdb]]ctest]", "[jdb]]ctest]"));
        valuePair.add(Pair.of("select * from [j[db]]ctest]", "[j[db]]ctest]"));

        // basic cases
        valuePair.add(Pair.of("SELECT * FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT * FROM jdbctest;", "jdbctest"));
        valuePair.add(Pair.of("SELECT * FROM /*hello this is a comment*/jdbctest;", "jdbctest"));
        valuePair.add(Pair.of("SELECT * FROM jdbctest ORDER BY blah...", "jdbctest"));
        valuePair.add(Pair.of("SELECT * FROM jdbctest WHERE blah...", "jdbctest"));
        valuePair.add(Pair.of("SELECT * FROM jdbctest HAVING blah...", "jdbctest"));
        valuePair.add(Pair.of("SELECT * FROM jdbctest OPTION blah...", "jdbctest"));
        valuePair.add(Pair.of("SELECT * FROM jdbctest GROUP BY blah...", "jdbctest"));

        // double quote literal
        valuePair.add(Pair.of("SELECT * FROM \"jdbc test\"", "\"jdbc test\""));
        valuePair.add(Pair.of("SELECT * FROM \"jdbc /*test*/\"", "\"jdbc /*test*/\""));
        valuePair.add(Pair.of("SELECT * FROM \"jdbc //test\"", "\"jdbc //test\""));
        valuePair.add(Pair.of("SELECT * FROM \"dbo\".\"jdbcDB\".\"jdbctest\"", "\"dbo\" . \"jdbcDB\" . \"jdbctest\""));
        valuePair.add(Pair.of("SELECT * FROM \"jdbctest\"", "\"jdbctest\""));

        // square bracket literal
        valuePair.add(Pair.of("SELECT * FROM [jdbctest]", "[jdbctest]"));
        valuePair.add(Pair.of("SELECT * FROM [dbo].[jdbcDB].[jdbctest]", "[dbo] . [jdbcDB] . [jdbctest]"));
        valuePair.add(Pair.of("SELECT * FROM [dbo].\"jdbcDB\".\"jdbctest\"", "[dbo] . \"jdbcDB\" . \"jdbctest\""));
        valuePair.add(Pair.of("SELECT * FROM [jdbc test]", "[jdbc test]"));
        valuePair.add(Pair.of("SELECT * FROM [jdbc /*test*/]", "[jdbc /*test*/]"));
        valuePair.add(Pair.of("SELECT * FROM [jdbc //test]", "[jdbc //test]"));

        // with parameters
        valuePair.add(Pair.of("SELECT c1,c2,c3 FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT (c1,c2,c3) FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT ?,?,? FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT (c1,?,c3) FROM jdbctest", "jdbctest"));

        // with special parameters
        valuePair.add(Pair.of("SELECT [c1],\"c2\" FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT [c1]]],\"c2\" FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT [c]]1],\"c2\" FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT [c1],\"[c2]\" FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT [\"c1],\"FROM\" FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT [\"c\"1\"],\"c2\" FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT ['FROM'1)],\"c2\" FROM jdbctest", "jdbctest"));
        valuePair.add(Pair.of("SELECT [((c)1{}],\"{{c}2)(\" FROM jdbctest", "jdbctest"));

        // with sub queries
        valuePair.add(Pair.of(
                "SELECT t.*, a+b AS total_sum FROM (SELECT SUM(col1) as a, SUM(col2) AS b FROM table ORDER BY a ASC) t ORDER BY total_sum DSC",
                "(SELECT SUM (col1 )as a , SUM (col2 )AS b FROM table ORDER BY a ASC ) t"));
        valuePair.add(Pair.of("SELECT col1 FROM myTestInts UNION "
                + "SELECT top 1 (select top 1 CONVERT(char(10), max(col1),121) a FROM myTestInts Order by a) FROM myTestInts Order by col1",
                "myTestInts UNION SELECT top 1 (select top 1 CONVERT (char (10 ), max (col1 ), 121 )a FROM myTestInts Order by a ) FROM myTestInts"));

        // Multiple Selects
        valuePair.add(Pair.of("SELECT * FROM table1;SELECT * FROM table2", "table1,table2"));
        valuePair.add(Pair.of("SELECT * FROM table1;SELECT * FROM table1", "table1"));

        valuePair.forEach(p -> assertEquals(p.getRight(), ParserUtils.getTableName(p.getLeft()).trim()));
    }

    /*
     * https://docs.microsoft.com/en-us/sql/t-sql/queries/select-examples-transact-sql?view=sql-server-2017
     */
    @Test
    public void selectExamplesTest() {
        ArrayList<Pair<String, String>> valuePair = new ArrayList<>();
        // A. Using SELECT to retrieve rows and columns
        valuePair.add(Pair.of("SELECT * FROM Production.Product ORDER BY Name ASC;", "Production . Product"));
        valuePair.add(
                Pair.of("SELECT p.* FROM Production.Product AS p ORDER BY Name ASC;", "Production . Product AS p"));
        valuePair.add(
                Pair.of("SELECT Name, ProductNumber, ListPrice AS Price FROM Production.Product ORDER BY Name ASC;",
                        "Production . Product"));
        valuePair.add(Pair.of(
                "SELECT Name, ProductNumber, ListPrice AS Price FROM Production.Product WHERE ProductLine = 'R' AND DaysToManufacture < 4 ORDER BY Name ASC;",
                "Production . Product"));
        // B. Using SELECT with column headings and calculations
        valuePair.add(Pair.of(
                "SELECT p.Name AS ProductName, NonDiscountSales = (OrderQty * UnitPrice), Discounts = ((OrderQty * UnitPrice) * UnitPriceDiscount) FROM Production.Product AS p INNER JOIN Sales.SalesOrderDetail AS sod ON p.ProductID = sod.ProductID ORDER BY ProductName DESC;",
                "Production . Product AS p INNER JOIN Sales . SalesOrderDetail AS sod ON p . ProductID = sod . ProductID"));
        valuePair.add(Pair.of(
                "SELECT 'Total income is', ((OrderQty * UnitPrice) * (1.0 - UnitPriceDiscount)), ' for ', p.Name AS ProductName FROM Production.Product AS p INNER JOIN Sales.SalesOrderDetail AS sod ON p.ProductID = sod.ProductID ORDER BY ProductName ASC;",
                "Production . Product AS p INNER JOIN Sales . SalesOrderDetail AS sod ON p . ProductID = sod . ProductID"));
        // C. Using DISTINCT with SELECT
        valuePair.add(Pair.of("SELECT DISTINCT JobTitle FROM HumanResources.Employee ORDER BY JobTitle;",
                "HumanResources . Employee"));
        // D. Creating tables with SELECT INTO
        valuePair.add(Pair.of("SELECT * INTO #Bicycles FROM AdventureWorks2012.Production.Product "
                + "WHERE ProductNumber LIKE 'BK%';", "AdventureWorks2012 . Production . Product"));
        valuePair.add(Pair.of("SELECT * INTO dbo.NewProducts FROM Production.Product "
                + "WHERE ListPrice > $25 AND ListPrice < $100;", "Production . Product"));
        // E. Using correlated subqueries
        valuePair.add(Pair.of(
                "SELECT DISTINCT Name\r\n" + "FROM Production.Product AS p WHERE EXISTS "
                        + "    (SELECT * FROM Production.ProductModel AS pm "
                        + "     WHERE p.ProductModelID = pm.ProductModelID "
                        + "           AND pm.Name LIKE 'Long-Sleeve Logo Jersey%');",
                "Production . Product AS p,Production . ProductModel AS pm"));
        valuePair.add(Pair.of(
                "SELECT DISTINCT Name FROM Production.Product WHERE ProductModelID IN "
                        + "    (SELECT ProductModelID FROM Production.ProductModel "
                        + "     WHERE Name LIKE 'Long-Sleeve Logo Jersey%');",
                "Production . Product,Production . ProductModel"));
        valuePair.add(Pair.of(
                "SELECT DISTINCT p.LastName, p.FirstName FROM Person.Person AS p "
                        + "JOIN HumanResources.Employee AS e "
                        + "ON e.BusinessEntityID = p.BusinessEntityID WHERE 5000.00 IN (SELECT Bonus "
                        + "FROM Sales.SalesPerson AS sp WHERE e.BusinessEntityID = sp.BusinessEntityID);",
                "Person . Person AS p JOIN HumanResources . Employee AS e ON e . BusinessEntityID = p . BusinessEntityID,Sales . SalesPerson AS sp"));
        // F. Using GROUP BY
        valuePair.add(Pair.of("SELECT SalesOrderID, SUM(LineTotal) AS SubTotal FROM Sales.SalesOrderDetail "
                + "GROUP BY SalesOrderID ORDER BY SalesOrderID;", "Sales . SalesOrderDetail"));
        // G. Using GROUP BY with multiple groups
        valuePair.add(Pair.of("SELECT ProductID, SpecialOfferID, AVG(UnitPrice) AS [Average Price], "
                + "SUM(LineTotal) AS SubTotal FROM Sales.SalesOrderDetail " + "GROUP BY ProductID, SpecialOfferID"
                + "ORDER BY ProductID;", "Sales . SalesOrderDetail"));
        // H. Using GROUP BY and WHERE
        valuePair.add(Pair.of(
                "SELECT ProductModelID, AVG(ListPrice) AS [Average List Price] FROM Production.Product "
                        + "WHERE ListPrice > $1000 GROUP BY ProductModelID ORDER BY ProductModelID;",
                "Production . Product"));
        // I. Using GROUP BY with an expression
        valuePair.add(Pair.of(
                "SELECT AVG(OrderQty) AS [Average Quantity], "
                        + "NonDiscountSales = (OrderQty * UnitPrice) FROM Sales.SalesOrderDetail "
                        + "GROUP BY (OrderQty * UnitPrice) ORDER BY (OrderQty * UnitPrice) DESC;",
                "Sales . SalesOrderDetail"));
        // J. Using GROUP BY with ORDER BY
        valuePair.add(Pair.of(
                "SELECT ProductID, AVG(UnitPrice) AS [Average Price] FROM Sales.SalesOrderDetail "
                        + "WHERE OrderQty > 10 GROUP BY ProductID ORDER BY AVG(UnitPrice);",
                "Sales . SalesOrderDetail"));
        // K. Using the HAVING clause
        valuePair.add(Pair.of(
                "SELECT ProductID FROM Sales.SalesOrderDetail GROUP BY ProductID HAVING AVG(OrderQty) > 5 ORDER BY ProductID",
                "Sales . SalesOrderDetail"));
        valuePair.add(Pair.of(
                "SELECT SalesOrderID, CarrierTrackingNumber FROM Sales.SalesOrderDetail "
                        + "GROUP BY SalesOrderID, CarrierTrackingNumber "
                        + "HAVING CarrierTrackingNumber LIKE '4BD%' ORDER BY SalesOrderID ;  ",
                "Sales . SalesOrderDetail"));
        // L. Using HAVING and GROUP BY
        valuePair.add(Pair.of(
                "SELECT ProductID FROM Sales.SalesOrderDetail WHERE UnitPrice < 25.00 "
                        + "GROUP BY ProductID HAVING AVG(OrderQty) > 5 ORDER BY ProductID;",
                "Sales . SalesOrderDetail"));
        // M. Using HAVING with SUM and AVG
        valuePair.add(Pair.of(
                "SELECT ProductID, AVG(OrderQty) AS AverageQuantity, SUM(LineTotal) AS Total\r\n"
                        + "FROM Sales.SalesOrderDetail\r\n" + "GROUP BY ProductID\r\n"
                        + "HAVING SUM(LineTotal) > $1000000.00\r\n" + "AND AVG(OrderQty) < 3;",
                "Sales . SalesOrderDetail"));
        valuePair.add(Pair.of(
                "SELECT ProductID, Total = SUM(LineTotal)\r\n" + "FROM Sales.SalesOrderDetail\r\n"
                        + "GROUP BY ProductID\r\n" + "HAVING SUM(LineTotal) > $2000000.00;",
                "Sales . SalesOrderDetail"));
        valuePair.add(Pair.of("SELECT ProductID, SUM(LineTotal) AS Total\r\n" + "FROM Sales.SalesOrderDetail\r\n"
                + "GROUP BY ProductID\r\n" + "HAVING COUNT(*) > 1500;", "Sales . SalesOrderDetail"));
        // N. Using the INDEX optimizer hint
        valuePair.add(Pair.of(
                "SELECT pp.FirstName, pp.LastName, e.NationalIDNumber "
                        + "FROM HumanResources.Employee AS e WITH (INDEX(AK_Employee_NationalIDNumber)) "
                        + "JOIN Person.Person AS pp on e.BusinessEntityID = pp.BusinessEntityID "
                        + "WHERE LastName = 'Johnson';",
                "HumanResources . Employee AS e WITH (INDEX (AK_Employee_NationalIDNumber )) JOIN Person . Person AS pp on e . BusinessEntityID = pp . BusinessEntityID"));
        valuePair.add(Pair.of(
                "SELECT pp.LastName, pp.FirstName, e.JobTitle "
                        + "FROM HumanResources.Employee AS e WITH (INDEX = 0) JOIN Person.Person AS pp "
                        + "ON e.BusinessEntityID = pp.BusinessEntityID WHERE LastName = 'Johnson';",
                "HumanResources . Employee AS e WITH (INDEX = 0 ) JOIN Person . Person AS pp ON e . BusinessEntityID = pp . BusinessEntityID"));
        // M. Using OPTION and the GROUP hints
        valuePair.add(Pair.of("SELECT ProductID, OrderQty, SUM(LineTotal) AS Total FROM Sales.SalesOrderDetail "
                + "WHERE UnitPrice < $5.00 GROUP BY ProductID, OrderQty "
                + "ORDER BY ProductID, OrderQty OPTION (HASH GROUP, FAST 10);", "Sales . SalesOrderDetail"));
        // O. Using the UNION query hint
        valuePair.add(Pair.of(
                "SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours "
                        + "FROM HumanResources.Employee AS e1 UNION "
                        + "SELECT BusinessEntityID, JobTitle, HireDate, VacationHours, SickLeaveHours "
                        + "FROM HumanResources.Employee AS e2 OPTION (MERGE UNION);",
                "HumanResources . Employee AS e1 UNION SELECT BusinessEntityID , JobTitle , HireDate , VacationHours , SickLeaveHours FROM HumanResources . Employee AS e2"));
        // P. Using a simple UNION
        valuePair.add(Pair.of("SELECT ProductModelID, Name FROM Production.ProductModel "
                + "WHERE ProductModelID NOT IN (3, 4) UNION SELECT ProductModelID, Name "
                + "FROM dbo.Gloves ORDER BY Name;", "Production . ProductModel,dbo . Gloves"));

        // Q. Using SELECT INTO with UNION
        valuePair.add(Pair.of("SELECT ProductModelID, Name INTO dbo.ProductResults "
                + "FROM Production.ProductModel WHERE ProductModelID NOT IN (3, 4) UNION "
                + "SELECT ProductModelID, Name FROM dbo.Gloves;", "Production . ProductModel,dbo . Gloves"));
        // R. Using UNION of two SELECT statements with ORDER BY*
        valuePair.add(Pair.of("SELECT ProductModelID, Name FROM Production.ProductModel "
                + "WHERE ProductModelID NOT IN (3, 4) UNION SELECT ProductModelID, Name "
                + "FROM dbo.Gloves ORDER BY Name;", "Production . ProductModel,dbo . Gloves"));
        // S. Using UNION of three SELECT statements to show the effects of ALL and parentheses
        valuePair.add(Pair.of(
                "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeOne UNION ALL "
                        + "SELECT LastName, FirstName ,JobTitle FROM dbo.EmployeeTwo UNION ALL "
                        + "SELECT LastName, FirstName,JobTitle FROM dbo.EmployeeThree;",
                "dbo . EmployeeOne UNION ALL SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeTwo UNION ALL SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeThree"));
        valuePair.add(Pair.of(
                "SELECT LastName, FirstName,JobTitle FROM dbo.EmployeeOne UNION "
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeTwo UNION "
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeThree;",
                "dbo . EmployeeOne UNION SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeTwo UNION SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeThree"));
        valuePair.add(Pair.of(
                "SELECT LastName, FirstName,JobTitle FROM [dbo].[EmployeeOne] UNION ALL ("
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeTwo UNION "
                        + "SELECT LastName, FirstName, JobTitle FROM dbo.EmployeeThree);",
                "[dbo] . [EmployeeOne] UNION ALL (SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeTwo UNION SELECT LastName , FirstName , JobTitle FROM dbo . EmployeeThree )"));

        valuePair.forEach(p -> assertEquals(p.getRight(), ParserUtils.getTableName(p.getLeft()).trim()));
    }
}
