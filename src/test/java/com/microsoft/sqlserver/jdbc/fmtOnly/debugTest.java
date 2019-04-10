package com.microsoft.sqlserver.jdbc.fmtOnly;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import javax.sql.PooledConnection;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ParserUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class debugTest extends AbstractTest {

    @Test
    public void test() {
        String SQL = "SELECT DISTINCT pp.LastName, pp.FirstName FROM Person.Person pp JOIN HumanResources.Employee e "
                + "ON e.BusinessEntityID = pp.BusinessEntityID WHERE pp.BusinessEntityID IN "
                + "(SELECT SalesPersonID FROM Sales.SalesOrderHeader WHERE SalesOrderID IN (SELECT SalesOrderID "
                + "FROM Sales.SalesOrderDetail WHERE ProductID IN (SELECT ProductID FROM Production.Product p "
                + "WHERE ProductNumber = ?)));";
        ParserUtils.getTableName(SQL);

        // for (String s : passing) {
        // ParserUtils.getTableName(s);
        // }
    }

    List<String> passing = Arrays.asList(
            "SELECT * FROM Sales.SalesOrderHeader AS h INNER JOIN Sales.SalesOrderDetail AS d WITH (FORCESEEK) "
                    + "ON h.SalesOrderID = d.SalesOrderID   \r\n" + "WHERE h.TotalDue > ? "
                    + "AND (? > d.OrderQty OR d.LineTotal < ? + ?);",
            "SELECT h.SalesOrderID, h.TotalDue, d.OrderQty " + "FROM Sales.SalesOrderHeader AS h "
                    + "INNER JOIN Sales.SalesOrderDetail AS d "
                    + "WITH (FORCESEEK (PK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID (SalesOrderID))) "
                    + "ON h.SalesOrderID = d.SalesOrderID " + "WHERE h.TotalDue > ? "
                    + "AND (d.OrderQty > ? OR d.LineTotal < ?;",
            "INSERT INTO Production.UnitMeasure (Name, UnitMeasureCode,  \r\n" + "    ModifiedDate)  \r\n"
                    + "VALUES (?, N'Square Feet ', '20080923'), (N'Y', ?, '20080923')\r\n"
                    + "    , (N'Y3', N'Cubic Yards', ?)",
            "INSERT INTO dbo.EmployeeSales  \r\n" + "    OUTPUT inserted.EmployeeID, inserted.FirstName, \r\n"
                    + "        inserted.LastName, inserted.YearlySales  \r\n"
                    + "    SELECT TOP (5) sp.BusinessEntityID, c.LastName, c.FirstName, sp.SalesYTD   \r\n"
                    + "    FROM Sales.SalesPerson AS sp  \r\n" + "    INNER JOIN Person.Person AS c  \r\n"
                    + "        ON sp.BusinessEntityID = c.BusinessEntityID  \r\n" + "    WHERE sp.SalesYTD > ?  \r\n"
                    + "    ORDER BY sp.SalesYTD DESC;",
            "INSERT INTO dbo.EmployeeSales  \r\n" + "    OUTPUT inserted.EmployeeID, inserted.FirstName, \r\n"
                    + "        inserted.LastName, inserted.YearlySales  \r\n"
                    + "    SELECT TOP (5) sp.BusinessEntityID, c.LastName, c.FirstName, sp.SalesYTD   \r\n"
                    + "    FROM Sales.SalesPerson AS sp  \r\n" + "    INNER JOIN Person.Person AS c  \r\n"
                    + "        ON sp.BusinessEntityID = c.BusinessEntityID  \r\n" + "    WHERE sp.SalesYTD > ?  \r\n"
                    + "    ORDER BY sp.SalesYTD DESC;",
            "INSERT INTO Production.Location WITH (XLOCK)  \r\n" + "(Name, CostRate, Availability)  \r\n"
                    + "VALUES ( ?, ?, ?);",
            "SELECT FirstName, LastName, StartDate AS FirstDay  \r\n" + "FROM DimEmployee   \r\n"
                    + "WHERE EndDate IS NOT NULL   \r\n" + "AND MaritalStatus = ?  \r\n" + "ORDER BY LastName; ",
            "SELECT DISTINCT pp.LastName, pp.FirstName \r\n"
                    + "FROM Person.Person pp JOIN HumanResources.Employee e\r\n"
                    + "ON e.BusinessEntityID = pp.BusinessEntityID WHERE pp.BusinessEntityID IN \r\n"
                    + "(SELECT SalesPersonID \r\n" + "FROM Sales.SalesOrderHeader\r\n" + "WHERE SalesOrderID IN \r\n"
                    + "(SELECT SalesOrderID \r\n" + "FROM Sales.SalesOrderDetail\r\n" + "WHERE ProductID IN \r\n"
                    + "(SELECT ProductID \r\n" + "FROM Production.Product p \r\n" + "WHERE ProductNumber = ?)));",
            "WITH DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS   \r\n" + "(  \r\n"
                    + "    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel  \r\n"
                    + "    FROM dbo.MyEmployees   \r\n" + "    WHERE ManagerID IS NULL  \r\n" + "    UNION ALL  \r\n"
                    + "    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1  \r\n"
                    + "    FROM dbo.MyEmployees AS e  \r\n" + "        INNER JOIN DirectReports AS d  \r\n"
                    + "        ON e.ManagerID = d.EmployeeID   \r\n" + ")  \r\n"
                    + "SELECT ManagerID, EmployeeID, Title, EmployeeLevel   \r\n" + "FROM DirectReports  \r\n"
                    + "WHERE EmployeeLevel <= ? ; ",
            "SELECT p.[Name] AS Product, p.ListPrice AS 'List Price'  \r\n" + "    FROM Production.Product AS p  \r\n"
                    + "    JOIN Production.ProductSubcategory AS s   \r\n"
                    + "      ON p.ProductSubcategoryID = s.ProductSubcategoryID  \r\n"
                    + "    WHERE s.[Name] LIKE ? AND p.ListPrice < ?; ",
            "SELECT ProductID, OrderQty, SUM(LineTotal) AS Total\r\n" + "FROM Sales.SalesOrderDetail\r\n"
                    + "WHERE UnitPrice < $?\r\n" + "GROUP BY ProductID, OrderQty\r\n"
                    + "ORDER BY ProductID, OrderQty\r\n" + "OPTION (HASH GROUP, FAST 10);",
            "SELECT * INTO dbo.NewProducts\r\n" + "FROM Production.Product\r\n" + "WHERE ListPrice > $? \r\n"
                    + "AND ListPrice < $?;",
            "SELECT DISTINCT p.LastName, p.FirstName \r\n" + "FROM Person.Person AS p \r\n"
                    + "JOIN HumanResources.Employee AS e\r\n"
                    + "    ON e.BusinessEntityID = p.BusinessEntityID WHERE ? IN\r\n" + "    (SELECT Bonus\r\n"
                    + "     FROM Sales.SalesPerson AS sp\r\n" + "     WHERE e.BusinessEntityID = sp.BusinessEntityID);",
            "SELECT ProductID FROM Sales.SalesOrderDetail WHERE UnitPrice < ? "
                    + "GROUP BY ProductID HAVING AVG(OrderQty) > ? ORDER BY ProductID;",
            "CREATE TABLE dbo.#Cars(Car_id int NOT NULL, " + "ColorCode varchar(10),ModelName varchar(20),Code int, "
                    + "DateEntered datetime) "
                    + "INSERT INTO dbo.#Cars (Car_id, ColorCode, ModelName, Code, DateEntered) VALUES (?,?,?,?,?)");
}
