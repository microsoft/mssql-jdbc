package com.microsoft.sqlserver.jdbc.fmtOnly;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ParserUtils;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class LexerTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /*
     * A collection of Common Table Expression T-SQL statements from the Microsoft docs.
     * @link
     * https://docs.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql?view=sql-server-2017
     */
    @Test
    public void testCTE() {
        // A. Creating a simple common table expression
        ParserUtils.compareCommonTableExpression("-- Define the CTE expression name and column list.  \r\n"
                + "WITH Sales_CTE (SalesPersonID, SalesOrderID, SalesYear)  \r\n" + "AS  \r\n"
                + "-- Define the CTE query.  \r\n" + "(  \r\n"
                + "    SELECT SalesPersonID, SalesOrderID, YEAR(OrderDate) AS SalesYear  \r\n"
                + "    FROM Sales.SalesOrderHeader  \r\n" + "    WHERE SalesPersonID IS NOT NULL  \r\n" + ")  \r\n"
                + "-- Define the outer query referencing the CTE name.  \r\n"
                + "SELECT SalesPersonID, COUNT(SalesOrderID) AS TotalSales, SalesYear  \r\n" + "FROM Sales_CTE  \r\n"
                + "GROUP BY SalesYear, SalesPersonID  \r\n" + "ORDER BY SalesPersonID, SalesYear;  ",
                "WITH Sales_CTE (SalesPersonID , SalesOrderID , SalesYear ) AS ( SELECT SalesPersonID , SalesOrderID , YEAR ( OrderDate ) AS SalesYear FROM Sales . SalesOrderHeader WHERE SalesPersonID IS NOT NULL )");

        // B. Using a common table expression to limit counts and report averages
        ParserUtils.compareCommonTableExpression("WITH Sales_CTE (SalesPersonID, NumberOfOrders)  \r\n" + "AS  \r\n"
                + "(  \r\n" + "    SELECT SalesPersonID, COUNT(*)  \r\n" + "    FROM Sales.SalesOrderHeader  \r\n"
                + "    WHERE SalesPersonID IS NOT NULL  \r\n" + "    GROUP BY SalesPersonID  \r\n" + ")  \r\n"
                + "SELECT AVG(NumberOfOrders) AS \"Average Sales Per Person\"  \r\n" + "FROM Sales_CTE;  \r\n" + "",
                "WITH Sales_CTE (SalesPersonID , NumberOfOrders ) AS ( SELECT SalesPersonID , COUNT ( * ) FROM Sales . SalesOrderHeader WHERE SalesPersonID IS NOT NULL GROUP BY SalesPersonID )");

        // C. Using multiple CTE definitions in a single query
        ParserUtils.compareCommonTableExpression("WITH Sales_CTE (SalesPersonID, TotalSales, SalesYear)  \r\n"
                + "AS  \r\n" + "-- Define the first CTE query.  \r\n" + "(  \r\n"
                + "    SELECT SalesPersonID, SUM(TotalDue) AS TotalSales, YEAR(OrderDate) AS SalesYear  \r\n"
                + "    FROM Sales.SalesOrderHeader  \r\n" + "    WHERE SalesPersonID IS NOT NULL  \r\n"
                + "       GROUP BY SalesPersonID, YEAR(OrderDate)  \r\n" + "  \r\n" + ")  \r\n"
                + ",   -- Use a comma to separate multiple CTE definitions.  \r\n" + "  \r\n"
                + "-- Define the second CTE query, which returns sales quota data by year for each sales person.  \r\n"
                + "Sales_Quota_CTE (BusinessEntityID, SalesQuota, SalesQuotaYear)  \r\n" + "AS  \r\n" + "(  \r\n"
                + "       SELECT BusinessEntityID, SUM(SalesQuota)AS SalesQuota, YEAR(QuotaDate) AS SalesQuotaYear  \r\n"
                + "       FROM Sales.SalesPersonQuotaHistory  \r\n"
                + "       GROUP BY BusinessEntityID, YEAR(QuotaDate)  \r\n" + ")  \r\n" + "  \r\n"
                + "-- Define the outer query by referencing columns from both CTEs.  \r\n"
                + "SELECT SalesPersonID  \r\n" + "  , SalesYear  \r\n"
                + "  , FORMAT(TotalSales,'C','en-us') AS TotalSales  \r\n" + "  , SalesQuotaYear  \r\n"
                + "  , FORMAT (SalesQuota,'C','en-us') AS SalesQuota  \r\n"
                + "  , FORMAT (TotalSales -SalesQuota, 'C','en-us') AS Amt_Above_or_Below_Quota  \r\n"
                + "FROM Sales_CTE  \r\n"
                + "JOIN Sales_Quota_CTE ON Sales_Quota_CTE.BusinessEntityID = Sales_CTE.SalesPersonID  \r\n"
                + "                    AND Sales_CTE.SalesYear = Sales_Quota_CTE.SalesQuotaYear  \r\n"
                + "ORDER BY SalesPersonID, SalesYear;",
                "WITH Sales_CTE (SalesPersonID , TotalSales , SalesYear ) AS ( SELECT SalesPersonID , SUM ( TotalDue ) AS TotalSales , YEAR ( OrderDate ) AS SalesYear FROM Sales . SalesOrderHeader WHERE SalesPersonID IS NOT NULL GROUP BY SalesPersonID , YEAR ( OrderDate ) ) , Sales_Quota_CTE (BusinessEntityID , SalesQuota , SalesQuotaYear ) AS ( SELECT BusinessEntityID , SUM ( SalesQuota ) AS SalesQuota , YEAR ( QuotaDate ) AS SalesQuotaYear FROM Sales . SalesPersonQuotaHistory GROUP BY BusinessEntityID , YEAR ( QuotaDate ) )");

        // D. Using a recursive common table expression to display multiple levels of recursion
        ParserUtils.compareCommonTableExpression(
                "WITH DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS   \r\n" + "(  \r\n"
                        + "    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel  \r\n"
                        + "    FROM dbo.MyEmployees   \r\n" + "    WHERE ManagerID IS NULL  \r\n"
                        + "    UNION ALL  \r\n"
                        + "    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1  \r\n"
                        + "    FROM dbo.MyEmployees AS e  \r\n" + "        INNER JOIN DirectReports AS d  \r\n"
                        + "        ON e.ManagerID = d.EmployeeID   \r\n" + ")  \r\n"
                        + "SELECT ManagerID, EmployeeID, Title, EmployeeLevel   \r\n" + "FROM DirectReports  \r\n"
                        + "ORDER BY ManagerID;",
                "WITH DirectReports (ManagerID , EmployeeID , Title , EmployeeLevel ) AS ( SELECT ManagerID , EmployeeID , Title , 0 AS EmployeeLevel FROM dbo . MyEmployees WHERE ManagerID IS NULL UNION ALL SELECT e . ManagerID , e . EmployeeID , e . Title , EmployeeLevel + 1 FROM dbo . MyEmployees AS e INNER JOIN DirectReports AS d ON e . ManagerID = d . EmployeeID )");

        // E. Using a recursive common table expression to display two levels of recursion
        ParserUtils.compareCommonTableExpression(
                "WITH DirectReports(ManagerID, EmployeeID, Title, EmployeeLevel) AS   \r\n" + "(  \r\n"
                        + "    SELECT ManagerID, EmployeeID, Title, 0 AS EmployeeLevel  \r\n"
                        + "    FROM dbo.MyEmployees   \r\n" + "    WHERE ManagerID IS NULL  \r\n"
                        + "    UNION ALL  \r\n"
                        + "    SELECT e.ManagerID, e.EmployeeID, e.Title, EmployeeLevel + 1  \r\n"
                        + "    FROM dbo.MyEmployees AS e  \r\n" + "        INNER JOIN DirectReports AS d  \r\n"
                        + "        ON e.ManagerID = d.EmployeeID   \r\n" + ")  \r\n"
                        + "SELECT ManagerID, EmployeeID, Title, EmployeeLevel   \r\n" + "FROM DirectReports  \r\n"
                        + "WHERE EmployeeLevel <= 2 ;",
                "WITH DirectReports (ManagerID , EmployeeID , Title , EmployeeLevel ) AS ( SELECT ManagerID , EmployeeID , Title , 0 AS EmployeeLevel FROM dbo . MyEmployees WHERE ManagerID IS NULL UNION ALL SELECT e . ManagerID , e . EmployeeID , e . Title , EmployeeLevel + 1 FROM dbo . MyEmployees AS e INNER JOIN DirectReports AS d ON e . ManagerID = d . EmployeeID )");

        // G. Using MAXRECURSION to cancel a statement
        ParserUtils.compareCommonTableExpression(
                "WITH cte (EmployeeID, ManagerID, Title)  \r\n" + "AS  \r\n" + "(  \r\n"
                        + "    SELECT EmployeeID, ManagerID, Title  \r\n" + "    FROM dbo.MyEmployees  \r\n"
                        + "    WHERE ManagerID IS NOT NULL  \r\n" + "  UNION ALL  \r\n"
                        + "    SELECT  e.EmployeeID, e.ManagerID, e.Title  \r\n" + "    FROM dbo.MyEmployees AS e  \r\n"
                        + "    JOIN cte ON e.ManagerID = cte.EmployeeID  \r\n" + ")  \r\n"
                        + "SELECT EmployeeID, ManagerID, Title  \r\n" + "FROM cte;",
                "WITH cte (EmployeeID , ManagerID , Title ) AS ( SELECT EmployeeID , ManagerID , Title FROM dbo . MyEmployees WHERE ManagerID IS NOT NULL UNION ALL SELECT e . EmployeeID , e . ManagerID , e . Title FROM dbo . MyEmployees AS e JOIN cte ON e . ManagerID = cte . EmployeeID )");

        // H. Using a common table expression to selectively step through a recursive relationship in a SELECT statement
        ParserUtils.compareCommonTableExpression(
                "WITH Parts(AssemblyID, ComponentID, PerAssemblyQty, EndDate, ComponentLevel) AS  \r\n" + "(  \r\n"
                        + "    SELECT b.ProductAssemblyID, b.ComponentID, b.PerAssemblyQty,  \r\n"
                        + "        b.EndDate, 0 AS ComponentLevel  \r\n"
                        + "    FROM Production.BillOfMaterials AS b  \r\n" + "    WHERE b.ProductAssemblyID = 800  \r\n"
                        + "          AND b.EndDate IS NULL  \r\n" + "    UNION ALL  \r\n"
                        + "    SELECT bom.ProductAssemblyID, bom.ComponentID, p.PerAssemblyQty,  \r\n"
                        + "        bom.EndDate, ComponentLevel + 1  \r\n"
                        + "    FROM Production.BillOfMaterials AS bom   \r\n" + "        INNER JOIN Parts AS p  \r\n"
                        + "        ON bom.ProductAssemblyID = p.ComponentID  \r\n"
                        + "        AND bom.EndDate IS NULL  \r\n" + ")  \r\n"
                        + "SELECT AssemblyID, ComponentID, Name, PerAssemblyQty, EndDate,  \r\n"
                        + "        ComponentLevel   \r\n" + "FROM Parts AS p  \r\n"
                        + "    INNER JOIN Production.Product AS pr  \r\n" + "    ON p.ComponentID = pr.ProductID  \r\n"
                        + "ORDER BY ComponentLevel, AssemblyID, ComponentID;",
                "WITH Parts (AssemblyID , ComponentID , PerAssemblyQty , EndDate , ComponentLevel ) AS ( SELECT b . ProductAssemblyID , b . ComponentID , b . PerAssemblyQty , b . EndDate , 0 AS ComponentLevel FROM Production . BillOfMaterials AS b WHERE b . ProductAssemblyID = 800 AND b . EndDate IS NULL UNION ALL SELECT bom . ProductAssemblyID , bom . ComponentID , p . PerAssemblyQty , bom . EndDate , ComponentLevel + 1 FROM Production . BillOfMaterials AS bom INNER JOIN Parts AS p ON bom . ProductAssemblyID = p . ComponentID AND bom . EndDate IS NULL )");

        // I. Using a recursive CTE in an UPDATE statement
        ParserUtils.compareCommonTableExpression(
                "WITH Parts(AssemblyID, ComponentID, PerAssemblyQty, EndDate, ComponentLevel) AS  \r\n" + "(  \r\n"
                        + "    SELECT b.ProductAssemblyID, b.ComponentID, b.PerAssemblyQty,  \r\n"
                        + "        b.EndDate, 0 AS ComponentLevel  \r\n"
                        + "    FROM Production.BillOfMaterials AS b  \r\n" + "    WHERE b.ProductAssemblyID = 800  \r\n"
                        + "          AND b.EndDate IS NULL  \r\n" + "    UNION ALL  \r\n"
                        + "    SELECT bom.ProductAssemblyID, bom.ComponentID, p.PerAssemblyQty,  \r\n"
                        + "        bom.EndDate, ComponentLevel + 1  \r\n"
                        + "    FROM Production.BillOfMaterials AS bom   \r\n" + "        INNER JOIN Parts AS p  \r\n"
                        + "        ON bom.ProductAssemblyID = p.ComponentID  \r\n"
                        + "        AND bom.EndDate IS NULL  \r\n" + ")  \r\n"
                        + "UPDATE Production.BillOfMaterials  \r\n" + "SET PerAssemblyQty = c.PerAssemblyQty * 2  \r\n"
                        + "FROM Production.BillOfMaterials AS c  \r\n"
                        + "JOIN Parts AS d ON c.ProductAssemblyID = d.AssemblyID  \r\n" + "WHERE d.ComponentLevel = 0;",
                "WITH Parts (AssemblyID , ComponentID , PerAssemblyQty , EndDate , ComponentLevel ) AS ( SELECT b . ProductAssemblyID , b . ComponentID , b . PerAssemblyQty , b . EndDate , 0 AS ComponentLevel FROM Production . BillOfMaterials AS b WHERE b . ProductAssemblyID = 800 AND b . EndDate IS NULL UNION ALL SELECT bom . ProductAssemblyID , bom . ComponentID , p . PerAssemblyQty , bom . EndDate , ComponentLevel + 1 FROM Production . BillOfMaterials AS bom INNER JOIN Parts AS p ON bom . ProductAssemblyID = p . ComponentID AND bom . EndDate IS NULL )");

        // J. Using multiple anchor and recursive members
        ParserUtils.compareCommonTableExpression("-- Create the recursive CTE to find all of Bonnie's ancestors.  \r\n"
                + "WITH Generation (ID) AS  \r\n" + "(  \r\n" + "-- First anchor member returns Bonnie's mother.  \r\n"
                + "    SELECT Mother   \r\n" + "    FROM dbo.Person  \r\n" + "    WHERE Name = 'Bonnie'  \r\n"
                + "UNION  \r\n" + "-- Second anchor member returns Bonnie's father.  \r\n" + "    SELECT Father   \r\n"
                + "    FROM dbo.Person  \r\n" + "    WHERE Name = 'Bonnie'  \r\n" + "UNION ALL  \r\n"
                + "-- First recursive member returns male ancestors of the previous generation.  \r\n"
                + "    SELECT Person.Father  \r\n" + "    FROM Generation, Person  \r\n"
                + "    WHERE Generation.ID=Person.ID  \r\n" + "UNION ALL  \r\n"
                + "-- Second recursive member returns female ancestors of the previous generation.  \r\n"
                + "    SELECT Person.Mother  \r\n" + "    FROM Generation, dbo.Person  \r\n"
                + "    WHERE Generation.ID=Person.ID  \r\n" + ")  \r\n"
                + "SELECT Person.ID, Person.Name, Person.Mother, Person.Father  \r\n"
                + "FROM Generation, dbo.Person  \r\n" + "WHERE Generation.ID = Person.ID;",
                "WITH Generation (ID ) AS ( SELECT Mother FROM dbo . Person WHERE Name = 'Bonnie' UNION SELECT Father FROM dbo . Person WHERE Name = 'Bonnie' UNION ALL SELECT Person . Father FROM Generation , Person WHERE Generation . ID = Person . ID UNION ALL SELECT Person . Mother FROM Generation , dbo . Person WHERE Generation . ID = Person . ID )");

        // K. Using analytical functions in a recursive CTE
        ParserUtils.compareCommonTableExpression("WITH vw AS  \r\n" + " (  \r\n" + "    SELECT itmIDComp, itmID  \r\n"
                + "    FROM @t1  \r\n" + "  \r\n" + "    UNION ALL  \r\n" + "  \r\n"
                + "    SELECT itmIDComp, itmID  \r\n" + "    FROM @t2  \r\n" + ")   \r\n" + ",r AS  \r\n" + " (  \r\n"
                + "    SELECT t.itmID AS itmIDComp  \r\n" + "           , NULL AS itmID  \r\n"
                + "           ,CAST(0 AS bigint) AS N  \r\n" + "           ,1 AS Lvl  \r\n"
                + "    FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) AS t (itmID)   \r\n"
                + "  \r\n" + "UNION ALL  \r\n" + "  \r\n" + "SELECT t.itmIDComp  \r\n" + "    , t.itmID  \r\n"
                + "    , ROW_NUMBER() OVER(PARTITION BY t.itmIDComp ORDER BY t.itmIDComp, t.itmID) AS N  \r\n"
                + "    , Lvl + 1  \r\n" + "FROM r   \r\n" + "    JOIN vw AS t ON t.itmID = r.itmIDComp  \r\n"
                + ")   \r\n" + "  \r\n" + "SELECT Lvl, N FROM r;",
                "WITH vw AS ( SELECT itmIDComp , itmID FROM @t1 UNION ALL SELECT itmIDComp , itmID FROM @t2 ) , r AS ( SELECT t . itmID AS itmIDComp , NULL AS itmID , CAST ( 0 AS bigint ) AS N , 1 AS Lvl FROM ( SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 ) AS t ( itmID ) UNION ALL SELECT t . itmIDComp , t . itmID , ROW_NUMBER ( ) OVER ( PARTITION BY t . itmIDComp ORDER BY t . itmIDComp , t . itmID ) AS N , Lvl + 1 FROM r JOIN vw AS t ON t . itmID = r . itmIDComp )");
    }

    @Test
    public void testEmptyString() {
        ParserUtils.compareTableName("", TestUtils.R_BUNDLE.getString("R_noTokensFoundInUserQuery"));
        ParserUtils.compareTableName(null, TestUtils.R_BUNDLE.getString("R_noTokensFoundInUserQuery"));
    }

}
