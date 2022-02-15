package com.microsoft.sqlserver.jdbc.fmtOnly;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ParserUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class UpdateTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /*
     * A collection of INSERT T-SQL statements from the Microsoft docs.
     * @link https://docs.microsoft.com/en-us/sql/t-sql/queries/update-transact-sql?view=sql-server-2017#UpdateExamples
     */
    @Test
    public void updateExamplesTest() {
        // A. Using DELETE with no WHERE clause
        ParserUtils.compareTableName("UPDATE Person.Address \r\n" + "SET ModifiedDate = GETDATE(); ",
                "Person . Address");

        // B. Updating multiple columns
        ParserUtils.compareTableName(
                "UPDATE Sales.SalesPerson \r\n" + "SET Bonus = 6000, CommissionPct = .10, SalesQuota = NULL;",
                "Sales . SalesPerson");

        // C. Using the WHERE clause
        ParserUtils.compareTableName("UPDATE Production.Product \r\n" + "SET Color = N'Metallic Red' \r\n"
                + "WHERE Name LIKE N'Road-250%' AND Color = N'Red'; ", "Production . Product");

        // D. Using the TOP clause
        ParserUtils.compareTableName(
                "UPDATE TOP (10) HumanResources.Employee\r\n" + "SET VacationHours = VacationHours * 1.25 ;",
                "HumanResources . Employee");
        ParserUtils.compareTableName(
                "UPDATE HumanResources.Employee \r\n" + "SET VacationHours = VacationHours + 8 \r\n"
                        + "FROM (SELECT TOP 10 BusinessEntityID FROM HumanResources.Employee \r\n"
                        + " ORDER BY HireDate ASC) AS th \r\n"
                        + "WHERE HumanResources.Employee.BusinessEntityID = th.BusinessEntityID; ",
                "HumanResources . Employee,(SELECT TOP 10 BusinessEntityID FROM HumanResources . Employee ORDER BY HireDate ASC ) AS th");

        // E. Using the WITH common_table_expression clause
        ParserUtils.compareTableName(
                "WITH Parts(AssemblyID, ComponentID, PerAssemblyQty, EndDate, ComponentLevel) AS \r\n" + "( \r\n"
                        + " SELECT b.ProductAssemblyID, b.ComponentID, b.PerAssemblyQty, \r\n"
                        + " b.EndDate, 0 AS ComponentLevel \r\n" + " FROM Production.BillOfMaterials AS b \r\n"
                        + " WHERE b.ProductAssemblyID = 800 \r\n" + " AND b.EndDate IS NULL \r\n" + " UNION ALL \r\n"
                        + " SELECT bom.ProductAssemblyID, bom.ComponentID, p.PerAssemblyQty, \r\n"
                        + " bom.EndDate, ComponentLevel + 1 \r\n" + " FROM Production.BillOfMaterials AS bom \r\n"
                        + " INNER JOIN Parts AS p \r\n" + " ON bom.ProductAssemblyID = p.ComponentID \r\n"
                        + " AND bom.EndDate IS NULL \r\n" + ") \r\n" + "UPDATE Production.BillOfMaterials \r\n"
                        + "SET PerAssemblyQty = c.PerAssemblyQty * 2 \r\n" + "FROM Production.BillOfMaterials AS c \r\n"
                        + "JOIN Parts AS d ON c.ProductAssemblyID = d.AssemblyID \r\n" + "WHERE d.ComponentLevel = 0;",
                "Production . BillOfMaterials,Production . BillOfMaterials AS c JOIN Parts AS d ON c . ProductAssemblyID = d . AssemblyID");

        // F. Using the WHERE CURRENT OF clause
        ParserUtils.compareTableName(
                "UPDATE HumanResources.EmployeePayHistory SET PayFrequency = 2 " + "WHERE CURRENT OF complex_cursor;",
                "HumanResources . EmployeePayHistory");

        // G. Specifying a computed value
        ParserUtils.compareTableName("UPDATE Production.Product SET ListPrice = ListPrice * 2; ",
                "Production . Product");

        // H. Specifying a compound operator
        ParserUtils.compareTableName("UPDATE Production.Product SET ListPrice += @NewPrice WHERE Color = N'Red'; ",
                "Production . Product");

        ParserUtils.compareTableName(
                "UPDATE Production.ScrapReason SET Name += ' - tool malfunction' WHERE ScrapReasonID BETWEEN 10 and 12; ",
                "Production . ScrapReason");

        // I. Specifying a subquery in the SET clause
        ParserUtils.compareTableName("UPDATE Sales.SalesPerson \r\n" + "SET SalesYTD = SalesYTD + \r\n"
                + " (SELECT SUM(so.SubTotal) \r\n" + " FROM Sales.SalesOrderHeader AS so \r\n"
                + " WHERE so.OrderDate = (SELECT MAX(OrderDate) \r\n" + " FROM Sales.SalesOrderHeader AS so2 \r\n"
                + " WHERE so2.SalesPersonID = so.SalesPersonID) \r\n"
                + " AND Sales.SalesPerson.BusinessEntityID = so.SalesPersonID \r\n" + " GROUP BY so.SalesPersonID);",
                "Sales . SalesPerson,Sales . SalesOrderHeader AS so,Sales . SalesOrderHeader AS so2");

        // J. Updating rows using DEFAULT values
        ParserUtils.compareTableName(
                "UPDATE Production.Location \r\n" + "SET CostRate = DEFAULT \r\n" + "WHERE CostRate > 20.00; ",
                "Production . Location");

        // K. Specifying a view as the target object
        ParserUtils.compareTableName("UPDATE Person.vStateProvinceCountryRegion \r\n"
                + "SET CountryRegionName = 'United States of America' \r\n"
                + "WHERE CountryRegionName = 'United States'; ", "Person . vStateProvinceCountryRegion");

        // L. Specifying a table alias as the target object
        ParserUtils.compareTableName(
                "UPDATE sr \r\n" + "SET sr.Name += ' - tool malfunction' \r\n"
                        + "FROM Production.ScrapReason AS sr \r\n" + "JOIN Production.WorkOrder AS wo \r\n"
                        + " ON sr.ScrapReasonID = wo.ScrapReasonID \r\n" + " AND wo.ScrappedQty > 300; ",
                "Production . ScrapReason AS sr JOIN Production . WorkOrder AS wo ON sr . ScrapReasonID = wo . ScrapReasonID");

        // M. Specifying a table variable as the target object
        ParserUtils.compareTableName(
                "INSERT INTO @MyTableVar (EmpID) \r\n" + " SELECT BusinessEntityID FROM HumanResources.Employee;",
                "@MyTableVar,HumanResources . Employee");

        // N. Using the UPDATE statement with information from another table
        ParserUtils.compareTableName(
                "UPDATE Sales.SalesPerson SET SalesYTD = SalesYTD + SubTotal " + "FROM Sales.SalesPerson AS sp  \r\n"
                        + "JOIN Sales.SalesOrderHeader AS so ON sp.BusinessEntityID = so.SalesPersonID "
                        + "     AND so.OrderDate = (SELECT MAX(OrderDate)  \r\n"
                        + "             FROM Sales.SalesOrderHeader  \r\n"
                        + "             WHERE SalesPersonID = sp.BusinessEntityID);  ",
                "Sales . SalesPerson,Sales . SalesPerson AS sp JOIN Sales . SalesOrderHeader AS so ON sp . BusinessEntityID = so . SalesPersonID,Sales . SalesOrderHeader");
        ParserUtils.compareTableName(
                "UPDATE Sales.SalesPerson  \r\n" + "SET SalesYTD = SalesYTD +   \r\n"
                        + "    (SELECT SUM(so.SubTotal)   \r\n" + "     FROM Sales.SalesOrderHeader AS so  \r\n"
                        + "     WHERE so.OrderDate = (SELECT MAX(OrderDate)  \r\n"
                        + "                           FROM Sales.SalesOrderHeader AS so2  \r\n"
                        + "                           WHERE so2.SalesPersonID = so.SalesPersonID)  \r\n"
                        + "     AND Sales.SalesPerson.BusinessEntityID = so.SalesPersonID  \r\n"
                        + "     GROUP BY so.SalesPersonID);",
                "Sales . SalesPerson,Sales . SalesOrderHeader AS so,Sales . SalesOrderHeader AS so2");

        // O. Updating data in a remote table by using a linked server
        ParserUtils.compareTableName(
                "UPDATE MyLinkedServer.AdventureWorks2012.HumanResources.Department  \r\n"
                        + "SET GroupName = N'Public Relations'  \r\n" + "WHERE DepartmentID = 4;  ",
                "MyLinkedServer . AdventureWorks2012 . HumanResources . Department");

        // P. Updating data in a remote table by using the OPENQUERY function
        ParserUtils.compareTableName(
                "UPDATE OPENQUERY (MyLinkedServer, 'SELECT GroupName FROM HumanResources.Department WHERE DepartmentID = 4')   \r\n"
                        + "SET GroupName = 'Sales and Marketing';  ",
                "OPENQUERY(MyLinkedServer , 'SELECT GroupName FROM HumanResources.Department WHERE DepartmentID = 4' )");

        // Q. Updating data in a remote table by using the OPENDATASOURCE function
        ParserUtils.compareTableName(
                "UPDATE OPENDATASOURCE('SQLNCLI', 'Data Source=<server name>;Integrated Security=SSPI').AdventureWorks2012.HumanResources.Department\r\n"
                        + "SET GroupName = 'Sales and Marketing' WHERE DepartmentID = 4;  ",
                "OPENDATASOURCE('SQLNCLI' , 'Data Source=<server name>;Integrated Security=SSPI' ) . AdventureWorks2012 . HumanResources . Department");

        // R. Using UPDATE with .WRITE to modify data in an nvarchar(max) column
        ParserUtils.compareTableName(
                "UPDATE Production.Document  \r\n" + "SET DocumentSummary .WRITE (N'features',28,10)  \r\n"
                        + "OUTPUT deleted.DocumentSummary,   \r\n" + "       inserted.DocumentSummary   \r\n"
                        + "    INTO @MyTableVar  \r\n" + "WHERE Title = N'Front Reflector Bracket Installation';",
                "Production . Document");

        // S. Using UPDATE with .WRITE to add and remove data in an nvarchar(max) column
        ParserUtils.compareTableName("UPDATE Production.Document  \r\n"
                + "SET DocumentSummary .WRITE (N' Appending data to the end of the column.', NULL, 0)  \r\n"
                + "WHERE Title = N'Crank Arm and Tire Maintenance';  ", "Production . Document");

        // T. Using UPDATE with OPENROWSET to modify a varbinary(max) column
        // ParserUtils.compareTableName(
        // "UPDATE Production.ProductPhoto \r\n" + "SET ThumbNailPhoto = ( \r\n" + " SELECT * \r\n"
        // + " FROM OPENROWSET(BULK 'c:Tires.jpg', SINGLE_BLOB) AS x ) \r\n"
        // + "WHERE ProductPhotoID = 1;",
        // "Production . ProductPhoto,OPENROWSET (BULK 'c:Tires.jpg' , SINGLE_BLOB ) AS x"));

        // U. Using UPDATE to modify FILESTREAM data
        ParserUtils.compareTableName("UPDATE Archive.dbo.Records SET [Chart] = CAST('Xray 1' as varbinary(max)) "
                + "WHERE [SerialNumber] = 2; ", "Archive . dbo . Records");

        // V. Using a system data type
        ParserUtils.compareTableName("UPDATE dbo.Cities  \r\n" + "SET Location = CONVERT(Point, '12.3:46.2')  \r\n"
                + "WHERE Name = 'Anchorage';", "dbo . Cities");

        // W. Invoking a method
        ParserUtils.compareTableName(
                "UPDATE dbo.Cities  \r\n" + "SET Location.SetXY(23.5, 23.5)  \r\n" + "WHERE Name = 'Anchorage';  ",
                "dbo . Cities");

        // X. Modifying the value of a property or data member
        ParserUtils.compareTableName(
                "UPDATE dbo.Cities  \r\n" + "SET Location.X = 23.5  \r\n" + "WHERE Name = 'Anchorage'; ",
                "dbo . Cities");

        // Y. Specifying a table hint
        ParserUtils.compareTableName("UPDATE Production.Product  \r\n" + "WITH (TABLOCK) "
                + "SET ListPrice = ListPrice * 1.10  \r\n" + "WHERE ProductNumber LIKE 'BK-%';",
                "Production . Product WITH (TABLOCK )");

        // Z. Specifying a query hint
        ParserUtils.compareTableName(
                "UPDATE Production.Product  \r\n" + "SET ListPrice = ListPrice * 1.10  \r\n"
                        + "WHERE ProductNumber LIKE @Product  \r\n" + "OPTION (OPTIMIZE FOR (@Product = 'BK-%') ); ",
                "Production . Product");

        // AA. Using UPDATE with the OUTPUT clause
        ParserUtils.compareTableName("UPDATE TOP (10) HumanResources.Employee  \r\n"
                + "SET VacationHours = VacationHours * 1.25,  \r\n" + "    ModifiedDate = GETDATE()   \r\n"
                + "OUTPUT inserted.BusinessEntityID,  \r\n" + "       deleted.VacationHours,  \r\n"
                + "       inserted.VacationHours,  \r\n" + "       inserted.ModifiedDate  \r\n" + "INTO @MyTableVar;  ",
                "HumanResources . Employee");

        // AB. Using UPDATE in a stored procedure
        ParserUtils.compareTableName("UPDATE HumanResources.Employee  \r\n" + "SET VacationHours =   \r\n"
                + "    ( CASE  \r\n" + "         WHEN SalariedFlag = 0 THEN VacationHours + @NewHours  \r\n"
                + "         ELSE @NewHours  \r\n" + "       END  \r\n" + "    )  \r\n" + "WHERE CurrentFlag = 1;",
                "HumanResources . Employee");

        // AC. Using UPDATE in a TRY...CATCH Block
        ParserUtils
                .compareTableName(
                        "BEGIN TRY  \r\n" + "    -- Intentionally generate a constraint violation error.  \r\n"
                                + "    UPDATE HumanResources.Department  \r\n" + "    SET Name = N'MyNewName'  \r\n"
                                + "    WHERE DepartmentID BETWEEN 1 AND 2;  \r\n" + "END TRY",
                        "HumanResources . Department");

        // AD. Using a simple UPDATE statement
        ParserUtils.compareTableName("UPDATE DimEmployee  \r\n" + "SET EndDate = '2010-12-31', CurrentFlag='False';",
                "DimEmployee");

        // AE. Using the UPDATE statement with a WHERE clause
        ParserUtils.compareTableName(
                "UPDATE DimEmployee  \r\n" + "SET FirstName = 'Gail'  \r\n" + "WHERE EmployeeKey = 500;",
                "DimEmployee");

        // AF. Using the UPDATE statement with label
        ParserUtils.compareTableName("UPDATE DimProduct  \r\n" + "SET ProductSubcategoryKey = 2   \r\n"
                + "WHERE ProductKey = 313  \r\n" + "OPTION (LABEL = N'label1');", "DimProduct");

        // AG. Using the UPDATE statement with information from another table
        ParserUtils.compareTableName("UPDATE YearlyTotalSales  \r\n" + "SET YearlySalesAmount=  \r\n"
                + "(SELECT SUM(SalesAmount) FROM FactInternetSales WHERE OrderDateKey >=20040000 AND OrderDateKey < 20050000)  \r\n"
                + "WHERE Year=2004;", "YearlyTotalSales,FactInternetSales");

        // AH. ANSI join replacement for update statements
        ParserUtils.compareTableName("-- Use an implicit join to perform the update\r\n"
                + "UPDATE  AnnualCategorySales\r\n"
                + "SET     AnnualCategorySales.TotalSalesAmount = CTAS_ACS.TotalSalesAmount\r\n"
                + "FROM    CTAS_acs\r\n"
                + "WHERE   CTAS_acs.[EnglishProductCategoryName] = AnnualCategorySales.[EnglishProductCategoryName]\r\n"
                + "AND     CTAS_acs.[CalendarYear]               = AnnualCategorySales.[CalendarYear]",
                "AnnualCategorySales,CTAS_acs");
    }
}
