package com.microsoft.sqlserver.jdbc.fmtOnly;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ParserUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class InsertTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void basicInsertTest() {
        // minor case sensitivity checking
        ParserUtils.compareTableName("insert into jdbctest values (1)", "jdbctest");
        ParserUtils.compareTableName("InSerT IntO jdbctest VALUES (2)", "jdbctest");
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES (3)", "jdbctest");

        // escape sequence
        ParserUtils.compareTableName("INSERT INTO [jdbctest]]] VALUES (1)", "[jdbctest]]]");
        ParserUtils.compareTableName("INSERT INTO [jdb]]ctest] VALUES (1)", "[jdb]]ctest]");
        ParserUtils.compareTableName("INSERT INTO [j[db]]ctest] VALUES (1)", "[j[db]]ctest]");

        // basic cases
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES (?,?,?)", "jdbctest");
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES (?,?,?);", "jdbctest");
        ParserUtils.compareTableName("INSERT INTO /*hello this is a comment*/jdbctest VALUES (1);", "jdbctest");

        // double quote literal
        ParserUtils.compareTableName("INSERT INTO \"jdbc test\" VALUES (1)", "\"jdbc test\"");
        ParserUtils.compareTableName("INSERT INTO \"jdbc /*test*/\" VALUES (1)", "\"jdbc /*test*/\"");
        ParserUtils.compareTableName("INSERT INTO \"jdbc //test\" VALUES (1)", "\"jdbc //test\"");
        ParserUtils.compareTableName("INSERT INTO \"dbo\".\"jdbcDB\".\"jdbctest\" VALUES (1)",
                "\"dbo\" . \"jdbcDB\" . \"jdbctest\"");
        ParserUtils.compareTableName("INSERT INTO \"jdbctest\" VALUES (1)", "\"jdbctest\"");

        // square bracket literal
        ParserUtils.compareTableName("INSERT INTO [jdbctest] VALUES (1)", "[jdbctest]");
        ParserUtils.compareTableName("INSERT INTO [dbo].[jdbcDB].[jdbctest] VALUES (1)",
                "[dbo] . [jdbcDB] . [jdbctest]");
        ParserUtils.compareTableName("INSERT INTO [dbo].\"jdbcDB\".\"jdbctest\" VALUES (1)",
                "[dbo] . \"jdbcDB\" . \"jdbctest\"");
        ParserUtils.compareTableName("INSERT INTO [jdbc test] VALUES (1)", "[jdbc test]");
        ParserUtils.compareTableName("INSERT INTO [jdbc /*test*/] VALUES (1)", "[jdbc /*test*/]");
        ParserUtils.compareTableName("INSERT INTO [jdbc //test] VALUES (1)", "[jdbc //test]");

        // with parameters
        ParserUtils.compareTableName("INSERT jdbctest VALUES (c1,c2,c3)", "jdbctest");
        ParserUtils.compareTableName("INSERT jdbctest VALUES (c1,c2,c3);", "jdbctest");
        ParserUtils.compareTableName("INSERT jdbctest VALUES (?,?,?)", "jdbctest");
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES (c1,?,c3)", "jdbctest");

        // with special parameters
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES ([c1],\"c2\")", "jdbctest");
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES ([c1]]],\"c2\")", "jdbctest");
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES ([c]]1],\"c2\")", "jdbctest");
        ParserUtils.compareTableName("INSERT INTO jdbctest VALUES ([c1],\"[c2]\")", "jdbctest");
        ParserUtils.compareTableName("INSERT jdbctest VALUES ([\"c1],\"FROM\")", "jdbctest");
        ParserUtils.compareTableName("INSERT jdbctest VALUES ([\"c\"1\"],\"c2\")", "jdbctest");
        ParserUtils.compareTableName("INSERT jdbctest VALUES (['FROM'1)],\"c2\")", "jdbctest");
        ParserUtils.compareTableName("INSERT jdbctest VALUES ([((c)1{}],\"{{c}2)(\")", "jdbctest");

        // with sub queries
        ParserUtils.compareTableName("INSERT INTO jdbctest SELECT c1,c2,c3 FROM jdbctest2 " + "WHERE c1 > 4.0",
                "jdbctest,jdbctest2");

        // Multiple Selects
        ParserUtils.compareTableName("INSERT INTO table1 VALUES (1);INSERT INTO table2 VALUES (1)", "table1,table2");
        ParserUtils.compareTableName("INSERT INTO table1 VALUES (1);INSERT INTO table1 VALUES (1)", "table1");

        // Special cases
        ParserUtils.compareTableName("INSERT INTO dbo.FastCustomers2009\r\n"
                + "SELECT T1.FirstName, T1.LastName, T1.YearlyIncome, T1.MaritalStatus FROM Insured_Customers T1 JOIN CarSensor_Data T2\r\n"
                + "ON (T1.CustomerKey = T2.CustomerKey)\r\n" + "WHERE T2.YearMeasured = ? and T2.Speed > ?;",
                "dbo . FastCustomers2009,Insured_Customers T1 JOIN CarSensor_Data T2 ON (T1 . CustomerKey = T2 . CustomerKey )");
    }

    /*
     * A collection of INSERT T-SQL statements from the Microsoft docs.
     * @link https://docs.microsoft.com/en-us/sql/t-sql/statements/insert-transact-sql?view=sql-server-2017
     */
    @Test
    public void insertExamplesTest() {
        // A. Inserting a single row of data
        ParserUtils.compareTableName("INSERT INTO Production.UnitMeasure VALUES (N'FT', N'Feet', '20080414');",
                "Production . UnitMeasure");

        // B. Inserting multiple rows of data
        ParserUtils.compareTableName("INSERT INTO Production.UnitMeasure "
                + "VALUES (N'FT2', N'Square Feet ', '20080923'), (N'Y', N'Yards', '20080923') "
                + ", (N'Y3', N'Cubic Yards', '20080923');", "Production . UnitMeasure");

        // C. Inserting data that is not in the same order as the table columns
        ParserUtils.compareTableName("INSERT INTO Production.UnitMeasure (Name, UnitMeasureCode, " + "ModifiedDate) "
                + "VALUES (N'Square Yards', N'Y2', GETDATE());", "Production . UnitMeasure");

        // D. Inserting data into a table with columns that have default values
        ParserUtils.compareTableName("INSERT INTO T1 DEFAULT VALUES;", "T1");

        // E. Inserting data into a table with an identity column
        ParserUtils.compareTableName("INSERT INTO T1 (column_1,column_2) VALUES (-99, 'Explicit identity value');  ",
                "T1");

        // F. Inserting data into a uniqueidentifier column by using NEWID()
        ParserUtils.compareTableName("INSERT INTO dbo.T1 (column_2) VALUES (NEWID());", "dbo . T1");

        // G. Inserting data into user-defined type columns
        ParserUtils.compareTableName("INSERT INTO dbo.Points (PointValue) VALUES (CAST ('1,99' AS Point));",
                "dbo . Points");

        // H. Using the SELECT and EXECUTE options to insert data from other tables
        ParserUtils.compareTableName("INSERT INTO dbo.EmployeeSales  \r\n"
                + "    SELECT 'SELECT', sp.BusinessEntityID, c.LastName, sp.SalesYTD   \r\n"
                + "    FROM Sales.SalesPerson AS sp  \r\n" + "    INNER JOIN Person.Person AS c  \r\n"
                + "        ON sp.BusinessEntityID = c.BusinessEntityID  \r\n"
                + "    WHERE sp.BusinessEntityID LIKE '2%'  \r\n" + "    ORDER BY sp.BusinessEntityID, c.LastName;",
                "dbo . EmployeeSales,Sales . SalesPerson AS sp INNER JOIN Person . Person AS c ON sp . BusinessEntityID = c . BusinessEntityID");
        ParserUtils.compareTableName("INSERT INTO dbo.EmployeeSales   \r\n" + "EXECUTE dbo.uspGetEmployeeSales;",
                "dbo . EmployeeSales");
        ParserUtils.compareTableName("INSERT INTO dbo.EmployeeSales   \r\n" + "EXECUTE   \r\n" + "('  \r\n"
                + "SELECT ''EXEC STRING'', sp.BusinessEntityID, c.LastName,   \r\n" + "    sp.SalesYTD   \r\n"
                + "    FROM Sales.SalesPerson AS sp   \r\n" + "    INNER JOIN Person.Person AS c  \r\n"
                + "        ON sp.BusinessEntityID = c.BusinessEntityID  \r\n"
                + "    WHERE sp.BusinessEntityID LIKE ''2%''  \r\n"
                + "    ORDER BY sp.BusinessEntityID, c.LastName  \r\n" + "');", "dbo . EmployeeSales");

        // I. Using WITH common table expression to define the data inserted
        ParserUtils.compareTableName("WITH EmployeeTemp (EmpID, LastName, FirstName, Phone,   \r\n"
                + "                   Address, City, StateProvince,   \r\n"
                + "                   PostalCode, CurrentFlag)  \r\n" + "AS (SELECT   \r\n"
                + "       e.BusinessEntityID, c.LastName, c.FirstName, pp.PhoneNumber,  \r\n"
                + "       a.AddressLine1, a.City, sp.StateProvinceCode,   \r\n"
                + "       a.PostalCode, e.CurrentFlag  \r\n" + "    FROM HumanResources.Employee e  \r\n"
                + "        INNER JOIN Person.BusinessEntityAddress AS bea  \r\n"
                + "        ON e.BusinessEntityID = bea.BusinessEntityID  \r\n"
                + "        INNER JOIN Person.Address AS a  \r\n" + "        ON bea.AddressID = a.AddressID  \r\n"
                + "        INNER JOIN Person.PersonPhone AS pp  \r\n"
                + "        ON e.BusinessEntityID = pp.BusinessEntityID  \r\n"
                + "        INNER JOIN Person.StateProvince AS sp  \r\n"
                + "        ON a.StateProvinceID = sp.StateProvinceID  \r\n"
                + "        INNER JOIN Person.Person as c  \r\n"
                + "        ON e.BusinessEntityID = c.BusinessEntityID  \r\n" + "    )  \r\n"
                + "INSERT INTO HumanResources.NewEmployee   \r\n"
                + "    SELECT EmpID, LastName, FirstName, Phone,   \r\n"
                + "           Address, City, StateProvince, PostalCode, CurrentFlag  \r\n" + "    FROM EmployeeTemp;",
                "HumanResources . NewEmployee,EmployeeTemp");

        // J. Using TOP to limit the data inserted from the source table
        ParserUtils.compareTableName(
                "INSERT TOP(5)INTO dbo.EmployeeSales  \r\n" + "    OUTPUT inserted.EmployeeID, inserted.FirstName, \r\n"
                        + "        inserted.LastName, inserted.YearlySales  \r\n"
                        + "    SELECT sp.BusinessEntityID, c.LastName, c.FirstName, sp.SalesYTD   \r\n"
                        + "    FROM Sales.SalesPerson AS sp  \r\n" + "    INNER JOIN Person.Person AS c  \r\n"
                        + "        ON sp.BusinessEntityID = c.BusinessEntityID  \r\n"
                        + "    WHERE sp.SalesYTD > 250000.00  \r\n" + "    ORDER BY sp.SalesYTD DESC;",
                "dbo . EmployeeSales,Sales . SalesPerson AS sp INNER JOIN Person . Person AS c ON sp . BusinessEntityID = c . BusinessEntityID");
        ParserUtils.compareTableName(
                "INSERT INTO dbo.EmployeeSales  \r\n" + "    OUTPUT inserted.EmployeeID, inserted.FirstName, \r\n"
                        + "        inserted.LastName, inserted.YearlySales  \r\n"
                        + "    SELECT TOP (5) sp.BusinessEntityID, c.LastName, c.FirstName, sp.SalesYTD   \r\n"
                        + "    FROM Sales.SalesPerson AS sp  \r\n" + "    INNER JOIN Person.Person AS c  \r\n"
                        + "        ON sp.BusinessEntityID = c.BusinessEntityID  \r\n"
                        + "    WHERE sp.SalesYTD > 250000.00  \r\n" + "    ORDER BY sp.SalesYTD DESC;",
                "dbo . EmployeeSales,Sales . SalesPerson AS sp INNER JOIN Person . Person AS c ON sp . BusinessEntityID = c . BusinessEntityID");

        // K. Inserting data by specifying a view
        ParserUtils.compareTableName("INSERT INTO V1 VALUES ('Row 1',1);  ", "V1");

        // L. Inserting data into a table variable
        ParserUtils.compareTableName("INSERT INTO @MyTableVar (LocationID, CostRate, ModifiedDate)  \r\n"
                + "    SELECT LocationID, CostRate, GETDATE() \r\n" + "    FROM Production.Location  \r\n"
                + "    WHERE CostRate > 0;  ", "@MyTableVar,Production . Location");

        // M. Inserting data into a remote table by using a linked server
        ParserUtils.compareTableName(
                "INSERT INTO MyLinkServer.AdventureWorks2012.HumanResources.Department (Name, GroupName)  \r\n"
                        + "VALUES (N'Public Relations', N'Executive General and Administration'); ",
                "MyLinkServer . AdventureWorks2012 . HumanResources . Department");

        // N. Inserting data into a remote table by using the OPENQUERY function
        ParserUtils.compareTableName(
                "INSERT OPENQUERY (MyLinkServer, 'SELECT Name, GroupName FROM AdventureWorks2012.HumanResources.Department') VALUES ('Environmental Impact', 'Engineering');",
                "OPENQUERY(MyLinkServer , 'SELECT Name, GroupName FROM AdventureWorks2012.HumanResources.Department' )");

        // P. Inserting into an external table created using PolyBase
        ParserUtils.compareTableName("INSERT INTO dbo.FastCustomer2009 "
                + "SELECT T.* FROM Insured_Customers T1 JOIN CarSensor_Data T2  \r\n"
                + "ON (T1.CustomerKey = T2.CustomerKey)  \r\n" + "WHERE T2.YearMeasured = 2009 and T2.Speed > 40;",
                "dbo . FastCustomer2009,Insured_Customers T1 JOIN CarSensor_Data T2 ON (T1 . CustomerKey = T2 . CustomerKey )");

        // Q. Inserting data into a heap with minimal logging
        ParserUtils.compareTableName("INSERT INTO Sales.SalesHistory WITH (TABLOCK)  \r\n" + "    (SalesOrderID,   \r\n"
                + "     SalesOrderDetailID,  \r\n" + "     CarrierTrackingNumber,   \r\n" + "     OrderQty,   \r\n"
                + "     ProductID,   \r\n" + "     SpecialOfferID,   \r\n" + "     UnitPrice,   \r\n"
                + "     UnitPriceDiscount,  \r\n" + "     LineTotal,   \r\n" + "     rowguid,   \r\n"
                + "     ModifiedDate)  \r\n" + "SELECT * FROM Sales.SalesOrderDetail;  ",
                "Sales . SalesHistory,Sales . SalesOrderDetail");

        // R. Using the OPENROWSET function with BULK to bulk load data into a table
        ParserUtils.compareTableName(
                "INSERT INTO HumanResources.Department WITH (IGNORE_TRIGGERS) (Name, GroupName)  \r\n"
                        + "SELECT b.Name, b.GroupName   \r\n" + "FROM OPENROWSET (  \r\n"
                        + "    BULK 'C:SQLFilesDepartmentData.txt',  \r\n"
                        + "    FORMATFILE = 'C:SQLFilesBulkloadFormatFile.xml',  \r\n"
                        + "    ROWS_PER_BATCH = 15000)AS b;",
                "HumanResources . Department,OPENROWSET(BULK 'C:SQLFilesDepartmentData.txt' , FORMATFILE = 'C:SQLFilesBulkloadFormatFile.xml' , ROWS_PER_BATCH = 15000 ) AS b");

        // S. Using the TABLOCK hint to specify a locking method
        ParserUtils.compareTableName("INSERT INTO Production.Location WITH (XLOCK)  \r\n"
                + "(Name, CostRate, Availability)  \r\n" + "VALUES ( N'Final Inventory', 15.00, 80.00);  ",
                "Production . Location");

        // T. Using OUTPUT with an INSERT statement TODO: FIX
        ParserUtils.compareTableName(
                "INSERT Production.ScrapReason  \r\n"
                        + "    OUTPUT INSERTED.ScrapReasonID, INSERTED.Name, INSERTED.ModifiedDate  \r\n"
                        + "        INTO @MyTableVar  \r\n" + "VALUES (N'Operator error', GETDATE());",
                "Production . ScrapReason");

        // U. Using OUTPUT with identity and computed columns
        ParserUtils.compareTableName(
                "INSERT INTO dbo.EmployeeSales (LastName, FirstName, CurrentSales)  \r\n"
                        + "  OUTPUT INSERTED.LastName,   \r\n" + "         INSERTED.FirstName,   \r\n"
                        + "         INSERTED.CurrentSales  \r\n" + "  INTO @MyTableVar  \r\n"
                        + "    SELECT c.LastName, c.FirstName, sp.SalesYTD  \r\n"
                        + "    FROM Sales.SalesPerson AS sp  \r\n" + "    INNER JOIN Person.Person AS c  \r\n"
                        + "        ON sp.BusinessEntityID = c.BusinessEntityID  \r\n"
                        + "    WHERE sp.BusinessEntityID LIKE '2%'  \r\n" + "    ORDER BY c.LastName, c.FirstName;",
                "dbo . EmployeeSales,Sales . SalesPerson AS sp INNER JOIN Person . Person AS c ON sp . BusinessEntityID = c . BusinessEntityID");

        /*
         * V. Inserting data returned from an OUTPUT clause. TODO: Table name extraction is actually working, but this
         * query won't work on SSMS because of MERGE clause.
         * ParserUtils.compareTableName("INSERT INTO Production.ZeroInventory (DeletedProductID, RemovedOnDate) \r\n" +
         * "SELECT ProductID, GETDATE() \r\n" + "FROM \r\n" + "( MERGE Production.ProductInventory AS pi \r\n" +
         * " USING (SELECT ProductID, SUM(OrderQty) FROM Sales.SalesOrderDetail AS sod \r\n" +
         * " JOIN Sales.SalesOrderHeader AS soh \r\n" + " ON sod.SalesOrderID = soh.SalesOrderID \r\n" +
         * " AND soh.OrderDate = '20070401' \r\n" + " GROUP BY ProductID) AS src (ProductID, OrderQty) \r\n" +
         * " ON (pi.ProductID = src.ProductID) \r\n" + " WHEN MATCHED AND pi.Quantity - src.OrderQty <= 0 \r\n" +
         * " THEN DELETE \r\n" + " WHEN MATCHED \r\n" + " THEN UPDATE SET pi.Quantity = pi.Quantity - src.OrderQty \r\n"
         * + " OUTPUT $action, deleted.ProductID) AS Changes (Action, ProductID) \r\n" + "WHERE Action = 'DELETE'; ",
         * "Production . ZeroInventory,(MERGE Production . ProductInventory AS pi USING (SELECT ProductID , SUM (OrderQty )FROM Sales . SalesOrderDetail AS sod JOIN Sales . SalesOrderHeader AS soh ON sod . SalesOrderID = soh . SalesOrderID AND soh . OrderDate = '20070401' GROUP BY ProductID )AS src (ProductID , OrderQty )ON (pi . ProductID = src . ProductID )WHEN MATCHED AND pi . Quantity - src . OrderQty <= 0 THEN DELETE WHEN MATCHED THEN UPDATE SET pi . Quantity = pi . Quantity - src . OrderQty OUTPUT $ action , deleted . ProductID ) AS Changes (Action , ProductID )"
         * ));
         */

        // W. Inserting data using the SELECT option
        ParserUtils.compareTableName(
                "INSERT INTO EmployeeTitles  \r\n" + "    SELECT EmployeeKey, LastName, Title   \r\n"
                        + "    FROM ssawPDW.dbo.DimEmployee  \r\n" + "    WHERE EndDate IS NULL;  ",
                "EmployeeTitles,ssawPDW . dbo . DimEmployee");

        // X. Specifying a label with the INSERT statement
        ParserUtils.compareTableName("INSERT INTO DimCurrency   \r\n" + "VALUES (500, N'C1', N'Currency1')  \r\n"
                + "OPTION ( LABEL = N'label1' );  ", "DimCurrency");

        // Y. Using a label and a query hint with the INSERT statement
        ParserUtils.compareTableName("INSERT INTO DimCustomer (CustomerKey, CustomerAlternateKey, \r\n"
                + "    FirstName, MiddleName, LastName )   \r\n"
                + "SELECT ProspectiveBuyerKey, ProspectAlternateKey, \r\n" + "    FirstName, MiddleName, LastName  \r\n"
                + "FROM ProspectiveBuyer p JOIN DimGeography g ON p.PostalCode = g.PostalCode  \r\n"
                + "WHERE g.CountryRegionCode = 'FR'  \r\n" + "OPTION ( LABEL = 'Add French Prospects', HASH JOIN);",
                "DimCustomer,ProspectiveBuyer p JOIN DimGeography g ON p . PostalCode = g . PostalCode");
    }
}
