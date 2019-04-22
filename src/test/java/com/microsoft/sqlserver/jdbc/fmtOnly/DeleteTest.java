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
public class DeleteTest extends AbstractTest {
    /*
     * https://docs.microsoft.com/en-us/sql/t-sql/statements/delete-transact-sql?view=sql-server-2017#examples
     */
    @Test
    public void deleteExamplesTest() {
        ArrayList<Pair<String, String>> valuePair = new ArrayList<>();

        // A. Using DELETE with no WHERE clause
        valuePair.add(Pair.of("DELETE FROM Sales.SalesPersonQuotaHistory;  ", "Sales . SalesPersonQuotaHistory"));
        // B. Using the WHERE clause to delete a set of rows
        valuePair.add(Pair.of("DELETE FROM Production.ProductCostHistory  \r\n" + "WHERE StandardCost > 1000.00;",
                "Production . ProductCostHistory"));
        valuePair.add(Pair.of(
                "DELETE Production.ProductCostHistory  \r\n" + "WHERE StandardCost BETWEEN 12.00 AND 14.00  \r\n"
                        + "      AND EndDate IS NULL;  \r\n"
                        + "PRINT 'Number of rows deleted is ' + CAST(@@ROWCOUNT as char(3));  ",
                "Production . ProductCostHistory"));

        // C. Using a cursor to determine the row to delete
        valuePair.add(Pair.of(
                "DELETE FROM HumanResources.EmployeePayHistory  \r\n" + "WHERE CURRENT OF complex_cursor;  \r\n"
                        + "CLOSE complex_cursor;  \r\n" + "DEALLOCATE complex_cursor;  ",
                "HumanResources . EmployeePayHistory"));

        // D. Using joins and subqueries to data in one table to delete rows in another table
        valuePair.add(Pair.of(
                "DELETE FROM Sales.SalesPersonQuotaHistory   \r\n" + "WHERE BusinessEntityID IN   \r\n"
                        + "    (SELECT BusinessEntityID   \r\n" + "     FROM Sales.SalesPerson   \r\n"
                        + "     WHERE SalesYTD > 2500000.00);  ",
                "Sales . SalesPersonQuotaHistory,Sales . SalesPerson"));
        valuePair.add(Pair.of(
                "DELETE FROM Sales.SalesPersonQuotaHistory   \r\n" + "FROM Sales.SalesPersonQuotaHistory AS spqh  \r\n"
                        + "INNER JOIN Sales.SalesPerson AS sp  \r\n"
                        + "ON spqh.BusinessEntityID = sp.BusinessEntityID  \r\n" + "WHERE sp.SalesYTD > 2500000.00;  ",
                "Sales . SalesPersonQuotaHistory,Sales . SalesPersonQuotaHistory AS spqh INNER JOIN Sales . SalesPerson AS sp ON spqh . BusinessEntityID = sp . BusinessEntityID"));

        valuePair.add(Pair.of("DELETE spqh  \r\n" + "  FROM  \r\n"
                + "        Sales.SalesPersonQuotaHistory AS spqh  \r\n" + "    INNER JOIN Sales.SalesPerson AS sp  \r\n"
                + "        ON spqh.BusinessEntityID = sp.BusinessEntityID  \r\n" + "  WHERE  sp.SalesYTD > 2500000.00;",
                "Sales . SalesPersonQuotaHistory AS spqh INNER JOIN Sales . SalesPerson AS sp ON spqh . BusinessEntityID = sp . BusinessEntityID"));

        // E. Using TOP to limit the number of rows deleted
        valuePair.add(Pair.of(
                "DELETE TOP (20)   \r\n" + "FROM Purchasing.PurchaseOrderDetail  \r\n" + "WHERE DueDate < '20020701';",
                "Purchasing . PurchaseOrderDetail"));
        valuePair.add(Pair.of(
                "DELETE FROM Purchasing.PurchaseOrderDetail  \r\n" + "WHERE PurchaseOrderDetailID IN  \r\n"
                        + "   (SELECT TOP 10 PurchaseOrderDetailID   \r\n"
                        + "    FROM Purchasing.PurchaseOrderDetail   \r\n" + "    ORDER BY DueDate ASC);  ",
                "Purchasing . PurchaseOrderDetail"));

        // F. Deleting data from a remote table by using a linked server
        valuePair.add(Pair.of(
                "DELETE MyLinkServer.AdventureWorks2012.HumanResources.Department \r\n" + "WHERE DepartmentID > 16;  ",
                "MyLinkServer . AdventureWorks2012 . HumanResources . Department"));

        // G. Deleting data from a remote table by using the OPENQUERY function
        valuePair.add(Pair.of(
                "DELETE OPENQUERY (MyLinkServer, 'SELECT Name, GroupName FROM AdventureWorks2012.HumanResources.Department WHERE DepartmentID = 18');",
                "OPENQUERY(MyLinkServer , 'SELECT Name, GroupName FROM AdventureWorks2012.HumanResources.Department WHERE DepartmentID = 18' )"));

        // H. Deleting data from a remote table by using the OPENDATASOURCE function
        valuePair.add(Pair.of(
                "DELETE FROM OPENDATASOURCE('SQLNCLI',  \r\n"
                        + "    'Data Source= <server_name>; Integrated Security=SSPI')  \r\n"
                        + "    .AdventureWorks2012.HumanResources.Department   \r\n" + "WHERE DepartmentID = 17;'",
                "OPENDATASOURCE('SQLNCLI' , 'Data Source= <server_name>; Integrated Security=SSPI' ) . AdventureWorks2012 . HumanResources . Department"));

        // I. Using DELETE with the OUTPUT clause
        valuePair.add(Pair.of("DELETE Sales.ShoppingCartItem OUTPUT DELETED.* WHERE ShoppingCartID = 20621;",
                "Sales . ShoppingCartItem"));

        // J. Using OUTPUT with <from_table_name> in a DELETE statement
        valuePair.add(Pair.of("DELETE Production.ProductProductPhoto  \r\n" + "OUTPUT DELETED.ProductID,  \r\n"
                + "       p.Name,  \r\n" + "       p.ProductModelID,  \r\n" + "       DELETED.ProductPhotoID  \r\n"
                + "    INTO @MyTableVar  \r\n" + "FROM Production.ProductProductPhoto AS ph  \r\n"
                + "JOIN Production.Product as p   \r\n" + "    ON ph.ProductID = p.ProductID   \r\n"
                + "    WHERE p.ProductModelID BETWEEN 120 and 130;  ",
                "Production . ProductProductPhoto,Production . ProductProductPhoto AS ph JOIN Production . Product as p ON ph . ProductID = p . ProductID"));

        // K. Delete all rows from a table
        valuePair.add(Pair.of("DELETE FROM Table1;", "Table1"));

        // L. DELETE a set of rows from a table
        valuePair.add(Pair.of("DELETE FROM Table1 WHERE StandardCost > 1000.00;", "Table1"));

        // M. Using LABEL with a DELETE statement
        valuePair.add(Pair.of("DELETE FROM Table1 OPTION ( LABEL = N'label1' );", "Table1"));

        // N. Using a label and a query hint with the DELETE statement
        valuePair.add(Pair.of(
                "DELETE FROM dbo.FactInternetSales  \r\n" + "WHERE ProductKey IN (   \r\n"
                        + "    SELECT T1.ProductKey FROM dbo.DimProduct T1   \r\n"
                        + "    JOIN dbo.DimProductSubcategory T2  \r\n"
                        + "    ON T1.ProductSubcategoryKey = T2.ProductSubcategoryKey  \r\n"
                        + "    WHERE T2.EnglishProductSubcategoryName = 'Road Bikes' )  \r\n"
                        + "OPTION ( LABEL = N'CustomJoin', HASH JOIN );",
                "dbo . FactInternetSales,dbo . DimProduct T1 JOIN dbo . DimProductSubcategory T2 ON T1 . ProductSubcategoryKey = T2 . ProductSubcategoryKey"));

        valuePair.forEach(p -> assertEquals(p.getRight(), ParserUtils.getTableName(p.getLeft()).trim()));
    }
}
