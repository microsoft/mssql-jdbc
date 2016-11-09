//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerBulkCopyOptions.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 
 
package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;

/**
 * A collection of settings that control how an instance of SQLServerBulkCopy behaves.  
 * Used when constructing a SQLServerBulkCopy instance to change how the writeToServer methods for that instance behave.
 */
public class SQLServerBulkCopyOptions 
{   
    /**
     * Number of rows in each batch.
     * 
     * A batch is complete when BatchSize rows have been processed or there are no more rows to send 
     * to the destination data source.  Zero (the default) indicates that each WriteToServer operation 
     * is a single batch.  If the SqlBulkCopy instance has been declared without the UseInternalTransaction 
     * option in effect, rows are sent to the server BatchSize rows at a time, but no transaction-related 
     * action is taken. If UseInternalTransaction is in effect, each batch of rows is inserted as a separate
     * transaction.
     * 
     * Default: 0
     */
    private int batchSize;
    
    /**
     * Number of seconds for the operation to complete before it times out.
     * 
     * A value of 0 indicates no limit; the bulk copy will wait indefinitely.  If the operation does time out, 
     * the transaction is not committed and all copied rows are removed from the destination table.
     * 
     * Default: 60
     */
    private int bulkCopyTimeout;
    
    /**
     * Check constraints while data is being inserted.
     * 
     * Default: false - constraints are not checked
     */
    private boolean checkConstraints;

    /**
     * When specified, cause the server to fire the insert triggers for the rows being inserted into the database.
     * 
     * Default: false - no triggers are fired.
     */
    private boolean fireTriggers;
    
    /**
     * Preserve source identity values.
     * 
     * Default: false - identity values are assigned by the destination.
     */
    private boolean keepIdentity;
    
    /**
     * Preserve null values in the destination table regardless of the settings for default values. 
     * 
     * Default: false - null values are replaced by default values where applicable.
     */
    private boolean keepNulls;
    
    /**
     * Obtain a bulk update lock for the duration of the bulk copy operation.
     * 
     * Default: false - row locks are used.
     */
    private boolean tableLock;
    
    /**
     * When specified, each batch of the bulk-copy operation will occur within a transaction. 
     * 
     * Default: false - no transaction
     */
    private boolean useInternalTransaction;
    
    private boolean allowEncryptedValueModifications;
    
    /**
     * Initializes an instance of the SQLServerBulkCopySettings class using defaults for all of the settings.
     */
    public SQLServerBulkCopyOptions() 
    {
        batchSize = 0;
        bulkCopyTimeout = 60;
        checkConstraints = false;
        fireTriggers = false;
        keepIdentity = false;
        keepNulls = false;
        tableLock = false;
        useInternalTransaction = false;
        allowEncryptedValueModifications = false;
    }
    
    /**
     * Gets the number of rows in each batch.  At the end of each batch, the rows in the batch are sent to the server.
     * 
     * @return Number of rows in each batch.
     */
    public int getBatchSize()
    {
        return batchSize;
    }
    
    /**
     * Sets the number of rows in each batch.  At the end of each batch, the rows in the batch are sent to the server.
     * 
     * @param batchSize Number of rows in each batch.
     * @throws SQLServerException If the batchSize being set is invalid.
     */
    public void setBatchSize(int batchSize) throws SQLServerException
    {
    	if( batchSize >= 0 )
    	{
    		this.batchSize = batchSize;
    	}
    	else
    	{
    	    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidNegativeArg"));
            Object[] msgArgs = {"batchSize"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
    	}
    }
    
    /**
     * Gets the number of seconds for the operation to complete before it times out.
     * 
     * @return Number of seconds before operation times out.
     */
    public int getBulkCopyTimeout()
    {
        return bulkCopyTimeout;
    }
    
    /**
     * Sets the number of seconds for the operation to complete before it times out.
     * 
     * @param timeout Number of seconds before operation times out.
     * @throws SQLServerException If the batchSize being set is invalid.
     */
    public void setBulkCopyTimeout(int timeout) throws SQLServerException
    {
        if( timeout >= 0 )
        {
            this.bulkCopyTimeout = timeout;
        }
        else
        {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidNegativeArg"));
            Object[] msgArgs = {"timeout"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
        }
    }

    /**
     * Indicates whether or not to preserve any source identity values.
     * 
     * @return True if source identity values are to be preserved; false if they are to be assigned by the destination.
     */
    public boolean isKeepIdentity() 
    {
        return keepIdentity;
    }

    /**
     * Sets whether or not to preserve any source identity values.
     * 
     * @param keepIdentity True if source identity values are to be preserved; false if they are to be assigned by the destination
     */
    public void setKeepIdentity(boolean keepIdentity) 
    {
        this.keepIdentity = keepIdentity;
    }

    /**
     * Indicates whether to preserve null values in the destination table regardless of the settings for default values,
     * or if they should be replaced by default values (where applicable).
     * 
     * @return True if null values should be preserved; false if null values should be replaced by default values where applicable.
     */
    public boolean isKeepNulls() 
    {
        return keepNulls;
    }

    /**
     * Sets whether to preserve null values in the destination table regardless of the settings for default values,
     * or if they should be replaced by default values (where applicable).
     * 
     * @param keepNulls True if null values should be preserved; false if null values should be replaced by default values where applicable.
     */
    public void setKeepNulls(boolean keepNulls) 
    {
        this.keepNulls = keepNulls;
    }

    /**
     * Indicates whether SQLServerBulkCopy should obtain a bulk update lock for the duration of the bulk copy operation.
     * 
     * @return True to obtain row locks; false otherwise.
     */
    public boolean isTableLock() 
    {
        return tableLock;
    }

    /**
     * Sets whether SQLServerBulkCopy should obtain a bulk update lock for the duration of the bulk copy operation.
     * 
     * @param tableLock True to obtain row locks; false otherwise.
     */
    public void setTableLock(boolean tableLock) 
    {
        this.tableLock = tableLock;
    }

    /**
     * Indicates whether each batch of the bulk-copy operation will occur within a transaction or not.
     * 
     * @return True if the batch will occur within a transaction; false otherwise.
     */
    public boolean isUseInternalTransaction() 
    {
        return useInternalTransaction;
    }

    /**
     * Sets whether each batch of the bulk-copy operation will occur within a transaction or not.
     * 
     * @param useInternalTransaction True if the batch will occur within a transaction; false otherwise.
     */
    public void setUseInternalTransaction(boolean useInternalTransaction) 
    {
        this.useInternalTransaction = useInternalTransaction;
    }

    /**
     * Indicates whether constraints are to be checked while data is being inserted or not.
     * 
     * @return True if constraints are to be checked; false otherwise.
     */
    public boolean isCheckConstraints() 
    {
        return checkConstraints;
    }

    /**
     * Sets whether constraints are to be checked while data is being inserted or not.
     * 
     * @param checkConstraints True if constraints are to be checked; false otherwise.
     */
    public void setCheckConstraints(boolean checkConstraints) 
    {
        this.checkConstraints = checkConstraints;
    }

    /**
     * Indicates if the server should fire insert triggers for rows being inserted into the database.
     * 
     * @return True triggers are enabled; false otherwise.
     */
    public boolean isFireTriggers() 
    {
        return fireTriggers;
    }

    /**
     * Sets whether the server should be set to fire insert triggers for rows being inserted into the database.
     * 
     * @param fireTriggers True triggers are to be enabled; false otherwise.
     */
    public void setFireTriggers(boolean fireTriggers) 
    {
        this.fireTriggers = fireTriggers;
    }    
    
    public boolean isAllowEncryptedValueModifications() 
    {
        return allowEncryptedValueModifications;
    }

    public void setAllowEncryptedValueModifications(boolean allowEncryptedValueModifications) 
    {
        this.allowEncryptedValueModifications = allowEncryptedValueModifications;
    }
}
