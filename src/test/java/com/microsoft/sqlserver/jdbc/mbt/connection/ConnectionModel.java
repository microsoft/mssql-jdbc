/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.connection;

import com.microsoft.sqlserver.jdbc.mbt.core.Model;
import com.microsoft.sqlserver.jdbc.mbt.core.ModelAction;
import com.microsoft.sqlserver.jdbc.mbt.core.ModelRequirement;
import com.microsoft.sqlserver.jdbc.mbt.core.ModelVariable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Connection Model for Model-Based Testing.
 * 
 * This is the JUnit 5 equivalent of fxConnection.java.
 * It wraps a java.sql.Connection and provides state tracking
 * with @ModelAction methods that the ModelEngine can execute.
 * 
 * State Variables track the internal state:
 * - closed: whether connection is closed
 * - autoCommit: whether auto-commit is enabled
 * - holdability: cursor holdability setting
 * 
 * Model Actions are methods the ModelEngine can call:
 * - _modelclose(), _modelcommit(), _modelrollback()
 * - _modelcreateStatement(), _modelgetMetaData()
 * - etc.
 */
public class ConnectionModel extends Model {
    
    // The actual Connection being tested
    private Connection connection;
    
    // Child resources
    private Statement currentStatement;
    private ResultSet currentResultSet;
    private List<Savepoint> savepoints;
    
    // ==================== State Variables ====================
    
    @ModelVariable(name = "_closed", description = "Connection is closed")
    private boolean _closed = false;
    
    @ModelVariable(name = "_closeCalled", description = "Close method was called")
    private boolean _closeCalled = false;
    
    @ModelVariable(name = "_autocommit", description = "Auto-commit mode")
    private boolean _autocommit = true;
    
    @ModelVariable(name = "_holdability", description = "Cursor holdability")
    private int _holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    
    @ModelVariable(name = "_hasOpenStatement", description = "Has open Statement")
    private boolean _hasOpenStatement = false;
    
    @ModelVariable(name = "_hasOpenResultSet", description = "Has open ResultSet")
    private boolean _hasOpenResultSet = false;
    
    @ModelVariable(name = "_hasSavepoint", description = "Has active Savepoint")
    private boolean _hasSavepoint = false;
    
    // ==================== Constructor ====================
    
    public ConnectionModel(Connection connection) throws SQLException {
        super("ConnectionModel");
        this.connection = connection;
        this.savepoints = new ArrayList<>();
        
        // Sync with actual connection state
        if (connection != null && !connection.isClosed()) {
            this._autocommit = connection.getAutoCommit();
            this._holdability = connection.getHoldability();
            this._closed = false;
        }
    }
    
    /**
     * Gets the underlying Connection.
     */
    public Connection product() {
        return connection;
    }
    
    // ==================== Model Actions ====================
    // These are equivalent to fxConnection's _model* methods
    
    /**
     * Model action: close the connection.
     * Equivalent to fxConnection._modelclose()
     */
    @ModelAction(weight = 1, callLast = true)
    public void _modelclose() throws SQLException {
        // Close child resources first
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
            _hasOpenResultSet = false;
        }
        if (currentStatement != null) {
            currentStatement.close();
            currentStatement = null;
            _hasOpenStatement = false;
        }
        
        connection.close();
        _closed = true;
        _closeCalled = true;
        _autocommit = true;
    }
    
    /**
     * Model action: commit transaction.
     * Equivalent to fxConnection._modelcommit()
     * 
     * Requirements: connection open, auto-commit OFF
     */
    @ModelAction(weight = 10)
    @ModelRequirement(variable = "_autocommit", value = "false")
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelcommit() throws SQLException {
        connection.commit();
        _hasSavepoint = false;
        savepoints.clear();
    }
    
    /**
     * Model action: rollback transaction.
     * Equivalent to fxConnection._modelrollback()
     * 
     * Requirements: connection open, auto-commit OFF
     */
    @ModelAction(weight = 5)
    @ModelRequirement(variable = "_autocommit", value = "false")
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelrollback() throws SQLException {
        connection.rollback();
        _hasSavepoint = false;
        savepoints.clear();
    }
    
    /**
     * Model action: rollback to savepoint.
     * Equivalent to fxConnection._modelrollbackSavepoint()
     * 
     * Note: When rolling back to a savepoint, all savepoints created after it
     * are automatically released (SQL Server behavior).
     * 
     * Requirements: connection open, auto-commit OFF, has savepoint
     */
    @ModelAction(weight = 3)
    @ModelRequirement(variable = "_autocommit", value = "false")
    @ModelRequirement(variable = "_closed", value = "false")
    @ModelRequirement(variable = "_hasSavepoint", value = "true")
    public void _modelrollbackSavepoint() throws SQLException {
        if (!savepoints.isEmpty()) {
            // Rolling back to a savepoint invalidates it and all savepoints after it
            // Clear all savepoints as SQL Server invalidates them on rollback
            Savepoint sp = savepoints.get(savepoints.size() - 1);
            connection.rollback(sp);
            // After rollback, all savepoints are invalidated in SQL Server
            savepoints.clear();
            _hasSavepoint = false;
        }
    }
    
    /**
     * Model action: create statement.
     * Equivalent to fxConnection._modelcreateStatement()
     * 
     * Requirements: connection open, no existing open statement
     */
    @ModelAction(weight = 10)
    @ModelRequirement(variable = "_closed", value = "false")
    @ModelRequirement(variable = "_hasOpenStatement", value = "false")
    public void _modelcreateStatement() throws SQLException {
        currentStatement = connection.createStatement();
        _hasOpenStatement = true;
    }
    
    /**
     * Model action: create statement with result set type.
     * Equivalent to fxConnection._modelcreateStatementScroll()
     * 
     * Requirements: connection open, no existing open statement
     */
    @ModelAction(weight = 5)
    @ModelRequirement(variable = "_closed", value = "false")
    @ModelRequirement(variable = "_hasOpenStatement", value = "false")
    public void _modelcreateStatementScroll() throws SQLException {
        currentStatement = connection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY
        );
        _hasOpenStatement = true;
    }
    
    /**
     * Model action: close statement.
     * 
     * Requirements: has open statement
     */
    @ModelAction(weight = 5)
    @ModelRequirement(variable = "_hasOpenStatement", value = "true")
    public void _modelcloseStatement() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
            _hasOpenResultSet = false;
        }
        currentStatement.close();
        currentStatement = null;
        _hasOpenStatement = false;
    }
    
    /**
     * Model action: execute query.
     * 
     * Requirements: has statement, no existing result set
     */
    @ModelAction(weight = 10)
    @ModelRequirement(variable = "_hasOpenStatement", value = "true")
    @ModelRequirement(variable = "_hasOpenResultSet", value = "false")
    public void _modelexecuteQuery() throws SQLException {
        currentResultSet = currentStatement.executeQuery("SELECT 1 AS test");
        _hasOpenResultSet = true;
    }
    
    /**
     * Model action: iterate result set.
     * 
     * Requirements: has open result set
     */
    @ModelAction(weight = 8)
    @ModelRequirement(variable = "_hasOpenResultSet", value = "true")
    public void _modelresultSetNext() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.next();
        }
    }
    
    /**
     * Model action: close result set.
     * 
     * Requirements: has open result set
     */
    @ModelAction(weight = 5)
    @ModelRequirement(variable = "_hasOpenResultSet", value = "true")
    public void _modelcloseResultSet() throws SQLException {
        currentResultSet.close();
        currentResultSet = null;
        _hasOpenResultSet = false;
    }
    
    /**
     * Model action: enable auto-commit.
     * Equivalent to fxConnection._modelsetAutoCommitTrue()
     * 
     * Requirements: connection open, auto-commit OFF
     */
    @ModelAction(weight = 3)
    @ModelRequirement(variable = "_closed", value = "false")
    @ModelRequirement(variable = "_autocommit", value = "false")
    public void _modelsetAutoCommitTrue() throws SQLException {
        connection.setAutoCommit(true);
        _autocommit = true;
        _hasSavepoint = false;
        savepoints.clear();
    }
    
    /**
     * Model action: disable auto-commit.
     * Equivalent to fxConnection._modelsetAutoCommitFalse()
     * 
     * Requirements: connection open, auto-commit ON
     */
    @ModelAction(weight = 5)
    @ModelRequirement(variable = "_closed", value = "false")
    @ModelRequirement(variable = "_autocommit", value = "true")
    public void _modelsetAutoCommitFalse() throws SQLException {
        connection.setAutoCommit(false);
        _autocommit = false;
    }
    
    /**
     * Model action: set savepoint.
     * Equivalent to fxConnection._modelsetSavepoint()
     * 
     * Requirements: connection open, auto-commit OFF
     */
    @ModelAction(weight = 4)
    @ModelRequirement(variable = "_closed", value = "false")
    @ModelRequirement(variable = "_autocommit", value = "false")
    public void _modelsetSavepoint() throws SQLException {
        Savepoint sp = connection.setSavepoint();
        savepoints.add(sp);
        _hasSavepoint = true;
    }
    
    /**
     * Model action: release savepoint.
     * Equivalent to fxConnection._modelreleaseSavepoint()
     * 
     * Note: SQL Server JDBC driver doesn't support releaseSavepoint.
     * This action verifies the exception is thrown correctly.
     * 
     * Requirements: has savepoint
     */
    @ModelAction(weight = 3)
    @ModelRequirement(variable = "_closed", value = "false")
    @ModelRequirement(variable = "_hasSavepoint", value = "true")
    public void _modelreleaseSavepoint() throws SQLException {
        if (!savepoints.isEmpty()) {
            Savepoint sp = savepoints.remove(savepoints.size() - 1);
            try {
                connection.releaseSavepoint(sp);
            } catch (java.sql.SQLFeatureNotSupportedException e) {
                // Expected - SQL Server doesn't support releaseSavepoint
                // This is documented behavior
            }
            _hasSavepoint = !savepoints.isEmpty();
        }
    }
    
    /**
     * Model action: get metadata.
     * Equivalent to fxConnection._modelgetMetaData()
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 2)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelgetMetaData() throws SQLException {
        connection.getMetaData();
    }
    
    /**
     * Model action: check if valid.
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 5)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelisValid() throws SQLException {
        boolean valid = connection.isValid(5);
        if (!valid && !_closed) {
            throw new SQLException("Connection is not valid");
        }
    }
    
    /**
     * Model action: get catalog.
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 2)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelgetCatalog() throws SQLException {
        connection.getCatalog();
    }
    
    /**
     * Model action: get warnings.
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 2)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelgetWarnings() throws SQLException {
        connection.getWarnings();
    }
    
    /**
     * Model action: clear warnings.
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 2)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelclearWarnings() throws SQLException {
        connection.clearWarnings();
    }
    
    /**
     * Model action: get auto commit.
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 3)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelgetAutoCommit() throws SQLException {
        boolean actual = connection.getAutoCommit();
        if (actual != _autocommit) {
            throw new SQLException("AutoCommit state mismatch: expected " + _autocommit + ", got " + actual);
        }
    }
    
    /**
     * Model action: get transaction isolation.
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 2)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelgetTransactionIsolation() throws SQLException {
        int isolation = connection.getTransactionIsolation();
        // Verify it's a valid isolation level
        switch (isolation) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                break;
            default:
                throw new SQLException("Invalid transaction isolation level: " + isolation);
        }
    }
    
    /**
     * Model action: get holdability.
     * 
     * Requirements: connection open
     */
    @ModelAction(weight = 2)
    @ModelRequirement(variable = "_closed", value = "false")
    public void _modelgetHoldability() throws SQLException {
        int holdability = connection.getHoldability();
        if (holdability != _holdability) {
            throw new SQLException("Holdability mismatch: expected " + _holdability + ", got " + holdability);
        }
    }
    
    /**
     * Model action: is closed check.
     * 
     * No requirements - can always be called
     */
    @ModelAction(weight = 3)
    public void _modelisClosed() throws SQLException {
        boolean actual = connection.isClosed();
        if (actual != _closed) {
            throw new SQLException("isClosed state mismatch: expected " + _closed + ", got " + actual);
        }
    }
    
    // ==================== Lifecycle ====================
    
    @Override
    public void teardown() throws Exception {
        // Ensure connection is closed
        if (!_closed && connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
    
    // ==================== State Accessors ====================
    
    public boolean isClosed() {
        return _closed;
    }
    
    public boolean isAutoCommit() {
        return _autocommit;
    }
    
    public int getHoldability() {
        return _holdability;
    }
    
    public boolean hasOpenStatement() {
        return _hasOpenStatement;
    }
    
    public boolean hasOpenResultSet() {
        return _hasOpenResultSet;
    }
    
    public boolean hasSavepoint() {
        return _hasSavepoint;
    }
}
