/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.spi.ObjectFactory;

/**
 * SQLServerDataSourceObjectFactory is an object factory to materialize datasources from JNDI.
 */

public final class SQLServerDataSourceObjectFactory implements ObjectFactory {

    // NOTE: Per ObjectFactory spec, the ObjectFactory class requires a public
    // class with public constructor.

    /**
     * Initializes a new instance of the SQLServerDataSourceObjectFactory class.
     */
    public SQLServerDataSourceObjectFactory() {
    }

    // getObjectInstance is a factory for rehydrating references to SQLServerDataSource and its child classes.
    // Caller gets the reference by calling SQLServerDataSource.getReference.
    // References are used by JNDI to persist and rehydrate objects.
    public Object getObjectInstance(Object ref,
            Name name,
            Context c,
            Hashtable<?, ?> h) throws SQLServerException {
        // Create a new instance of a DataSource class from the given reference.
        try {
            javax.naming.Reference r = (javax.naming.Reference) ref;
            // First get "class" property from reference.
            javax.naming.RefAddr ra = r.get("class");

            // Our reference will always have a "class" RefAddr.
            if (null == ra) {
                SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
            }

            String className = (String) ra.getContent();

            if (null == className)
                SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);

            // Check that we have the expected class name inside our reference.
            if (("com.microsoft.sqlserver.jdbc.SQLServerDataSource").equals(className)
                    || ("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource").equals(className)
                    || ("com.microsoft.sqlserver.jdbc.SQLServerXADataSource").equals(className)) {

                // Create class instance and initialize using reference.
                Class<?> dataSourceClass = Class.forName(className);
                Object dataSourceClassInstance = dataSourceClass.getDeclaredConstructor().newInstance();

                // If this class we created does not cast to SQLServerDataSource, then caller
                // passed in the wrong reference to our factory.
                SQLServerDataSource ds = (SQLServerDataSource) dataSourceClassInstance;
                ds.initializeFromReference(r);
                return dataSourceClassInstance;
            }
            // Class not found, throw invalid reference exception.
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        catch (ClassNotFoundException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        catch (InstantiationException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        catch (IllegalAccessException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        catch (IllegalArgumentException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        catch (InvocationTargetException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        catch (NoSuchMethodException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        catch (SecurityException e) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
        }
        // no chance of getting here but to keep the compiler happy
        return null;

    }

}
