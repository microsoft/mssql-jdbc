//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerDataSourceObjectFactory.java
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


import javax.naming.*;
import javax.naming.spi.*;
import java.util.*;
import java.lang.reflect.*;

/**
* SQLServerDataSourceObjectFactory is an object factory to materialize datasources from JNDI.
*/

public final class SQLServerDataSourceObjectFactory implements ObjectFactory {

	// NOTE: Per ObjectFactory spec, the ObjectFactory class requires a public 
	// class with public constructor.
	public SQLServerDataSourceObjectFactory() {}

	// getObjectInstance is a factory for rehydrating references to SQLServerDataSource and its child classes.
	// Caller gets the reference by calling SQLServerDataSource.getReference.
	// References are used by JNDI to persist and rehydrate objects.
	public Object getObjectInstance(Object ref, Name name, Context c, Hashtable<?,?> h)  throws SQLServerException 
	{
		// Create a new instance of a DataSource class from the given reference.
		try 
		{
			javax.naming.Reference r = (javax.naming.Reference) ref;
			// First get "class" property from reference.
			javax.naming.RefAddr ra = r.get("class");

			// Our reference will always have a "class" RefAddr.
			if (null == ra)	
                     {
                        SQLServerException.makeFromDriverError(null, null,
                            SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
                     }
 
			String className = (String) ra.getContent();

			if (null == className) 
                        SQLServerException.makeFromDriverError(null, null,
                            SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);

			// Check that we have the expected class name inside our reference.
			if (("com.microsoft.sqlserver.jdbc.SQLServerDataSource").equals(className) ||
				("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource").equals(className) ||
				("com.microsoft.sqlserver.jdbc.SQLServerXADataSource").equals(className) ) 
			{

				// Create class instance and initialize using reference.
				Class<?> dataSourceClass = Class.forName(className);
				Object dataSourceClassInstance = dataSourceClass.newInstance();

				// If this class we created does not cast to SQLServerDataSource, then caller
				// passed in the wrong reference to our factory.
				SQLServerDataSource ds = (SQLServerDataSource) dataSourceClassInstance;
				ds.initializeFromReference(r);
				return dataSourceClassInstance;
			}
			// Class not found, throw invalid reference exception.
			SQLServerException.makeFromDriverError(null, null,
                            SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
		}
		catch (ClassNotFoundException e) 
		{
			SQLServerException.makeFromDriverError(null, null,
                            SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
		}
		catch (InstantiationException e) 
		{
			SQLServerException.makeFromDriverError(null, null,
                            SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
		}
		catch (IllegalAccessException e) 
		{
			SQLServerException.makeFromDriverError(null, null,
                            SQLServerException.getErrString("R_invalidDataSourceReference"), null, true);
		}
        // no chance of getting here but to keep the compiler happy
        return null;

	}


}

