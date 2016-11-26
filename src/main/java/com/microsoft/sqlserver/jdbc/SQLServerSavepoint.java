//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerSavepoint.java
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

import java.sql.Savepoint;
import java.text.MessageFormat;

/**
* SQLServerSavepoint implements JDBC 3.0 savepoints. A savepoint is checkpoint to
* which a transaction can be rolled back. Savepoints are defined relative
* to a connection.
* <li>
* The API javadoc for JDBC API methods that this class implements are not repeated here. Please
* see Sun's JDBC API interfaces javadoc for those details.
*/

public final class SQLServerSavepoint implements Savepoint{
  private final String sName;
  private final int nId;
  private final SQLServerConnection con;

  /**
   * Create a new savepoint
   * @param con the connection
   * @param sName the savepoint name
   */
  public SQLServerSavepoint(SQLServerConnection con, String sName) 
  {
    this.con = con;
    if (sName == null) 
    {
        nId = con.getNextSavepointId();
        this.sName = null;
    }
    else
    {
        this.sName = sName;
        nId = 0;
    }
  }

  public String getSavepointName() throws SQLServerException {
    if (sName == null)
      SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_savepointNotNamed"), null, false);
    return sName;
  }

  /**
   * Get the savepoint label
   * @return the name
   */
  public String getLabel()  {
    if (sName == null)
      return "S"+nId;
    else
      return sName;
  }

  public boolean isNamed() {
    return sName!=null;
  }

  public int getSavepointId() throws SQLServerException {
    if (sName != null)
    	{
    	MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_savepointNamed"));
	Object[] msgArgs = {new String(sName)};
      SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, false);
    	}
    return nId;
  }
}
