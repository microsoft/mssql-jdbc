package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;

abstract class SQLServerLob {	
	abstract void fillFromStream() throws SQLException;
}
