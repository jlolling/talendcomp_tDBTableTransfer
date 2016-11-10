package de.cimt.talendcomp.tabletransfer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class DBHelper {
	
	public abstract void setupSelectStatement(Statement statement) throws Exception;
	
	public static boolean isMySQLConnection(Connection conn) throws SQLException {
		DatabaseMetaData dbmd = conn.getMetaData();
		if (dbmd.getDriverName().toLowerCase().contains("mysql") || dbmd.getDriverName().toLowerCase().contains("maria")) {
			return true;
		} else {
			return false;
		}
	}

}
