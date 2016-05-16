package de.jlo.talendcomp.tabletransfer;

import java.lang.reflect.Method;
import java.sql.Statement;

public class MySQLHelper implements DBHelper {
	
	private void enableStreaming(java.sql.Statement stmt) throws Exception {
		if (stmt != null) {
			Class<?> stmtClass = stmt.getClass();
			if (stmt instanceof com.mysql.jdbc.Statement) {
				((com.mysql.jdbc.Statement) stmt).enableStreamingResults();
				System.out.println("Streaming enabled.");
			} else if (stmtClass.getName().equals("org.apache.commons.dbcp2.DelegatingPreparedStatement")) {
				Method method = stmtClass.getMethod("getDelegate", (Class<?>[]) null);
				Object result = method.invoke(stmt, (Object[]) null);
				if (result instanceof java.sql.Statement) {
					enableStreaming((java.sql.Statement) result);
				} else {
					System.err.println("Got no statement object. Instead we got: " + result);
				}
			} else if (stmtClass.getName().equals("org.apache.commons.dbcp2.DelegatingStatement")) {
				Method method = stmtClass.getMethod("getDelegate", (Class<?>[]) null);
				Object result = method.invoke(stmt, (Object[]) null);
				if (result instanceof java.sql.Statement) {
					enableStreaming((java.sql.Statement) result);
				} else {
					System.err.println("Got no statement object. Instead we got: " + result);
				}
			} else {
				System.err.println("Incompatible statement class found: " + stmt.getClass().getName());
			}
		}
	}

	@Override
	public void setupStatement(Statement statement) throws Exception {
		enableStreaming(statement);
	}

}
