package de.jlo.talendcomp.tabletransfer;

import java.lang.reflect.Method;
import java.sql.Statement;

public class MySQLHelper extends DBHelper {
	
	private void enableStreaming(java.sql.Statement stmt) throws Exception {
		if (stmt != null) {
			Class<?> stmtClass = stmt.getClass();
			boolean enablingSucceeded = false;
			try {
				Method method = stmtClass.getMethod("enableStreamingResults", (Class<?>[]) null);
				method.invoke(stmt, (Object[]) null);
				enablingSucceeded = true;
			} catch (NoSuchMethodException nsme) {
				// ignore intentionally
			}
			if (enablingSucceeded == false) {
				// because it is a wrapper class from pools e.g.
				try {
					// most likely we will get the actual statement by fetching the delegate
					Method method = stmtClass.getMethod("getDelegate", (Class<?>[]) null);
					Object result = method.invoke(stmt, (Object[]) null);
					if (result instanceof java.sql.Statement) {
						enableStreaming((java.sql.Statement) result);
					}
				} catch (NoSuchMethodException nsme) {
					// ignore intentionally
				}
			}
		}
	}

	@Override
	public void setupSelectStatement(Statement statement) throws Exception {
		enableStreaming(statement);
	}

}
