/**
 * Copyright 2022 Jan Lolling jan.lolling@gmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
