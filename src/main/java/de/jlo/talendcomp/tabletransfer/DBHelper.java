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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

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
	
	private static final Pattern noneUtf8Pattern = Pattern.compile("[^\\u0000-\\uFFFF]");

	public static String stripNoneUTF8(String text) {
		if (text == null || text.isEmpty()) {
			return text;
		}
		return noneUtf8Pattern.matcher(text).replaceAll("");
	}

}
