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
package de.jlo.datamodel.ext.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.jlo.datamodel.SQLProcedure;
import de.jlo.datamodel.SQLStatement;
import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.ext.GenericDatabaseExtension;

public class EXASolutionExtension extends GenericDatabaseExtension {
	
	private Logger logger = LogManager.getLogger(EXASolutionExtension.class);
	private static final String driverClassName = "com.exasol.jdbc.EXADriver";
	
	public EXASolutionExtension() {
        addDriverClassName(driverClassName);
        addSQLKeyword("flush statistics");
        addSQLKeyword("distribute");
        addSQLKeyword("profile");
        addSQLDatatype("geometry");
        addSQLDatatype("timestamp with local time zone");
        addSQLDatatype("interval year to month");
        addSQLDatatype("interval day to second");
	}

	@Override
	public String setupViewSQLCode(Connection conn, SQLTable table) {
		StringBuilder sb = new StringBuilder();
		sb.append("select VIEW_TEXT from SYS.EXA_ALL_VIEWS where VIEW_SCHEMA='");
		sb.append(table.getName().toUpperCase());
		sb.append("' and VIEW_NAME='");
		sb.append(table.getSchema().getName().toUpperCase());
		sb.append("'");
		String code = null;
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			if (rs.next()) {
				code = rs.getString(1);
			}
			rs.close();
			stat.close();
			if (code != null && code.length() > 1) {
				table.setSourceCode(code);
			}
		} catch (SQLException e) {
			logger.error("setupViewSQLCode for view=" + table.getName() + " failed:" + e.getMessage(), e);
		} 
		return sb.toString();
	}

	@Override
	public String setupProcedureSQLCode(Connection conn, SQLProcedure proc) {
		StringBuilder sb = new StringBuilder();
		sb.append("select FUNCTION_TEXT from SYS.EXA_ALL_FUNCTIONS where FUNCTION_NAME='");
		sb.append(proc.getName().toUpperCase());
		sb.append("' and FUNCTION_SCHEMA='");
		sb.append(proc.getSchema().getName().toUpperCase());
		sb.append("'");
		String code = null;
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			if (rs.next()) {
				code = rs.getString(1);
			}
			rs.close();
			stat.close();
			if (code != null && code.length() > 1) {
				proc.setCode(code);
			}
		} catch (SQLException e) {
			logger.error("setupProcedureSQLCode for proc=" + proc.getName() + " failed:" + e.getMessage(), e);
		} 
		return sb.toString();
	}

	@Override
	public boolean hasExplainFeature() {
		return true;
	}

	@Override
	public String getExplainSQL(String currentStatement) {
		if (currentStatement != null) {
			currentStatement = currentStatement.trim();
			StringBuilder sb = new StringBuilder();
			sb.append("alter session set profile='on';");
			sb.append(SQLStatement.ignoreResultSetComment + "\n");
			sb.append(currentStatement);
			if (currentStatement.endsWith(";") == false) {
				sb.append(";\n");
			}
			sb.append("alter session set profile='off';\n");
			sb.append("flush statistics;\n");
			sb.append("select * from EXA_STATISTICS.EXA_USER_PROFILE_LAST_DAY\n");
			sb.append("where session_id = current_session order by stmt_id desc;");
			return sb.toString();
		} else {
			return "";
		}
	}

}