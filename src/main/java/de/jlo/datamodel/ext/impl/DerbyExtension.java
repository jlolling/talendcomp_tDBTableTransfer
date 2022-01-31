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

import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.ext.GenericDatabaseExtension;

public class DerbyExtension extends GenericDatabaseExtension {

	private static final Logger logger = LogManager.getLogger(DerbyExtension.class);
	private static final String name = "Derby Extension";
	
	public DerbyExtension() {
		addDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
		addDriverClassName("org.apache.derby.jdbc.ClientDriver");
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
			sb.append("call syscs_util.syscs_set_runtimestatistics(1);\n");
			sb.append(currentStatement);
			if (currentStatement.endsWith(";") == false) {
				sb.append(";\n");
			}
			sb.append("VALUES SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();\n");
			sb.append("call syscs_util.syscs_set_runtimestatistics(0);");
			return sb.toString();
		} else {
			return "";
		}
	}

	@Override
	public String setupViewSQLCode(Connection conn, SQLTable table) {
		if (table.isView()) {
			if (logger.isDebugEnabled()) {
				logger.debug("setupViewSQLCode view=" + table.getAbsoluteName());
			}
			StringBuilder sb = new StringBuilder();
			sb.append("select v.VIEWDEFINITION ");
			sb.append(" from SYS.SYSVIEWS v, SYS.SYSTABLES t, SYS.SYSSCHEMAS s");
			sb.append(" where t.TABLEID = v.TABLEID");
			sb.append(" and t.SCHEMAID = s.SCHEMAID");
			sb.append(" and t.TABLENAME = '");
			sb.append(table.getName());
			sb.append("' and s.SCHEMANAME = '");
			sb.append(table.getSchema().getName());
			sb.append("'");
			String source = null;
			try {
				Statement stat = conn.createStatement();
				ResultSet rs = stat.executeQuery(sb.toString());
				if (rs.next()) {
					source = rs.getString(1);
					if (source != null && source.isEmpty() == false) {
						table.setSourceCode(source);
					}
				}
				rs.close();
				stat.close();
			} catch (SQLException sqle) {
				logger.error("setupViewSQLCode for table " + table.getAbsoluteName() + " failed: " + sqle.getMessage(), sqle);
			}
			return source;
		}
		return null;
	}

	@Override
	public String getName() {
		return name;
	}

}
