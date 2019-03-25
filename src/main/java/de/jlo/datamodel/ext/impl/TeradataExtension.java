package de.jlo.datamodel.ext.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import de.jlo.datamodel.SQLProcedure;
import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.StringReplacer;
import de.jlo.datamodel.ext.GenericDatabaseExtension;

public class TeradataExtension extends GenericDatabaseExtension {

	private static final String name = "Teradata Extension";
	private static final Logger logger = Logger.getLogger(TeradataExtension.class);

	public TeradataExtension() {
		addDriverClassName("com.teradata.jdbc.TeraDriver");
		addSQLKeyword("timezone_minute");
		addSQLKeyword("timezone_hour");
		addSQLKeyword("no");
		addSQLKeyword("before");
		addSQLKeyword("after");
		addSQLKeyword("collect");
		addSQLKeyword("row_number");
		addSQLKeywords("merge","portion","extract");
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public boolean hasExplainFeature() {
		return true;
	}

	@Override
	public String getExplainSQL(String currentStatement) {
		StringBuilder sb = new StringBuilder();
		// add recommendations for collecting statistics to the explain output
		sb.append("diagnostic helpstats on for session;\n");
		sb.append("explain\n");
		sb.append(currentStatement);
		return sb.toString();
	}

	@Override
	public String setupViewSQLCode(Connection conn, SQLTable table) {
		if (table.isView()) {
			if (logger.isDebugEnabled()) {
				logger.debug("setupViewSQLCode view=" + table.getAbsoluteName());
			}
			StringBuilder sb = new StringBuilder();
			sb.append("show view ");
			sb.append(table.getSchema().getName());
			sb.append(".");
			sb.append(table.getName());
			String source = null;
			try {
				Statement stat = conn.createStatement();
				ResultSet rs = stat.executeQuery(sb.toString());
				if (rs.next()) {
					source = rs.getString(1);
					if (source != null && source.isEmpty() == false) {
						table.setSourceCode(StringReplacer.fixLineBreaks(source));
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
	public String setupProcedureSQLCode(Connection conn, SQLProcedure proc) {
		if (logger.isDebugEnabled()) {
			logger.debug("setupProcedureSQLCode procedure=" + proc.getAbsoluteName());
		}
		StringBuilder sb = new StringBuilder();
		sb.append("show procedure ");
		sb.append(proc.getSchema().getName());
		sb.append(".");
		sb.append(proc.getName());
		StringBuilder code = new StringBuilder();
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			while (rs.next()) {
				code.append(StringReplacer.fixLineBreaks(rs.getString(1)));
				code.append("\n");
			}
			rs.close();
			stat.close();
			if (sb.length() > 0) {
				proc.setCode(code.toString());
			}
		} catch (SQLException sqle) {
			logger.error("setupProcedureSQLCode for procedure " + proc.getAbsoluteName() + " failed: " + sqle.getMessage(), sqle);
		}
		return proc.getCode();
	}
	
}
