package de.jlo.datamodel.ext.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import de.jlo.datamodel.BasicDataType;
import de.jlo.datamodel.Field;
import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.ext.GenericDatabaseExtension;

public class MySQLExtension extends GenericDatabaseExtension {

	private static final String name = "MySQL Extension";
	private static final Logger logger = Logger.getLogger(MySQLExtension.class);
	private SimpleDateFormat sdfTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public MySQLExtension() {
		addDriverClassName("com.mysql.jdbc.Driver");
		addDriverClassName("org.gjt.mm.mysql.Driver");
		addSQLKeyword("str_to_date");
		addSQLKeyword("adddate");
		addSQLKeyword("addtime");
		addSQLKeyword("date_add");
		addSQLKeyword("date_sub");
		addSQLKeyword("datediff");
		addSQLKeyword("makedate");
	}
	
	@Override
	public boolean hasExplainFeature() {
		return true;
	}

	@Override
	public String getExplainSQL(String currentStatement) {
		StringBuilder sb = new StringBuilder();
		sb.append("explain\n");
		sb.append(currentStatement);
		return sb.toString();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setupDataType(Field field) {
        if (field.getTypeName().toLowerCase().indexOf("int") != -1) {
            field.setTypeSQLCode(field.getTypeName());
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("serial".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("serial");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if (field.getTypeName().toLowerCase().indexOf("double") != -1) {
            field.setTypeSQLCode("double");
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("bool".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeName("boolean");
    		field.setBasicType(BasicDataType.BOOLEAN.getId());
        } else if ("bit".equalsIgnoreCase(field.getTypeName())) {
    		field.setTypeSQLCode("bit");
    		field.setBasicType(BasicDataType.BOOLEAN.getId());
        } else if ("text".equalsIgnoreCase(field.getTypeName())) {
    		field.setTypeSQLCode("text");
    		field.setBasicType(BasicDataType.CLOB.getId());
        } else if ("mediumtext".equalsIgnoreCase(field.getTypeName())) {
    		field.setTypeSQLCode("mediumtext");
    		field.setBasicType(BasicDataType.CLOB.getId());
        }
	}

	@Override
	public String setupViewSQLCode(Connection conn, SQLTable table) {
		if (table.isView()) {
			if (logger.isDebugEnabled()) {
				logger.debug("setupViewSQLCode view=" + table.getAbsoluteName());
			}
			StringBuilder sb = new StringBuilder();
			sb.append("select VIEW_DEFINITION from information_schema.VIEWS where TABLE_SCHEMA='");
			sb.append(table.getSchema().getName());
			sb.append("' and TABLE_NAME='");
			sb.append(table.getName());
			sb.append("'");
			String source = null;
			try {
				Statement stat = conn.createStatement();
				ResultSet rs = stat.executeQuery(sb.toString());
				if (rs.next()) {
					source = rs.getString(1);
					if (source != null && source.isEmpty() == false) {
						source = "create or replace view " + table.getName() + " as\n" + source;
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
	public boolean hasSQLLimitFeature() {
		return true;
	}

	@Override
	public String getLimitExpression(int max) {
		return "limit " + max;
	}

	@Override
	public boolean isLimitExpressionAWhereCondition() {
		return false;
	}
	
	@Override
	public void setupConnection(Connection conn) {
	}
	
	@Override
	public void setupStatement(Statement stat) {
		if (stat.getClass().getName().contains("com.mysql.jdbc.")) {
			try {
				stat.getClass().getMethod("enableStreamingResults", new Class[] {}).invoke(stat, new Object[] {});
			} catch (Exception e) {
				logger.debug(e.getMessage());
			}
		}			
	}
	
	@Override
	public String getTimestampToSQLExpression(Date value) {
		String template = getDateToSQLExpressionPattern();
		return template.replace("{date}", sdfTimestamp.format(value));
	}

	@Override
	public String getDateToSQLExpressionPattern() {
		return "STR_TO_DATE('{date}','%Y-%m-%d')";
	}
	
	@Override
	public String getTimestampToSQLExpressionPattern() {
		return "STR_TO_DATE('{date}','%Y-%m-%d %h:%i:%s')";
	}
	
	@Override
	public String getUpdateCommentStatement(String tableName, String comment) {
		return null; // because MySQL does not have an update comment statement for tables
	}

	@Override
	public String getIdentifierQuoteString() {
		return "`";
	}

	@Override
	public boolean isApplicable(String driverClass) {
		if (driverClass.toLowerCase().contains("mysql") || driverClass.toLowerCase().contains("maria")) {
			return true;
		} else {
			return false;
		}
	}

}
