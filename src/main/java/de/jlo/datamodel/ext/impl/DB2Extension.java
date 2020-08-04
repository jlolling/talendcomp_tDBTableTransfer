package de.jlo.datamodel.ext.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.jlo.datamodel.BasicDataType;
import de.jlo.datamodel.Field;
import de.jlo.datamodel.SQLProcedure;
import de.jlo.datamodel.SQLProcedure.Parameter;
import de.jlo.datamodel.SQLSchema;
import de.jlo.datamodel.SQLSequence;
import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.ext.GenericDatabaseExtension;

public class DB2Extension extends GenericDatabaseExtension {

	private Logger logger = LoggerFactory.getLogger(DB2Extension.class);
	private static final String driverClassName = "com.ibm.db2.jcc.DB2Driver";
	private static final String name = "DB2 Extension";

	public DB2Extension() {
        addDriverClassName(driverClassName);
		addSQLKeyword("current");
		addSQLKeywords("merge","portion");
		addSQLKeyword("using");
		addSQLKeyword("matched");
		addSQLKeyword("business_time");
		addSQLKeyword("system_time");
		addSQLKeyword("period");
		addSQLKeyword("versioning");
		addSQLKeyword("use");
		addSQLKeyword("of");
		addSQLKeywords("generated", "always", "by", "default", "identity");
		addSQLKeyword("transaction");
		addSQLKeyword("implicitly");
		addSQLKeyword("hidden");
		addSQLKeyword("overlaps");
	}
	
	@Override
	public boolean hasExplainFeature() {
		return true;
	}

	@Override
	public String getExplainSQL(String currentStatement) {
		if (currentStatement != null) {
			currentStatement = currentStatement.trim();
			int hashCodeStatement = currentStatement.hashCode();
			String queryTag = "sqlr-" + hashCodeStatement;
			StringBuilder sb = new StringBuilder();
			sb.append("explain plan set QUERYTAG = '");
			sb.append(queryTag);
			sb.append("' for\n");
			sb.append(currentStatement);
			if (currentStatement.endsWith(";") == false) {
				sb.append(";\n");
			}
			sb.append("select exs.*, exo.* from EXPLAIN_STATEMENT exs ");
			sb.append("left join EXPLAIN_OBJECT exo on exo.EXPLAIN_TIME=exs.EXPLAIN_TIME and exo.EXPLAIN_LEVEL=exs.EXPLAIN_LEVEL and exo.STMTNO=exs.STMTNO and exo.SECTNO=exs.SECTNO ");
			sb.append("where QUERYTAG='");
			sb.append(queryTag);
			sb.append("' order by exs.EXPLAIN_TIME desc, exs.STMTNO, exs.SECTNO, exs.EXPLAIN_LEVEL");
			return sb.toString();
		} else {
			return "";
		}
	}

	@Override
	public void setupDataType(Field field) {
        if ("int2".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("smallint");
    		field.setBasicType(de.jlo.datamodel.BasicDataType.INTEGER.getId());
        } else if ("smallint".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("smallint");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("int4".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("integer");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("bigint".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("bigint");
    		field.setBasicType(BasicDataType.LONG.getId());
        } else if ("integer".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("integer");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("serial".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("serial");
    		field.setBasicType(BasicDataType.LONG.getId());
        } else if ("int8".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("bigint");
    		field.setBasicType(BasicDataType.LONG.getId());
        } else if ("float8".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("double precision");
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("float4".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("single precision");
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("numeric".equalsIgnoreCase(field.getTypeName())) {
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("decimal".equalsIgnoreCase(field.getTypeName())) {
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("varchar".equalsIgnoreCase(field.getTypeName())) {
        	if (field.getLength() > 16000) {
        		field.setTypeSQLCode("clob");
        		field.setBasicType(BasicDataType.BINARY.getId());
        	}
        } else if ("bool".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("boolean");
    		field.setBasicType(BasicDataType.BOOLEAN.getId());
        } else if ("clob".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("clob");
    		field.setBasicType(BasicDataType.CHARACTER.getId());
        }
	}

	@Override
	public void setupDataType(Parameter parameter) {
        if ("int2".equalsIgnoreCase(parameter.getTypeName())) {
            parameter.setTypeName("smallint");
            parameter.setLength(0);
        } else if ("smallint".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setLength(0);
        } else if ("bigint".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setLength(0);
        } else if ("int4".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("integer");
        	parameter.setLength(0);
        } else if ("integer".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("integer");
            parameter.setLength(0);
        } else if ("serial".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("serial");
            parameter.setLength(0);
        } else if ("int8".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("bigint");
            parameter.setLength(0);
        } else if ("float8".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("double precision");
            parameter.setLength(0);
        } else if ("float4".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("single precision");
            parameter.setLength(0);
        } else if ("varchar".equalsIgnoreCase(parameter.getTypeName())) {
        	if (parameter.getLength() > 2048) {
        		parameter.setTypeName("text");
        		parameter.setLength(0);
        	}
        } else if ("bool".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("boolean");
            parameter.setLength(0);
        } else if ("clob".equalsIgnoreCase(parameter.getTypeName())) {
    		parameter.setLength(0);
        }
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String setupViewSQLCode(Connection conn, SQLTable table) {
		StringBuilder sb = new StringBuilder();
		sb.append("select TEXT from syscat.views where viewname='");
		sb.append(table.getName().toUpperCase());
		sb.append("' and VIEWSCHEMA='");
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
		sb.append("select TEXT from SYSCAT.PROCEDURES where PROCNAME='");
		sb.append(proc.getName().toUpperCase());
		sb.append("' and PROCSCHEMA='");
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
			if (code != null && code.length() > 2) {
				proc.setCode(code);
			}
		} catch (SQLException e) {
			logger.error("setupProcedureSQLCode for proc=" + proc.getName() + " failed:" + e.getMessage(), e);
		} 
		return sb.toString();
	}

	@Override
	public boolean isApplicable(String driverClass) {
		if (driverClass.toLowerCase().contains("db2")) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean hasSequenceFeature() {
		return true;
	}

	@Override
	public List<SQLSequence> listSequences(Connection conn, SQLSchema schema) {
		schema.setLoadingSequences(true);
		StringBuilder sb = new StringBuilder();
		sb.append("select SEQNAME,START,MAXVALUE,INCREMENT from SYSCAT.SEQUENCES where SEQSCHEMA='");
		sb.append(schema.getName().toUpperCase());
		sb.append("'");
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			while (rs.next()) {
				SQLSequence seq = new SQLSequence(schema, rs.getString(1));
				seq.setStartsWith(rs.getLong(2));
				seq.setEndsWith(rs.getLong(3));
				seq.setStepWith(rs.getLong(4));
				setupSequenceSQLCode(conn, seq);
				schema.addSequence(seq);
			}
			rs.close();
			stat.close();
			schema.setSequencesLoaded();
		} catch (SQLException sqle) {
			logger.error("listSequences for schema=" + schema + " failed: " + sqle.getMessage(), sqle);
		}
		schema.setLoadingSequences(false);
		return schema.getSequences();
	}

	@Override
	public String getSequenceNextValSQL(SQLSequence sequence) {
		StringBuilder sql = new StringBuilder();
		sql.append("(nextval for ");
		sql.append(sequence.getSchema().getName());
		sql.append(".");
		sql.append(sequence.getName());
		sql.append(")");
		return sql.toString();
	}

}
