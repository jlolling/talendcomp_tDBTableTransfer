package de.jlo.datamodel.ext.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import de.jlo.datamodel.SQLTrigger;
import de.jlo.datamodel.ext.GenericDatabaseExtension;

public class OracleExtension extends GenericDatabaseExtension {

	private static Logger logger = LoggerFactory.getLogger(OracleExtension.class);
	private static final String name = "Oracle Extension";

	public OracleExtension() {
		super();
		addDriverClassName("oracle.jdbc.driver.OracleDriver");
	}

	@Override
	public boolean hasExplainFeature() {
		return true;
	}

	@Override
	public String getExplainSQL(String currentStatement) {
		StringBuilder sb = new StringBuilder();
		sb.append("explain plan for\n");
		sb.append(currentStatement);
		sb.append(";\n");
		sb.append("select * from table(dbms_xplan.display());");
		return sb.toString();
	}

	@Override
	public String setupViewSQLCode(Connection conn, SQLTable table) {
		StringBuilder sb = new StringBuilder();
		sb.append("select dbms_metadata.get_ddl('VIEW','");
		sb.append(table.getName().toUpperCase());
		sb.append("', '");
		sb.append(table.getSchema().getName().toUpperCase());
		sb.append("') from dual");
		StringBuilder code = new StringBuilder();
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			if (rs.next()) {
				code.append(rs.getString(1).trim());
			}
			rs.close();
			stat.close();
			if (code.length() > 1) {
				table.setSourceCode(code.toString());
			}
		} catch (SQLException e) {
			logger.error("setupViewSQLCode for view=" + table.getName() + " failed:" + e.getMessage(), e);
		} 
		return sb.toString();
	}

	@Override
	public String setupProcedureSQLCode(Connection conn, SQLProcedure proc) {
		StringBuilder sb = new StringBuilder();
		sb.append("select TEXT from ALL_SOURCE where NAME='");
		sb.append(proc.getName().toUpperCase());
		sb.append("' and OWNER='");
		sb.append(proc.getSchema().getName().toUpperCase());
		sb.append("' order by LINE");
		StringBuilder code = new StringBuilder();
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			boolean firstLoop = true;
			while (rs.next()) {
				if (firstLoop) {
					code.append("create or replace ");
					firstLoop = false;
				}
				code.append(rs.getString(1));
			}
			rs.close();
			stat.close();
			if (code.length() > 1) {
				proc.setCode(code.toString());
			}
		} catch (SQLException e) {
			logger.error("setupProcedureSQLCode for proc=" + proc.getName() + " failed:" + e.getMessage(), e);
		} 
		return sb.toString();
	}

	@Override
	public void setupDataType(Field field) {
		if ("integer".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("integer");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("bigint".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("bigint");
    		field.setBasicType(BasicDataType.LONG.getId());
        } else if ("number".equalsIgnoreCase(field.getTypeName())) {
        	if (field.getLength() == 22 && field.getDecimalDigits() == 0) {
        		// 22,0 does not means nothing!!
                field.setTypeSQLCode("number");
        	}
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        }
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setupDataType(Parameter parameter) {
		// do nothing
	}

	@Override
	public List<String> getAdditionalSQLKeywords() {
		List<String> list = new ArrayList<String>();
		list.add("nvl");
		list.add("coalesce");
		list.add("last_day");
		return list;
	}

	@Override
	public List<String> getAdditionalSQLDatatypes() {
		List<String> list = new ArrayList<String>();
		return list;
	}

	@Override
	public List<String> getAdditionalProcedureKeywords() {
		List<String> list = new ArrayList<String>();
		return list;
	}

	@Override
	public boolean hasSQLLimitFeature() {
		return true;
	}

	@Override
	public String getLimitExpression(int max) {
		return "rownum <= " + max;
	}

	@Override
	public boolean isLimitExpressionAWhereCondition() {
		return true;
	}
	
	@Override
	public boolean isApplicable(String driverClass) {
		if (driverClass.toLowerCase().contains("oracle")) {
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public String setupTriggerSQLCode(Connection conn, SQLTrigger trigger) {
		StringBuilder sb = new StringBuilder();
		sb.append("select dbms_metadata.get_ddl('TRIGGER','");
		sb.append(trigger.getName().toUpperCase());
		sb.append("', '");
		sb.append(trigger.getTable().getSchema().getName().toUpperCase());
		sb.append("') from dual");
		StringBuilder code = new StringBuilder();
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			while (rs.next()) {
				code.append(rs.getString(1).trim());
			}
			rs.close();
			stat.close();
			return code.toString();
		} catch (SQLException e) {
			logger.error("getTriggerCode for trigger=" + trigger.getName() + " failed:" + e.getMessage(), e);
		} 
		return sb.toString();
	}

	/*
	@Override
	public String setupSequenceSQLCode(Connection conn, SQLSequence sequence) {
		StringBuilder sb = new StringBuilder();
		sb.append("select dbms_metadata.get_ddl('SEQUENCE','");
		sb.append(sequence.getName().toUpperCase());
		sb.append("', '");
		sb.append(sequence.getSchema().getName().toUpperCase());
		sb.append("') from dual");
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			while (rs.next()) {
				sequence.setCreateCode(rs.getString(1).trim());
			}
			rs.close();
			stat.close();
			sequence.setNextvalCode(getSequenceNextValSQL(sequence));
			return sequence.getCreateCode();
		} catch (SQLException e) {
			logger.error("setupSequenceSQLCode for trigger=" + sequence.getName() + " failed:" + e.getMessage(), e);
		} 
		return null;
	}
	*/
	@Override
	public boolean hasSequenceFeature() {
		return true;
	}

	@Override
	public List<SQLSequence> listSequences(Connection conn, SQLSchema schema) {
		schema.setLoadingSequences(true);
		StringBuilder sb = new StringBuilder();
		sb.append("select SEQUENCE_NAME,MIN_VALUE,MAX_VALUE,INCREMENT_BY,LAST_NUMBER from all_sequences");
		sb.append(" where SEQUENCE_OWNER='");
		sb.append(schema.getName().toUpperCase());
		sb.append("'");
		try {
			Statement stat = conn.createStatement();
			ResultSet rs = stat.executeQuery(sb.toString());
			while (rs.next()) {
				SQLSequence seq = new SQLSequence(schema, rs.getString(1));
				seq.setStartsWith(rs.getLong(2));
				seq.setEndsWith(rs.getBigDecimal(3).longValue());
				seq.setStepWith(rs.getLong(4));
				seq.setCurrentValue(rs.getLong(5));
				setupSequenceSQLCode(conn, seq);
				schema.addSequence(seq);
			}
			rs.close();
			stat.close();
			schema.setSequencesLoaded();
		} catch (SQLException sqle) {
			logger.error("listSequences for schema=" + schema + " failed: " + sqle.getMessage(), sqle);
			schema.setSequencesLoaded();
		}
		schema.setLoadingSequences(false);
		return schema.getSequences();
	}

	@Override
	public String getSequenceNextValSQL(SQLSequence sequence) {
		StringBuilder sql = new StringBuilder();
		sql.append(sequence.getSchema().getName());
		sql.append(".");
		sql.append(sequence.getName());
		sql.append(".nextval");
		return sql.toString();
	}

}
