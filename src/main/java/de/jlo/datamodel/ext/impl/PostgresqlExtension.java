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

public class PostgresqlExtension extends GenericDatabaseExtension {

	private static Logger logger = LoggerFactory.getLogger(PostgresqlExtension.class);
	private static final String driverClassName = "org.postgresql.Driver";
	private static final String name = "PostgreSQL Extension";
	
	public PostgresqlExtension() {
		addDriverClassName(driverClassName);
		addSQLDatatypes("json", "jsonb", "int4", "int8", "float", "float8","_int4", "_int8", "_float", "_float8", "_byte", "uuid", "interval");
		addSQLKeywords(
				"on", 
				"conflict", 
				"unnest", 
				"vacuum", 
				"vacuum full", 
				"substring", 
				"array_agg", 
				"date_trunc", 
				"date_trunc",
				"substring",
				"regexp_replace",
				"regexp_matches",
				"regexp_split_to_array",
				"position",
				"overlay",
				"overlay",
				"bit_length",
				"char_length",
				"character_length",
				"btrim",
				"format",
				"do",
				"instead",
				"conflict",
				"excluded",
				"substring");
		addProcedureKeyword("returns");
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
	public String setupViewSQLCode(Connection conn, SQLTable table) {
		if (table.isView()) {
			if (logger.isDebugEnabled()) {
				logger.debug("setupViewSQLCode view=" + table.getAbsoluteName());
			}
			String source = null;
			try {
				Statement stat = conn.createStatement();
				ResultSet rs = stat.executeQuery("select pg_get_viewdef('" + table.getAbsoluteName() + "', true)");
				if (rs.next()) {
					source = rs.getString(1);
					if (source != null && source.isEmpty() == false) {
						if (table.isMaterializedView()) {
							source = "create materialized view " + table.getName() + " as\n" + source;
							table.setSourceCode(source);
						} else {
							source = "create or replace view " + table.getName() + " as\n" + source;
							table.setSourceCode(source);
						}
					}
				}
				rs.close();
				stat.close();
			} catch (SQLException sqle) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					// ignore
				}
				logger.error("setupViewSQLCode for table " + table.getAbsoluteName() + " failed: " + sqle.getMessage(), sqle);
			}
			return source;
		}
		return null;
	}

	@Override
	public String setupProcedureSQLCode(Connection conn, SQLProcedure proc) {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("setupProcedureSQLCode proc=" + proc.getAbsoluteName());
			}
			StringBuilder query = new StringBuilder();
			query.append("select pg_get_functiondef(p.oid), l.lanname");
			query.append(" from");
			query.append(" pg_catalog.pg_proc p, ");
			query.append(" pg_catalog.pg_language l,");
			query.append(" pg_catalog.pg_namespace n");
			query.append("\nwhere p.proname = '");
			query.append(proc.getName());
			query.append("'");
			query.append("\nand p.prolang = l.oid");
			query.append("\nand p.pronamespace = n.oid");
			query.append("\nand n.nspname = '");
			query.append(proc.getSchema().getName());
			query.append("'");
			if (proc.getParameterCount() > 0) {
				for (int i = 0; i < proc.getParameterCount(); i++) {
					Parameter p = proc.getParameterAt(i);
					query.append("\nand (p.proargmodes is null or (p.proargmodes)[" + (i + 1) + "] = ");
					if (p.isOutputParameter()) {
						query.append("'o'");
					} else {
						query.append("'i'");
					}
					query.append(")");
					query.append("\nand (p.proargnames)[" + (i + 1) + "] = ");
					query.append("'");
					query.append(p.getName());
					query.append("'");
				}
			}
			Statement stat = conn.createStatement();
			if (logger.isDebugEnabled()) {
				logger.debug("Load procedure code with query: " + query.toString());
			}
			ResultSet rs = stat.executeQuery(query.toString());
			if (rs.next()) {
				proc.setCode(rs.getString(1));
			}
			rs.close();
			stat.close();
			return proc.getCode();
		} catch (SQLException sqle) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				// ignore
			}
			logger.error("setupProcedureSQLCode for proc " + proc.getAbsoluteName() + " failed: " + sqle.getMessage(), sqle);
		}
		return null;
	}

	@Override
	public void setupDataType(Field field) {
        if ("int2".equalsIgnoreCase(field.getTypeName())) {
            field.setTypeSQLCode("smallint");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("int4".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("integer");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("integer".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("integer");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("serial".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("serial");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("bigserial".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("bigserial");
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if ("int8".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("bigint");
    		field.setBasicType(BasicDataType.LONG.getId());
        } else if ("float8".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("double precision");
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("float4".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("single precision");
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("varchar".equalsIgnoreCase(field.getTypeName())) {
        	if (field.getLength() > 2048) {
        		field.setTypeSQLCode("text");
        		field.setBasicType(BasicDataType.CLOB.getId());
        	}
        } else if ("json".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("json");
    		field.setBasicType(BasicDataType.CHARACTER.getId());
        } else if ("jsonb".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("jsonb");
    		field.setBasicType(BasicDataType.CHARACTER.getId());
        } else if ("bool".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("boolean");
    		field.setBasicType(BasicDataType.BOOLEAN.getId());
        } else if ("text".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("text");
    		field.setBasicType(BasicDataType.CLOB.getId());
        } else if ("uuid".equalsIgnoreCase(field.getTypeName())) {
        	field.setTypeSQLCode("uuid");
    		field.setBasicType(BasicDataType.CHARACTER.getId());
        } else if (field.getTypeName().startsWith("_")) {
        	// array types
        	field.setTypeSQLCode(field.getTypeName());
        }
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setupDataType(Parameter parameter) {
        if ("int2".equalsIgnoreCase(parameter.getTypeName())) {
            parameter.setTypeName("smallint");
            parameter.setLength(0);
        } else if ("int4".equalsIgnoreCase(parameter.getTypeName())) {
        	parameter.setTypeName("integer");
        	parameter.setLength(0);
        } else if ("integer".equalsIgnoreCase(parameter.getTypeName())) {
            parameter.setLength(0);
        } else if ("bigint".equalsIgnoreCase(parameter.getTypeName())) {
            parameter.setLength(0);
        } else if ("serial".equalsIgnoreCase(parameter.getTypeName())) {
            parameter.setLength(0);
        } else if ("bigserial".equalsIgnoreCase(parameter.getTypeName())) {
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
        } else if ("text".equalsIgnoreCase(parameter.getTypeName())) {
    		parameter.setLength(0);
        }
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
	public boolean isApplicable(String driverClass) {
		if (driverClass.toLowerCase().contains("postgre")) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public List<SQLSequence> listSequences(Connection conn, SQLSchema schema) {
		if (logger.isDebugEnabled()) {
			logger.debug("listSequences schema=" + schema.getName());
		}
		schema.setLoadingSequences(true);
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT sequence_name,start_value,maximum_value,increment FROM information_schema.sequences\n");
		sb.append("where sequence_schema='");
		sb.append(schema.getName().toLowerCase());
		sb.append("'");
		try {
			Statement stat = conn.createStatement();
			if (logger.isDebugEnabled()) {
				logger.debug("listSequences SQL=" + sb.toString());
			}
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
			try {
				if (conn.getAutoCommit() == false) {
					conn.rollback();
				}
			} catch (SQLException e1) {
				// ignore
			}
			logger.error("listSequences for schema=" + schema + " failed: " + sqle.getMessage(), sqle);
		}
		schema.setLoadingSequences(false);
		return schema.getSequences();
	}

	@Override
	public boolean hasSequenceFeature() {
		return true;
	}

	@Override
	public String getSequenceNextValSQL(SQLSequence sequence) {
		StringBuilder sql = new StringBuilder();
		sql.append("nextval('");
		sql.append(sequence.getSchema().getName());
		sql.append(".");
		sql.append(sequence.getName());
		sql.append("')");
		return sql.toString();
	}

	@Override
	public void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				Statement s = conn.createStatement();
				s.execute("select pg_terminate_backend(pg_backend_pid())");
			} catch (Exception e) {
				// ignore
			}
			try {
				conn.close();
			} catch (Exception e) {
				// ignore
			}
		} 
	}

	@Override
	public void cancelLastStatement(Connection conn) {
		if (conn != null) {
			try {
				Statement s = conn.createStatement();
				s.execute("select pg_cancel_backend(pg_backend_pid())");
			} catch (Exception e) {
				// ignore
			}
		} 
	}

	@Override
	public boolean loadTables(Connection conn, SQLSchema schema) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("loadTables schema=" + schema.getName());
		}
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT \n");
		sql.append("    max(pn.nspname) AS schema_name, \n");
		sql.append("    max(p.relname) AS table_name,\n");
		sql.append("    (case when max(p.relkind) = 'v' then 'VIEW'\n");
		sql.append("          when max(p.relkind) = 'm' then 'MATERIALIZED VIEW'\n");
		sql.append("          when max(p.relkind) = 'r' then 'TABLE'\n");
		sql.append("          else null end) as table_type,\n");
		sql.append("    (case when max(ihc.inhparent) is not null then true else false end) as is_inherated,\n");
		sql.append("    count(ihp.inhrelid) as count_partitions\n");
		sql.append("FROM pg_class as p\n");
		sql.append("JOIN pg_namespace pn ON pn.oid = p.relnamespace\n");
		sql.append("left JOIN pg_inherits as ihp ON (ihp.inhparent=p.oid)\n");
		sql.append("left JOIN pg_inherits as ihc ON (ihc.inhrelid=p.oid)\n");
		sql.append("WHERE pn.nspname = '");
		sql.append(schema.getKey());
		sql.append("'\n");
		sql.append("and p.relkind in ('v','r','m')\n");
		sql.append("group by p.oid\n");
		sql.append("order by table_name");
		Statement stat = conn.createStatement();
		if (logger.isDebugEnabled()) {
			logger.debug("loadTables SQL=" + sql.toString());
		}
		ResultSet rs = stat.executeQuery(sql.toString());
		schema.clearTables();
		while (rs.next()) {
			String name = rs.getString("table_name");
			String type = rs.getString("table_type");
			boolean isInherated = rs.getBoolean("is_inherated");
			int countPartitions = rs.getInt("count_partitions");
			SQLTable table = new SQLTable(schema.getModel(), schema, name);
			table.setType(type);
			table.setInheritated(isInherated);
			table.setCountPartitions(countPartitions);
			schema.addTable(table);
		}
		rs.close();
		stat.close();
		return true;
	}
	
/*
	@Override
	public String getSelectCountRows(SQLTable table) {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT reltuples::bigint AS estimate from pg_class where oid = to_regclass('");
		sb.append(table.getAbsoluteName());
		sb.append("')");
		return sb.toString();
	}
*/
	
	
}
