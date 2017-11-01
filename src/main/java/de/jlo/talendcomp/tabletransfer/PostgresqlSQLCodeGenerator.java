package de.jlo.talendcomp.tabletransfer;

import dbtools.SQLPSParam;
import dbtools.SQLStatement;
import sqlrunner.datamodel.SQLField;
import sqlrunner.datamodel.SQLTable;
import sqlrunner.generator.SQLCodeGenerator;

public class PostgresqlSQLCodeGenerator extends SQLCodeGenerator {
	
	public SQLStatement buildPSInsertSQLStatement(SQLTable table, boolean fullName, boolean onConflictIgnore, boolean onConflictUpdate) {
    	setupEnclosureChar(table);
		final SQLStatement sqlPs = new SQLStatement();
		sqlPs.setPrepared(true);
		int paramIndex = 0;
		final StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append(" ("); 
		SQLField field = null;
		boolean hasPrimaryKey = false;
		boolean hasNonePrimaryKeyFields = false;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(',');
			}
			sb.append(getEncapsulatedName(field.getName()));
			if (field.isPrimaryKey()) {
				hasPrimaryKey = true;
			} else {
				hasNonePrimaryKeyFields = true;
			}
		}
		sb.append(")\n values("); 
		SQLPSParam psParam = null;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(',');
			}
			sb.append("?"); 
			psParam = new SQLPSParam();
			psParam.setName(field.getName());
			psParam.setIndex(++paramIndex);
			psParam.setBasicType(field.getBasicType());
			sqlPs.addParam(psParam);
		}
		sb.append(")"); 
		if (onConflictIgnore || onConflictUpdate) {
			if (hasPrimaryKey) {
				sb.append("\n on conflict (");
				boolean firstLoop = true;
				for (int i = 0; i < table.getFieldCount(); i++) {
					field = table.getFieldAt(i);
					if (field.isPrimaryKey()) {
						if (firstLoop) {
							firstLoop = false;
						} else {
							sb.append(',');
						}
						sb.append(field.getName());
					}
				}
				sb.append(") ");
				if (onConflictIgnore || (hasNonePrimaryKeyFields == false)) {
					// we cannot do an update on pk fields
					sb.append("do nothing");
				} else if (onConflictUpdate && hasNonePrimaryKeyFields) {
					// we can only update if we have pk and value fields
					sb.append("do update set ");
					firstLoop = true;
					for (int i = 0; i < table.getFieldCount(); i++) {
						field = table.getFieldAt(i);
						if (field.isPrimaryKey() == false) {
							if (firstLoop) {
								firstLoop = false;
							} else {
								sb.append(',');
							}
							sb.append("\n\t");
							sb.append(field.getName());
							sb.append(" = excluded.");
							sb.append(field.getName());
						}
					}
				}
			}
		}
		sqlPs.setSQL(sb.toString());
		return sqlPs;
	}

}
