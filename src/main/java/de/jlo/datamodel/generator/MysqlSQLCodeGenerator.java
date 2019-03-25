package de.jlo.datamodel.generator;

import de.jlo.datamodel.SQLField;
import de.jlo.datamodel.SQLPSParam;
import de.jlo.datamodel.SQLStatement;
import de.jlo.datamodel.SQLTable;

public class MysqlSQLCodeGenerator extends SQLCodeGenerator {
	
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
		boolean hasNonePrimaryKeyFields = false;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(',');
			}
			sb.append(getEncapsulatedName(field.getName()));
			if (field.isPrimaryKey() == false) {
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
		if (onConflictIgnore || (hasNonePrimaryKeyFields == false)) {
			sb.append("\n on duplicate key update ");
			// build dummy assignment col=col 
			field = table.getFieldAt(0);
			sb.append(field.getName());
			sb.append("=");
			sb.append(field.getName());
		} else if (onConflictUpdate && hasNonePrimaryKeyFields) {
			sb.append("\n on duplicate key update ");
			// build assignment for the none-key fields
			boolean firstLoop = true;
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
					sb.append(" = values(");
					sb.append(field.getName());
					sb.append(")");
				}
			}
		}
		sqlPs.setSQL(sb.toString());
		return sqlPs;
	}

}
