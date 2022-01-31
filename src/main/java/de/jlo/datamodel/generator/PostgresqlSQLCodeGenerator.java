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
package de.jlo.datamodel.generator;

import de.jlo.datamodel.SQLField;
import de.jlo.datamodel.SQLPSParam;
import de.jlo.datamodel.SQLStatement;
import de.jlo.datamodel.SQLTable;

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
		boolean hasFieldsNotPartOfPK = false;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(',');
			}
			sb.append(getEncapsulatedName(field.getName()));
			if (field.isPrimaryKey()) {
				hasPrimaryKey = true;
			} else {
				hasFieldsNotPartOfPK = true;
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
						sb.append(getEncapsulatedName(field.getName()));
					}
				}
				sb.append(") ");
				if (onConflictIgnore || (hasFieldsNotPartOfPK == false)) {
					// we cannot do an update on pk fields
					sb.append("do nothing");
				} else if (onConflictUpdate && hasFieldsNotPartOfPK) {
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
							sb.append(getEncapsulatedName(field.getName()));
							sb.append(" = excluded.");
							sb.append(getEncapsulatedName(field.getName()));
						}
					}
				}
			}
		}
		sqlPs.setSQL(sb.toString());
		return sqlPs;
	}

}
