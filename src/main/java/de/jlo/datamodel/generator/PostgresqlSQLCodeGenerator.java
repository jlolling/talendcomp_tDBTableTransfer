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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.jlo.datamodel.SQLField;
import de.jlo.datamodel.SQLPSParam;
import de.jlo.datamodel.SQLStatement;
import de.jlo.datamodel.SQLTable;

public class PostgresqlSQLCodeGenerator extends SQLCodeGenerator {

	private Logger logger = LogManager.getLogger(PostgresqlSQLCodeGenerator.class);
	
	public SQLStatement buildInsertSQLStatement(SQLTable table, boolean fullName, boolean onConflictIgnore, boolean onConflictUpdate) {
    	setupEnclosureChar(table);
		final SQLStatement sqlPs = new SQLStatement();
		sqlPs.setPrepared(true);
		final StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName(), true));
		} else {
			sb.append(getEncapsulatedName(table.getName(), true));
		}
		sb.append(" ("); 
		SQLField field = null;
		boolean hasPrimaryKey = false;
		boolean hasFieldsNotPartOfPK = false;
		boolean firstLoop = true;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.getUsageType() == SQLField.USAGE_UPD_ONLY) {
				continue;
			}
			if (firstLoop) {
				firstLoop = false;
			} else {
				sb.append(',');
			}
			sb.append(getEncapsulatedName(field.getName(), false));
			if (field.isPrimaryKey()) {
				hasPrimaryKey = true;
			} else {
				hasFieldsNotPartOfPK = true;
			}
		}
		sb.append(")\n values ("); 
		int paramIndex = 0;
		SQLPSParam psParam = null;
		firstLoop = true;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.getUsageType() == SQLField.USAGE_UPD_ONLY) {
				continue;
			}
			if (firstLoop) {
				firstLoop = false;
			} else {
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
				firstLoop = true;
				for (int i = 0; i < table.getFieldCount(); i++) {
					field = table.getFieldAt(i);
					if (field.isPrimaryKey()) {
						if (firstLoop) {
							firstLoop = false;
						} else {
							sb.append(',');
						}
						sb.append(getEncapsulatedName(field.getName(), false));
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
						if (field.getUsageType() == SQLField.USAGE_INS_ONLY) {
							continue;
						}
						if (field.isPrimaryKey() == false) {
							if (firstLoop) {
								firstLoop = false;
							} else {
								sb.append(',');
							}
							sb.append("\n\t");
							sb.append(getEncapsulatedName(field.getName(), false));
							sb.append(" = ");
							if (field.isFixedValue()) {
								sb.append("?");
								psParam = new SQLPSParam();
								psParam.setName(field.getName());
								psParam.setIndex(++paramIndex);
								psParam.setBasicType(field.getBasicType());
								sqlPs.addParam(psParam);
							} else {
								// if the field comes from the normal read fields
								sb.append("excluded.");
								sb.append(getEncapsulatedName(field.getName(), false));
							}
						}
					}
				}
			} else if (onConflictUpdate) {
				logger.warn("Table: " + table.getAbsoluteName() + " does not have a primary key. On conflict clause cannot be done.");
			}
		}
		sqlPs.setSQL(sb.toString());
		return sqlPs;
	}

}
