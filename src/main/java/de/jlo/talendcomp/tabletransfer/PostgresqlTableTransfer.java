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
package de.jlo.talendcomp.tabletransfer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.jlo.datamodel.generator.PostgresqlSQLCodeGenerator;

public class PostgresqlTableTransfer extends TableTransfer {
	
	private PostgresqlSQLCodeGenerator codeGenerator = null;
	private boolean onConflictIgnore = false;
	private boolean onConflictUpdate = false;
	
	@Override
	public PostgresqlSQLCodeGenerator getTargetCodeGenerator() throws SQLException {
		if (codeGenerator == null) {
			codeGenerator = new PostgresqlSQLCodeGenerator();
			setupKeywords(getTargetConnection(), codeGenerator);
		}
		return codeGenerator;
	}

	@Override
	protected PreparedStatement createTargetStatement() throws Exception {
		if (isRunOnlyUpdates()) {
			targetSQLStatement = getTargetCodeGenerator().buildUpdateSQLStatement(getTargetSQLTable(), true);
		} else {
			targetSQLStatement = getTargetCodeGenerator().buildInsertSQLStatement(getTargetSQLTable(), true, onConflictIgnore, onConflictUpdate);
		}
		info("PG Target statement:\n" + targetSQLStatement.getSQL());
		if (targetSQLStatement.getCountParameters() == 0) {
			throw new Exception("Target statement has no parameters!");
		}
		String sql = targetSQLStatement.getSQL();
		if (getApplicationName() != null) {
			sql = "/* ApplicationName=" + getApplicationName() + " */\n" + sql;
		}
		targetPreparedStatement = getTargetConnection().prepareStatement(sql);
		return targetPreparedStatement;
	}

	public boolean isOnConflictIgnore() {
		return onConflictIgnore;
	}

	public void setOnConflictIgnore(Boolean onConflictIgnore) {
		if (onConflictIgnore != null) {
			this.onConflictIgnore = onConflictIgnore;
		}
	}

	public boolean isOnConflictUpdate() {
		return onConflictUpdate;
	}

	public void setOnConflictUpdate(Boolean onConflictUpdate) {
		if (onConflictUpdate != null) {
			this.onConflictUpdate = onConflictUpdate;
		}
	}
	
}
