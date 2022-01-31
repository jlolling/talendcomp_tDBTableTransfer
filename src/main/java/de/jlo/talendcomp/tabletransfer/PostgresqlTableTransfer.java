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

import de.jlo.datamodel.SQLTable;
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
	protected PreparedStatement createTargetInsertStatement() throws Exception {
		SQLTable table = getTargetSQLTable();
		if (table.isFieldsLoaded() == false) {
			table.loadColumns(true);
		}
		targetInsertStatement = getTargetCodeGenerator().buildPSInsertSQLStatement(table, true, onConflictIgnore, onConflictUpdate);
		if (isDebugEnabled()) {
			debug("createTargetInsertStatement SQL:" + targetInsertStatement.getSQL());
		}
		targetPSInsert = getTargetConnection().prepareStatement(targetInsertStatement.getSQL());
		return targetPSInsert;
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
