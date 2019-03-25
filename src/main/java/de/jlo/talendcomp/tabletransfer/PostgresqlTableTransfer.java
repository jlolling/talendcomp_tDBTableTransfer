package de.jlo.talendcomp.tabletransfer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.jlo.datamodel.SQLTable;

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
