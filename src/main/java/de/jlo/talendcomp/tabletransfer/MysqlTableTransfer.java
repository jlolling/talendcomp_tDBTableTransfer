package de.jlo.talendcomp.tabletransfer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.jlo.datamodel.generator.MysqlSQLCodeGenerator;

public class MysqlTableTransfer extends TableTransfer {
	
	private MysqlSQLCodeGenerator codeGenerator = null;
	private boolean onConflictIgnore = false;
	private boolean onConflictUpdate = false;
	
	@Override
	public MysqlSQLCodeGenerator getTargetCodeGenerator() throws SQLException {
		if (codeGenerator == null) {
			codeGenerator = new MysqlSQLCodeGenerator();
			setupKeywords(getTargetConnection(), codeGenerator);
		}
		return codeGenerator;
	}

	@Override
	protected PreparedStatement createTargetInsertStatement() throws Exception {
		targetInsertStatement = getTargetCodeGenerator().buildPSInsertSQLStatement(getTargetSQLTable(), true, onConflictIgnore, onConflictUpdate);
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
