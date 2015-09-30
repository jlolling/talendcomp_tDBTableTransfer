package de.jlo.talendcomp.tabletransfer;

import java.sql.Statement;

public interface DBHelper {
	
	public void setupStatement(Statement statement) throws Exception;

}
