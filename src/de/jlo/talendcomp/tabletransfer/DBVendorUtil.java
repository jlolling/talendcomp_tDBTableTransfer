package de.jlo.talendcomp.tabletransfer;

import java.sql.Statement;

public interface DBVendorUtil {
	
	public void setupStatement(Statement statement) throws Exception;

}
