package de.jlo.datamodel.ext;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import de.jlo.datamodel.ext.impl.DB2Extension;
import de.jlo.datamodel.ext.impl.DerbyExtension;
import de.jlo.datamodel.ext.impl.EXASolutionExtension;
import de.jlo.datamodel.ext.impl.MSSqlExtension;
import de.jlo.datamodel.ext.impl.MySQLExtension;
import de.jlo.datamodel.ext.impl.OracleExtension;
import de.jlo.datamodel.ext.impl.PostgresqlExtension;
import de.jlo.datamodel.ext.impl.TeradataExtension;

public class DatabaseExtensionFactory {
	
	private static final Logger logger = Logger.getLogger(DatabaseExtensionFactory.class);
	private static List<DatabaseExtension> listExtensions = new ArrayList<DatabaseExtension>();
	private static DatabaseExtension genericExtension = new GenericDatabaseExtension();
	
	public static DatabaseExtension getGenericDatabaseExtension() {
		return genericExtension;
	}
	
	public static synchronized DatabaseExtension getDatabaseExtension(String driverClass) {
		if (driverClass == null) {
			return genericExtension;
		}
		init();
		for (DatabaseExtension ext : listExtensions) {
			if (ext.isApplicable(driverClass)) {
				if (logger.isDebugEnabled()) {
					logger.debug("Use DatabaseExtension: " + ext.getClass().getCanonicalName() + " for driverClass: " + driverClass);
				}
				return ext;
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Use default DatabaseExtension: " + genericExtension.getClass().getCanonicalName() + " for driverClass: " + driverClass);
		}
		return genericExtension;
	}

	private static void init() {
		if (listExtensions.size() == 0) {
			listExtensions.add(new PostgresqlExtension());
			listExtensions.add(new OracleExtension());
			listExtensions.add(new DB2Extension());
			listExtensions.add(new DerbyExtension());
			listExtensions.add(new MySQLExtension());
			listExtensions.add(new MSSqlExtension());
			listExtensions.add(new TeradataExtension());
			listExtensions.add(new EXASolutionExtension());
		}
	}

}
