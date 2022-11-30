import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

public class PlayPostgresTableTransfer extends TalendTest {

	public static void main(String[] args) {
		try {
			setupLogging();
			new PlayPostgresTableTransfer().testTransfer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static void setupLogging() {
		LoggerContext context = (LoggerContext) LogManager.getContext(false);
		Configuration config = context.getConfiguration();
		LoggerConfig rootConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
		rootConfig.setLevel(Level.DEBUG);
	}
	
	public void setupSourceConnection() throws Exception {
		System.out.println("setupSourceConnection...");
		String url_tPostgresqlConnection_4 = "jdbc:postgresql://ubuntuserver.local:5432/postgres";

		String dbUser_tPostgresqlConnection_4 = "postgres";

		final String decryptedPassword_tPostgresqlConnection_4 = "postgres";
		String dbPwd_tPostgresqlConnection_4 = decryptedPassword_tPostgresqlConnection_4;

		java.sql.Connection conn_tPostgresqlConnection_4 = null;

		String driverClass_tPostgresqlConnection_4 = "org.postgresql.Driver";
		java.lang.Class.forName(driverClass_tPostgresqlConnection_4);

		conn_tPostgresqlConnection_4 = java.sql.DriverManager
				.getConnection(url_tPostgresqlConnection_4,
						dbUser_tPostgresqlConnection_4,
						dbPwd_tPostgresqlConnection_4);
		globalMap.put("conn_tPostgresqlConnection_4",
				conn_tPostgresqlConnection_4);
		if (null != conn_tPostgresqlConnection_4) {

			conn_tPostgresqlConnection_4.setAutoCommit(true);
		}

		globalMap.put("schema_" + "tPostgresqlConnection_4",
				"deploymentmanager");

		globalMap.put("conn_" + "tPostgresqlConnection_4",
				conn_tPostgresqlConnection_4);

	}
	
	public void setupTargetConnection() throws Exception {
		System.out.println("setupTargetConnection...");
		String url_tPostgresqlConnection_4 = "jdbc:postgresql://ubuntuserver.local:5432/postgres";

		String dbUser_tPostgresqlConnection_4 = "postgres";

		final String decryptedPassword_tPostgresqlConnection_4 = "postgres";
		String dbPwd_tPostgresqlConnection_4 = decryptedPassword_tPostgresqlConnection_4;

		java.sql.Connection conn_tPostgresqlConnection_4 = null;

		String driverClass_tPostgresqlConnection_4 = "org.postgresql.Driver";
		java.lang.Class.forName(driverClass_tPostgresqlConnection_4);

		conn_tPostgresqlConnection_4 = java.sql.DriverManager
				.getConnection(url_tPostgresqlConnection_4,
						dbUser_tPostgresqlConnection_4,
						dbPwd_tPostgresqlConnection_4);
		globalMap.put("conn_tPostgresqlConnection_3",
				conn_tPostgresqlConnection_4);
		if (null != conn_tPostgresqlConnection_4) {

			conn_tPostgresqlConnection_4.setAutoCommit(false);
		}

		globalMap.put("schema_" + "tPostgresqlConnection_3",
				"deploymentmanager");

		globalMap.put("conn_" + "tPostgresqlConnection_3",
				conn_tPostgresqlConnection_4);

	}

	public void testTransfer() throws Exception {
		setupSourceConnection();
		setupTargetConnection();
		System.out.println("start transfer...");
		int tos_count_tPostgresqlTableTransfer_1 = 0;
		
		de.jlo.talendcomp.tabletransfer.PostgresqlTableTransfer tPostgresqlTableTransfer_1 = new de.jlo.talendcomp.tabletransfer.PostgresqlTableTransfer();
		tPostgresqlTableTransfer_1.setDoCommit(true);
		tPostgresqlTableTransfer_1.setOnConflictIgnore(false);
		tPostgresqlTableTransfer_1.setOnConflictUpdate(true);
		tPostgresqlTableTransfer_1.setExportBooleanAsNumber(false);
		tPostgresqlTableTransfer_1.setOutputToTable(true);
		// configure connections
		tPostgresqlTableTransfer_1
				.setSourceConnection((java.sql.Connection) globalMap
						.get("conn_" + "tPostgresqlConnection_4"));
		tPostgresqlTableTransfer_1.setSourceFetchSize("100");
		tPostgresqlTableTransfer_1
				.setTargetConnection((java.sql.Connection) globalMap
						.get("conn_" + "tPostgresqlConnection_3"));
		tPostgresqlTableTransfer_1.setTargetBatchSize("100");
		tPostgresqlTableTransfer_1.setKeepDataModels(false, null);
		tPostgresqlTableTransfer_1.setupDataModels();
		// use this table as source (query will be generated)
		{
			String schemaName = (String) globalMap
					.get("dbschema_tPostgresqlConnection_4");
			if (schemaName == null) {
				schemaName = (String) globalMap
						.get("db_tPostgresqlConnection_4");
			}
			if (schemaName == null) {
				schemaName = (String) globalMap
						.get("tableschema_tPostgresqlConnection_4");
			}
			if (schemaName == null) {
				schemaName = (String) globalMap
						.get("schema_tPostgresqlConnection_4");
			}
			if (schemaName == null) {
				schemaName = (String) globalMap
						.get("dbname_tPostgresqlConnection_4");
			}
			if (schemaName != null) {
				tPostgresqlTableTransfer_1.setSourceTable(schemaName
						+ "." + "job_tasks");
			} else {
				tPostgresqlTableTransfer_1
						.setSourceTable("job_tasks");
			}
		}
		// configure target table
		// use this table as source (query will be generated)
		tPostgresqlTableTransfer_1.setTargetTable(((String) globalMap
				.get("schema_" + "tPostgresqlConnection_3"))
				+ "."
				+ "job_tasks_copy");
		// initialize statements
		tPostgresqlTableTransfer_1.setup();
		// memorize query
		globalMap.put("tPostgresqlTableTransfer_1_SOURCE_QUERY",
				tPostgresqlTableTransfer_1.getSourceQuery());
		globalMap.put("tPostgresqlTableTransfer_1_SOURCE_TABLE",
				"job_instance_status");
		globalMap.put("tPostgresqlTableTransfer_1_TARGET_TABLE",
				"job_instance_status_1");
		tPostgresqlTableTransfer_1.executeSQLOnTarget("truncate table "
				+ tPostgresqlTableTransfer_1.getTargetTable());
		// log interval
		long logInterval_tPostgresqlTableTransfer_1 = 500l;

		/**
		 * [tPostgresqlTableTransfer_1 begin ] stop
		 */

		/**
		 * [tPostgresqlTableTransfer_1 main ] start
		 */

		currentComponent = "tPostgresqlTableTransfer_1";

		// start transfers
		tPostgresqlTableTransfer_1.execute();
		// wait until executing finished
		while (tPostgresqlTableTransfer_1.isRunning()) {
			if (logInterval_tPostgresqlTableTransfer_1 > 0) {
				// memorize key figures
				if (Thread.currentThread().isInterrupted()) {
					tPostgresqlTableTransfer_1.stop();
				}
				long duration_tPostgresqlTableTransfer_1 = (System
						.currentTimeMillis() - tPostgresqlTableTransfer_1
						.getStartTime()) / 1000l;
				double insertsPerSecond_tPostgresqlTableTransfer_1 = 0;
				if (tPostgresqlTableTransfer_1.getStartTime() > 0
						&& duration_tPostgresqlTableTransfer_1 > 0) {
					insertsPerSecond_tPostgresqlTableTransfer_1 = tPostgresqlTableTransfer_1
							.getCurrentCountInserts()
							/ duration_tPostgresqlTableTransfer_1;
					insertsPerSecond_tPostgresqlTableTransfer_1 = de.jlo.talendcomp.tabletransfer.TableTransfer
							.roundScale2(insertsPerSecond_tPostgresqlTableTransfer_1);
					globalMap.put("tPostgresqlTableTransfer_1_NB_LINE",
							tPostgresqlTableTransfer_1
									.getCurrentCountReads());
					globalMap.put(
							"tPostgresqlTableTransfer_1_NB_INSERTS",
							tPostgresqlTableTransfer_1
									.getCurrentCountInserts());
					System.out.println("tPostgresqlTableTransfer_1 ["
									+ tPostgresqlTableTransfer_1
											.getTargetTable()
									+ "] read:"
									+ tPostgresqlTableTransfer_1
											.getCurrentCountReads()
									+ " inserted:"
									+ tPostgresqlTableTransfer_1
											.getCurrentCountInserts()
									+ " rate inserts:"
									+ insertsPerSecond_tPostgresqlTableTransfer_1
									+ " rows/s");
				} else {
					System.out.println("tPostgresqlTableTransfer_1: ["
									+ tPostgresqlTableTransfer_1
											.getTargetTable()
									+ "] Execute query...");
				}
			}
			try {
				if (logInterval_tPostgresqlTableTransfer_1 > 0) {
					Thread.sleep(logInterval_tPostgresqlTableTransfer_1);
				} else {
					Thread.sleep(1000);
				}
			} catch (InterruptedException e) {
				// the stop of this job will be detected here
				tPostgresqlTableTransfer_1.stop();
			}
		}
		globalMap.put("tPostgresqlTableTransfer_1_NB_LINE",
				tPostgresqlTableTransfer_1.getCurrentCountReads());
		globalMap.put("tPostgresqlTableTransfer_1_NB_INSERTS",
				tPostgresqlTableTransfer_1.getCurrentCountInserts());
		System.out.println("tPostgresqlTableTransfer_1 ["
				+ tPostgresqlTableTransfer_1.getTargetTable()
				+ "] read:"
				+ tPostgresqlTableTransfer_1.getCurrentCountReads()
				+ " inserted:"
				+ tPostgresqlTableTransfer_1.getCurrentCountInserts());
		if (tPostgresqlTableTransfer_1.isSuccessful() == false) {
			globalMap.put("tPostgresqlTableTransfer_1_ERROR_MESSAGE",
					tPostgresqlTableTransfer_1.getErrorMessage());
			if (tPostgresqlTableTransfer_1.getErrorException() != null) {
				throw tPostgresqlTableTransfer_1.getErrorException();
			} else {
				throw new Exception(
						tPostgresqlTableTransfer_1.getErrorMessage());
			}
		}

		tos_count_tPostgresqlTableTransfer_1++;

		/**
		 * [tPostgresqlTableTransfer_1 main ] stop
		 */

		/**
		 * [tPostgresqlTableTransfer_1 end ] start
		 */

		currentComponent = "tPostgresqlTableTransfer_1";

		// disconnect in case of we use data sources means we put back
		// the connection into the pool
		tPostgresqlTableTransfer_1.disconnect();
		globalMap.put("tPostgresqlTableTransfer_1_TIME_RANGE_START",
				tPostgresqlTableTransfer_1.getTimeRangeStart());
		globalMap.put("tPostgresqlTableTransfer_1_TIME_RANGE_END",
				tPostgresqlTableTransfer_1.getTimeRangeEnd());
		globalMap.put("tPostgresqlTableTransfer_1_VALUE_RANGE_START",
				tPostgresqlTableTransfer_1.getValueRangeStart());
		globalMap.put("tPostgresqlTableTransfer_1_VALUE_RANGE_END",
				tPostgresqlTableTransfer_1.getValueRangeEnd());

		ok_Hash.put("tPostgresqlTableTransfer_1", true);

		/**
		 * [tPostgresqlTableTransfer_1 end ] stop
		 */
		System.out.println("Finished");
	}

}
