import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PlayTableTransfer extends TalendTest {

	public static void main(String[] args) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d = sdf.parse("0010-00-00 00:00:00");
		System.out.println(sdf.format(d) + " = " + d.getTime());
		PlayTableTransfer test = new PlayTableTransfer();
		try {
			//test.setupConnection();
			//test.testSimpleTalendInput();
			//stest.testToFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void setupConnection() throws Exception {
		String url_tMysqlConnection_3 = "jdbc:mysql://on-0337-jll.local:3306/MOBILE_META_DEV?rewriteBatchedStatements=true";

		String userName_tMysqlConnection_3 = "tisadmin";

		final String decryptedPassword_tMysqlConnection_3 = "tisadmin";
		String password_tMysqlConnection_3 = decryptedPassword_tMysqlConnection_3;

		java.sql.Connection conn_tMysqlConnection_3 = null;

		java.lang.Class.forName("org.gjt.mm.mysql.Driver");

		conn_tMysqlConnection_3 = java.sql.DriverManager.getConnection(
				url_tMysqlConnection_3, userName_tMysqlConnection_3,
				password_tMysqlConnection_3);
		globalMap.put("conn_tMysqlConnection_3",
				conn_tMysqlConnection_3);
		if (null != conn_tMysqlConnection_3) {

			conn_tMysqlConnection_3.setAutoCommit(false);
		}

		globalMap.put("conn_tMysqlConnection_3",
				conn_tMysqlConnection_3);

		globalMap.put("db_tMysqlConnection_3",
				"MOBILE_META_DEV");
	}
	
	public void testSimpleTalendInput() throws Exception {
		currentComponent = "tMysqlInput_1";

		int tos_count_tMysqlInput_1 = 0;

		java.util.Calendar calendar_tMysqlInput_1 = java.util.Calendar
				.getInstance();
		calendar_tMysqlInput_1.set(0, 0, 0, 0, 0, 0);
		java.util.Date year0_tMysqlInput_1 = calendar_tMysqlInput_1
				.getTime();
		int nb_line_tMysqlInput_1 = 0;
		java.sql.Connection conn_tMysqlInput_1 = null;
		conn_tMysqlInput_1 = (java.sql.Connection) globalMap
				.get("conn_tMysqlConnection_3");

		java.sql.Statement stmt_tMysqlInput_1 = conn_tMysqlInput_1
				.createStatement();

		String dbquery_tMysqlInput_1 = "\nselect\nDATE_AS_INT,\nDATE_AS_DATE,\nDAY_OF_YEAR_AS_INT,\nDAY_OF_WEEK_AS_INT,\nWEEK_DAY_NAME,\nWEEK_DAY_SHORT_NAME,\nWEEK_AS_INT,\nWEEK_START_DATE,\nWEEK_END_DATE,\nYEAR_OF_WEEK_AS_INT,\nDAY_OF_MONTH_AS_INT,\nMONTH_AS_INT,\nMONTH_NAME,\nMONTH_SHORT_NAME,\nMONTH_START_DATE,\nMONTH_END_DATE,\nQUARTER_AS_INT,\nCAL_YEAR_AS_INT,\nFIN_YEAR_AS_INT,\nFIN_MONTH_AS_INT,\nFIN_QUARTER_AS_INT,\nUTC_MILLISECONDS,\nIS_LAST_DAY_OF_MONTH\nfrom JOB_CALENDAR\n";

		globalMap.put("tMysqlInput_1_QUERY", dbquery_tMysqlInput_1);
		java.sql.ResultSet rs_tMysqlInput_1 = null;
		try {
			rs_tMysqlInput_1 = stmt_tMysqlInput_1
					.executeQuery(dbquery_tMysqlInput_1);
			java.sql.ResultSetMetaData rsmd_tMysqlInput_1 = rs_tMysqlInput_1
					.getMetaData();
			int colQtyInRs_tMysqlInput_1 = rsmd_tMysqlInput_1
					.getColumnCount();

			String tmpContent_tMysqlInput_1 = null;

			while (rs_tMysqlInput_1.next()) {
				nb_line_tMysqlInput_1++;
				

			}
		} catch (Exception e) {
			throw e;
		}
	}
	
	public void testToFile() throws Exception {
		
		currentComponent = "tMysqlTableTransfer_1";

		int tos_count_tMysqlTableTransfer_1 = 0;

		de.jlo.talendcomp.tabletransfer.TableTransfer tMysqlTableTransfer_1 = new de.jlo.talendcomp.tabletransfer.TableTransfer();
		tMysqlTableTransfer_1 = new de.jlo.talendcomp.tabletransfer.TableTransfer();
		tMysqlTableTransfer_1.setExportBooleanAsNumber(true);
		tMysqlTableTransfer_1.setOutputToTable(false);
		// configure connections
		tMysqlTableTransfer_1
				.setSourceConnection((java.sql.Connection) globalMap
						.get("conn_" + "tMysqlConnection_3"));
		tMysqlTableTransfer_1.setSourceFetchSize("10000");
		tMysqlTableTransfer_1.setupDataModels();
		// use our own query as source
		String tMysqlTableTransfer_1_query = "\nselect\nDATE_AS_INT,\nDATE_AS_DATE,\nDAY_OF_YEAR_AS_INT,\nDAY_OF_WEEK_AS_INT,\nWEEK_DAY_NAME,\nWEEK_DAY_SHORT_NAME,\nWEEK_AS_INT,\nWEEK_START_DATE,\nWEEK_END_DATE,\nYEAR_OF_WEEK_AS_INT,\nDAY_OF_MONTH_AS_INT,\nMONTH_AS_INT,\nMONTH_NAME,\nMONTH_SHORT_NAME,\nMONTH_START_DATE,\nMONTH_END_DATE,\nQUARTER_AS_INT,\nCAL_YEAR_AS_INT,\nFIN_YEAR_AS_INT,\nFIN_MONTH_AS_INT,\nFIN_QUARTER_AS_INT,\nUTC_MILLISECONDS,\nIS_LAST_DAY_OF_MONTH\nfrom JOB_CALENDAR\n";
		tMysqlTableTransfer_1
				.setSourceQuery(tMysqlTableTransfer_1_query);
		// setup backup
		{
			String backupFilePath = "/Users/jan/Desktop/job_calendar.csv";
			globalMap.put("tMysqlTableTransfer_1_BACKUP_FILE",
					tMysqlTableTransfer_1
							.setBackupFilePath(backupFilePath));
		}
		// initialize statements
		tMysqlTableTransfer_1.setup();
		// memorize query
		globalMap.put("tMysqlTableTransfer_1_SOURCE_QUERY",
				tMysqlTableTransfer_1.getSourceQuery());
		globalMap.put("tMysqlTableTransfer_1_SOURCE_TABLE",
				"JOB_CALENDAR");
		// log source query
		System.out.println("Source query statement:"
				+ tMysqlTableTransfer_1.getSourceQuery());
		// log interval
		long logInterval_tMysqlTableTransfer_1 = Integer.parseInt("5") * 1000;

		/**
		 * [tMysqlTableTransfer_1 begin ] stop
		 */
		/**
		 * [tMysqlTableTransfer_1 main ] start
		 */

		currentComponent = "tMysqlTableTransfer_1";

		// start transfers
		tMysqlTableTransfer_1.execute();
		// wait until executing finished
		while (tMysqlTableTransfer_1.isRunning()) {
			if (logInterval_tMysqlTableTransfer_1 > 0) {
				// memorize key figures
				if (Thread.currentThread().isInterrupted()) {
					tMysqlTableTransfer_1.stop();
				}
				long duration_tMysqlTableTransfer_1 = (System
						.currentTimeMillis() - tMysqlTableTransfer_1
						.getStartTime()) / 1000l;
				double insertsPerSecond_tMysqlTableTransfer_1 = 0;
				if (tMysqlTableTransfer_1.getStartTime() > 0
						&& duration_tMysqlTableTransfer_1 > 0) {
					insertsPerSecond_tMysqlTableTransfer_1 = tMysqlTableTransfer_1
							.getCurrentCountInserts()
							/ duration_tMysqlTableTransfer_1;
					insertsPerSecond_tMysqlTableTransfer_1 = de.jlo.talendcomp.tabletransfer.TableTransfer
							.roundScale2(insertsPerSecond_tMysqlTableTransfer_1);
					globalMap.put("tMysqlTableTransfer_1_NB_LINE",
							tMysqlTableTransfer_1
									.getCurrentCountReads());
					globalMap.put("tMysqlTableTransfer_1_NB_INSERTS",
							tMysqlTableTransfer_1
									.getCurrentCountInserts());
					System.out.println("tMysqlTableTransfer_1 ["
							+ tMysqlTableTransfer_1.getBackupFilePath()
							+ "] read:"
							+ tMysqlTableTransfer_1
									.getCurrentCountReads()
							+ " written:"
							+ tMysqlTableTransfer_1
									.getCurrentCountInserts()
							+ " rate writes:"
							+ insertsPerSecond_tMysqlTableTransfer_1
							+ " rows/s");
				} else {
					System.out.println("Starting....");
				}
			}
			try {
				if (logInterval_tMysqlTableTransfer_1 > 0) {
					Thread.sleep(logInterval_tMysqlTableTransfer_1);
				} else {
					Thread.sleep(1000);
				}
			} catch (InterruptedException e) {
				// the stop of this job will be detected here
				tMysqlTableTransfer_1.stop();
			}
		}
		globalMap.put("tMysqlTableTransfer_1_NB_LINE",
				tMysqlTableTransfer_1.getCurrentCountReads());
		globalMap.put("tMysqlTableTransfer_1_NB_INSERTS",
				tMysqlTableTransfer_1.getCurrentCountInserts());
		System.out.println("tMysqlTableTransfer_1 ["
				+ tMysqlTableTransfer_1.getBackupFilePath() + "] read:"
				+ tMysqlTableTransfer_1.getCurrentCountReads()
				+ " written:"
				+ tMysqlTableTransfer_1.getCurrentCountInserts());
		if (tMysqlTableTransfer_1.isSuccessful() == false) {
			globalMap.put("tMysqlTableTransfer_1_ERROR_MESSAGE",
					tMysqlTableTransfer_1.getErrorMessage());
			if (tMysqlTableTransfer_1.getErrorException() != null) {
				throw tMysqlTableTransfer_1.getErrorException();
			} else {
				throw new Exception(
						tMysqlTableTransfer_1.getErrorMessage());
			}
		}

	}

}
