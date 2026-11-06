package de.jlo.talendcomp.tabletransfer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test against the PostgreSQL test database from docker/docker-compose.yml.
 * Start it with: docker compose -f docker/docker-compose.yml up -d
 * The tests are skipped if the database is not reachable.
 * The connection can be changed with the system properties tabletransfer.test.url, .user and .password
 */
public class PostgresqlTableTransferTest {

	private static final String URL = System.getProperty("tabletransfer.test.url", "jdbc:postgresql://localhost:5432/postgres");
	private static final String USER = System.getProperty("tabletransfer.test.user", "postgres");
	private static final String PASSWORD = System.getProperty("tabletransfer.test.password", "postgres");
	private static final String SCHEMA = "deploymentmanager";
	private static final long TIMEOUT_MILLIS = 120000;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private Connection checkConnection;

	@Before
	public void setUp() throws Exception {
		try {
			checkConnection = createConnection(true);
		} catch (SQLException e) {
			Assume.assumeNoException("Test database not reachable: " + URL, e);
		}
		executeUpdate("truncate table " + SCHEMA + ".job_tasks_copy");
		executeUpdate("truncate table " + SCHEMA + ".job_tasks_reject");
	}

	@After
	public void tearDown() throws Exception {
		if (checkConnection != null) {
			checkConnection.close();
		}
	}

	@Test
	public void testTransferTable() throws Exception {
		PostgresqlTableTransfer tt = createTransfer(100, true);
		tt.setSourceTable(SCHEMA + ".job_tasks");
		tt.setTargetTable(SCHEMA + ".job_tasks_copy");
		tt.setup();
		runTransfer(tt);
		assertTrue("Transfer not successful: " + tt.getErrorMessage(), tt.isSuccessful());
		assertEquals(100000, tt.getCurrentCountReads());
		assertEquals(100000, tt.getCurrentCountInserts());
		assertEquals(100000, count(SCHEMA + ".job_tasks_copy"));
		assertEquals("Target rows differ from source rows", 0,
				queryInt("select count(*) from (select * from " + SCHEMA + ".job_tasks except select * from " + SCHEMA + ".job_tasks_copy) d"));
	}

	@Test
	public void testTransferLastPartialBatch() throws Exception {
		PostgresqlTableTransfer tt = createTransfer(1000, true);
		tt.setSourceTable(SCHEMA + ".job_tasks_small");
		tt.setTargetTable(SCHEMA + ".job_tasks_copy");
		tt.setup();
		runTransfer(tt);
		assertTrue("Transfer not successful: " + tt.getErrorMessage(), tt.isSuccessful());
		assertEquals(1234, tt.getCurrentCountInserts());
		assertEquals(1234, count(SCHEMA + ".job_tasks_copy"));
	}

	@Test
	public void testTransferQueryWithBackupFile() throws Exception {
		File backupFile = new File(tempFolder.getRoot(), "job_tasks.csv");
		PostgresqlTableTransfer tt = createTransfer(100, true);
		tt.setSourceQuery("select * from " + SCHEMA + ".job_tasks where task_id <= 500;");
		tt.setTargetTable(SCHEMA + ".job_tasks_copy");
		tt.setBackupFilePath(backupFile.getAbsolutePath());
		tt.setup();
		runTransfer(tt);
		assertTrue("Transfer not successful: " + tt.getErrorMessage(), tt.isSuccessful());
		assertEquals(500, count(SCHEMA + ".job_tasks_copy"));
		assertTrue("Backup file missing: " + backupFile, backupFile.exists());
		assertEquals(500, Files.readAllLines(backupFile.toPath(), StandardCharsets.UTF_8).size());
	}

	@Test
	public void testOnConflictUpdate() throws Exception {
		PostgresqlTableTransfer tt = createTransfer(100, true);
		tt.setOnConflictUpdate(true);
		tt.setSourceTable(SCHEMA + ".job_tasks_duplicates");
		tt.setTargetTable(SCHEMA + ".job_tasks_copy");
		tt.setup();
		runTransfer(tt);
		assertTrue("Transfer not successful: " + tt.getErrorMessage(), tt.isSuccessful());
		assertEquals(1021, tt.getCurrentCountReads());
		assertEquals(1000, count(SCHEMA + ".job_tasks_copy"));
	}

	@Test
	public void testDieOnErrorStopsTransfer() throws Exception {
		PostgresqlTableTransfer tt = createTransfer(100, true);
		tt.setSourceQuery("select * from " + SCHEMA + ".job_tasks where task_id <= 1234 order by task_id");
		tt.setTargetTable(SCHEMA + ".job_tasks_reject");
		tt.setup();
		runTransfer(tt);
		assertFalse("Transfer must fail because of the check constraint", tt.isSuccessful());
		assertNotNull(tt.getErrorMessage());
		assertTrue(tt.getErrorMessage(), tt.getErrorMessage().contains("job_tasks_reject_priority_check"));
		// the first batch contains task_id 6 with priority 6, nothing can be written
		assertEquals(0, count(SCHEMA + ".job_tasks_reject"));
	}

	@Test
	public void testContinueOnErrorSkipsFailedRecords() throws Exception {
		// batch size 1: every rejected record is its own failed batch
		PostgresqlTableTransfer tt = createTransfer(1, false);
		tt.setSourceQuery("select * from " + SCHEMA + ".job_tasks where task_id <= 1234 order by task_id");
		tt.setTargetTable(SCHEMA + ".job_tasks_reject");
		tt.setup();
		runTransfer(tt);
		assertFalse("Transfer must be marked as not successful", tt.isSuccessful());
		assertEquals("All records must be read", 1234, tt.getCurrentCountReads());
		// 176 records have priority = 6 (task_id % 7 = 6)
		assertEquals(1234 - 176, count(SCHEMA + ".job_tasks_reject"));
		assertEquals(0, queryInt("select count(*) from " + SCHEMA + ".job_tasks_reject where priority = 6"));
	}

	@Test
	public void testContinueOnErrorWritesLastPartialBatch() throws Exception {
		// the first batch of 100 fails, the following batches including the last partial batch must be written
		PostgresqlTableTransfer tt = createTransfer(100, false);
		tt.setSourceQuery("select * from " + SCHEMA + ".job_tasks where task_id <= 1234 and (task_id <= 100 or priority <> 6) order by task_id");
		tt.setTargetTable(SCHEMA + ".job_tasks_reject");
		tt.setup();
		runTransfer(tt);
		assertFalse("Transfer must be marked as not successful", tt.isSuccessful());
		int expected = queryInt("select count(*) from " + SCHEMA + ".job_tasks where task_id between 101 and 1234 and priority <> 6");
		assertEquals(expected, count(SCHEMA + ".job_tasks_reject"));
		assertEquals(1234, queryInt("select max(task_id) from " + SCHEMA + ".job_tasks_reject"));
	}

	@Test
	public void testContinueOnErrorDuplicateKeys() throws Exception {
		PostgresqlTableTransfer tt = createTransfer(1, false);
		tt.setSourceQuery("select * from " + SCHEMA + ".job_tasks_duplicates order by task_id");
		tt.setTargetTable(SCHEMA + ".job_tasks_copy");
		tt.setup();
		runTransfer(tt);
		assertFalse("Transfer must be marked as not successful", tt.isSuccessful());
		assertTrue(tt.getErrorMessage(), tt.getErrorMessage().contains("job_tasks_copy_pkey"));
		assertEquals(1021, tt.getCurrentCountReads());
		assertEquals(1000, count(SCHEMA + ".job_tasks_copy"));
	}

	private PostgresqlTableTransfer createTransfer(int batchSize, boolean dieOnError) throws Exception {
		PostgresqlTableTransfer tt = new PostgresqlTableTransfer();
		tt.setLogger(LogManager.getLogger(PostgresqlTableTransferTest.class));
		tt.setDoCommit(true);
		tt.setOutputToTable(true);
		tt.setDieOnError(dieOnError);
		tt.setSourceConnection(createConnection(true));
		tt.setSourceFetchSize(100);
		tt.setTargetConnection(createConnection(false));
		tt.setTargetBatchSize(batchSize);
		return tt;
	}

	/**
	 * runs the transfer the same way as the generated Talend code does
	 */
	private void runTransfer(TableTransfer tt) throws Exception {
		long start = System.currentTimeMillis();
		tt.execute();
		while (tt.isRunning()) {
			if (System.currentTimeMillis() - start > TIMEOUT_MILLIS) {
				tt.stop();
				fail("Transfer does not finish within " + TIMEOUT_MILLIS + "ms");
			}
			Thread.sleep(100);
		}
		tt.disconnect();
	}

	private static Connection createConnection(boolean autoCommit) throws SQLException {
		Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
		conn.setAutoCommit(autoCommit);
		return conn;
	}

	private void executeUpdate(String sql) throws SQLException {
		try (Statement stat = checkConnection.createStatement()) {
			stat.executeUpdate(sql);
		}
	}

	private int queryInt(String sql) throws SQLException {
		try (Statement stat = checkConnection.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
			rs.next();
			return rs.getInt(1);
		}
	}

	private int count(String table) throws SQLException {
		return queryInt("select count(*) from " + table);
	}

}
