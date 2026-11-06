package de.jlo.talendcomp.tabletransfer;

import static org.junit.Assert.assertEquals;

import java.sql.BatchUpdateException;
import java.sql.Statement;

import org.junit.Test;

public class FailedLinesTest {

	private static final int F = Statement.EXECUTE_FAILED;

	private static String failedLines(int[] counts, int countBeforeBatch, int batchCount) {
		return TableTransfer.getFailedLines(new BatchUpdateException("test", counts), countBeforeBatch, batchCount);
	}

	@Test
	public void testDriverStopsAtFailure() {
		// 5 statements succeeded, the 6th failed
		assertEquals("line number 206", failedLines(new int[] {1, 1, 1, 1, 1}, 200, 100));
	}

	@Test
	public void testDriverStopsAtFirstStatement() {
		assertEquals("line number 201", failedLines(new int[0], 200, 100));
	}

	@Test
	public void testDriverContinuesAfterFailure() {
		assertEquals("line number 203", failedLines(new int[] {1, Statement.SUCCESS_NO_INFO, F, 1, F}, 200, 5));
	}

	@Test
	public void testDriverRejectsWholeBatch() {
		// PostgreSQL marks all statements as failed
		assertEquals("batch with line numbers 201 - 205", failedLines(new int[] {F, F, F, F, F}, 200, 5));
	}

	@Test
	public void testSingleRecordBatch() {
		assertEquals("line number 7", failedLines(new int[] {F}, 6, 1));
		assertEquals("line number 7", failedLines(null, 6, 1));
	}

	@Test
	public void testNoUpdateCounts() {
		assertEquals("batch with line numbers 1 - 100", failedLines(null, 0, 100));
	}

	@Test
	public void testAllCountsWithoutFailureMark() {
		assertEquals("batch with line numbers 101 - 103", failedLines(new int[] {1, 1, 1}, 100, 3));
	}

}
