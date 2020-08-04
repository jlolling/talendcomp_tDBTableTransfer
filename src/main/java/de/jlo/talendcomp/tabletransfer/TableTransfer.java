/**
 * Copyright 2015 Jan Lolling jan.lolling@gmail.com
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.jlo.datamodel.SQLDataModel;
import de.jlo.datamodel.SQLField;
import de.jlo.datamodel.SQLPSParam;
import de.jlo.datamodel.SQLSchema;
import de.jlo.datamodel.SQLStatement;
import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.StringReplacer;
import de.jlo.datamodel.generator.SQLCodeGenerator;

public class TableTransfer {

	private Logger logger = LoggerFactory.getLogger(TableTransfer.class);
	private Properties properties = new Properties();
	private Connection sourceConnection;
	private Connection targetConnection;
	protected SQLStatement targetInsertStatement;
	private Statement sourceSelectStatement;
	protected PreparedStatement targetPSInsert;
	private SQLDataModel sourceModel;
	private static final Map<String, SQLDataModel> sqlModelCache = new HashMap<String, SQLDataModel>();
	private SQLDataModel targetModel;
	private SQLTable sourceTable;
	private String sourceQuery;
	private SQLTable targetTable;
	private static final int RETURN_CODE_OK = 0;
	private static final int RETURN_CODE_ERROR_INPUT = 1;
	private static final int RETURN_CODE_ERROR_OUPUT = 1;
	private static final int RETURN_CODE_WARN = 5;
	private int returnCode = RETURN_CODE_OK;
	private String errorMessage;
	private Exception errorException;
	private BlockingQueue<Object> tableQueue;
	private BlockingQueue<Object> fileQueue;
	private final Object closeFlag = new String("The End");
	private List<String> listSourceFieldNames;
	private List<String> listSourceFieldTypeNames;
	private Thread readerThread;
	private Thread writerThread;
	private Thread writerBackupThread;
	private volatile int countInsertsAdded = 0;
	private volatile int countInsertsInDB = 0;
	private volatile int countFileRows = 0;
	private volatile int countRead = 0;
	private volatile boolean runningDb = false;
	private volatile boolean runningFile = false;
	private long startTime;
	public static final String SOURCE_FETCHSIZE = "source.fetchSize";
	public static final String SOURCE_TABLE = "source.table";
	public static final String SOURCE_WHERE = "source.whereClause";
	public static final String SOURCE_QUERY = "source.query";
	public static final String TARGET_BATCHSIZE = "target.batchSize";
	public static final String TARGET_TABLE = "target.table";
	public static final String DIE_ON_ERROR = "abortIfErrors";
	private boolean dieOnError = true;
	private boolean initialized = false;
	private List<String> excludeFieldList = new ArrayList<String>();
	private List<ColumnValue> fixedColumnValueList = new ArrayList<ColumnValue>();
	private boolean outputToTable = true;
	private boolean outputToFile = false;
	private File backupFile = null;
	private File backupFileTmp = null;
	private String backupFileCharSet = "UTF-8";
	private String fieldSeparator = ";";
	private String fieldEclosure = "\"";
	private String nullReplacement = "\\N";
	private BufferedWriter backupOutputWriter = null;
	private SimpleDateFormat sdfOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private boolean ignoreReadFieldErrors = true;
	private Pattern patternForBackslash = null;
	private Pattern patternForQuota = null;
	private String replacementForBackslash = null;
	private String replacementForQuota = null;
	private SQLCodeGenerator sourceCodeGenerator = null;
	private SQLCodeGenerator targetCodeGenerator = null;
	private boolean exportBooleanAsNumber = true;
	private Map<String, String> dbJavaTypeMap = new HashMap<String, String>();
	private boolean debug = false;
	private boolean keepDataModels = false;
	private String modelKey = null;
	private Map<Integer, String> outputClassMap = new HashMap<Integer, String>();
	private String valueRangeColumn = null;
	private String timeRangeColumn = null;
	private int valueRangeColumnIndex = -1;
	private int timeRangeColumnIndex = -1;
	private Date timeRangeStart = null;
	private Date timeRangeEnd = null;
	private String valueRangeStart = null;
	private String valueRangeEnd = null;
	private boolean doCommit = true;
	private boolean strictFieldMatching = false;
	private boolean trimFields = false;
	private String checkConnectionStatement = "select 1";
	private boolean withinWriteAction = false;
	
	public void addDbJavaTypeMapping(String dbType, String javaType) {
		if (dbType != null && dbType.trim().isEmpty() == false) {
			if (javaType != null && javaType.trim().isEmpty() == false) {
				dbJavaTypeMap.put(dbType, javaType);
			}
		}
	}
	
	public void addExcludeField(String name) {
		if (name != null && name.trim().isEmpty() == false) {
			excludeFieldList.add(name.trim().toLowerCase());
		}
	}
	
	public void setColumnValue(String name, Object value) {
		if (name != null && name.trim().isEmpty() == false) {
			ColumnValue cv = new ColumnValue(name.trim());
			cv.setValue(value);
			fixedColumnValueList.add(cv);
		}
	}
	
	public final int getCurrentCountInserts() {
		return Math.max(countInsertsInDB, countFileRows);
	}
	
	public final int getCurrentCountReads() {
		return countRead;
	}
	
	public final long getStartTime() {
		return startTime;
	}
	
	public final boolean isRunning() {
		return runningDb || runningFile;
	}
	
	/**
	 * executes the transfer with separate read and write threads
	 * @throws Exception
	 */
	public final void execute() throws Exception {
		if (initialized == false) {
			throw new Exception("Not initialized!");
		}
		countRead = 0;
		countInsertsAdded = 0;
		countInsertsInDB = 0;
		startWriting();
		startReading();
	}
	
	private final void startReading() {
		readerThread = new Thread() {
			@Override
			public void run() {
				read();
			}
		};
		readerThread.setDaemon(false);
		readerThread.start();
	}
	
	private final void startWriting() throws Exception {
		if (outputToTable) {
			debug("Start writer thread...");
			runningDb = true;
			writerThread = new Thread() {
				@Override
				public void run() {
					writeTable();
				}
			};
			writerThread.setDaemon(false);
			writerThread.start();
		}
		if (outputToFile) {
			runningFile = true;
			if (isDebugEnabled()) {
				debug("Create backup file: " + backupFile.getAbsolutePath());
			}
			backupFileTmp = new File(backupFile.getAbsolutePath() + ".tmp");
			backupOutputWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(backupFileTmp), backupFileCharSet));
			if (isDebugEnabled()) {
				debug("Backup file established.");
			}
			writerBackupThread = new Thread() {
				@Override
				public void run() {
					writeFile();
				}
			};
			writerBackupThread.setDaemon(false);
			writerBackupThread.start();
		}
	}
	
	/**
	 * stops the execution (all threads)
	 */
	public final void stop() {
		if (readerThread != null) {
			readerThread.interrupt();
		}
		if (writerThread != null) {
			writerThread.interrupt();
		}
		if (writerBackupThread != null) {
			writerBackupThread.interrupt();
		}
	}
	
	/**
	 * disconnects from the database 
	 */
	public final void disconnect() {
		if (isDebugEnabled()) {
			debug("Close source connection...");
		}
		if (sourceConnection != null) {
			try {
				if (sourceConnection.isClosed() == false) {
					sourceConnection.close();
				}
			} catch (SQLException e) {
				error("disconnect from source failed: " + e.getMessage(), e);
			}
		}
		if (outputToTable) {
			if (isDebugEnabled()) {
				debug("Close target connection...");
			}
			if (targetConnection != null) {
				try {
					if (targetConnection.isClosed() == false) {
						targetConnection.close();
					}
				} catch (SQLException e) {
					error("disconnect from target failed: " + e.getMessage(), e);
				}
			}
		}
	}
	
	private final void read() {
		if (isDebugEnabled()) {
			if (sourceTable != null) {
				debug("Start fetch data from source table: " + sourceTable.getAbsoluteName());
			} else {
				debug("Start fetch data from the given source query");
			}
		}
		try {
			if (isDebugEnabled()) {
				debug("Execute source query: " + sourceQuery);
				debug("Source select statement uses fetch size: " + sourceSelectStatement.getFetchSize());
			}
			final ResultSet rs = sourceSelectStatement.executeQuery(sourceQuery);
			rs.setFetchSize(getFetchSize());
			if (isDebugEnabled()) {
				debug("Analyse result set...");
			}
			final ResultSetMetaData rsMeta = rs.getMetaData();
			final int countColumns = rsMeta.getColumnCount();
			listSourceFieldNames = new ArrayList<String>(countColumns);
			listSourceFieldTypeNames = new ArrayList<String>(countColumns);
			for (int i = 1; i <= countColumns; i++) {
				String name = rsMeta.getColumnName(i).toLowerCase();
				if (name.equalsIgnoreCase(valueRangeColumn)) {
					valueRangeColumnIndex = i;
					if (isDebugEnabled()) {
						debug("Collect min/max for value-range from column: " + name + " at index: " + valueRangeColumnIndex);
					}
				} else if (name.equalsIgnoreCase(timeRangeColumn)) {
					timeRangeColumnIndex = i;
					if (isDebugEnabled()) {
						debug("Collect min/max for time-range from column: " + name + " at index: " + timeRangeColumnIndex);
					}
				}
				String type = rsMeta.getColumnTypeName(i).toUpperCase();
				listSourceFieldNames.add(name);
				listSourceFieldTypeNames.add(type);
				if (isDebugEnabled()) {
					debug("Name: " + name + ",  Type: " + type);
				}
			}
			// add fixed column value names
			for (ColumnValue cv : fixedColumnValueList) {
				listSourceFieldNames.add(cv.getColumnName().toLowerCase());
				if (isDebugEnabled()) {
					debug("Name: " + cv.getColumnName());
				}
			}
			if (isDebugEnabled()) {
				debug("Start fetching data...");
			}
			startTime = System.currentTimeMillis();
			while (rs.next()) {
				final Object[] row = fillRow(rs, countColumns);
				if (valueRangeColumnIndex > 0) {
					checkValueRange(row[valueRangeColumnIndex - 1]);
				}
				if (timeRangeColumnIndex > 0) {
					checkTimeRange(row[timeRangeColumnIndex - 1]);
				}
				if (outputToTable) {
					tableQueue.put(row);
				}
				if (outputToFile) {
					if (writerBackupThread == null || writerBackupThread.isAlive() == false) {
						if (outputToTable) {
							warn("Backup process died. Switch off backup", null);
							outputToFile = false;
						} else {
							throw new Exception("No output will work. The component is in backup only mode and the backup thread is not started or dead. Stop processing.");
						}
					} else {
						fileQueue.put(row);
					}
				}
				countRead++;
				if (Thread.currentThread().isInterrupted()) {
					break;
				}
			}
			rs.close();
			if (isDebugEnabled()) {
				if (sourceTable != null) {
					debug("Finished fetch data from source table " + sourceTable.getAbsoluteName() + " count read:" + countRead);
				} else {
					debug("Finished fetch data from source query, count read:" + countRead);
				}
			}
		} catch (SQLException e) {
			String message = e.getMessage();
			SQLException en = e.getNextException();
			if (en != null) {
				message = "\nNext Exception:" + en.getMessage();
			}
			error("Read failed in line number " + countRead + " message:" + message, e);
			returnCode = RETURN_CODE_ERROR_INPUT;
		} catch (InterruptedException ie) {
			error("Read interrupted (send data sets)", ie);
			returnCode = RETURN_CODE_ERROR_INPUT;
		} catch (Exception ie) {
			error("Read failed: " + ie.getMessage(), ie);
			returnCode = RETURN_CODE_ERROR_INPUT;
		} catch (Error ie) {
			error("Read failed with Error: " + ie.getMessage(), ie);
			returnCode = RETURN_CODE_ERROR_INPUT;
		} finally {
			try {
				if (outputToTable) {
					if (isDebugEnabled()) {
						debug("Stopping write table thread...");
					}
					tableQueue.put(closeFlag);
				}
				if (outputToFile) {
					if (isDebugEnabled()) {
						debug("Stopping write file thread...");
					}
					fileQueue.put(closeFlag);
				}
			} catch (InterruptedException e) {
				error("read interrupted (send close flag)", e);
				returnCode = RETURN_CODE_ERROR_INPUT;
			}
			try {
				if (sourceConnection.getAutoCommit() == false) {
					sourceConnection.commit();
				}
				sourceSelectStatement.close();
			} catch (SQLException e) {}
		}
		if (isDebugEnabled()) {
			debug("End read.");
		}
	}
	
	private final Object[] fillRow(ResultSet rs, int countDBColumns) throws SQLException {
		String dbType = null;
		String javaType = null;
		final Object[] row = new Object[countDBColumns + fixedColumnValueList.size()];
		int columnIndex = 0;
		while (columnIndex < countDBColumns) {
			dbType = listSourceFieldTypeNames.get(columnIndex);
			if (dbType != null) {
				javaType = dbJavaTypeMap.get(dbType);
			} else {
				javaType = null;
			}
			try {
				if (javaType == null) {
					if (trimFields) {
						Object v = rs.getObject(columnIndex + 1);
						if (v instanceof String) {
							row[columnIndex] = ((String) v).trim();
						} else {
							row[columnIndex] = v;
						}
					} else {
						row[columnIndex] = rs.getObject(columnIndex + 1);
					}
				} else if ("date".equals(javaType)) {
					row[columnIndex] = rs.getDate(columnIndex + 1);
				} else if ("time".equals(javaType)) {
					row[columnIndex] = rs.getTime(columnIndex + 1);
				} else if ("timestamp".equals(javaType)) {
					row[columnIndex] = rs.getTimestamp(columnIndex + 1);
				} else if ("string".equals(javaType)) {
					String s = rs.getString(columnIndex + 1);
					if (trimFields && s != null) {
						row[columnIndex] = s.trim();
					} else {
						row[columnIndex] = s;
					}
				} else if ("boolean".equals(javaType)) {
					row[columnIndex] = rs.getBoolean(columnIndex + 1);
				} else if ("short".equals(javaType)) {
					row[columnIndex] = rs.getShort(columnIndex + 1);
				} else if ("byte".equals(javaType)) {
					row[columnIndex] = rs.getByte(columnIndex + 1);
				} else if ("integer".equals(javaType)) {
					row[columnIndex] = rs.getInt(columnIndex + 1);
				} else if ("long".equals(javaType)) {
					row[columnIndex] = rs.getLong(columnIndex + 1);
				} else if ("bigdecimal".equals(javaType)) {
					row[columnIndex] = rs.getBigDecimal(columnIndex + 1);
				} else if ("biginteger".equals(javaType)) {
					row[columnIndex] = new BigInteger(rs.getString(columnIndex + 1));
				} else if ("double".equals(javaType)) {
					row[columnIndex] = rs.getDouble(columnIndex + 1);
				} else if ("float".equals(javaType)) {
					row[columnIndex] = rs.getFloat(columnIndex + 1);
				} else {
					row[columnIndex] = rs.getObject(columnIndex + 1);
				}
			} catch (SQLException e) {
				if (ignoreReadFieldErrors == false) {
					throw e;
				} else {
					warn("Ignore database error while reading field with index: " + columnIndex + " in row: " + countRead + " message: " + e.getMessage(), e);
					row[columnIndex] = null;
				}
			}
			columnIndex++;
		}
		for (ColumnValue cv : fixedColumnValueList) {
			row[columnIndex++] = cv.getValue();
		}
		return row;
	}
	
	public void executeKeepAliveStatementForTargetConnection() throws Exception {
		if (withinWriteAction == false && checkConnectionStatement != null && checkConnectionStatement.trim().isEmpty() == false) {
			try {
				debug("Execute keep alive statement on target connection...");
				Statement checkStat = targetConnection.createStatement();
				checkStat.execute(checkConnectionStatement);
				checkStat.close();
			} catch (Exception e) {
				stop();
				throw new Exception("Check target connection with statement: " + checkConnectionStatement + " failed: " + e.getMessage(), e);
			}
		}
	}
	
	private final void writeTable() {
		if (isDebugEnabled()) {
			debug("Start writing data into target table " + targetTable.getAbsoluteName());
		}
		final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "1"));
		int currentBatchCount = 0;
		try {
			boolean autocommitTemp = false;
			try {
				if (targetConnection == null) {
					throw new Exception("Write into table: " + targetTable.getAbsoluteName() + " failed because target connection is null");
				}
				if (targetConnection.isClosed()) {
					throw new Exception("Write into table: " + targetTable.getAbsoluteName() + " failed because target connection is closed");
				}
				autocommitTemp = targetConnection.getAutoCommit();
			} catch (Exception e2) {
				warn("Failed to detect autocommit state: " + e2.getMessage(), e2);
			}
			final boolean autocommit = autocommitTemp;
			boolean endFlagReceived = false;
			while (endFlagReceived == false) {
				try {
					final List<Object> queueObjects = new ArrayList<Object>(batchSize);
					withinWriteAction = false;
					// poll waits for a time until new records arrives
					Object one = tableQueue.poll(10000, TimeUnit.MILLISECONDS);
					if (one == null) {
						continue;
					} else {
						queueObjects.add(one);
					}
					// drain never waits! Thats why we have to use poll before!
					tableQueue.drainTo(queueObjects, batchSize); // pull elements from queue to this given list
//					debug("Got " + queueObjects.size() + " records from queue.");
					for (Object item : queueObjects) {
						if (item == closeFlag) {
							if (isDebugEnabled()) {
								debug("Write table thread: Stop flag received.");
							}
							endFlagReceived = true;
							break;
						} else {
							withinWriteAction = true;
							prepareInsertStatement((Object[]) item);
							targetPSInsert.addBatch();
							countInsertsAdded++;
							currentBatchCount++;
							if (currentBatchCount == batchSize) {
								debug("Write execute insert batch ends with recno: " + countInsertsAdded);
								targetPSInsert.executeBatch();
								countInsertsInDB = countInsertsAdded;
								if (doCommit) {
									if (autocommit == false) {
										targetConnection.commit();
									}
								}
								currentBatchCount = 0;
							}
						}
						if (Thread.currentThread().isInterrupted()) {
							break;
						}
					}
				} catch (InterruptedException e) {
					error("Write interrupted in line " + countInsertsAdded, e);
					returnCode = RETURN_CODE_ERROR_OUPUT;
					break;
				} catch (SQLException sqle) {
					if (sqle instanceof BatchUpdateException) {
						BatchUpdateException be = (BatchUpdateException) sqle;
						debug("Write execute insert batch ends with recno: " + countInsertsAdded);
						int[] counts = be.getUpdateCounts();
						int batchIndex = 0;
						for (int c : counts) {
							batchIndex = batchIndex + c;
						}
						batchIndex = (countInsertsAdded - batchSize) + batchIndex; // set the batchIndex as the absolute over all index
						error("Write failed in line number " + batchIndex + " message:" + sqle.getMessage(), sqle);
					} else {
						error("Write failed in line number " + countInsertsAdded + " message:" + sqle.getMessage(), sqle);
					}
					SQLException ne = sqle.getNextException();					
					if (ne != null) {
						error("Next exception:" + ne.getMessage(), ne);
					}
					if (dieOnError) {
						returnCode = RETURN_CODE_ERROR_OUPUT;
						try {
							if (autocommit == false) {
								targetConnection.rollback();
							}
						} catch (SQLException e) {
							error("Write rollback failed: " + e.getMessage(), e);
						}
						break;
					} else {
						if (doCommit) {
							try {
								if (autocommit == false) {
									targetConnection.commit();
								}
							} catch (SQLException e) {
								error("Write commit failed: " + e.getMessage(), e);
							}
						}
					}
				} catch (Exception e1) {
					returnCode = RETURN_CODE_ERROR_OUPUT;
					error("Write failed:" + e1.getMessage(), e1);
					break;
				}
			}
			if (currentBatchCount > 0 && returnCode == RETURN_CODE_OK) {
				try {
					if (isDebugEnabled()) {
						debug("write execute final insert batch");
					}
					targetPSInsert.executeBatch();
					countInsertsInDB = countInsertsInDB + currentBatchCount;
					if (doCommit) {
						if (autocommit == false) {
							targetConnection.commit();
						}
					}
					currentBatchCount = 0;
				} catch (SQLException sqle) {
					returnCode = RETURN_CODE_ERROR_OUPUT;
					if (sqle instanceof BatchUpdateException) {
						BatchUpdateException be = (BatchUpdateException) sqle;
						int[] counts = be.getUpdateCounts();
						int batchIndex = 0;
						for (int c : counts) {
							batchIndex = batchIndex + c;
						}
						batchIndex = (countInsertsAdded - batchSize) + batchIndex; // set the batchIndex to the absolute over all index
						error("Write failed in line number " + batchIndex + " message:" + sqle.getMessage(), sqle);
					} else {
						error("Write failed in line number " + countInsertsAdded + " message:" + sqle.getMessage(), sqle);
					}
					SQLException ne = sqle.getNextException();					
					if (ne != null) {
						error("Next exception:" + ne.getMessage(), ne);
					}
					try {
						targetConnection.rollback();
					} catch (SQLException e) {
						error("write rollback failed:" + e.getMessage(), e);
					}
				}
			}
			if (returnCode == RETURN_CODE_ERROR_INPUT) {
				error("Read has been failed. Stop write in table.", null);
			}
		} finally {
			try {
				targetPSInsert.close();
			} catch (SQLException e) {}
			runningDb = false;
			if (isDebugEnabled()) {
				debug("Finished write data into target table " + targetTable.getAbsoluteName() + ", count inserts:" + countInsertsInDB);
			}
		}
		if (isDebugEnabled()) {
			debug("Write table finished.");
		}
	}
	
	protected final Object getRowValue(final String columnName, final Object[] row) throws Exception {
		if (listSourceFieldNames == null) {
			throw new Exception("List of source fields is not initialized");
		}
		if (listSourceFieldNames.isEmpty()) {
			throw new Exception("List of source fields is empty");
		}
		final int index = listSourceFieldNames.indexOf(columnName.toLowerCase());
		if (index != -1) {
			return row[index];
		} else {
			if (strictFieldMatching) {
				StringBuilder sb = new StringBuilder();
				sb.append("Following target columns does not have a matching column in the source query: ");
				boolean firstLoop = true;
				for (SQLPSParam p : targetInsertStatement.getParams()) {
					String targetColumnName = p.getName().toLowerCase();
					if (listSourceFieldNames.contains(targetColumnName) == false) {
						if (firstLoop) {
							firstLoop = false;
						} else {
							sb.append(",");
						}
						sb.append(targetColumnName);
					}
				}	
				firstLoop = true;
				sb.append("\nList of source query columns: ");
				for (String sourceColumn : listSourceFieldNames) {
					if (firstLoop) {
						firstLoop = false;
					} else {
						sb.append(",");
					}
					sb.append(sourceColumn);
				}
				throw new Exception("Transfer into table: " + targetTable.getAbsoluteName() + " in strict mode failed: " + sb.toString());
			} else {
				return null;
			}
		}
	}
	
	
	
	protected final void prepareInsertStatement(final Object[] row) throws Exception {
		for (SQLPSParam p : targetInsertStatement.getParams()) {
			final Object value = getRowValue(p.getName(), row);
			if (value != null) {
				String className = outputClassMap.get(p.getIndex());
				if (className == null) {
					className = value.getClass().getSimpleName();
					outputClassMap.put(p.getIndex(), className);
					if (isDebugEnabled()) {
						debug("Output class mapping: #" + p.getIndex() + " (" + p.getName() + ") use: " + className);
					}
				}
				if ("BigDecimal".equals(className)) {
					targetPSInsert.setBigDecimal(p.getIndex(), (BigDecimal) value);
				} else if ("BigInteger".equals(className)) {
					targetPSInsert.setLong(p.getIndex(), ((BigInteger) value).longValue());
				} else if ("Double".equals(className)) {
					targetPSInsert.setDouble(p.getIndex(), (Double) value);
				} else if ("Float".equals(className)) {
					targetPSInsert.setFloat(p.getIndex(), (Float) value);
				} else if ("Long".equals(className)) {
					targetPSInsert.setLong(p.getIndex(), (Long) value);
				} else if ("Integer".equals(className)) {
					targetPSInsert.setInt(p.getIndex(), (Integer) value);
				} else if ("Short".equals(className)) {
					targetPSInsert.setShort(p.getIndex(), (Short) value);
				} else if ("String".equals(className)) {
					targetPSInsert.setString(p.getIndex(), (String) value);
				} else if ("Date".equals(className)) {
					targetPSInsert.setDate(p.getIndex(), (java.sql.Date) value);
				} else if ("Timestamp".equals(className)) {
					targetPSInsert.setTimestamp(p.getIndex(), (Timestamp) value);
				} else if ("Time".equals(className)) {
					targetPSInsert.setTime(p.getIndex(), (Time) value);
				} else if ("Boolean".equals(className)) {
					targetPSInsert.setBoolean(p.getIndex(), (Boolean) value);
				} else if ("String".equals(className)) {
					targetPSInsert.setString(p.getIndex(), (String) value);
				} else {
					targetPSInsert.setObject(p.getIndex(), value);
				}
			} else {
				targetPSInsert.setNull(p.getIndex(), targetTable.getField(p.getName()).getType());
			}
		}
	}
	
	public final void executeSQLOnTarget(final String sqlStatement) throws Exception {
		info("On target: Execute statement: " + sqlStatement);
		if (targetConnection == null || targetConnection.isClosed()) {
			error("Execute statement on target failed because connection is null or closed", null);
			throw new Exception("Write into table: " + targetTable.getAbsoluteName() + " failed. Execute statement on target failed because connection is null or closed");
		}
		try {
			final Statement stat = targetConnection.createStatement();
			stat.execute(sqlStatement);
			stat.close();
			info("On target: " + targetTable.getAbsoluteName() + ": Execute statement finished successfully.");
		} catch (SQLException sqle) {
			String message = "On target: " + targetTable.getAbsoluteName() + ": Execute statement failed sql=" + sqlStatement + " message: " + sqle.getMessage();
			throw new Exception(message, sqle);
		}
	}
	
	public void commitSource() throws SQLException {
		sourceConnection.commit();
	}
	
	public void commitTarget() throws SQLException {
		if (targetConnection == null || targetConnection.isClosed()) {
			throw new IllegalStateException("writeTable failed because target connection is null or closed");
		}
		targetConnection.commit();
	}
	
	/**
	 * setup statements and internal structures
	 * @throws Exception
	 */
	public final void setup() throws Exception {
		createSourceSelectStatement();
		if (outputToTable) {
			createTargetInsertStatement();
		}
		final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "100"));
		final int fetchSize = Integer.parseInt(properties.getProperty(SOURCE_FETCHSIZE, "100"));
		final int queueSize = Math.max(batchSize, fetchSize);
		if (outputToTable) {
			tableQueue = new ArrayBlockingQueue<Object>(queueSize);
		}
		if (outputToFile) {
			fileQueue = new ArrayBlockingQueue<Object>(queueSize);
		}
		dieOnError = Boolean.parseBoolean(properties.getProperty(DIE_ON_ERROR, "true"));
		patternForBackslash = Pattern.compile("\\", Pattern.LITERAL);
		patternForQuota = Pattern.compile("\"", Pattern.LITERAL);
		replacementForBackslash = Matcher.quoteReplacement("\\\\");
		if (replacementForQuota != null) {
			replacementForQuota = Matcher.quoteReplacement(replacementForQuota);
		} else {
			replacementForQuota = Matcher.quoteReplacement("\\\"");
		}
		initialized = true;
	}
	
	protected final SQLTable getSourceSQLTable() throws Exception {
		final String tableAndSchemaName = properties.getProperty(SOURCE_TABLE);
		if (sourceTable == null || sourceTable.getAbsoluteName().equalsIgnoreCase(tableAndSchemaName) == false) {
			String schemaName = getSchemaName(tableAndSchemaName);
			if (schemaName == null) {
				schemaName = sourceConnection.getSchema();
			}
			if (schemaName == null) {
				schemaName = getSourceDatabase();
			}
			SQLSchema schema = sourceModel.getSchema(schemaName);
			if (schema == null) {
				throw new Exception("getSourceTable failed: schema " + schemaName + " not available");
			}
			String tableName = getTableName(tableAndSchemaName);
			sourceTable = schema.getTable(tableName);
			if (sourceTable == null) {
				throw new Exception("getSourceTable failed: table " + schemaName + "." + tableName + " not available");
			}
			if (sourceTable.isFieldsLoaded() == false) {
				sourceTable.loadColumns(true);
			}
			for (String exclFieldName : excludeFieldList) {
				SQLField field = sourceTable.getField(exclFieldName);
				if (field != null) {
					sourceTable.removeSQLField(field);
				}
			}
		}
		return sourceTable;
	}
	
	protected String getSourceDatabase() throws SQLException {
		String cat = sourceConnection.getCatalog();
		if (cat == null || cat.trim().isEmpty()) {
			cat = "public";
		}
		return cat;
	}
	
	protected String getTargetDatabase() throws SQLException {
		String cat = targetConnection.getCatalog();
		if (cat == null || cat.trim().isEmpty()) {
			cat = "public";
		}
		return cat;
	}

	protected final SQLTable getTargetSQLTable() throws Exception {
		final String tableAndSchemaName = properties.getProperty(TARGET_TABLE);
		if (targetTable == null || targetTable.getAbsoluteName().equalsIgnoreCase(tableAndSchemaName) == false) {
			String schemaName = getSchemaName(tableAndSchemaName);
			if (schemaName == null) {
				schemaName = targetConnection.getSchema();
			}
			if (schemaName == null) {
				schemaName = getTargetDatabase();
			}
			SQLSchema schema = targetModel.getSchema(schemaName);
			if (schema == null) {
				throw new Exception("Get information about target table: " + tableAndSchemaName + " failed: schema " + schemaName + " not available");
			}
			String tableName = getTableName(tableAndSchemaName);
			if (tableName.startsWith("\"")) {
				tableName = tableName.substring(1, tableName.length() - 1);
			}
			targetTable = schema.getTable(tableName);
			if (targetTable == null) {
				throw new Exception("Get information about target table: " + schemaName + "." + tableName + " not available");
			}
			for (String exclFieldName : excludeFieldList) {
				boolean exclude = true;
				// take care we exclude a column only if we do not have a fixed value
				for (ColumnValue cv : fixedColumnValueList) {
					if (cv.getColumnName().equalsIgnoreCase(exclFieldName)) {
						exclude = false;
						break;
					}
				}
				if (exclude) {
					SQLField field = targetTable.getField(exclFieldName);
					if (field != null) {
						targetTable.removeSQLField(field);
					}
				}
			}
		}
		return targetTable;
	}
	
	protected Statement createSourceSelectStatement() throws Exception {
		sourceQuery = properties.getProperty(SOURCE_QUERY);
		if (sourceQuery == null) {
			SQLTable table = getSourceSQLTable();
			if (table.isFieldsLoaded() == false) {
				table.loadColumns(true);
			}
			sourceQuery = getSourceCodeGenerator().buildSelectStatement(table, true) + buildSourceWhereSQL();
			properties.put(SOURCE_QUERY, sourceQuery);
		}
		if (isDebugEnabled()) {
			debug("createSourceSelectStatement SQL:" + sourceQuery);
		}
		sourceSelectStatement = sourceConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		int fetchSize = getFetchSize();
		if (fetchSize > 0) {
			debug("set source fetch size: " + fetchSize);
			sourceSelectStatement.setFetchSize(fetchSize);
		}
		// we have to check that here because we do not know which source database type we use.
		if (DBHelper.isMySQLConnection(sourceConnection)) {
			DBHelper util = (DBHelper) Class.forName("de.jlo.talendcomp.tabletransfer.MySQLHelper").newInstance();
			util.setupSelectStatement(sourceSelectStatement);
		}
		return sourceSelectStatement;
	}
	
	protected int getFetchSize() {
		int fetchSize = 100;
		try {
			fetchSize = Integer.parseInt(properties.getProperty(SOURCE_FETCHSIZE, "1000"));
		} catch (Exception e) {
			warn("getFetchSize failed: " + e.getMessage(), e);
		}
		return fetchSize;
	}
	
	protected PreparedStatement createTargetInsertStatement() throws Exception {
		SQLTable table = getTargetSQLTable();
		if (table.isFieldsLoaded() == false) {
			table.loadColumns(true);
		}
		targetInsertStatement = getTargetCodeGenerator().buildPSInsertSQLStatement(table, true);
		if (isDebugEnabled()) {
			debug("createTargetInsertStatement SQL:" + targetInsertStatement.getSQL());
		}
		targetPSInsert = targetConnection.prepareStatement(targetInsertStatement.getSQL());
		return targetPSInsert;
	}
	
	protected final String buildSourceWhereSQL() {
		String where = properties.getProperty(SOURCE_WHERE);
		if (where != null && where.trim().isEmpty() == false) {
			where = replacePlaceholders(where);
			if (where.startsWith("where")) {
				return " " + where;
			} else {
				return " where " + where;
			}
		} else {
			return "";
		}
	}
	
	protected final String replacePlaceholders(String stringWithPlaceholders) {
		boolean ready = false;
		final List<String> listPlaceHolders = new ArrayList<String>();
		int p0 = -1;
		int p1 = -1;
		while (ready == false) {
			p0 = stringWithPlaceholders.indexOf('{', p1 + 1);
			p1 = stringWithPlaceholders.indexOf('}', p0 + 1);
			if (p0 != -1 && p1 != -1) {
				String key = stringWithPlaceholders.substring(p0 + 1, p1);
				listPlaceHolders.add(key);
			} else {
				ready = true;
			}
		}
		final StringReplacer sr = new StringReplacer(stringWithPlaceholders);
		for (String key : listPlaceHolders) {
			String value = properties.getProperty(key);
			if (value == null) {
				warn("replacePlaceholders for string " + stringWithPlaceholders + " failed in key:" + key + " reason: missing value", null);
				returnCode = RETURN_CODE_WARN;
				value = "";
			}
			sr.replace("{" + key + "}", value.trim());
		}
		return sr.getResultText();
	}
	
	protected final String getSchemaName(String schemaAndTable) {
		final int pos = schemaAndTable.indexOf('.');
		if (pos > 0) {
			return schemaAndTable.substring(0, pos);
		} else {
			return null;
		}
	}
	
	protected final String getTableName(String schemaAndTable) {
		int pos = schemaAndTable.indexOf('.');
		if (pos > 0) {
			return schemaAndTable.substring(pos + 1, schemaAndTable.length());
		} else {
			return schemaAndTable;
		}
	}
	
	public final void setupDataModels() throws Exception {
		if (keepDataModels) {
			synchronized (sqlModelCache) {
				sourceModel = sqlModelCache.get("source_" + modelKey);
				if (sourceModel == null) {
					sourceModel = new SQLDataModel(sourceConnection);
					sourceModel.loadCatalogs();
					sqlModelCache.put("source_" + modelKey, sourceModel);
				} else {
					sourceModel.setConnection(sourceConnection);
				}
			}
		} else {
			sourceModel = new SQLDataModel(sourceConnection);
			sourceModel.loadCatalogs();
		}
		if (sourceConnection != null && sourceConnection.getAutoCommit() == false) {
			sourceConnection.commit();
		}
		if (outputToTable) {
			if (keepDataModels) {
				synchronized (sqlModelCache) {
					targetModel = sqlModelCache.get("target_" + modelKey);
					if (targetModel == null) {
						targetModel = new SQLDataModel(targetConnection);
						targetModel.loadCatalogs();
						sqlModelCache.put("target_" + modelKey, targetModel);
					} else {
						targetModel.setConnection(targetConnection);
					}
				}
			} else {
				targetModel = new SQLDataModel(targetConnection);
				targetModel.loadCatalogs();
			}
			if (targetConnection.getAutoCommit() == false) {
				targetConnection.commit();
			}
		}
	}
	
    public void loadProperties(String filePath) {
        try {
            final FileInputStream fis = new FileInputStream(filePath);
            properties.load(fis);
            fis.close();
        } catch (IOException e) {
        	error("LoadProperties from " + filePath + " failed: " + e.getMessage(), e);
			returnCode = RETURN_CODE_ERROR_INPUT;
        }
    }
    
    public String getSourceFetchSize() {
    	return properties.getProperty(SOURCE_FETCHSIZE);
    }
    
    public void setSourceFetchSize(String fetchSize) {
    	properties.setProperty(SOURCE_FETCHSIZE, fetchSize);
    }
    
    public void setSourceFetchSize(Integer fetchSize) {
    	if (fetchSize != null) {
    		setSourceFetchSize(String.valueOf(fetchSize));
    	}
    }
    
    public String getTargetInsertStatement() {
    	return targetInsertStatement.getSQL();
    }
    
	public String getSourceQuery() {
		return properties.getProperty(SOURCE_QUERY);
	}

	public void setSourceQuery(String sourceQuery) {
		if (sourceQuery != null) {
			sourceQuery = sourceQuery.trim();
		}
		if (sourceQuery.endsWith(";")) {
			sourceQuery = sourceQuery.substring(0, sourceQuery.length() - 1);
		}
		properties.setProperty(SOURCE_QUERY, sourceQuery);
	}

    public String getSourceTable() throws SQLException {
    	return getSourceCodeGenerator().getEncapsulatedName(properties.getProperty(SOURCE_TABLE));
    }
    
    public void setSourceTable(String tableAndSchema) {
    	if (tableAndSchema == null || tableAndSchema.trim().isEmpty()) {
    		throw new IllegalArgumentException("Source schema.table cannot be null or empty! (Got: " + tableAndSchema + ")");
    	} else if (tableAndSchema.endsWith(".null") || tableAndSchema.endsWith(".")) {
    		throw new IllegalArgumentException("Source table cannot be null or empty! (Got: " + tableAndSchema + ")");
    	} else if (tableAndSchema.startsWith("null.") || tableAndSchema.startsWith(".")) {
    		throw new IllegalArgumentException("Source schema cannot be null or empty! (Got: " + tableAndSchema + ")");
    	}
    	properties.setProperty(SOURCE_TABLE, tableAndSchema);
    	properties.remove(SOURCE_QUERY); // remove query from previous run
    }
    
    public String getSourceWhereClause() {
    	return properties.getProperty(SOURCE_WHERE);
    }
    
    public void setSourceWhereClause(String whereClause) {
    	properties.setProperty(SOURCE_WHERE, whereClause);
    }
    
    public String getTargetBatchSize() {
    	return properties.getProperty(TARGET_BATCHSIZE);
    }
    
    public void setTargetBatchSize(String batchSize) {
    	properties.setProperty(TARGET_BATCHSIZE, batchSize);
    }
    
    public void setTargetBatchSize(Integer batchSize) {
    	if (batchSize != null) {
    		setTargetBatchSize(String.valueOf(batchSize));
    	}
    }
    
    public String getTargetTable() throws SQLException {
    	return getTargetCodeGenerator().getEncapsulatedName(properties.getProperty(TARGET_TABLE));
    }
    
    public void setTargetTable(String tableAndSchema) {
    	if (tableAndSchema == null || tableAndSchema.trim().isEmpty()) {
    		throw new IllegalArgumentException("Target schema.table cannot be null or empty! (Got: " + tableAndSchema + ")");
    	} else if (tableAndSchema.endsWith(".null") || tableAndSchema.endsWith(".")) {
    		throw new IllegalArgumentException("Target table cannot be null or empty! (Got: " + tableAndSchema + ")");
    	} else if (tableAndSchema.startsWith("null.") || tableAndSchema.startsWith(".")) {
    		throw new IllegalArgumentException("Target schema cannot be null or empty! (Got: " + tableAndSchema + ")");
    	}
    	properties.setProperty(TARGET_TABLE, tableAndSchema);
    }

    public int getReturnCode() {
    	return returnCode;
    }
    
    public void setProperty(String key, String value) {
    	properties.setProperty(key, value);
    }
    
    public String getProperty(String key) {
    	return properties.getProperty(key);
    }
    
    public boolean isSuccessful() {
    	return returnCode == RETURN_CODE_OK;
    }
    
	public final void warn(String message, Exception t) {
		if (logger != null) {
			if (t != null) {
				logger.warn(message, t);
			} else {
				logger.warn(message);
			}
		} else {
			System.err.println("WARN: " + message);
			if (t != null) {
				t.printStackTrace();
			}
		}
		if (message != null) {
			errorMessage = message;
		}
		if (t != null) {
			errorException = t;
		}
	}
	
	public final boolean isDebugEnabled() {
		if (logger != null) {
			return logger.isDebugEnabled();
		} else {
			return debug;
		}
	}
	
	public final void debug(String message) {
		if (logger != null && logger.isDebugEnabled()) {
			logger.debug(message);
		} else if (debug) {
			System.out.println("DEBUG: " + message);
		}
	}

	public final void info(String message) {
		if (logger != null) {
			logger.info(message);
		} else {
			System.out.println("INFO: " + message);
		}
	}

	public final void error(String message, Throwable t) {
		if (logger != null) {
			if (t != null) {
				logger.error(message, t);
			} else {
				logger.error(message);
			}
		} else {
			System.err.println("ERROR: " + message);
			if (t != null) {
				t.printStackTrace();
			}
		}
		if (message != null) {
			errorMessage = message;
		}
		if (t instanceof Exception) {
			errorException = (Exception) t;
		}
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public Exception getErrorException() {
		return errorException;
	}
	
	public Connection getSourceConnection() {
		return sourceConnection;
	}
	
	public Statement getSourceStatement() {
		return sourceSelectStatement;
	}

	private boolean isClosed(Connection connection) {
		try {
			return connection.isClosed();
		} catch (SQLException e) {
			logger.error("Check if connection is closed failed:" + e.getMessage(), e);
			return true;
		}
	}
	
	/**
	 * set an connected connection
	 * In this case it is not useful to call connect() or disconnect()
	 * @param sourceConnection
	 */
	public void setSourceConnection(Connection sourceConnection) {
		if (sourceConnection == null) {
			throw new IllegalArgumentException("Source connection cannot be null!");
		} else if (sourceConnection == targetConnection) {
			throw new IllegalArgumentException("Source connection cannot be the same as used for the target! Establish for source and target different connection instances!");
		} else if (isClosed(sourceConnection)) {
			throw new IllegalArgumentException("Source connection is already closed!");
		}
		this.sourceConnection = sourceConnection;
	}

	public Connection getTargetConnection() {
		return targetConnection;
	}

	/**
	 * set an connected connection
	 * In this case it is not useful to call connect() or disconnect()
	 * @param targetConnection
	 */
	public void setTargetConnection(Connection targetConnection) throws Exception {
		if (targetConnection == null) {
			throw new IllegalArgumentException("Target connection cannot be null!");
		} else if (sourceConnection == targetConnection) {
			throw new IllegalArgumentException("Target connection cannot be the same as used for the source! Establish for source and target different connection instances!");
		}
		if (targetConnection.isReadOnly()) {
			throw new Exception("Target connection cannot be in read only mode!");
		} else if (isClosed(targetConnection)) {
			throw new IllegalArgumentException("Target connection is already closed!");
		}
		this.targetConnection = targetConnection;
	}
	
	public static final double roundScale2(Double number) {
		if (number != null) {
			return Math.round(number * 100d) / 100d;
		} else {
			return 0;
		}
	}

	public String getBackupFilePath() {
		if (backupFile != null) {
			return backupFile.getAbsolutePath();
		} else {
			return null;
		}
	}

	public String setBackupFilePath(String backupFilePath) throws Exception {
		if (backupFilePath != null && backupFilePath.trim().isEmpty() == false) {
			File test = new File(backupFilePath);
			if (test.isDirectory()) {
				if (test.exists() == false) {
					test.mkdirs();
					if (test.exists() == false) {
						throw new Exception("Backup dir: " + test.getAbsolutePath() + " could not be created!");
					}
				}
				test = new File(test, getTargetTable() + ".csv");
				this.backupFile = test;
				outputToFile = true;
				return backupFile.getAbsolutePath();
			} else {
				File dir = test.getParentFile();
				if (dir == null) {
					throw new Exception("Backup file has to be an absolute path (directory or file)!");
				} else if (dir.exists() == false) {
					dir.mkdirs();
					if (dir.exists() == false) {
						throw new Exception("Backup dir: " + dir.getAbsolutePath() + " could not be created!");
					}
				}
				this.backupFile = test;
				outputToFile = true;
				return backupFile.getAbsolutePath();
			}
		}
		return null;
	}
	
	private String convertToString(Object value) {
		if (value instanceof String) {
			String sValue = (String) value;
			if (sValue.isEmpty()) {
				return "";
			} else {
				// escape the " and the \
				Matcher m1 = patternForBackslash.matcher(sValue);
				sValue = m1.replaceAll(replacementForBackslash);
				Matcher m2 = patternForQuota.matcher(sValue);
				sValue = m2.replaceAll(replacementForQuota);
				return sValue;
			}
		} else if (value instanceof Date) {
			return sdfOut.format((Date) value);
		} else if (value instanceof Boolean) {
			if (exportBooleanAsNumber) {
				return ((Boolean) value) ? "1" : "0";
			} else {
				return Boolean.toString((Boolean) value);
			}
		} else if (value instanceof Short) {
			return Short.toString((Short) value);
		} else if (value instanceof Integer) {
			return Integer.toString((Integer) value);
		} else if (value instanceof Long) {
			return Long.toString((Long) value);
		} else if (value instanceof Double) {
			return Double.toString((Double) value);
		} else if (value instanceof Float) {
			return Float.toString((Float) value);
		} else if (value instanceof BigDecimal) {
			return ((BigDecimal) value).toPlainString();
		} else if (value != null) {
			return value.toString();
		} else {
			return nullReplacement;
		}
	}
	
	private void writeRowInFile(Object[] row) throws Exception {
		if (row != null) {
			boolean firstLoop = true;
			for (Object value : row) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					backupOutputWriter.write(fieldSeparator);
				}
				if (value != null) {
					backupOutputWriter.write(fieldEclosure);
					backupOutputWriter.write(convertToString(value));
					backupOutputWriter.write(fieldEclosure);
				} else {
					backupOutputWriter.write(nullReplacement);
				}
			}
			backupOutputWriter.write("\n");
			countFileRows++;
		}
	}
	
	private void writeFile() {
		try {
			countFileRows = 0;
			debug("Start writing data in file: " + backupFile.getAbsolutePath());
			final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "1"));
			boolean endFlagReceived = false;
			while (endFlagReceived == false) {
				try {
					final List<Object> queueObjects = new ArrayList<Object>(batchSize);
					// poll waits for a time until new records arrives
					Object one = fileQueue.poll(10000, TimeUnit.MILLISECONDS);
					if (one == null) {
						continue;
					} else {
						queueObjects.add(one);
					}
					// drain never waits! Thats why we have to use poll before!
					fileQueue.drainTo(queueObjects, batchSize);
					for (Object item : queueObjects) {
						if (item == closeFlag) {
							debug("Write file thread: Stop flag received.");
							endFlagReceived = true;
							break;
						} else {
							writeRowInFile((Object[]) item);
						}
						if (Thread.currentThread().isInterrupted()) {
							break;
						}
					}
				} catch (Exception e) {
					error("write file failed in line number " + countFileRows + " message:" + e.getMessage(), e);
					if (dieOnError) {
						returnCode = RETURN_CODE_ERROR_OUPUT;
						break;
					}
				}
			}
			try {
				backupOutputWriter.flush();
				backupOutputWriter.close();
			} catch (Exception e) {
				error("Close file failed: " + e.getMessage(), e);
			}
			runningFile = false;
			if (returnCode == RETURN_CODE_OK) {
				debug("Finished write data into file " + backupFile.getAbsolutePath() + ", count rows:" + countFileRows);
				debug("Rename tmp file: " + backupFileTmp.getAbsolutePath() + " to target file: " + backupFile.getAbsolutePath());
				backupFileTmp.renameTo(backupFile);
			} else if (returnCode == RETURN_CODE_ERROR_INPUT) {
				debug("Finished write data into file " + backupFile.getAbsolutePath() + ", count rows:" + countFileRows);
				warn("Read has been failed. Rename file as error file.", null);
				File errorFile = new File(backupFile.getAbsolutePath() + ".error");
				backupFileTmp.renameTo(errorFile);
			} else if (returnCode == RETURN_CODE_ERROR_OUPUT) {
				debug("Finished write data into file " + backupFile.getAbsolutePath() + ", count rows:" + countFileRows);
				warn("Write to file has been failed. Rename file as error file.", null);
				File errorFile = new File(backupFile.getAbsolutePath() + ".error");
				backupFileTmp.renameTo(errorFile);
			}
		} catch (Exception e) {
			try {
				backupOutputWriter.flush();
				backupOutputWriter.close();
			} catch (Exception e1) {
				error("Close file failed: " + e.getMessage(), e);
			}
			runningFile = false;
			error("Write data into file " + backupFile.getAbsolutePath() + " count rows:" + countFileRows + " failed: " + e.getMessage(), e);
		}
		debug("Writing file has been finished.");
	}

	public boolean isOutputToTable() {
		return outputToTable;
	}

	public void setOutputToTable(boolean outputToTable) {
		this.outputToTable = outputToTable;
	}
	
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public boolean isExportBooleanAsNumber() {
		return exportBooleanAsNumber;
	}

	public void setExportBooleanAsNumber(Boolean exportBooleanAsNumber) {
		if (exportBooleanAsNumber != null) {
			this.exportBooleanAsNumber = exportBooleanAsNumber.booleanValue();
		}
	}

	public boolean isKeepDataModels() {
		return keepDataModels;
	}

	public void setKeepDataModels(boolean keepDataModels, String key) {
		this.keepDataModels = keepDataModels;
		if (keepDataModels && (key == null || key.trim().isEmpty())) {
			throw new IllegalArgumentException("If the model should kept statically the key cannot be null or empty!");
		}
		this.modelKey = key;
	}

	public String getValueRangeColumn() {
		return valueRangeColumn;
	}

	public void setValueRangeColumn(String valueRangeColumn) {
		if (valueRangeColumn != null && valueRangeColumn.trim().isEmpty() == false) {
			this.valueRangeColumn = valueRangeColumn;
		}
	}

	public String getTimeRangeColumn() {
		return timeRangeColumn;
	}

	public void setTimeRangeColumn(String timeRangeColumn) {
		if (timeRangeColumn != null && timeRangeColumn.trim().isEmpty() == false) {
			this.timeRangeColumn = timeRangeColumn;
		}
	}

	public Date getTimeRangeStart() {
		return timeRangeStart;
	}

	public Date getTimeRangeEnd() {
		return timeRangeEnd;
	}

	public String getValueRangeStart() {
		return valueRangeStart;
	}

	public String getValueRangeEnd() {
		return valueRangeEnd;
	}
	
	private void checkTimeRange(Object value) {
		if (value instanceof Long) {
			checkTimeRange((Long) value);
		} else if (value instanceof Date) {
			checkTimeRange((Date) value);
		}
	}
	
	private void checkTimeRange(Long timeRangeLong) {
		if (timeRangeLong != null) {
			Date timeRangeDate = new Date(timeRangeLong);
			if (this.timeRangeStart == null || this.timeRangeStart.after(timeRangeDate)) {
				this.timeRangeStart = timeRangeDate;
			}
			if (this.timeRangeEnd == null || this.timeRangeEnd.before(timeRangeDate)) {
				this.timeRangeEnd = timeRangeDate;
			}
		}
	}

	private void checkTimeRange(Date timeRangeDate) {
		if (timeRangeDate != null) {
			if (this.timeRangeStart == null || this.timeRangeStart.after(timeRangeDate)) {
				this.timeRangeStart = timeRangeDate;
			}
			if (this.timeRangeEnd == null || this.timeRangeEnd.before(timeRangeDate)) {
				this.timeRangeEnd = timeRangeDate;
			}
		}
	}
	
	private void checkValueRange(Object value) {
		if (value instanceof String) {
			checkValueRange((String) value);
		} else if (value instanceof Integer) {
			checkValueRange((Integer) value);
		} else if (value instanceof Long) {
			checkValueRange((Long) value);
	    } else if (value instanceof BigDecimal) {
			checkValueRange((BigDecimal) value);
	    } else if (value instanceof BigInteger) {
			checkValueRange((BigInteger) value);
		} else if (value instanceof Short) {
			checkValueRange((Short) value);
		} else if (value instanceof Byte) {
			checkValueRange((Byte) value);
		} else if (value instanceof Character) {
			checkValueRange((Character) value);
		} else if (value instanceof Double) {
			checkValueRange((Double) value);
		} else if (value instanceof Float) {
			checkValueRange((Float) value);
		}
	}
	
	private void checkValueRange(String newValue) {
		if (newValue != null && newValue.trim().isEmpty() == false) {
			if (valueRangeStart == null) {
				valueRangeStart = newValue.trim();
			} else {
				if (valueRangeStart.compareTo(newValue) > 0) {
					valueRangeStart = newValue;
				}
			}
			if (valueRangeEnd == null) {
				valueRangeEnd = newValue.trim();
			} else {
				if (valueRangeEnd.compareTo(newValue) < 0) {
					valueRangeEnd = newValue;
				}
			}
		}
	}

	private void checkValueRange(Long newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				long cv = Long.valueOf(valueRangeStart);
				if (cv > newValue) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				long cv = Long.valueOf(valueRangeEnd);
				if (cv < newValue) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	private void checkValueRange(Character newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				char cv = valueRangeStart.charAt(0);
				if (cv > newValue) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				char cv = valueRangeEnd.charAt(0);
				if (cv < newValue) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	private void checkValueRange(Double newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				double cv = Double.valueOf(valueRangeStart);
				if (cv > newValue) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				double cv = Double.valueOf(valueRangeEnd);
				if (cv < newValue) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	private void checkValueRange(Float newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				float cv = Float.valueOf(valueRangeStart);
				if (cv > newValue) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				float cv = Float.valueOf(valueRangeEnd);
				if (cv < newValue) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	private void checkValueRange(Integer newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				int cv = Integer.valueOf(valueRangeStart);
				if (cv > newValue) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				int cv = Integer.valueOf(valueRangeEnd);
				if (cv < newValue) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	private void checkValueRange(Short newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				short cv = Short.valueOf(valueRangeStart);
				if (cv > newValue) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				short cv = Short.valueOf(valueRangeEnd);
				if (cv < newValue) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	private void checkValueRange(Byte newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				byte cv = Byte.valueOf(valueRangeStart);
				if (cv > newValue) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				byte cv = Byte.valueOf(valueRangeEnd);
				if (cv < newValue) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	private void checkValueRange(BigDecimal newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				BigDecimal cv = new BigDecimal(valueRangeStart);
				if (cv.compareTo(newValue) > 0) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				BigDecimal cv = new BigDecimal(valueRangeEnd);
				if (cv.compareTo(newValue) < 0) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}	
	
	public void checkValueRange(BigInteger newValue) {
		if (newValue != null) {
			if (valueRangeStart == null || valueRangeStart.isEmpty()) {
				valueRangeStart = String.valueOf(newValue);
			} else {
				BigInteger cv = new BigInteger(valueRangeStart);
				if (cv.compareTo(newValue) > 0) {
					valueRangeStart = String.valueOf(newValue);
				}
			}
			if (valueRangeEnd == null || valueRangeEnd.isEmpty()) {
				valueRangeEnd = String.valueOf(newValue);
			} else {
				BigInteger cv = new BigInteger(valueRangeEnd);
				if (cv.compareTo(newValue) < 0) {
					valueRangeEnd = String.valueOf(newValue);
				}
			}
		}
	}

	public boolean isDoCommit() {
		return doCommit;
	}

	public void setDoCommit(Boolean doCommit) {
		if (doCommit != null) {
			this.doCommit = doCommit;
		}
	}
	
	protected void setupKeywords(Connection conn, SQLCodeGenerator codeGen) throws SQLException {
		if (conn == null) {
			throw new IllegalArgumentException("Connection cannot be null");
		}
		DatabaseMetaData dbmd = conn.getMetaData();
		codeGen.setEnclosureChar(dbmd.getIdentifierQuoteString());
		String numKeyWords = dbmd.getNumericFunctions();
		if (numKeyWords != null && numKeyWords.trim().isEmpty() == false) {
			String[] words =  numKeyWords.split(",");
			for (String w : words) {
				codeGen.addKeyword(w.trim());
			}
		}
		String sqlKeyWords = dbmd.getSQLKeywords();
		if (sqlKeyWords != null && sqlKeyWords.trim().isEmpty() == false) {
			String[] words =  sqlKeyWords.split(",");
			for (String w : words) {
				codeGen.addKeyword(w.trim());
			}
		}
		String stringKeyWords = dbmd.getStringFunctions();
		if (stringKeyWords != null && stringKeyWords.trim().isEmpty() == false) {
			String[] words =  sqlKeyWords.split(",");
			for (String w : words) {
				codeGen.addKeyword(w.trim());
			}
		}
	}

	public SQLCodeGenerator getSourceCodeGenerator() throws SQLException {
		if (sourceCodeGenerator == null) {
			sourceCodeGenerator = new SQLCodeGenerator();
			setupKeywords(sourceConnection, sourceCodeGenerator);
		}
		return sourceCodeGenerator;
	}
	
	public SQLCodeGenerator getTargetCodeGenerator() throws SQLException {
		if (targetCodeGenerator == null) {
			targetCodeGenerator = new SQLCodeGenerator();
			setupKeywords(targetConnection, targetCodeGenerator);
		}
		return targetCodeGenerator;
	}

	public String getNullReplacement() {
		return nullReplacement;
	}

	public void setNullReplacement(String nullReplacement) {
		if (nullReplacement == null) {
			nullReplacement = "";
		}
		this.nullReplacement = nullReplacement;
	}

	public String getReplacementForQuota() {
		return replacementForQuota;
	}

	public void setReplacementForQuota(String replacementForQuota) {
		this.replacementForQuota = replacementForQuota;
	}

	public String getBackupFileCharSet() {
		return backupFileCharSet;
	}

	public void setBackupFileCharSet(String backupFileCharSet) {
		if (backupFileCharSet != null && backupFileCharSet.trim().isEmpty() == false) {
			this.backupFileCharSet = backupFileCharSet;
		}
	}

	public boolean isStrictFieldMatching() {
		return strictFieldMatching;
	}

	public void setStrictFieldMatching(Boolean strictFieldMatching) {
		if (strictFieldMatching != null) {
			this.strictFieldMatching = strictFieldMatching.booleanValue();
		}
	}

	public boolean isTrimFields() {
		return trimFields;
	}

	public void setTrimFields(boolean trimFields) {
		this.trimFields = trimFields;
	}

	public String getCheckConnectionStatement() {
		return checkConnectionStatement;
	}

	public void setCheckConnectionStatement(String checkConnectionStatement) {
		this.checkConnectionStatement = checkConnectionStatement;
	}

}
