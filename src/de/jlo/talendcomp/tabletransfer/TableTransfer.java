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
import java.sql.Connection;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import sqlrunner.datamodel.SQLDataModel;
import sqlrunner.datamodel.SQLField;
import sqlrunner.datamodel.SQLSchema;
import sqlrunner.datamodel.SQLTable;
import sqlrunner.generator.SQLCodeGenerator;
import sqlrunner.text.StringReplacer;
import dbtools.SQLPSParam;
import dbtools.SQLStatement;

public final class TableTransfer {

	private Logger logger = null; //Logger.getLogger(TableTransfer.class);
	private Properties properties = new Properties();
	private Connection sourceConnection;
	private Connection targetConnection;
	private SQLStatement targetInsertStatement;
	private Statement sourceSelectStatement;
	private PreparedStatement targetPSInsert;
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
	private List<String> listResultSetFieldNames;
	private List<String> listResultSetFieldTypeNames;
	private Thread readerThread;
	private Thread writerThread;
	private Thread writerBackupThread;
	private volatile int countInserts = 0;
	private volatile int countFileRows = 0;
	private volatile int countRead = 0;
	private volatile boolean runningDb = false;
	private volatile boolean runningFile = false;
	private long startTime;
	public static final String SOURCE_URL = "source.url";
	public static final String SOURCE_USER = "source.user";
	public static final String SOURCE_PASSWORD = "source.password";
	public static final String SOURCE_DRIVER = "source.driverClass";
	public static final String SOURCE_FETCHSIZE = "source.fetchSize";
	public static final String SOURCE_PROPERTIES = "source.properties";
	public static final String SOURCE_TABLE = "source.table";
	public static final String SOURCE_WHERE = "source.whereClause";
	public static final String SOURCE_QUERY = "source.query";
	public static final String TARGET_URL = "target.url";
	public static final String TARGET_USER = "target.user";
	public static final String TARGET_PASSWORD = "target.password";
	public static final String TARGET_DRIVER = "target.driverClass";
	public static final String TARGET_BATCHSIZE = "target.batchSize";
	public static final String TARGET_TABLE = "target.table";
	public static final String TARGET_PROPERTIES = "target.properties";
	public static final String DIE_ON_ERROR = "abortIfErrors";
	private boolean dieOnError = true;
	private boolean initialized = false;
	private List<String> excludeFieldList = new ArrayList<String>();
	private List<ColumnValue> fixedColumnValueList = new ArrayList<ColumnValue>();
	private boolean outputToTable = true;
	private boolean outputToFile = false;
	private File backupFile = null;
	private File backupFileTmp = null;
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
	private SQLCodeGenerator codeGenerator = new SQLCodeGenerator();
	private boolean exportBooleanAsNumber = true;
	private Map<String, String> dbJavaTypeMap = new HashMap<String, String>();
	private boolean debug = false;
	private boolean keepDataModels = false;
	private String modelKey = null;
	private Map<Integer, String> outputClassMap = new HashMap<Integer, String>();
	
	public void enableLog4J(boolean enable) {
		if (enable) {
			logger = Logger.getLogger(TableTransfer.class);
		} else {
			logger = null;
		}
	}
	
	public void addDbJavaTypeMapping(String dbType, String javaType) {
		if (dbType != null && dbType.trim().isEmpty() == false) {
			if (javaType != null && javaType.trim().isEmpty() == false) {
				dbJavaTypeMap.put(dbType, javaType);
			}
		}
	}
	
	public void addExcludeField(String name) {
		if (name != null && name.trim().isEmpty() == false) {
			excludeFieldList.add(name.trim());
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
		return Math.max(countInserts, countFileRows);
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
		countInserts = 0;
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
			info("Create backup file: " + backupFile.getAbsolutePath());
			backupFileTmp = new File(backupFile.getAbsolutePath() + ".tmp");
			backupOutputWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(backupFileTmp), "UTF-8"));
			info("Backup file established.");
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
		info("Close source connection...");
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
			info("Close target connection...");
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
		if (sourceTable != null) {
			info("Start fetch data from source table " + sourceTable.getAbsoluteName());
		} else {
			info("Start fetch data from source query " + sourceQuery);
		}
		try {
			final ResultSet rs = sourceSelectStatement.executeQuery(sourceQuery);
			info("Analyse result set...");
			final ResultSetMetaData rsMeta = rs.getMetaData();
			final int countColumns = rsMeta.getColumnCount();
			listResultSetFieldNames = new ArrayList<String>(countColumns);
			listResultSetFieldTypeNames = new ArrayList<String>(countColumns);
			for (int i = 1; i <= countColumns; i++) {
				String name = rsMeta.getColumnName(i).toLowerCase();
				String type = rsMeta.getColumnTypeName(i).toUpperCase();
				listResultSetFieldNames.add(name);
				listResultSetFieldTypeNames.add(type);
				debug("Name: " + name + ",  Type: " + type);
			}
			// add fixed column value names
			for (ColumnValue cv : fixedColumnValueList) {
				listResultSetFieldNames.add(cv.getColumnName());
				debug("Name: " + cv.getColumnName());
			}
			info("Start fetching data...");
			startTime = System.currentTimeMillis();
			while (rs.next()) {
				final Object[] row = fillRow(rs, countColumns);
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
			if (sourceTable != null) {
				info("Finished fetch data from source table " + sourceTable.getAbsoluteName() + " count read:" + countRead);
			} else {
				info("Finished fetch data from source query, count read:" + countRead);
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
		} finally {
			try {
				if (outputToTable) {
					info("Stopping write table thread...");
					tableQueue.put(closeFlag);
				}
				if (outputToFile) {
					info("Stopping write file thread...");
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
		info("End read.");
	}
	
	private final Object[] fillRow(ResultSet rs, int countDBColumns) throws SQLException {
		String dbType = null;
		String javaType = null;
		Object[] row = new Object[countDBColumns + fixedColumnValueList.size()];
		int columnIndex = 0;
		while (columnIndex < countDBColumns) {
			dbType = listResultSetFieldTypeNames.get(columnIndex);
			if (dbType != null) {
				javaType = dbJavaTypeMap.get(dbType);
			} else {
				javaType = null;
			}
			try {
				if (javaType == null) {
					row[columnIndex] = rs.getObject(columnIndex + 1);
				} else if ("date".equals(javaType)) {
					row[columnIndex] = rs.getDate(columnIndex + 1);
				} else if ("time".equals(javaType)) {
					row[columnIndex] = rs.getTime(columnIndex + 1);
				} else if ("timestamp".equals(javaType)) {
					row[columnIndex] = rs.getTimestamp(columnIndex + 1);
				} else if ("string".equals(javaType)) {
					row[columnIndex] = rs.getString(columnIndex + 1);
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
	
	private final void writeTable() {
		info("Start writing data into target table " + targetTable.getAbsoluteName());
		final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "1"));
		int currentBatchCount = 0;
		try {
			boolean autocommitTemp = false;
			try {
				autocommitTemp = targetConnection.getAutoCommit();
			} catch (SQLException e2) {
				warn("Failed to detect autocommit state: " + e2.getMessage(), e2);
			}
			final boolean autocommit = autocommitTemp;
			boolean endFlagReceived = false;
			while (endFlagReceived == false) {
				try {
					final List<Object> queueObjects = new ArrayList<Object>(batchSize);
					tableQueue.drainTo(queueObjects, batchSize);
					for (Object item : queueObjects) {
						if (item == closeFlag) {
							info("Write table thread: Stop flag received.");
							endFlagReceived = true;
							break;
						} else {
							prepareInsertStatement((Object[]) item);
							targetPSInsert.addBatch();
							countInserts++;
							currentBatchCount++;
							if (currentBatchCount == batchSize) {
								debug("Write execute insert batch");
								targetPSInsert.executeBatch();
								if (autocommit == false) {
									targetConnection.commit();
								}
								currentBatchCount = 0;
							}
						}
						if (Thread.currentThread().isInterrupted()) {
							break;
						}
					}
				} catch (InterruptedException e) {
					error("Write interrupted in line " + countInserts, e);
					returnCode = RETURN_CODE_ERROR_OUPUT;
					break;
				} catch (SQLException sqle) {
					error("Write failed in line number " + countInserts + " message:" + sqle.getMessage(), sqle);
					if (sqle.getNextException() != null) {
						error("Next exception:" + sqle.getNextException().getMessage(), sqle.getNextException());
					}
					if (dieOnError) {
						returnCode = RETURN_CODE_ERROR_OUPUT;
						try {
							if (autocommit == false) {
								targetConnection.rollback();
							}
						} catch (SQLException e) {
							error("write rollback failed: " + e.getMessage(), e);
						}
						break;
					} else {
						try {
							if (autocommit == false) {
								targetConnection.commit();
							}
						} catch (SQLException e) {
							error("write commit failed: " + e.getMessage(), e);
						}
					}
				} catch (Exception e1) {
					returnCode = RETURN_CODE_ERROR_OUPUT;
					error("write failed:" + e1.getMessage(), e1);
					break;
				}
			}
			if (currentBatchCount > 0 && returnCode == RETURN_CODE_OK) {
				try {
					debug("write execute final insert batch");
					targetPSInsert.executeBatch();
					if (autocommit == false) {
						targetConnection.commit();
					}
					currentBatchCount = 0;
				} catch (SQLException sqle) {
					returnCode = RETURN_CODE_ERROR_OUPUT;
					error("write failed executing last batch message:" + sqle.getMessage(), sqle);
					if (sqle.getNextException() != null) {
						error("write failed embedded error:" + sqle.getNextException().getMessage(), sqle.getNextException());
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
			info("Finished write data into target table " + targetTable.getAbsoluteName() + ", count inserts:" + countInserts);
		}
		info("Write table finished.");
	}
	
	private final Object getRowValue(final String columnName, final Object[] row) {
		final int index = listResultSetFieldNames.indexOf(columnName.toLowerCase());
		if (index != -1) {
			return row[index];
		} else {
			return null;
		}
	}
	
	private final void prepareInsertStatement(final Object[] row) throws Exception {
		for (SQLPSParam p : targetInsertStatement.getParams()) {
			final Object value = getRowValue(p.getName(), row);
			if (value != null) {
				String className = outputClassMap.get(p.getIndex());
				if (className == null) {
					className = value.getClass().getSimpleName();
					outputClassMap.put(p.getIndex(), className);
					debug("Output class mapping: #" + p.getIndex() + " (" + p.getName() + ") use: " + className);
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
		if (targetConnection == null || targetConnection.isClosed()) {
			throw new Exception("executeSQLOnTarget failed because target connection is null or closed");
		}
		try {
			final Statement stat = targetConnection.createStatement();
			stat.execute(sqlStatement);
			stat.close();
		} catch (SQLException sqle) {
			error("executeSQLOnTarget sql=" + sqlStatement + " failed: " + sqle.getMessage(), sqle);
			throw sqle;
		}
	}
	
	public void commitSource() throws SQLException {
		sourceConnection.commit();
	}
	
	public void commitTarget() throws SQLException {
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
		final int queueSize = Math.max(batchSize, fetchSize) + 100;
		if (outputToTable) {
			tableQueue = new LinkedBlockingQueue<Object>(queueSize);
		}
		if (outputToFile) {
			fileQueue = new LinkedBlockingQueue<Object>(queueSize);
		}
		dieOnError = Boolean.parseBoolean(properties.getProperty(DIE_ON_ERROR, "true"));
		patternForBackslash = Pattern.compile("\\", Pattern.LITERAL);
		patternForQuota = Pattern.compile("\"", Pattern.LITERAL);
		replacementForBackslash = Matcher.quoteReplacement("\\\\");
		replacementForQuota = Matcher.quoteReplacement("\\\"");
		initialized = true;
	}
	
	private final SQLTable getSourceSQLTable() throws Exception {
		final String tableAndSchemaName = properties.getProperty(SOURCE_TABLE);
		if (sourceTable == null || sourceTable.getAbsoluteName().equalsIgnoreCase(tableAndSchemaName) == false) {
			String schemaName = getSchemaName(tableAndSchemaName);
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
			for (String exclFieldName : excludeFieldList) {
				SQLField field = sourceTable.getField(exclFieldName);
				if (field != null) {
					sourceTable.removeSQLField(field);
				}
			}
		}
		return sourceTable;
	}
	
	private String getSourceDatabase() throws SQLException {
		String cat = sourceConnection.getCatalog();
		if (cat == null || cat.trim().isEmpty()) {
			cat = sourceModel.getLoginSchemaName();
		}
		return cat;
	}
	
	private String getTargetDatabase() throws SQLException {
		String cat = targetConnection.getCatalog();
		if (cat == null || cat.trim().isEmpty()) {
			cat = targetModel.getLoginSchemaName();
		}
		return cat;
	}

	private final SQLTable getTargetSQLTable() throws Exception {
		final String tableAndSchemaName = properties.getProperty(TARGET_TABLE);
		if (targetTable == null || targetTable.getAbsoluteName().equalsIgnoreCase(tableAndSchemaName) == false) {
			String schemaName = getSchemaName(tableAndSchemaName);
			if (schemaName == null) {
				schemaName = getTargetDatabase();
			}
			SQLSchema schema = targetModel.getSchema(schemaName);
			if (schema == null) {
				throw new Exception("getTargetSQLTable failed: schema " + schemaName + " not available");
			}
			String tableName = getTableName(tableAndSchemaName);
			targetTable = schema.getTable(tableName);
			if (targetTable == null) {
				throw new Exception("getTargetSQLTable failed: table " + schemaName + "." + tableName + " not available");
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
	
	private boolean isMysql() {
		boolean mysqlDriverPresent = false;
		try {
			Class.forName("com.mysql.jdbc.Statement");
			mysqlDriverPresent = true;
		} catch (ClassNotFoundException e) {
			debug("No MySQL class loaded.");
		}
		return mysqlDriverPresent;
	}
	
	private Statement createSourceSelectStatement() throws Exception {
		sourceQuery = properties.getProperty(SOURCE_QUERY);
		if (sourceQuery == null) {
			sourceQuery = codeGenerator.buildSelectStatement(getSourceSQLTable(), true) + buildSourceWhereSQL();
			properties.put(SOURCE_QUERY, sourceQuery);
		}
		debug("createSourceSelectStatement SQL:" + sourceQuery);
		sourceSelectStatement = sourceConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		int fetchSize = getFetchSize();
		if (fetchSize > 0) {
			sourceSelectStatement.setFetchSize(fetchSize);
		}
		if (isMysql()) {
			DBHelper util = (DBHelper) Class.forName("de.jlo.talendcomp.tabletransfer.MySQLHelper").newInstance();
			util.setupStatement(sourceSelectStatement);
		}
		return sourceSelectStatement;
	}
	
	private int getFetchSize() {
		int fetchSize = 0;
		try {
			fetchSize = Integer.parseInt(properties.getProperty(SOURCE_FETCHSIZE, "0"));
		} catch (Exception e) {
			warn("getFetchSize failed: " + e.getMessage(), e);
		}
		return fetchSize;
	}
	
	private PreparedStatement createTargetInsertStatement() throws Exception {
		targetInsertStatement = codeGenerator.buildPSInsertSQLStatement(getTargetSQLTable(), true);
		debug("createTargetInsertStatement SQL:" + targetInsertStatement.getSQL());
		targetPSInsert = targetConnection.prepareStatement(targetInsertStatement.getSQL());
		return targetPSInsert;
	}
	
	private final String buildSourceWhereSQL() {
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
	
	private final String replacePlaceholders(String stringWithPlaceholders) {
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
	
	private final String getSchemaName(String schemaAndTable) {
		final int pos = schemaAndTable.indexOf('.');
		if (pos > 0) {
			return schemaAndTable.substring(0, pos);
		} else {
			return null;
		}
	}
	
	private final String getTableName(String schemaAndTable) {
		int pos = schemaAndTable.indexOf('.');
		if (pos > 0) {
			return schemaAndTable.substring(pos + 1, schemaAndTable.length());
		} else {
			return schemaAndTable;
		}
	}
	
	public final void setupDataModels() throws SQLException {
		if (keepDataModels) {
			sourceModel = sqlModelCache.get("source_" + modelKey);
			if (sourceModel == null) {
				sourceModel = new SQLDataModel(sourceConnection);
				sourceModel.loadSchemas();
				sqlModelCache.put("source_" + modelKey, sourceModel);
			} else {
				sourceModel.setConnection(sourceConnection);
			}
		} else {
			sourceModel = new SQLDataModel(sourceConnection);
			sourceModel.loadSchemas();
		}
		if (sourceConnection != null && sourceConnection.getAutoCommit() == false) {
			sourceConnection.commit();
		}
		if (outputToTable) {
			if (keepDataModels) {
				targetModel = sqlModelCache.get("target_" + modelKey);
				if (targetModel == null) {
					targetModel = new SQLDataModel(targetConnection);
					targetModel.loadSchemas();
					sqlModelCache.put("target_" + modelKey, targetModel);
				} else {
					targetModel.setConnection(targetConnection);
				}
			} else {
				targetModel = new SQLDataModel(targetConnection);
				targetModel.loadSchemas();
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
    
    public void setSourceURL(String url) {
    	properties.setProperty(SOURCE_URL, url);
    }
    
    public String getSourceURL() {
    	return properties.getProperty(SOURCE_URL);
    }
    
    public void setSourceUser(String sourceUser) {
    	properties.setProperty(SOURCE_USER, sourceUser);
    }
    
    public String getSourceUser() {
    	return properties.getProperty(SOURCE_USER);
    }
    
    public String getSourcePassword() {
    	return properties.getProperty(SOURCE_PASSWORD);
    }
    
    public void setSourcePassword(String passwd) {
    	properties.setProperty(SOURCE_PASSWORD, passwd);
    }
    
    public String getSourceDriverClass() {
    	return properties.getProperty(SOURCE_DRIVER);
    }
    
    public void setSourceDriverClass(String driverClassName) {
    	properties.setProperty(SOURCE_DRIVER, driverClassName);
    }
    
    public String getSourceFetchSize() {
    	return properties.getProperty(SOURCE_FETCHSIZE);
    }
    
    public void setSourceFetchSize(String fetchSize) {
    	properties.setProperty(SOURCE_FETCHSIZE, fetchSize);
    }
    
    public String getTargetInsertStatement() {
    	return targetInsertStatement.getSQL();
    }
    
	public String getSourceQuery() {
		return properties.getProperty(SOURCE_QUERY);
	}

	public void setSourceQuery(String sourceQuery) {
		properties.setProperty(SOURCE_QUERY, sourceQuery);
	}

    public String getSourceTable() {
    	return codeGenerator.getEncapsulatedName(properties.getProperty(SOURCE_TABLE));
    }
    
    public void setSourceTable(String tableAndSchema) {
    	properties.setProperty(SOURCE_TABLE, tableAndSchema);
    	properties.remove(SOURCE_QUERY); // remove query from previous run
    }
    
    public String getSourceWhereClause() {
    	return properties.getProperty(SOURCE_WHERE);
    }
    
    public void setSourceWhereClause(String whereClause) {
    	properties.setProperty(SOURCE_WHERE, whereClause);
    }
    
    public void setSourceProperties(String propertiesString) {
    	properties.setProperty(SOURCE_PROPERTIES, propertiesString);
    }
    
    public String getSourceProperties() {
    	return properties.getProperty(SOURCE_PROPERTIES);
    }

    public void setTargetURL(String url) {
    	properties.setProperty(TARGET_URL, url);
    }
    
    public String getTargetURL() {
    	return properties.getProperty(TARGET_URL);
    }
    
    public void setTargetUser(String targetUser) {
    	properties.setProperty(TARGET_USER, targetUser);
    }
    
    public String getTargetUser() {
    	return properties.getProperty(TARGET_USER);
    }
    
    public String getTargetPassword() {
    	return properties.getProperty(TARGET_PASSWORD);
    }
    
    public void setTargetPassword(String passwd) {
    	properties.setProperty(TARGET_PASSWORD, passwd);
    }
    
    public String getTargetDriverClass() {
    	return properties.getProperty(TARGET_DRIVER);
    }
    
    public void setTargetDriverClass(String driverClassName) {
    	properties.setProperty(TARGET_DRIVER, driverClassName);
    }
    
    public String getTargetBatchSize() {
    	return properties.getProperty(TARGET_BATCHSIZE);
    }
    
    public void setTargetBatchSize(String batchSize) {
    	properties.setProperty(TARGET_BATCHSIZE, batchSize);
    }
    
    public String getTargetTable() {
    	return codeGenerator.getEncapsulatedName(properties.getProperty(TARGET_TABLE));
    }
    
    public void setTargetTable(String tableAndSchema) {
    	properties.setProperty(TARGET_TABLE, tableAndSchema);
    }

    public void setTargetProperties(String propertiesString) {
    	properties.setProperty(TARGET_PROPERTIES, propertiesString);
    }
    
    public String getTargetProperties() {
    	return properties.getProperty(TARGET_PROPERTIES);
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
    
	private final void warn(String message, Exception t) {
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
	
	private final void info(String message) {
		if (logger != null) {
			logger.info(message);
		} else {
			System.out.println(message);
		}
	}

	private final void debug(String message) {
		if (logger != null && logger.isDebugEnabled()) {
			logger.debug(message);
		} else if (debug) {
			System.out.println("DEBUG: " + message);
		}
	}

	private final void error(String message, Exception t) {
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
		if (t != null) {
			errorException = t;
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
	
	private void writeRowToBackup(Object[] row) throws Exception {
		if (row != null) {
			boolean firstLoop = true;
			for (Object value : row) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					backupOutputWriter.write(fieldSeparator);
				}
				backupOutputWriter.write(fieldEclosure);
				backupOutputWriter.write(convertToString(value));
				backupOutputWriter.write(fieldEclosure);
			}
			backupOutputWriter.write("\n");
			countFileRows++;
		}
	}
	
	private void writeFile() {
		try {
			countFileRows = 0;
			info("Start writing data in file: " + backupFile.getAbsolutePath());
			final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "1"));
			boolean endFlagReceived = false;
			while (endFlagReceived == false) {
				try {
					final List<Object> queueObjects = new ArrayList<Object>(batchSize);
					fileQueue.drainTo(queueObjects, batchSize);
					for (Object item : queueObjects) {
						if (item == closeFlag) {
							info("Write file thread: Stop flag received.");
							endFlagReceived = true;
							break;
						} else {
							writeRowToBackup((Object[]) item);
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
				info("Finished write data into file " + backupFile.getAbsolutePath() + ", count rows:" + countFileRows);
				info("Rename tmp file: " + backupFileTmp.getAbsolutePath() + " to target file: " + backupFile.getAbsolutePath());
				backupFileTmp.renameTo(backupFile);
			} else if (returnCode == RETURN_CODE_ERROR_INPUT) {
				info("Finished write data into file " + backupFile.getAbsolutePath() + ", count rows:" + countFileRows);
				warn("Read has been failed. Rename file as error file.", null);
				File errorFile = new File(backupFile.getAbsolutePath() + ".error");
				backupFileTmp.renameTo(errorFile);
			} else if (returnCode == RETURN_CODE_ERROR_OUPUT) {
				info("Finished write data into file " + backupFile.getAbsolutePath() + ", count rows:" + countFileRows);
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
		info("Write file finished");
	}

	public boolean isOutputToTable() {
		return outputToTable;
	}

	public void setOutputToTable(boolean outputToTable) {
		this.outputToTable = outputToTable;
	}
	
	public void setDebug(boolean debug) {
		if (logger != null) {
			Logger.getRootLogger().setLevel(Level.DEBUG);
		}
		this.debug = debug;
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
	
}
