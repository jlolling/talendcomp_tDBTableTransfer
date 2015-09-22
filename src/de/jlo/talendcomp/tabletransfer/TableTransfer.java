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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import sqlrunner.datamodel.SQLDataModel;
import sqlrunner.datamodel.SQLField;
import sqlrunner.datamodel.SQLSchema;
import sqlrunner.datamodel.SQLTable;
import sqlrunner.generator.SQLCodeGenerator;
import sqlrunner.text.StringReplacer;
import dbtools.ConnectionDescription;
import dbtools.DatabaseSessionPool;
import dbtools.SQLPSParam;
import dbtools.SQLStatement;

public final class TableTransfer {

	private Logger logger = Logger.getLogger(TableTransfer.class);
	private Properties properties = new Properties();
	private ConnectionDescription sourceConnDesc;
	private ConnectionDescription targetConnDesc;
	private Connection sourceConnection;
	private Connection targetConnection;
	private SQLStatement targetInsertStatement;
	private PreparedStatement sourcePSSelect;
	private PreparedStatement targetPSInsert;
	private SQLDataModel sourceModel;
	private SQLDataModel targetModel;
	private SQLTable sourceTable;
	private String sourceQuery;
	private SQLTable targetTable;
	private static final int RETURN_CODE_OK = 0;
	private static final int RETURN_CODE_ERROR = 1;
	private static final int RETURN_CODE_WARN = 5;
	private int returnCode = RETURN_CODE_OK;
	private String errorMessage;
	private Exception errorException;
	private BlockingQueue<Object> queue;
	private BlockingQueue<Object> backupQueue;
	private final Object closeFlag = new String("The End");
	private List<String> listResultSetFieldNames;
	private Thread readerThread;
	private Thread writerThread;
	private Thread writerBackupThread;
	private volatile int countInserts = 0;
	private volatile int countRead = 0;
	private volatile boolean running = false;
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
	private boolean backupData = false;
	private File backupFile = null;
	private String fieldSeparator = ";";
	private String fieldEclosure = "\"";
	private String nullReplacement = "\\N";
	private BufferedWriter backupOutputWriter = null;
	private SimpleDateFormat sdfOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void addExcludeField(String name) {
		if (name != null && name.trim().isEmpty() == false) {
			excludeFieldList.add(name.trim());
		}
	}
	
	public final int getCurrentCountInserts() {
		return countInserts;
	}
	
	public final int getCurrentCountReads() {
		return countRead;
	}
	
	public final long getStartTime() {
		return startTime;
	}
	
	public final boolean isRunning() {
		return running;
	}
	
	public static final Connection createConnection(ConnectionDescription cd) throws Exception {
		// register driver
		Class.forName(cd.getDriverClassName());
		// create connection
		Connection conn = DriverManager.getConnection(cd.getUrl(), getConnectionProperties(cd));
		return conn;
	}
	
	private static final Properties getConnectionProperties(ConnectionDescription cd) {
        Properties properties = new Properties();
        if (cd.getPropertiesString() != null) {
            StringTokenizer st = new StringTokenizer(cd.getPropertiesString(), ";");
            String token = null;
            String key = null;
            String value = null;
            int pos = 0;
            while (st.hasMoreTokens()) {
                token = st.nextToken();
                pos = token.indexOf('=');
                if (pos != -1) {
                    key = token.substring(0, pos).trim();
                    value = token.substring(pos + 1).trim();
                    if (key.length() > 0 && value.length() > 0) {
                        properties.put(key, value);
                    }
                }
            }
        }
        properties.put("user", cd.getUser());
        properties.put("password", cd.getPasswd());
        return properties;
	}
	
	/**
	 * executes the transfer with separate read and write threads
	 * @throws Exception
	 */
	public final void execute() throws Exception {
		if (initialized == false) {
			throw new Exception("Not initialized!");
		}
		running = true;
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
		writerThread = new Thread() {
			@Override
			public void run() {
				writeDB();
			}
		};
		writerThread.setDaemon(false);
		writerThread.start();
		if (backupData) {
			System.out.println("Create backup file: " + backupFile.getAbsolutePath());
			backupOutputWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(backupFile), "UTF-8"));
			System.out.println("Backup file established.");
			writerBackupThread = new Thread() {
				@Override
				public void run() {
					writeBackup();
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
	}
	
	/**
	 * disconnects from the database 
	 */
	public final void disconnect() {
		DatabaseSessionPool.close(sourceConnDesc.getUniqueId());
		DatabaseSessionPool.close(targetConnDesc.getUniqueId());
		if (sourceConnection != null) {
			try {
				sourceConnection.close();
			} catch (SQLException e) {
				logger.error("disconnect from source failed: " + e.getMessage(), e);
			}
		}
		if (targetConnection != null) {
			try {
				targetConnection.close();
			} catch (SQLException e) {
				logger.error("disconnect from target failed: " + e.getMessage(), e);
			}
		}
	}
	
	private final void read() {
		if (sourceTable != null) {
			logger.info("Start fetch data from source table " + sourceTable.getAbsoluteName());
		} else {
			logger.info("Start fetch data from source query " + sourceQuery);
		}
		try {
			final ResultSet rs = sourcePSSelect.executeQuery();
			logger.info("Analyse result set...");
			final ResultSetMetaData rsMeta = rs.getMetaData();
			final int countColumns = rsMeta.getColumnCount();
			listResultSetFieldNames = new ArrayList<String>(countColumns);
			for (int i = 1; i <= countColumns; i++) {
				listResultSetFieldNames.add(rsMeta.getColumnName(i).toLowerCase());
			}
			logger.info("Start fetching data...");
			startTime = System.currentTimeMillis();
			while (rs.next()) {
				final Object[] row = new Object[countColumns];
				fillRow(row, rs);
				queue.put(row);
				countRead++;
				if (backupData) {
					if (writerBackupThread.isAlive() == false) {
						warn("Backup process died. Switch off backup");
						backupData = false;
					} else {
						backupQueue.put(row);
					}
				}
				if (Thread.currentThread().isInterrupted()) {
					break;
				}
			}
			rs.close();
			logger.info("Finished fetch data from source table " + sourceTable.getAbsoluteName() + " count read:" + countRead);
		} catch (SQLException e) {
			String message = e.getMessage();
			SQLException en = e.getNextException();
			if (en != null) {
				message = "\nNext Exception:" + en.getMessage();
			}
			error("read failed in line number " + countRead + " message:" + message, e);
			returnCode = RETURN_CODE_ERROR;
		} catch (InterruptedException ie) {
			error("read interrupted (send data sets)", ie);
			returnCode = RETURN_CODE_ERROR;
		} finally {
			try {
				queue.put(closeFlag);
				if (backupData) {
					if (writerBackupThread.isAlive() == false) {
						warn("Backup process died. Switch off backup");
						backupData = false;
					} else {
						backupQueue.put(closeFlag);
					}
				}
			} catch (InterruptedException e) {
				error("read interrupted (send close flag)", e);
				returnCode = RETURN_CODE_ERROR;
			}
			try {
				if (sourceConnection.getAutoCommit() == false) {
					sourceConnection.commit();
				}
				sourcePSSelect.close();
			} catch (SQLException e) {}
		}
	}
	
	private final void fillRow(Object[] row, ResultSet rs) throws SQLException {
		for (int i = 0; i < row.length; i++) {
			row[i] = rs.getObject(i + 1);
		}
	}
	
	private final void writeDB() {
		logger.info("Start writing data into target table " + targetTable.getAbsoluteName());
		final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "1"));
		int currentBatchCount = 0;
		try {
			boolean autocommitTemp = false;
			try {
				autocommitTemp = targetConnection.getAutoCommit();
			} catch (SQLException e2) {
				logger.warn("Failed to detect autocommit state: " + e2.getMessage(), e2);
			}
			final boolean autocommit = autocommitTemp;
			boolean endFlagReceived = false;
			while (endFlagReceived == false) {
				try {
					final List<Object> queueObjects = new ArrayList<Object>(batchSize);
					queue.drainTo(queueObjects, batchSize);
					for (Object item : queueObjects) {
						if (item == closeFlag) {
							logger.info("write finished");
							endFlagReceived = true;
							break;
						} else {
							prepareInsertStatement((Object[]) item);
							targetPSInsert.addBatch();
							countInserts++;
							currentBatchCount++;
							if (currentBatchCount == batchSize) {
								if (logger.isDebugEnabled()) {
									logger.debug("write execute insert batch");
								}
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
					error("write interrupted in line " + countInserts, e);
					returnCode = RETURN_CODE_ERROR;
					break;
				} catch (SQLException sqle) {
					error("write failed in line number " + countInserts + " message:" + sqle.getMessage(), sqle);
					if (sqle.getNextException() != null) {
						error("Embedded error:" + sqle.getNextException().getMessage(), sqle.getNextException());
					}
					if (dieOnError) {
						returnCode = RETURN_CODE_ERROR;
						try {
							if (autocommit == false) {
								targetConnection.rollback();
							}
						} catch (SQLException e) {
							logger.error("write rollback failed: " + e.getMessage(), e);
						}
						break;
					} else {
						try {
							if (autocommit == false) {
								targetConnection.commit();
							}
						} catch (SQLException e) {
							logger.error("write commit failed: " + e.getMessage(), e);
						}
					}
				} catch (Exception e1) {
					returnCode = RETURN_CODE_ERROR;
					error("write failed:" + e1.getMessage(), e1);
					break;
				}
			}
			if (currentBatchCount > 0 && returnCode == 0) {
				try {
					if (logger.isDebugEnabled()) {
						logger.debug("write execute final insert batch");
					}
					targetPSInsert.executeBatch();
					if (autocommit == false) {
						targetConnection.commit();
					}
					currentBatchCount = 0;
				} catch (SQLException sqle) {
					returnCode = RETURN_CODE_ERROR;
					error("write failed executing last batch message:" + sqle.getMessage(), sqle);
					if (sqle.getNextException() != null) {
						error("write failed embedded error:" + sqle.getNextException().getMessage(), sqle.getNextException());
					}
					try {
						targetConnection.rollback();
					} catch (SQLException e) {
						logger.error("write rollback failed:" + e.getMessage(), e);
					}
				}
			}
		} finally {
			try {
				targetPSInsert.close();
			} catch (SQLException e) {}
			running = false;
			logger.info("Finished write data into target table " + targetTable.getAbsoluteName() + " count inserts:" + countInserts);
		}
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
				targetPSInsert.setObject(p.getIndex(), value);
			} else {
				targetPSInsert.setNull(p.getIndex(), targetTable.getField(p.getName()).getType());
			}
		}
	}
	
	/**
	 * connects to the database
	 * @throws Exception
	 */
	public final void connect() throws Exception {
		connectSource();
		connectTarget();
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
			logger.error("executeSQLOnTarget sql=" + sqlStatement + " failed: " + sqle.getMessage(), sqle);
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
	public final void setupExecute() throws Exception {
		createSourceSelectStatement();
		createTargetInsertStatement();
		final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "100"));
		final int fetchSize = Integer.parseInt(properties.getProperty(SOURCE_FETCHSIZE, "100"));
		final int queueSize = Math.max(batchSize, fetchSize) + 100;
		queue = new LinkedBlockingQueue<Object>(queueSize);
		if (backupData) {
			backupQueue = new LinkedBlockingQueue<Object>(queueSize);
		}
		dieOnError = Boolean.parseBoolean(properties.getProperty(DIE_ON_ERROR, "true"));
		initialized = true;
	}
	
	private final SQLTable getSourceSQLTable() throws Exception {
		final String tableAndSchemaName = properties.getProperty(SOURCE_TABLE);
		if (sourceTable == null || sourceTable.getAbsoluteName().equalsIgnoreCase(tableAndSchemaName) == false) {
			String schemaName = getSchemaName(tableAndSchemaName);
			if (schemaName == null) {
				schemaName = sourceModel.getLoginSchemaName();
			}
			SQLSchema schema = sourceModel.getSchema(schemaName);
			if (schema == null) {
				throw new Exception("getSourceTable failed: schema " + schemaName + " not available");
			}
			String tableName = getTableName(tableAndSchemaName);
			sourceTable = schema.getTable(tableName);
			if (sourceTable == null) {
				throw new Exception("getSourceTable failed: table " + tableAndSchemaName + " not available");
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
	
	private final SQLTable getTargetSQLTable() throws Exception {
		final String tableAndSchemaName = properties.getProperty(TARGET_TABLE);
		if (targetTable == null || targetTable.getAbsoluteName().equalsIgnoreCase(tableAndSchemaName) == false) {
			String schemaName = getSchemaName(tableAndSchemaName);
			if (schemaName == null) {
				schemaName = targetModel.getLoginSchemaName();
			}
			SQLSchema schema = targetModel.getSchema(schemaName);
			if (schema == null) {
				throw new Exception("getTargetTable failed: schema " + schemaName + " not available");
			}
			String tableName = getTableName(tableAndSchemaName);
			targetTable = schema.getTable(tableName);
			if (targetTable == null) {
				throw new Exception("getTargetTable failed: table " + tableAndSchemaName + " not available");
			}
			for (String exclFieldName : excludeFieldList) {
				SQLField field = targetTable.getField(exclFieldName);
				if (field != null) {
					targetTable.removeSQLField(field);
				}
			}
		}
		return targetTable;
	}
	
	private PreparedStatement createSourceSelectStatement() throws Exception {
		sourceQuery = properties.getProperty(SOURCE_QUERY);
		if (sourceQuery == null) {
			sourceQuery = SQLCodeGenerator.buildSelectStatement(getSourceSQLTable(), true) + buildSourceWhereSQL();
			properties.put(SOURCE_QUERY, sourceQuery);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("createSourceSelectStatement SQL:" + sourceQuery);
		}
		sourcePSSelect = sourceConnection.prepareStatement(sourceQuery, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		int fetchSize = getFetchSize();
		if (fetchSize > 0) {
			sourcePSSelect.setFetchSize(fetchSize);
		}
		return sourcePSSelect;
	}
	
	private int getFetchSize() {
		int fetchSize = 0;
		try {
			fetchSize = Integer.parseInt(properties.getProperty(SOURCE_FETCHSIZE, "0"));
		} catch (Exception e) {
			logger.warn("getFetchSize failed: " + e.getMessage(), e);
		}
		return fetchSize;
	}
	
	private PreparedStatement createTargetInsertStatement() throws Exception {
		targetInsertStatement = SQLCodeGenerator.buildPSInsertSQLStatement(getTargetSQLTable(), true);
		if (logger.isDebugEnabled()) {
			logger.debug("createTargetInsertStatement SQL:" + targetInsertStatement.getSQL());
		}
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
				logger.warn("replacePlaceholders for string " + stringWithPlaceholders + " failed in key:" + key + " reason: missing value");
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
			return null;
		}
	}
	
	private final void connectSource() throws Exception {
		String user = properties.getProperty(SOURCE_USER);
		String url = properties.getProperty(SOURCE_URL);
		String password = properties.getProperty(SOURCE_PASSWORD);
		String driverClass = properties.getProperty(SOURCE_DRIVER);
		if (user == null || url == null || password == null || driverClass == null) {
			throw new Exception("Properties for source connection missing!");
		}
		sourceConnDesc = new ConnectionDescription();
		sourceConnDesc.setAutoCommit(true);
		sourceConnDesc.setUser(user);
		sourceConnDesc.setUrl(url);
		sourceConnDesc.setPasswd(password);
		sourceConnDesc.setDriverClassName(driverClass);
		sourceConnDesc.setDefaultFetchSize(getFetchSize());
		sourceConnDesc.setPropertiesString(properties.getProperty(SOURCE_PROPERTIES));
		if (DatabaseSessionPool.getConnectionDescription(sourceConnDesc.getUniqueId()) == null) {
			DatabaseSessionPool.addConnectionDescription(sourceConnDesc.getUniqueId(), sourceConnDesc);
		}
		sourceConnection = createConnection(sourceConnDesc);
		sourceConnection.setReadOnly(true);
	}
	
	private final void connectTarget() throws Exception {
		String user = properties.getProperty(TARGET_USER);
		String url = properties.getProperty(TARGET_URL);
		String password = properties.getProperty(TARGET_PASSWORD);
		String driverClass = properties.getProperty(TARGET_DRIVER);
		if (user == null || url == null || password == null || driverClass == null) {
			throw new Exception("Properties for target connection missing!");
		}
		targetConnDesc = new ConnectionDescription();
		targetConnDesc.setAutoCommit(false);
		targetConnDesc.setUser(user);
		targetConnDesc.setUrl(url);
		targetConnDesc.setPasswd(password);
		targetConnDesc.setDriverClassName(driverClass);
		targetConnDesc.setPropertiesString(properties.getProperty(TARGET_PROPERTIES));
		if (DatabaseSessionPool.getConnectionDescription(targetConnDesc.getUniqueId()) == null) {
			DatabaseSessionPool.addConnectionDescription(targetConnDesc.getUniqueId(), targetConnDesc);
		}
		targetConnection = createConnection(targetConnDesc);
	}
	
	public final void setupDataModels() throws SQLException {
		if (sourceConnDesc != null) {
			sourceModel = new SQLDataModel(sourceConnDesc);
		} else {
			sourceModel = new SQLDataModel(sourceConnection);
		}
		sourceModel.loadSchemas();
		sourceConnection.commit();
		if (targetConnDesc != null) {
			targetModel = new SQLDataModel(targetConnDesc);
		} else {
			targetModel = new SQLDataModel(targetConnection);
		}
		targetModel.loadSchemas();
		targetConnection.commit();
	}
	
    public void loadProperties(String filePath) {
        try {
            final FileInputStream fis = new FileInputStream(filePath);
            properties.load(fis);
            fis.close();
        } catch (IOException e) {
        	error("LoadProperties from " + filePath + " failed: " + e.getMessage(), e);
			returnCode = RETURN_CODE_ERROR;
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
    	return properties.getProperty(SOURCE_TABLE);
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
    	return properties.getProperty(TARGET_TABLE);
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
    
	public boolean isDieOnError() {
		return Boolean.parseBoolean(properties.getProperty(DIE_ON_ERROR, "true"));
	}

	public void setDieOnError(String dieOnError) {
		properties.setProperty(DIE_ON_ERROR, dieOnError);
	}

	public void setDieOnError(boolean dieOnError) {
		properties.setProperty(DIE_ON_ERROR, String.valueOf(dieOnError));
	}
	
	private final void warn(String message) {
		logger.warn(message);
	}

	private final void error(String message, Exception t) {
		logger.error(message, t);
		errorMessage = message;
		errorException = t;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public Exception getErrorException() {
		return errorException;
	}
	
	public int getCountDatabaseSessions() {
		return DatabaseSessionPool.getPoolSize();
	}

	public Connection getSourceConnection() {
		return sourceConnection;
	}
	
	public PreparedStatement getSourceStatement() {
		return sourcePSSelect;
	}

	/**
	 * set an connected connection
	 * In this case it is not useful to call connect() or disconnect()
	 * @param sourceConnection
	 */
	public void setSourceConnection(Connection sourceConnection) {
		if (sourceConnection == null) {
			throw new IllegalArgumentException("sourceConnection cannot be null!");
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
			throw new IllegalArgumentException("targetConnection cannot be null!");
		}
		if (targetConnection.isReadOnly()) {
			throw new Exception("Target connection cannot be in read only mode!");
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

	public void setBackupFilePath(String backupFilePath) throws Exception {
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
				backupData = true;
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
				backupData = true;
			}
		}
	}
	
	private String convertToString(Object value) {
		if (value instanceof String) {
			if (((String) value).isEmpty()) {
				return nullReplacement;
			} else {
				return (String) value;
			}
		} else if (value instanceof Date) {
			return sdfOut.format((Date) value);
		} else if (value instanceof Boolean) {
			return Boolean.toString((Boolean) value);
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
		}
	}
	
	private void writeBackup() {
		logger.info("Start writing backup data to file: " + backupFile.getAbsolutePath());
		final int batchSize = Integer.parseInt(properties.getProperty(TARGET_BATCHSIZE, "1"));
		boolean endFlagReceived = false;
		while (endFlagReceived == false) {
			try {
				final List<Object> queueObjects = new ArrayList<Object>(batchSize);
				backupQueue.drainTo(queueObjects, batchSize);
				for (Object item : queueObjects) {
					if (item == closeFlag) {
						break;
					} else {
						writeRowToBackup((Object[]) item);
					}
					if (Thread.currentThread().isInterrupted()) {
						break;
					}
				}
				backupOutputWriter.flush();
			} catch (Exception e) {
				error("write backup failed in line number " + countInserts + " message:" + e.getMessage(), e);
				if (dieOnError) {
					returnCode = RETURN_CODE_ERROR;
					break;
				}
			}
		}
		logger.info("write backup finished");
		try {
			backupOutputWriter.flush();
			backupOutputWriter.close();
		} catch (Exception e) {
			error("Close backup file failed: " + e.getMessage(), e);
		}
	}

}
