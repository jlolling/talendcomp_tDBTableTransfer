/**
 * Copyright 2022 Jan Lolling jan.lolling@gmail.com
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
package de.jlo.datamodel;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.jlo.datamodel.ext.DatabaseExtension;
import de.jlo.datamodel.ext.DatabaseExtensionFactory;

public final class SQLDataModel extends SQLObject implements Comparable<SQLDataModel> {

	private static final Logger logger = LogManager.getLogger(SQLDataModel.class);
	private String errorMessage;
	private final List<SQLCatalog> catalogs = new ArrayList<SQLCatalog>();
	private boolean loadingSchemas = false;
	private boolean loadingCatalogs = false;
	private boolean useLowerCaseIdentifiers = false;
	private boolean useUpperCaseIdentifiers = false;
	private boolean userCaseSensitiveIdentifiers = false;
	public final char delimiter = 0x00;
	private boolean schemasLoaded = false;
	private boolean catalogsLoaded = false;
	private SQLSchema currentSQLSchema = null;
	private DatabaseExtension databaseExtension;
	private Connection connection;
	private boolean commitAfterReading = true;
	
	public boolean isUseLowerCaeIdentifiers() {
		return useLowerCaseIdentifiers;
	}

	public boolean isUseUpperCaseIdentifiers() {
		return useUpperCaseIdentifiers;
	}

	public boolean isUserCaseSensitiveIdentifiers() {
		return userCaseSensitiveIdentifiers;
	}

	public SQLDataModel(Connection connection) throws Exception {
		super(null, "given-connection");
		if (connection == null) {
			throw new IllegalArgumentException("connection cannot be null");
		}
		if (connection.isClosed()) {
			throw new IllegalStateException("connection is closed already");
		}
		this.connection = connection;
		String driverClass = null;
		driverClass = connection.getMetaData().getDriverName();
		databaseExtension = DatabaseExtensionFactory.getDatabaseExtension(driverClass);
	}
	
	public void refresh() {
		loadCatalogs();
	}
	
	public boolean isSchemaLoaded() {
		return schemasLoaded;
	}
	
	public boolean isCatalogsLoaded() {
		return catalogsLoaded;
	}

	public boolean loadCatalogs() {
		if (loadingCatalogs) {
			return false;
		}
		loadingCatalogs = true;
		if (Thread.currentThread().isInterrupted()) {
			loadingSchemas = false;
			return false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("loadCatalogs");
		}
		Connection conn = connection;
		if (conn == null) {
			return false;
		}
		boolean ok = false;
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			if (dbmd != null) {
				useLowerCaseIdentifiers = dbmd.storesLowerCaseIdentifiers();
				useUpperCaseIdentifiers = dbmd.storesUpperCaseIdentifiers();
				userCaseSensitiveIdentifiers = dbmd.storesMixedCaseIdentifiers();
				catalogsLoaded = false;
				ResultSet rsCatalogs = dbmd.getCatalogs();
				catalogs.clear();
				while (rsCatalogs.next()) {
					String catName = rsCatalogs.getString("TABLE_CAT");
					if (catName != null && catName.isEmpty() == false) {
						// sometimes IBM DB" returns null as name 
	 					addCatalog(new SQLCatalog(this, catName));
					}
				}
				rsCatalogs.close();
				if (catalogs.isEmpty()) {
					catalogs.add(new DefaultCatalog(this));
				}
				catalogsLoaded = true;
				if (commitAfterReading && conn.getAutoCommit() == false) {
					conn.commit();
				}
			}
		} catch (SQLException sqle) {
			try {
				if (conn.getAutoCommit() == false) {
					conn.rollback();
				}
			} catch (SQLException e1) {
				// ignore
			}
			errorMessage = "loadCatalogs failed: " + sqle.getMessage();
			logger.error(errorMessage, sqle);
		} finally {
			loadingCatalogs = false;
		}
		for (SQLCatalog catalog : catalogs) {
			loadSchemas(catalog);
		}
		return ok;
	}
	
	public boolean loadSchemas(SQLCatalog catalog) {
		if (loadingSchemas) {
			return false;
		}
		catalog.setLoadingSchemas(true);
		loadingSchemas = true;
		if (Thread.currentThread().isInterrupted()) {
			loadingSchemas = false;
			return false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("loadSchemas for catalog: " + catalog.getName());
		}
		Connection conn = connection;
		if (conn == null) {
			return false;
		}
		boolean ok = false;
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			if (dbmd != null) {
				dbmd = conn.getMetaData();
				catalog.clear();
				final ResultSet rsSchemas = dbmd.getSchemas();
				while (rsSchemas.next()) {
					if (Thread.currentThread().isInterrupted()) {
						break;
					}
					String name = rsSchemas.getString("TABLE_SCHEM");
					SQLSchema schema = new SQLSchema(this, name);
					catalog.addSQLSchema(schema);
				}
				rsSchemas.close();
				if (catalog.getCountSchemas() == 0) {
					SQLSchema schema = new DefaultSchema(this);
					catalog.addSQLSchema(schema);
				}
				ok = true;
				schemasLoaded = true;
				if (commitAfterReading && conn.getAutoCommit() == false) {
					conn.commit();
				}
			}
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				// ignore
			}
			errorMessage = "loadSchemas (schemas) for catalog: " + catalog.getName() + " failed: " + e.getMessage();
			logger.error(errorMessage);
			return false;
		} finally {
			loadingSchemas = false;
			catalog.setLoadingSchemas(false);
		}
		return ok;
	}
	
	protected void clearCatalogs() {
		catalogs.clear();
	}
	
	public void addCatalog(SQLCatalog catalog) {
		if (catalogs.contains(catalog) == false) {
			catalogs.add(catalog);
		}
	}

	public SQLTable getSQLTable(String schemaName, String tableName) {
		if (isIdentifierName(tableName) == false) {
			return null;
		}
		for (SQLCatalog cat : catalogs) {
			for (SQLSchema schema : cat.getSchemas()) {
				if (schema.getName().equalsIgnoreCase(schemaName)) {
					SQLTable table = schema.getTable(tableName);
					if (table != null) {
						return table;
					}
				}
			}
		}
		return null;
	}
	
	public static boolean isIdentifierName(String name) {
		if (name == null || name.trim().isEmpty()) {
			return false;
		}
		for (int i = 0, n = name.length(); i < n; i++) {
			if (Character.isJavaIdentifierPart(name.charAt(i)) == false) {
				return false;
			}
		}
		return true;
	}

	public SQLSchema getSchema(String schemaName) {
		String currentCatalog = null;
		int pos = schemaName.indexOf('.');
		if (pos > 0) {
			currentCatalog = schemaName.substring(0, pos);
		}
		for (SQLCatalog cat : catalogs) {
			if (currentCatalog != null) {
				if (currentCatalog.equals(cat.getName()) == false) {
					continue;
				}
			}
			for (SQLSchema schema : cat.getSchemas()) {
				if (schema.getName().equalsIgnoreCase(schemaName)) {
					return schema;
				}
			}
		}
		return null;
	}

	public SQLTable getSQLTable(String schemaDotTableName) {
		int pos = schemaDotTableName.indexOf('.');
		if (pos == -1) {
			if (currentSQLSchema == null) {
				throw new IllegalStateException("current schema is null");
			}
			return currentSQLSchema.getTable(schemaDotTableName);
		} else {
			String schemaName = schemaDotTableName.substring(0, pos);
			String tableName = schemaDotTableName.substring(pos + 1);
			return getSQLTable(schemaName, tableName);
		}
	}

	public SQLField getSQLField(String schemaDotTableDotFieldName) {
		int pos = schemaDotTableDotFieldName.indexOf('.');
		if (pos == -1) {
			throw new IllegalArgumentException(
					schemaDotTableDotFieldName
					+ " is not a absolute name");
		} else {
			String schemaName = schemaDotTableDotFieldName.substring(0, pos);
			SQLSchema schema = getSchema(schemaName);
			if (schema != null) {
				int pos1 = schemaDotTableDotFieldName.indexOf('.', pos + 1);
				if (pos1 == -1) {
					throw new IllegalArgumentException(
							schemaDotTableDotFieldName
									+ " is not a absolute name");
				} else {
					String tableName = schemaDotTableDotFieldName.substring(pos + 1, pos1);
					SQLTable table = schema.getTable(tableName);
					if (table != null) {
						String fieldName = schemaDotTableDotFieldName.substring(pos1);
						return table.getField(fieldName);
					}
				}
			}
		}
		return null;
	}
	
	public SQLCatalog getSQLCatalog(String name) {
		for (SQLCatalog c : catalogs) {
			if (name.equalsIgnoreCase(c.getName())) {
				return c;
			}
		}
		return null;
	}
	
	public List<SQLCatalog> getCatalogs() {
		return catalogs;
	}

	public SQLField getSQLField(
			String schemaName, 
			String tableName,
			String fieldName) {
		SQLSchema schema = getSchema(schemaName);
		if (schema != null) {
			SQLTable table = schema.getTable(tableName);
			if (table != null) {
				return table.getField(fieldName);
			}
		}
		return null;
	}
	
	public boolean loadTables(SQLSchema schema) {
		if (schema.isLoadingTables()) {
			return false;
		}
		schema.setLoadingTables(true);
		if (Thread.currentThread().isInterrupted()) {
			return false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("loadTables schema=" + schema);
		}
		boolean ok = false;
		Connection conn = connection;
		if (conn == null) {
			return false;
		}
		try {
			ok = databaseExtension.loadTables(conn, schema);
			schema.setTablesLoaded();
		} catch (SQLException sqle) {
			try {
				if (conn.getAutoCommit() == false) {
					conn.rollback();
				}
			} catch (SQLException e1) {
				// ignore
			}
			errorMessage = "loadTables for schema=" + schema + " failed: " + sqle.getMessage();
			return false;
		} finally {
			schema.setLoadingTables(false);
		}
		try {
			if (commitAfterReading && conn.getAutoCommit() == false) {
				conn.commit();
			}
		} catch (SQLException e) {
			// ignore
		}
		return ok;
	}
	
	public boolean loadSequences(SQLSchema schema) {
		if (databaseExtension.hasSequenceFeature()) {
			if (schema.isLoadingTables()) {
				return false;
			}
			if (Thread.currentThread().isInterrupted()) {
				return false;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("loadSequences schema=" + schema);
			}
			Connection conn = connection;
			if (conn == null) {
				return false;
			}
			databaseExtension.listSequences(conn, schema);
			try {
				if (commitAfterReading && conn.getAutoCommit() == false) {
					conn.commit();
				}
			} catch (SQLException e) {
				// ignore it
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * load procedure and functions
	 * 
	 * @param schema
	 *            object that represent a schema
	 * @return true if everything went well
	 */
	public boolean loadProcedures(SQLSchema schema) {
		if (schema.isLoadingProcedures()) {
			return false;
		}
		// makes this thread save
		schema.setLoadingProcedures(true);
		if (logger.isDebugEnabled()) {
			logger.debug("loadProcedures schema=" + schema);
		}
		boolean ok = false;
		Connection conn = connection;
		if (conn == null) {
			return false;
		}
		try {
			databaseExtension.loadProcedures(conn, schema);
			for (int i = 0; i < schema.getProcedureCount(); i++) {
				SQLProcedure p = schema.getProcedureAt(i);
				databaseExtension.setupProcedureSQLCode(conn, p);
			}
			ok = true;
			if (commitAfterReading && conn.getAutoCommit() == false) {
				conn.commit();
			}
		} catch (SQLException sqle) {
			try {
				if (conn.getAutoCommit() == false) {
					conn.rollback();
				}
			} catch (SQLException e1) {
				// ignore
			}
			// creates a error message to show it in GUI
			errorMessage = "loadProcedures schema=" 
				+ schema 
				+ " failed: "
				+ sqle.getMessage();
			return false;
		} finally {
			schema.setLoadingProcedures(false);
		}
		return ok;
	}
	
	boolean loadColumns(SQLTable table) {
		return loadColumns(table, false);
	}

	boolean loadColumns(SQLTable table, boolean ignoreIndices) {
		if (table.isLoadingColumns()) {
			return false;
		}
		table.setLoadingColumns(true);
		if (Thread.currentThread().isInterrupted()) {
			table.setLoadingColumns(false);
			return false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("loadColumns table=" + table);
		}
		Connection conn = connection;
		if (conn == null) {
			table.setLoadingColumns(false);
			return false;
		}
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			if (dbmd != null) {
				SQLField field = null;
				try {
					table.clearFields();
					final ResultSet rs = dbmd.getColumns(
							table.getSchema().getCatalog().getKey(), 
							table.getSchema().getKey(), 
							table.getName(), 
							null);
					if (rs != null) {
						while (rs.next()) {
							if (Thread.currentThread().isInterrupted()) {
								break;
							}
							String tableName = rs.getString("TABLE_NAME");
							if (table.getName().equalsIgnoreCase(tableName)) {
								String name = rs.getString("COLUMN_NAME");
								field = new SQLField(this, table, name);
								field.setType(rs.getInt("DATA_TYPE"));
								field.setTypeName(rs.getString("TYPE_NAME"));
								field.setLength(rs.getInt("COLUMN_SIZE"));
								field.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
								field.setOrdinalPosition(rs.getInt("ORDINAL_POSITION"));
								field.setNullValueAllowed(rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
								databaseExtension.setupDataType(field);
								table.addField(field);
							}
						}
						rs.close();
						if (commitAfterReading && conn.getAutoCommit() == false) {
							conn.commit();
						}
					} else {
						logger.error("No ResultSet received for getColumns(" + table.getSchema().getCatalog().getKey() + "," + table.getSchema().getKey() + "," + table.getName());
					}
					if (table.isView()) {
						if (table.isMaterializedView()) {
							if (ignoreIndices == false) {
								loadIndexes(table);
							}
						}
						if (ignoreIndices == false) {
							databaseExtension.setupViewSQLCode(conn, table);
						}
					} else {
						loadConstraints(table);
						if (ignoreIndices == false) {
							loadIndexes(table);
						}
					}
				} catch (SQLException sqle) {
					try {
						if (conn.getAutoCommit() == false) {
							conn.rollback();
						}
					} catch (SQLException e1) {
						// ignore
					}
					logger.error("loadColumns (get columns) for table=" + table + " failed: " + sqle.getMessage());
					table.setLoadingColumns(false);
					return false;
				} finally {
					table.setLoadingColumns(false);
					table.setFieldsLoaded();
				}
			}	
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				// ignore
			}
			logger.error("loadColumns failed: " + e.getMessage(), e);
		} finally {
			table.setLoadingColumns(false);
			table.setFieldsLoaded();
		}
		return true;
	}
	
	private boolean loadConstraints(SQLTable table) {
		if (table.isLoadingConstraints()) {
			return false;
		}
		table.setLoadingConstraints(true);
		if (Thread.currentThread().isInterrupted()) {
			return false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("loadConstraints table=" + table);
		}
		Connection conn = connection;
		if (conn == null) {
			return false;
		}
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			if (dbmd != null) {
				table.clearConstraints();
				try {
					final ResultSet rs = dbmd.getPrimaryKeys(
							table.getSchema().getCatalog().getKey(), 
							table.getSchema().getKey(), 
							table.getName());
					if (rs != null) {
						while (rs.next()) {
							if (Thread.currentThread().isInterrupted()) {
								break;
							}
							String tableName = rs.getString("TABLE_NAME");
							if (table.getName().equalsIgnoreCase(tableName)) {
								String name = rs.getString("PK_NAME");
								SQLConstraint constraint = table.getPrimaryKeyConstraint();
								if (constraint == null) {
									constraint = new SQLConstraint(
											this, 
											table,
											SQLConstraint.PRIMARY_KEY, 
											name);
									table.setPrimaryKeyConstraint(constraint);
								}
								constraint.addPrimaryKeyFieldName(
										rs.getString("COLUMN_NAME"), 
										rs.getShort("KEY_SEQ"));
								if (logger.isDebugEnabled()) {
									logger.debug("pk constraint changed: " + constraint);
								}
							}
						}
						rs.close();
						if (commitAfterReading && conn.getAutoCommit() == false) {
							conn.commit();
						}
					}
				} catch (SQLException sqle) {
					try {
						if (conn.getAutoCommit() == false) {
							conn.rollback();
						}
					} catch (SQLException e1) {
						// ignore
					}
					logger.error("loadConstraints (pk) for table=" + table + " failed: " + sqle.getMessage());
					try {
						connection.rollback();
					} catch (SQLException e1) {
						// ignore
					}
					table.setLoadingConstraints(false);
					return false;
				}
				if (Thread.currentThread().isInterrupted()) {
					return false;
				}
				try {
					final ResultSet rs = dbmd.getImportedKeys(
							table.getSchema().getCatalog().getKey(), 
							table.getSchema().getKey(), 
							table.getName());
					if (rs != null) {
						while (rs.next()) {
							if (Thread.currentThread().isInterrupted()) {
								break;
							}
							String name = rs.getString("FK_NAME");
							String referencedSchema = rs.getString("PKTABLE_SCHEM");
							String referencedTable = rs.getString("PKTABLE_NAME");
							SQLConstraint constraint = table.getConstraint(name);
							if (constraint == null) {
								constraint = new SQLConstraint(
										this, 
										table,
										SQLConstraint.FOREIGN_KEY, 
										name);
								if (referencedSchema == null || referencedSchema.isEmpty()) {
									referencedSchema = table.getSchema().getName();
								}
								constraint.setReferencedTableName(referencedSchema + "." + referencedTable);
								table.addConstraint(constraint);
							}
							constraint.addForeignKeyColumnNamePair(
									rs.getString("FKCOLUMN_NAME"), 
									rs.getString("PKCOLUMN_NAME"), 
									rs.getShort("KEY_SEQ"));
							if (logger.isDebugEnabled()) {
								logger.debug("fk constraint changed: " + constraint);
							}
						}
						rs.close();
						table.setConstraintsLoaded();
					}
					if (logger.isDebugEnabled()) {
						List<SQLConstraint> list = table.getConstraints();
						for (int i = 0; i < list.size(); i++) {
							logger.debug(" * " + list.get(i));
						}
					}
				} catch (SQLException sqle) {
					logger.error("loadConstraints (fk) for table=" + table + " failed: " + sqle.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
					table.setLoadingConstraints(false);
					return false;
				}
			}
		} catch (Exception e) {
			logger.error("loadConstraints failed: " + e.getMessage(), e);
			return false;
		} finally {
			table.setLoadingConstraints(false);
		}
		return true;
	}
	
	private boolean loadIndexes(SQLTable table) {
		if (table.isLoadingIndexes()) {
			return false;
		}
		table.setLoadingIndexes(true);
		if (Thread.currentThread().isInterrupted()) {
			return false;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("loadIndexes table=" + table);
		}
		Connection conn = connection;
		if (conn == null) {
			return false;
		}
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			if (dbmd != null) {
				table.clearIndexes();
				SQLIndex index = null;
				ResultSet rs = dbmd.getIndexInfo(null, table.getSchema().getName(), table.getName(), false, true);
				if (rs != null) {
					while (rs.next()) {
						if (Thread.currentThread().isInterrupted()) {
							break;
						}
						String tableName = rs.getString("TABLE_NAME");
						if (table.getName().equalsIgnoreCase(tableName)) {
							String indexName = rs.getString("INDEX_NAME");
							if (indexName == null) {
								continue; // because Oracle sends here null some times
							}
							boolean unique = false;
							Object nonUnique = rs.getObject("NON_UNIQUE");
							if (nonUnique instanceof Boolean) {
								unique = !((Boolean) nonUnique).booleanValue();
							} else if (nonUnique instanceof Integer) {
								unique = ((Integer) nonUnique).intValue() == 0;
							} else if (nonUnique instanceof String) {
								unique = !Boolean.parseBoolean((String) nonUnique);
							}
							short type = rs.getShort("TYPE");
							short ordinalPosition = rs.getShort("ORDINAL_POSITION");
							String columnName = rs.getString("COLUMN_NAME");
							String sortOrder = rs.getString("ASC_OR_DESC");
							int cardinality = rs.getInt("CARDINALITY");
							String filterCondition = rs.getString("FILTER_CONDITION");
							index = table.getIndexByName(indexName);
							if (index == null) {
								if (logger.isDebugEnabled()) {
									logger.debug("add index " + indexName);
								}
								index = new SQLIndex(table.getModel(), indexName, table);
								index.setUnique(unique);
								index.setType(type);
								index.setCardinality(cardinality);
								index.setFilterCondition(filterCondition);
								table.addIndex(index);
							}
							index.addIndexField(columnName, ordinalPosition, sortOrder);
						}
					}
					table.setIndexesLoaded();
					rs.close();
					if (commitAfterReading && conn.getAutoCommit() == false) {
						conn.commit();
					}
					return true;
				}
			}
		} catch (Exception e) {
			try {
				if (conn.getAutoCommit() == false) {
					conn.rollback();
				}
			} catch (SQLException e1) {
				// ignore
			}
			logger.error("loadIndexes failed: " + e.getMessage(), e);
			return false;
		} finally {
			table.setLoadingIndexes(false);
		}
		return false;
	}

	public String getLastErrorMessage() {
		return errorMessage;
	}

	public static void removeViews(SQLSchema schema) {
		SQLTable sqlTable = null;
		for (int x = 0; x < schema.getTableCount(); x++) {
			sqlTable = schema.getTableAt(x);
			if (sqlTable.getType().equals(SQLTable.TYPE_VIEW)) {
				schema.removeSQLTable(sqlTable);
				x--;
			}
		}
	}

	public List<SQLSchema> getSchemas() {
		List<SQLSchema> list = new ArrayList<SQLSchema>();
		for (SQLCatalog c : catalogs) {
			for (SQLSchema s : c.getSchemas()) {
				if (list.contains(s) == false) {
					list.add(s);
				}
			}
		}
		return list;
	}
	
	public DatabaseExtension getDatabaseExtension() {
		return databaseExtension;
	}
	
	/** this method is only allowed if the data model already had a connection and use a new one
	 * to continue working!
	 * @param connection
	 */
	public void setConnection(Connection connection) {
		if (this.connection == null) {
			throw new IllegalStateException("Use the method setConnection only to renew an existing connection!");
		}
		if (connection != null) {
			this.connection = connection;
		} else {
			throw new IllegalArgumentException("connection cannot be null!");
		}
	}

	public void setCurrentSQLSchema(SQLSchema currentSQLSchema) {
		this.currentSQLSchema = currentSQLSchema;
	}

	@Override
	public int compareTo(SQLDataModel o) {
		return 0;
	}

	public boolean isCommitAfterReading() {
		return commitAfterReading;
	}

	public void setCommitAfterReading(boolean commitAfterReading) {
		this.commitAfterReading = commitAfterReading;
	}
	
}
