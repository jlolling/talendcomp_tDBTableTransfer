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
package de.jlo.datamodel.ext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import de.jlo.datamodel.Field;
import de.jlo.datamodel.SQLProcedure;
import de.jlo.datamodel.SQLSchema;
import de.jlo.datamodel.SQLSequence;
import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.SQLTrigger;

public interface DatabaseExtension {
	
	/**
	 * setup connections if necessary
	 * @param c connection to tweak
	 */
	public void setupConnection(Connection c);
	
	/**
	 * setup connections if necessary
	 * @param c connection to tweak
	 */
	public void setupStatement(Statement c);

	/**
	 * check if extension fits to the database type
	 * @param driverClass description of connection
	 * @return true if it matches
	 */
	public boolean isApplicable(String driverClass);

	/**
	 * 
	 * @return true if database has explain capabilities
	 */
	public boolean hasExplainFeature();

	/**
	 * Creates a script to do the explain job
	 * @param currentStatement
	 * @return sql code to explain the current statement
	 */
	public String getExplainSQL(String currentStatement);
	
	/**
	 * load tables and add them to the schema and set the flag tablesLoaded
	 * @param connection
	 * @param schema
	 * @return true if successfully loaded
	 */
	public boolean loadTables(Connection conn, SQLSchema schema) throws SQLException;
	
	/**
	 * load tables and add them to the schema and does NOT set the flag tables loaded
	 * @param connection
	 * @param schema
	 * @return true if successfully loaded
	 */
	public boolean loadTables(Connection conn, SQLSchema schema, String tableNamePattern) throws SQLException;

	/**
	 * sets the code into the view
	 * @param table (must be of type VIEW)
	 * @return the code
	 */
	public String setupViewSQLCode(Connection conn, SQLTable table);
	
	/**
	 * sets the code into the procedure
	 * @param proc
	 * @return the code
	 */
	public String setupProcedureSQLCode(Connection conn, SQLProcedure proc);
	
	/**
	 * sets the code into the procedure
	 * @param proc
	 * @return the code
	 */
	public String setupTriggerSQLCode(Connection conn, SQLTrigger proc);

	/**
	 * sets the code into the procedure
	 * @param conn
	 * @param sequence
	 * @return the code
	 */
	public String setupSequenceSQLCode(Connection conn, SQLSequence sequence);
	
	/**
	 * change data type to more appropriated types
	 * @param field
	 */
	public void setupDataType(Field field);
	
	/**
	 * change data type to more appropriated types
	 * @param field
	 */
	public void setupDataType(SQLProcedure.Parameter parameter);

	public String getName();
	
	/**
	 * returns a list of additional keywords, which are depend in the database vendor
	 * @return always a list (e.g. an empty list if there are no additional keywords) 
	 */
	public List<String> getAdditionalSQLKeywords();

	/**
	 * returns a list of additional data types, which are depend in the database vendor
	 * @return always a list (e.g. an empty list if there are no additional data types) 
	 */
	public List<String> getAdditionalSQLDatatypes();

	/**
	 * returns a list of additional keywords for stored procedures, which are depend in the database vendor
	 * @return always a list (e.g. an empty list if there are no additional keywords for stored procedures) 
	 */
	public List<String> getAdditionalProcedureKeywords();
	
	/**
	 * create SQL to set or update comment for a SQLTable
	 * @param tableName to comment
	 * @param comment 
	 * @return SQL code
	 */
	public String getUpdateCommentStatement(String tableName, String comment);
	
	/**
	 * create SQL to set or update comment for a SQLField
	 * @param tableName
	 * @param fieldName
	 * @param comment
	 * @return SQL code
	 */
	public String getUpdateCommentStatement(String tableName, String fieldName, String comment);
		
	/**
	 * returns true if SQL dialect contains limit keyword
	 * @return
	 */
	public boolean hasSQLLimitFeature();
	
	/**
	 * returns true if SQL dialect use a where condition to limit count datasets
	 * @return
	 */
	public boolean isLimitExpressionAWhereCondition();

	/**
	 * returns the changed statement with dataset limitation
	 * @param max count datasets
	 * @return sql fragement to limit an query
	 */
	public String getLimitExpression(int max);
	
	/**
	 * return the SQL code to convert a date object into a SQL expression
	 * @param value the date object which needs to convert into a SQL expression
	 * @return the necessary SQL code 
	 */
	public String getDateToSQLExpression(java.util.Date value);
	
	/**
	 * return the SQL code template to convert a date object into a SQL expression
	 * @return the necessary SQL code template
	 */
	public String getDateToSQLExpressionPattern();

	/**
	 * return the SQL code to convert a date object into a SQL expression
	 * @param value the date object which needs to convert into a SQL expression
	 * @return the necessary SQL code 
	 */
	public String getTimestampToSQLExpression(java.util.Date value);
	
	/**
	 * return the SQL code template to convert a date object into a SQL expression
	 * @return the necessary SQL code template
	 */
	public String getTimestampToSQLExpressionPattern();
	
	/**
	 * returns the char which should be used to enclosure identifiers
	 * @return
	 */
	public String getIdentifierQuoteString();
	
	/**
	 * returns the char which should be used to enclosure identifiers
	 * for closing the enclosure.
	 * @return
	 */
	public String getIdentifierQuoteStringClose();

	public void setIdentifierQuoteString(String quote);
	
	public boolean hasSequenceFeature();
	
	public List<SQLSequence> listSequences(Connection conn, SQLSchema schema);
	
	/**
	 * returns the SQL code to get the next value from the sequence
	 * @param sequence
	 * @return SQL code
	 */
	public String getSequenceNextValSQL(SQLSequence sequence);
	
	/**
	 * returns the select statement which best counts the rows for a table
	 * @param table
	 * @return
	 */
	public String getSelectCountRows(SQLTable table);
	
	/**
	 * loads the procedures
	 * @param schema
	 * @return true if successfully loaded, otherwise false if a generic method should be used
	 */
	public boolean loadProcedures(Connection conn, SQLSchema schema) throws SQLException;
	
	/**
	 * securely close a connection with terminating backends
	 * @param conn
	 */
	public void closeConnection(Connection conn);
	
	/**
	 * Cancel the last statement on this connection
	 * @param conn
	 */
	public void cancelLastStatement(Connection conn);
	
}
