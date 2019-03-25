package de.jlo.datamodel.generator;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import de.jlo.datamodel.BasicDataType;
import de.jlo.datamodel.ModelComparator;
import de.jlo.datamodel.SQLConstraint;
import de.jlo.datamodel.SQLDataModel;
import de.jlo.datamodel.SQLField;
import de.jlo.datamodel.SQLFieldNotNullConstraint;
import de.jlo.datamodel.SQLIndex;
import de.jlo.datamodel.SQLObject;
import de.jlo.datamodel.SQLPSParam;
import de.jlo.datamodel.SQLProcedure;
import de.jlo.datamodel.SQLSchema;
import de.jlo.datamodel.SQLSequence;
import de.jlo.datamodel.SQLStatement;
import de.jlo.datamodel.SQLTable;
import de.jlo.datamodel.StringReplacer;
import de.jlo.datamodel.SQLProcedure.Parameter;
import de.jlo.datamodel.ext.DatabaseExtension;

/**
 * SQL Code Generator
 * @author Jan Lolling
 */
public class SQLCodeGenerator {
	
	private List<String> keywordList = new ArrayList<String>();
	private String ec = "\"";
	private static SQLCodeGenerator instance;
	
	public void setEnclosureChar(String c) {
		ec = c;
	}
	
	public static SQLCodeGenerator getInstance() {
		if (instance == null) {
			instance = new SQLCodeGenerator();
		}
		return instance;
	}
	
	public SQLCodeGenerator() {
		keywordList.add("type");
		keywordList.add("user");
		keywordList.add("password");
		keywordList.add("table");
		keywordList.add("view");
		keywordList.add("date");
		keywordList.add("timestamp");
		keywordList.add("key");
		keywordList.add("value");
		keywordList.add("database");
		keywordList.add("table");
		keywordList.add("schema");
	}

	public void addKeyword(String word) {
		if (keywordList.contains(word) == false) {
			keywordList.add(word);
		}
	}
	
	public boolean containsKeyword(String identifier) {
		if (identifier != null) {
			String[] names = identifier.split("\\.");
			for (String name : names) {
				for (String w : keywordList) {
					if (w.equalsIgnoreCase(name)) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	public String getEncapsulatedName(String name) {
		if (containsKeyword(name) || name.indexOf('-') != -1 || name.indexOf(' ') != -1 || name.indexOf("$") != -1) {
			// we need encapsulation
			StringBuilder sb = new StringBuilder();
			StringTokenizer st = new StringTokenizer(name, ".");
			String s = null;
			boolean firstLoop = true;
			while (st.hasMoreTokens()) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(".");
				}
				s = st.nextToken();
				if (containsKeyword(s) || s.contains("-") || s.contains(" ") || s.contains("$")) {
					if (s.contains(ec) == false) {
						s = ec + s + ec;
					}
				}
				sb.append(s);
			}
			return sb.toString();
		} else {
			return name;
		}
	}
	
	public String buildSelectStatement(SQLTable table, boolean withSchemaName) {
		return buildSelectStatement(table, withSchemaName, false);
	}
    
	public String buildSelectStatement(SQLTable table, boolean withSchemaName, boolean coalesce) {
        if (table != null) {
	    	setupEnclosureChar(table);
            final StringBuilder sb = new StringBuilder();
            if (table.getFieldCount() > 0) {
                sb.append("select\n");
                SQLField field;
                boolean firstLoop = true;
                for (int i = 0; i < table.getFieldCount(); i++) {
                    if (firstLoop) {
                        firstLoop = false;
                    } else {
                        sb.append(",\n");
                    }
                    field = table.getFieldAt(i);
                    if (coalesce) {
                    	if (field.getBasicType() == BasicDataType.CHARACTER.getId()) {
                        	sb.append("coalesce(");
                        	sb.append(getEncapsulatedName(field.getName()));
                        	sb.append(", '') as ");
                        	sb.append(getEncapsulatedName(field.getName()));
                    	} else if (BasicDataType.isNumberType(field.getBasicType())) {
                        	sb.append("coalesce(");
                        	sb.append(getEncapsulatedName(field.getName()));
                        	sb.append(", 0) as ");
                        	sb.append(getEncapsulatedName(field.getName()));
                    	} else {
                        	sb.append(getEncapsulatedName(field.getName()));
                    	}
                    } else {
                        sb.append(getEncapsulatedName(field.getName()));
                    }
                }
                sb.append("\nfrom ");
                if (withSchemaName) {
                    sb.append(getEncapsulatedName(table.getAbsoluteName()));
                } else {
                    sb.append(getEncapsulatedName(table.getName()));
                }
                return sb.toString();
            } else {
                return null;
            } // if (table.getFields().size() > 0)
        } else { // if (tablename != null)
            return null;
        } // if (tablename != null)
	}

	public String buildInsertStatement(SQLTable table, boolean withSchemaName) {
        if (table != null) {
	    	setupEnclosureChar(table);
            final StringBuilder sb = new StringBuilder();
            if (table.getFieldCount() > 0) {
                sb.append("insert into "); 
                if (withSchemaName) {
                    sb.append(getEncapsulatedName(table.getAbsoluteName()));
                } else {
                    sb.append(getEncapsulatedName(table.getName()));
                }
                sb.append("\n ("); 
                SQLField field;
                boolean firstLoop = true;
                // Felder definieren
                for (int i = 0; i < table.getFieldCount(); i++) {
                    if (firstLoop) {
                        firstLoop = false;
                    } else {
                        sb.append(",\n  "); 
                    }
                    field = table.getFieldAt(i);
                    sb.append(getEncapsulatedName(field.getName()));
                }
                sb.append(")\nvalues\n (\n )"); 
                return sb.toString();
            } else {
                return null;
            } // if (table.getFields().size() > 0)
        } else {
            return null;
        }
	}
	
	public String buildCreateStatement(SQLIndex index, boolean fullName) {
		return buildCreateStatement(index, fullName, null);
	}
	
	public String buildCreateStatement(SQLProcedure proc, boolean fullName, String alternativeSchemaName) {
		String code = proc.getCode();
		if (code != null && code.isEmpty() == false) {
			if (fullName) {
				StringReplacer sr = new StringReplacer(code);
				String originalSchemaName = proc.getSchema().getName();
				if (alternativeSchemaName == null) {
					alternativeSchemaName = originalSchemaName;
				}
				if (code.indexOf(originalSchemaName + "." + proc.getName()) != -1) {
					// schema+name
					sr.replace(originalSchemaName + "." + proc.getName(), alternativeSchemaName + "." + proc.getName());
				}
				return sr.getResultText();
			} else {
				return code;
			}
		} else {
			if (proc.isFunction()) {
				return "-- create or replace function " + proc.getName() + " (no source available)";
			} else {
				return "-- create or replace procedure " + proc.getName() + " (no source available)";
			}
		}
	}
	
	public String buildCreateStatement(SQLSequence seq, boolean fullName, String alternativeSchemaName) {
		String code = seq.getCreateCode();
		if (code != null && code.isEmpty() == false) {
			if (fullName) {
				StringReplacer sr = new StringReplacer(code);
				String originalSchemaName = seq.getSchema().getName();
				if (alternativeSchemaName == null) {
					alternativeSchemaName = originalSchemaName;
				}
				if (code.indexOf(originalSchemaName + "." + seq.getName()) != -1) {
					// schema+name
					sr.replace(originalSchemaName + "." + seq.getName(), alternativeSchemaName + "." + seq.getName());
				}
				return sr.getResultText();
			} else {
				return code;
			}
		} else {
			return "-- create or replace sequence " + seq.getName() + " (no source available)";
		}
	}

	public String buildCreateStatement(SQLIndex index, boolean fullName, String alternativeSchemaName) { 
		if (index != null) {
	    	setupEnclosureChar(index);
			StringBuilder sb = new StringBuilder();
			sb.append("create ");
			if (index.isUnique()) {
				sb.append("unique ");
			}
			sb.append("index ");
			sb.append(index.getName());
			sb.append(" on ");
			if (fullName) {
				if (alternativeSchemaName != null) {
					sb.append(alternativeSchemaName);
					sb.append('.');
					sb.append(index.getTable().getName());					
				} else {
					sb.append(index.getTable().getAbsoluteName());
				}
			} else {
				sb.append(index.getTable().getName());
			}
			sb.append("(");
			for (int i = 0, n = index.getCountFields(); i < n; i++) {
				if (i > 0) {
					sb.append(",");
				}
				sb.append(index.getFieldByOrdinalPosition(i + 1).getName());
			}
			sb.append(")");
			return sb.toString();
		} else {
			return null;
		}
	}

	public String buildDropStatement(SQLIndex index, boolean fullName, String alternativeSchemaName) {
		if (index != null) {
	    	setupEnclosureChar(index);
			StringBuilder sb = new StringBuilder();
			sb.append("drop index ");
			if (fullName) {
				if (alternativeSchemaName != null) {
					sb.append(alternativeSchemaName);
					sb.append(".");
					sb.append(index.getName());
				} else {
					sb.append(index.getTable().getSchema().getName());
					sb.append(".");
					sb.append(index.getName());
				}
			} else {
				sb.append(index.getName());
			}
			return sb.toString();
		} else {
			return null;
		}
	}
	
	public String buildCreateStatement(SQLTable table, boolean fullName) {
		return buildCreateStatement(table, fullName, null, false, false, false);
	}
	
	public String buildCreateStatement(SQLTable table, boolean fullName, boolean viewAsTable) {
		return buildCreateStatement(table, fullName, null, false, false, viewAsTable);
	}
	
	protected void setupEnclosureChar(SQLObject so) {
		if (so != null) {
			DatabaseExtension ext = getDatabaseExtension(so);
			if (ext != null) {
				ec = ext.getIdentifierQuoteString();
			}
		} else {
			ec = "\"";
		}
	}
	
	protected static DatabaseExtension getDatabaseExtension(SQLObject so) {
		SQLDataModel model = so.getModel();
		if (model != null) {
			return model.getDatabaseExtension();
		} else {
			return null;
		}
	}

	public String buildCreateStatement(SQLTable table, boolean fullName, String alternativeSchemaName, boolean withDefault, boolean notnull, boolean viewAsTable) {
        if (table != null) {
        	setupEnclosureChar(table);
        	String tableName = null;
        	if (fullName) {
            	if (alternativeSchemaName != null) {
            		tableName = alternativeSchemaName + '.' + getEncapsulatedName(table.getName());
            	} else {
            		tableName = getEncapsulatedName(table.getAbsoluteName());
            	}
            } else {
            	tableName = getEncapsulatedName(table.getName());
            }
            if (table.isFieldsLoaded() == false) {
                table.loadColumns();
            }
        	if (table.isTable() || viewAsTable) {
                final StringBuilder sb = new StringBuilder();
                if (table.getFieldCount() > 0) {
                	if (table.getComment() != null && table.getComment().trim().isEmpty() == false) {
            			sb.append("/*\n");
            			sb.append(table.getComment());
            			sb.append("\n*/\n");
                	}
                	sb.append("-- drop table ");
                	sb.append(tableName);
                	sb.append(";\n");
                    sb.append("create table ");
            		sb.append(tableName);
                    sb.append(" (\n");
                    SQLField field = null;
                    boolean firstLoop = true;
                    // create code for fields
                    for (int i = 0; i < table.getFieldCount(); i++) {
                        if (firstLoop) {
                            firstLoop = false;
                        } else {
                        	String comment = field.getComment();
                        	if (comment != null && comment.trim().isEmpty() == false) {
                                sb.append(", -- ");
                                sb.append(comment.replace('\n', ' '));
                                sb.append("\n");
                        	} else {
                                sb.append(",\n");
                        	}
                        }
                        field = table.getFieldAt(i);
                        sb.append("   ");
                        sb.append(buildFieldDeclaration(field));
                        if (!field.isNullValueAllowed()) {
                            sb.append(" not null");
                        }
                        sb.append(field.getDefaultValueSQL());
                    }
                    // add primary key constraint
                    firstLoop = true;
                    SQLConstraint pkConstraint = table.getPrimaryKeyConstraint();
                    if (pkConstraint != null) {
                    	String sql = buildConstraintSQLCode(pkConstraint, fullName, alternativeSchemaName);
                    	if (sql != null && sql.isEmpty() == false) {
                            sb.append(",\n   ");
                            sb.append(sql);
                    	}
                    }
                    // add rest of constraints
                    for (SQLConstraint constraint : table.getConstraints()) {
                    	if (constraint.getType() != SQLConstraint.PRIMARY_KEY) {
                        	String sql = buildConstraintSQLCode(constraint, fullName, alternativeSchemaName);
                        	if (sql != null && sql.isEmpty() == false) {
                                sb.append(",\n   ");
                                sb.append(sql);
                        	}
                    	}
                    }
                    sb.append(");\n");
                    boolean firstIndex = true;
                    if (table.countIndexes() > 0) {
                    	for (SQLIndex index : table.getIndexes()) {
                    		// check index for primary key and avoid build statement
                    		if (table.getConstraint(index.getName()) == null) {
                    			if (firstIndex) {
                    				firstIndex = false;
                                	sb.append("\n");
                    			}
                            	sb.append(buildCreateStatement(index, fullName, alternativeSchemaName));
                				sb.append(";\n");
                    		}
                    	}
                    }
                    return sb.toString();
                } else {
                    return null;
                } // if (table.getFields().size() > 0)
        	} else {
        		StringBuilder sb = new StringBuilder();
            	if (table.getComment() != null && table.getComment().trim().isEmpty() == false) {
        			sb.append("/*\n");
        			sb.append(table.getComment());
        			sb.append("\n*/\n");
            	}
            	String source = table.getSourceCode();
            	if (source != null) {
                	sb.append("-- drop view ");
                	sb.append(tableName);
                	sb.append(";\n"); 
            		if (fullName) {
            			StringReplacer sr = new StringReplacer(table.getSourceCode());
            			if (alternativeSchemaName != null) {
            				if (source.indexOf((alternativeSchemaName + "." + table.getName())) == -1) {
            					// only if it necessary
                				sr.replace(table.getName(), alternativeSchemaName + "." + table.getName());
            				}
            			} else {
            				if (source.indexOf(table.getAbsoluteName()) == -1) {
                				sr.replace(table.getName(), table.getAbsoluteName());
            				}
            			}
            			return sr.getResultText();
            		} else {
                    	sb.append(table.getSourceCode());
            		}
                	sb.append(";\n"); 
            	} else {
            		sb.append("-- create view ");
                    if (fullName) {
                    	if (alternativeSchemaName != null) {
                    		sb.append(alternativeSchemaName);
                    		sb.append('.');
                            sb.append(getEncapsulatedName(table.getName()));
                    	} else {
                            sb.append(getEncapsulatedName(table.getAbsoluteName()));
                    	}
                    } else {
                        sb.append(getEncapsulatedName(table.getName()));
                    }
                    sb.append("\n-- source code not available");
            	}
            	return sb.toString();
        	}
        } else { // if (tablename != null)
            return null;
        } // if (tablename != null)
	}
	
	public String buildFieldDeclaration(SQLField field) {
    	setupEnclosureChar(field);
		StringBuilder sb = new StringBuilder(32);
        sb.append(getEncapsulatedName(field.getName()));
        sb.append(' ');
        sb.append(getFieldType(field));
        return sb.toString();
	}
	
	public String getFieldType(SQLField field) {
		if (field.getTypeSQLCode() != null) {
			return field.getTypeSQLCode();
		}
		StringBuilder sb = new StringBuilder(32);
        if (BasicDataType.isNumberType(field.getBasicType())) {
            sb.append(field.getTypeName());
        	if (field.getLength() > 0) {
                sb.append('(');
                sb.append(String.valueOf(field.getLength()));
                sb.append(',');
                sb.append(String.valueOf(field.getDecimalDigits()));
                sb.append(')');
        	}
        } else if (field.getBasicType() == BasicDataType.CHARACTER.getId()) {
            sb.append(field.getTypeName());
            if ((field.getLength() > 0)
                    && (field.getTypeName()
                            .equalsIgnoreCase("CLOB") == false)
                    && (field.getTypeName()
                            .equalsIgnoreCase("TEXT") == false)
                    && (field.getTypeName()
                            .equalsIgnoreCase("BLOB") == false)) {
                sb.append('(');
                sb.append(String.valueOf(field.getLength()));
                sb.append(')');
            }
        } else if (field.getBasicType() == BasicDataType.BOOLEAN.getId()) {
        	sb.append("boolean");
        } else {
            sb.append(field.getTypeName());
        }
        return sb.toString();
	}

	public String buildPSInsertStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append("\n ("); 
		SQLField field = null;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(",\n  "); 
			}
			sb.append(getEncapsulatedName(field.getName()));
		}
		sb.append(")\nvalues\n ("); 
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(",\n  "); 
			}
			sb.append("?"); 
		}
		sb.append(")"); 
		return sb.toString();
	}

	public SQLStatement buildPSInsertSQLStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final SQLStatement sqlPs = new SQLStatement();
		sqlPs.setPrepared(true);
		int paramIndex = 0;
		final StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append(" ("); 
		SQLField field = null;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(',');
			}
			sb.append(getEncapsulatedName(field.getName()));
		}
		sb.append(")\n values("); 
		SQLPSParam psParam = null;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (i > 0) {
				sb.append(',');
			}
			sb.append("?"); 
			psParam = new SQLPSParam();
			psParam.setName(field.getName());
			psParam.setIndex(++paramIndex);
			psParam.setBasicType(field.getBasicType());
			sqlPs.addParam(psParam);
		}
		sb.append(")"); 
		sqlPs.setSQL(sb.toString());
		return sqlPs;
	}
	
	public String buildCountAllStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final StringBuilder sb = new StringBuilder();
		sb.append("select count(*) from ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		return sb.toString();
	}

	public String buildPSCountStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final StringBuilder sb = new StringBuilder();
		sb.append("select\n count(*)\nfrom ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append("\nwhere\n "); 
		SQLField field = null;
		boolean firstLoop = true;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.isPrimaryKey()) {
				if (!firstLoop) {
					sb.append("\n and "); 
				} else {
					firstLoop = false;
				}
				sb.append(getEncapsulatedName(field.getName()));
				sb.append("=?"); 
			}
		}
		return sb.toString();
	}

	public SQLStatement buildPSCountSQLStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final StringBuilder sb = new StringBuilder();
		final SQLStatement sqlPs = new SQLStatement();
		sqlPs.setPrepared(true);
		sb.append("select\n count(*) \nfrom ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append(" where\n ");
		SQLField field = null;
		boolean firstLoop = true;
		int paramIndex = 0;
		boolean hasPrimaryKeys = false;
		SQLPSParam psParam = null;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.isPrimaryKey()) {
				if (!firstLoop) {
					sb.append("\n and "); 
				} else {
					firstLoop = false;
				}
				paramIndex++;
				sb.append(getEncapsulatedName(field.getName()));
				sb.append("=?"); 
				psParam = new SQLPSParam();
				psParam.setName(field.getName());
				psParam.setIndex(paramIndex);
				psParam.setBasicType(field.getBasicType());
				sqlPs.addParam(psParam);
				hasPrimaryKeys = true;
			}
		}
		sqlPs.setSqlCodeValid(hasPrimaryKeys);
		sqlPs.setSQL(sb.toString());
		return sqlPs;
	}

	public String buildPSUpdateStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final StringBuilder sb = new StringBuilder();
		sb.append("update ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append("\nset "); 
		SQLField field = null;
		// build set clauses
		boolean firstLoop = true;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (!field.isPrimaryKey()) {
				if (!firstLoop) {
					sb.append(",\n    ");
				} else {
					firstLoop = false;
				}
				sb.append(getEncapsulatedName(field.getName()));
				sb.append("=?");
			}
		}
		firstLoop = true;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.isPrimaryKey()) {
				if (firstLoop) {
                    sb.append("\nwhere\n ");
					firstLoop = false;
				} else {
					sb.append("\n and ");
				}
				sb.append(getEncapsulatedName(field.getName()));
				sb.append("=?");
			}
		}
		return sb.toString();
	}

	public String buildPSDeleteStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final StringBuilder sb = new StringBuilder();
		sb.append("delete from ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		SQLField field = null;
		// build set clauses
		boolean firstLoop = true;
		firstLoop = true;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.isPrimaryKey()) {
				if (firstLoop) {
            		sb.append("\nwhere\n "); 
					firstLoop = false;
				} else {
					sb.append("\n and "); 
				}
				sb.append(getEncapsulatedName(field.getName()));
				sb.append("=?"); 
			}
		}
		return sb.toString();
	}

	public SQLStatement buildPSUpdateSQLStatement(SQLTable table, boolean fullName) {
    	setupEnclosureChar(table);
		final SQLStatement sqlPs = new SQLStatement();
		sqlPs.setPrepared(true);
		SQLPSParam psParam = null;
		final StringBuilder sb = new StringBuilder();
		sb.append("update ");
		if (fullName) {
			sb.append(getEncapsulatedName(table.getAbsoluteName()));
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append("\nset "); 
		SQLField field = null;
		boolean firstLoop = true;
		int paramIndex = 0;
		boolean hasSetField = false;
		boolean hasPrimaryKey = false;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.isPrimaryKey() == false) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",\n    "); 
				}
				paramIndex++;
				sb.append(getEncapsulatedName(field.getName()));
				sb.append("=?"); 
				psParam = new SQLPSParam();
				psParam.setName(field.getName());
				psParam.setIndex(paramIndex);
				psParam.setBasicType(field.getBasicType());
				sqlPs.addParam(psParam);
				hasSetField = true;
			}
		}
		firstLoop = true;
		for (int i = 0; i < table.getFieldCount(); i++) {
			field = table.getFieldAt(i);
			if (field.isPrimaryKey()) {
				if (firstLoop) {
            		sb.append("\nwhere "); 
					firstLoop = false;
				} else {
					sb.append(" and \n      "); 
				}
				paramIndex++;
				sb.append(getEncapsulatedName(field.getName()));
				sb.append("=?"); 
				psParam = new SQLPSParam();
				psParam.setName(field.getName());
				psParam.setIndex(paramIndex);
				psParam.setBasicType(field.getBasicType());
				sqlPs.addParam(psParam);
				hasPrimaryKey = true;
			}
		}
		sqlPs.setSqlCodeValid(hasSetField && hasPrimaryKey);
		sqlPs.setSQL(sb.toString());
		return sqlPs;
	}

	public String buildCreateTableStatements(SQLSchema schema, boolean fullName) {
        if (schema != null) {
        	setupEnclosureChar(schema);
            List<SQLTable> listSortedTables = getTablesSortedByReference(schema);
            final StringBuilder text = new StringBuilder();
            String ctString;
            for (SQLTable sqlTable : listSortedTables) {
                if (sqlTable.isTable()) {
                    ctString = buildCreateStatement(sqlTable, fullName);
                    if (ctString != null) {
                        text.append(ctString);
                        if (ctString.trim().endsWith(";") == false) {
                        	text.append(";");
                        }
                        text.append("\n\n");
                    }
                }
            }
            for (SQLTable sqlTable : listSortedTables) {
            	if (sqlTable.isView()) {
                	ctString = buildCreateStatement(sqlTable, fullName);
                    if (ctString.trim().endsWith(";") == false) {
                    	text.append(";");
                    }
                    text.append("\n\n");
                }
            }
            return text.toString();
        } else {
            return null;
        }
	}

	public String buildDropTableStatements(
			SQLSchema schema,
			boolean withSchemaName) {
        if (schema != null) {
        	setupEnclosureChar(schema);
            List<SQLTable> listSortedTables = getTablesSortedByReference(schema);
            final StringBuilder text = new StringBuilder();
            String dtString;
            SQLTable sqlTable = null;
            for (int x = listSortedTables.size() - 1; x >= 0; x--) {
                sqlTable = listSortedTables.get(x);
                if (sqlTable.isView()) {
                    dtString = buildDropStatement(sqlTable, withSchemaName);
                    if (dtString != null) {
                    	text.append(dtString);
                        text.append(";\n");
                    }
                }
            }
            text.append("\n");
            for (int x = listSortedTables.size() - 1; x >= 0; x--) {
                sqlTable = listSortedTables.get(x);
                if (sqlTable.isTable()) {
                    dtString = buildDropStatement(sqlTable, withSchemaName);
                    if (dtString != null) {
                    	text.append(dtString);
                        text.append(";\n");
                    }
                }
            }
            return text.toString();
        } else {
            return null;
        }
	}
	
    public String buildDropStatement(SQLTable table, boolean withSchemaName) {
    	return buildDropStatement(table, withSchemaName, null);
    }
    
    public String buildDropStatement(SQLTable table, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(table);
        if (table.isTable()) {
            if (withSchemaName) {
            	if (alternativeSchemaName != null) {
                    return "drop table " + getEncapsulatedName(alternativeSchemaName + "." + table.getName());
            	} else {
                    return "drop table " + getEncapsulatedName(table.getAbsoluteName());
            	}
            } else {
                return "drop table " + getEncapsulatedName(table.getName());
            }
        } else if (table.isMaterializedView()) {
            if (withSchemaName) {
            	if (alternativeSchemaName != null) {
                    return "drop materialized view " + getEncapsulatedName(alternativeSchemaName + "." + table.getName());
            	} else {
                    return "drop materialized view " + getEncapsulatedName(table.getAbsoluteName());
            	}
            } else {
                return "drop materialized view " + getEncapsulatedName(table.getName());
            }
        } else if (table.isView()) {
            if (withSchemaName) {
            	if (alternativeSchemaName != null) {
                    return "drop view " + getEncapsulatedName(alternativeSchemaName + "." + table.getName());
            	} else {
                    return "drop view " + getEncapsulatedName(table.getAbsoluteName());
            	}
            } else {
                return "drop view " + getEncapsulatedName(table.getName());
            }
        } else {
            return null;
        }
    }

    public String buildDropStatement(SQLField field, boolean withSchemaName) {
    	return buildDropStatement(field, withSchemaName, null);
    }	
    	
    public String buildDropStatement(SQLField field, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(field);
    	SQLTable table = field.getSQLTable();
    	if (table.isTable()) {
    		StringBuilder sb = new StringBuilder(64);
    		sb.append("alter table ");
    		if (withSchemaName) {
    			if (alternativeSchemaName != null) {
    				sb.append(getEncapsulatedName(alternativeSchemaName));
    				sb.append(".");
        			sb.append(getEncapsulatedName(table.getName()));
    			} else {
        			sb.append(getEncapsulatedName(table.getAbsoluteName()));
    			}
    		} else {
    			sb.append(getEncapsulatedName(table.getName()));
    		}
    		sb.append(" drop column ");
    		sb.append(getEncapsulatedName(field.getName()));
    		return sb.toString();
    	} else {
    		return "";
    	}
    }
    
    public String buildCreateStatement(SQLField field, boolean withSchemaName) {
    	return buildCreateStatement(field, withSchemaName, null);
    }
    
    public String buildCreateStatement(SQLField field, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(field);
    	SQLTable table = field.getSQLTable();
    	if (table.isTable()) {
    		StringBuilder sb = new StringBuilder(64);
    		sb.append("alter table ");
    		if (withSchemaName) {
    			if (alternativeSchemaName != null) {
    				sb.append(getEncapsulatedName(alternativeSchemaName));
    				sb.append(".");
        			sb.append(getEncapsulatedName(table.getName()));
    			} else {
        			sb.append(getEncapsulatedName(table.getAbsoluteName()));
    			}
    		} else {
    			sb.append(getEncapsulatedName(table.getName()));
    		}
    		sb.append(" add ");
    		sb.append(buildFieldDeclaration(field));
            if (!field.isNullValueAllowed()) {
                sb.append(" not null");
            }
    		return sb.toString();
    	} else {
    		return "";
    	}
    }
    
    public String buildAlterStatement(SQLField field, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(field);
    	SQLTable table = field.getSQLTable();
    	if (table.isTable()) {
    		StringBuilder sb = new StringBuilder(64);
    		sb.append("alter table ");
    		if (withSchemaName) {
    			if (alternativeSchemaName != null) {
    				sb.append(getEncapsulatedName(alternativeSchemaName));
    				sb.append(".");
        			sb.append(getEncapsulatedName(table.getName()));
    			} else {
        			sb.append(getEncapsulatedName(table.getAbsoluteName()));
    			}
    		} else {
    			sb.append(getEncapsulatedName(table.getName()));
    		}
    		sb.append(" alter column ");
        	setupEnclosureChar(field);
            sb.append(getEncapsulatedName(field.getName()));
            sb.append(" type ");
            sb.append(getFieldType(field));
            if (!field.isNullValueAllowed()) {
                sb.append(" not null");
            }
    		return sb.toString();
    	} else {
    		return "";
    	}
    }

    public String buildAddToTableStatement(SQLConstraint constraint, boolean withSchemaName) {
    	return buildAddToTableStatement(constraint, withSchemaName, null);
    }
    
    public String buildAddToTableStatement(SQLConstraint constraint, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(constraint);
		StringBuilder sb = new StringBuilder(64);
		sb.append("alter table ");
		SQLTable table = constraint.getTable();
		if (withSchemaName) {
			if (alternativeSchemaName != null) {
				sb.append(getEncapsulatedName(alternativeSchemaName));
				sb.append(".");
				sb.append(getEncapsulatedName(table.getName()));
			} else {
				sb.append(getEncapsulatedName(table.getAbsoluteName()));
			}
		} else {
			sb.append(table.getName());
		}
		sb.append(" add ");
		sb.append(buildConstraintSQLCode(constraint, withSchemaName, alternativeSchemaName));
		return sb.toString();
    }
    
    public String buildConstraintSQLCode(SQLConstraint constraint, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(constraint);
        if (constraint.getType() == SQLConstraint.PRIMARY_KEY) {
            StringBuilder sb = new StringBuilder();
            if (constraint.getName().equalsIgnoreCase("primary") == false) {
                sb.append("constraint ");
                sb.append(getEncapsulatedName(constraint.getName()));
                sb.append(" ");
            }
            sb.append("primary key (");
            for (int i = 0; i < constraint.getPkColumnList().size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(getEncapsulatedName(constraint.getPkColumnList().get(i).getPkColumnName()));
            }
            sb.append(')');
            return sb.toString();
        } else if (constraint.getType() == SQLConstraint.FOREIGN_KEY && constraint.getReferencedTable() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("constraint ");
            sb.append(getEncapsulatedName(constraint.getName()));
            sb.append(" foreign key (");
            for (int i = 0; i < constraint.getFkColumnPairList().size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(getEncapsulatedName(constraint.getFkColumnPairList().get(i).getFkColumnName()));
            }
            sb.append(") references ");
            if (withSchemaName) {
            	if (alternativeSchemaName != null) {
            		sb.append(alternativeSchemaName);
            		sb.append(".");
            		sb.append(getEncapsulatedName(constraint.getReferencedTable().getName()));
            	} else {
                    sb.append(getEncapsulatedName(constraint.getReferencedTable().getAbsoluteName()));
            	}
            } else {
                sb.append(getEncapsulatedName(constraint.getReferencedTable().getName()));
            }
            sb.append("(");
            for (int i = 0; i < constraint.getFkColumnPairList().size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(getEncapsulatedName(constraint.getFkColumnPairList().get(i).getPkColumnName()));
            }
            sb.append(")");
            return sb.toString();
        } else if (constraint.getType() == SQLConstraint.UNIQUE_KEY) {
            StringBuilder sb = new StringBuilder();
            sb.append("constraint ");
            sb.append(getEncapsulatedName(constraint.getName()));
            sb.append(" unique (");
            for (int i = 0; i < constraint.getPkColumnList().size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(getEncapsulatedName(constraint.getPkColumnList().get(i).getPkColumnName()));
            }
            sb.append(')');
            return sb.toString();
        } else {
            return "";
        }
    }
    
    public String buildSetNotNullConstraint(SQLField field, boolean withSchemaName) {
    	return buildSetNotNullConstraint(field, withSchemaName, null);
    }
    
    public String buildSetNotNullConstraint(SQLField field, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(field);
		StringBuilder sb = new StringBuilder(64);
		sb.append("alter table ");
		SQLTable table = field.getSQLTable();
		if (withSchemaName) {
			if (alternativeSchemaName != null) {
				sb.append(getEncapsulatedName(alternativeSchemaName));
				sb.append(".");
				sb.append(getEncapsulatedName(table.getName()));
			} else {
				sb.append(getEncapsulatedName(table.getAbsoluteName()));
			}
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append(" alter column ");
		sb.append(getEncapsulatedName(field.getName()));
		sb.append(" set not null");
		return sb.toString();
    }
    
    public String buildDropNotNullConstraint(SQLField field, boolean withSchemaName) {
    	return buildDropNotNullConstraint(field, withSchemaName, null);
    }
    
    public String buildDropNotNullConstraint(SQLField field, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(field);
		StringBuilder sb = new StringBuilder(64);
		sb.append("alter table ");
		SQLTable table = field.getSQLTable();
		if (withSchemaName) {
			if (alternativeSchemaName != null) {
				sb.append(getEncapsulatedName(alternativeSchemaName));
				sb.append(".");
				sb.append(getEncapsulatedName(table.getName()));
			} else {
				sb.append(getEncapsulatedName(table.getAbsoluteName()));
			}
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append(" alter column ");
		sb.append(getEncapsulatedName(field.getName()));
		sb.append(" drop not null");
		return sb.toString();
    }

    public String buildDropStatement(SQLConstraint constraint, boolean withSchemaName) {
    	return buildDropStatement(constraint, withSchemaName, null);
    }
    
    public String buildDropStatement(SQLConstraint constraint, boolean withSchemaName, String alternativeSchemaName) {
    	setupEnclosureChar(constraint);
		StringBuilder sb = new StringBuilder(64);
		sb.append("alter table ");
		SQLTable table = constraint.getTable();
		if (withSchemaName) {
			if (alternativeSchemaName != null) {
				sb.append(getEncapsulatedName(alternativeSchemaName));
				sb.append(".");
				sb.append(getEncapsulatedName(table.getName()));
			} else {
				sb.append(getEncapsulatedName(table.getAbsoluteName()));
			}
		} else {
			sb.append(getEncapsulatedName(table.getName()));
		}
		sb.append(" drop constraint ");
		sb.append(getEncapsulatedName(constraint.getName()));
		return sb.toString();
    }

    public String buildDeleteStatement(SQLTable table, boolean withSchemaName) {
    	setupEnclosureChar(table);
        if (table.isTable()) {
            if (withSchemaName) {
                return "delete from " + getEncapsulatedName(table.getAbsoluteName());
            } else {
                return "delete from " + getEncapsulatedName(table.getName());
            }
        } else {
            return null;
        }
    }

	public String buildDeleteTableStatements(
			SQLSchema schema,
			boolean withSchemaName) {
        if (schema != null) {
        	setupEnclosureChar(schema);
            List<SQLTable> listSortedTables = getTablesSortedByReference(schema);
            final StringBuilder text = new StringBuilder();
            String dtString;
            SQLTable sqlTable = null;
            for (int x = listSortedTables.size() - 1; x >= 0; x--) {
                sqlTable = listSortedTables.get(x);
                dtString = buildDeleteStatement(sqlTable, withSchemaName);
                if (dtString != null) {
                    text.append(dtString);
                    text.append(";\n");
                }
            }
            return text.toString();
        } else {
            return null;
        }
	}
	
	public String buildPreparedCallStatement(SQLProcedure p, boolean fullName) {
    	setupEnclosureChar(p);
		final StringBuilder sb = new StringBuilder();
		sb.append("{");
		// check if it is a function 
		if (p.isFunction()) {
			sb.append("? = ");
		}
		sb.append("call ");
		if (fullName) {
			sb.append(p.getAbsoluteName());
		} else {
			sb.append(p.getName());
		}
		sb.append("(");
		for (int i = 0; i < p.getParameterCount(); i++) {
			Parameter param = p.getParameterAt(i);
			if (i > 0) {
				sb.append(",");
			}
			sb.append("?/*#");
			sb.append(i + 1);
			sb.append(" ");
			sb.append(param.getTypeName());
			sb.append(" ");
			sb.append(param.getName());
			if (param.getIoType() == DatabaseMetaData.functionColumnInOut) {
				sb.append("[io]");
			} else if (param.getIoType() == DatabaseMetaData.functionColumnOut) {
				sb.append("[o]");
			}
			sb.append("*/");
		}
		sb.append(")}");
		return sb.toString();
	}
	
	public String buildSQLCallStatement(SQLProcedure p, boolean fullName) {
    	setupEnclosureChar(p);
		final StringBuilder sb = new StringBuilder();
		if (fullName) {
			sb.append(p.getAbsoluteName());
		} else {
			sb.append(p.getName());
		}
		boolean firstLoop = true;
		sb.append("(");
		for (int i = 0; i < p.getParameterCount(); i++) {
			Parameter param = p.getParameterAt(i);
			if (param.isSingleReturnValue() == false) {
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",");
				}
				sb.append(param.getName());
				sb.append("/*");
				sb.append(param.getTypeName());
				if (param.getIoType() == DatabaseMetaData.functionColumnInOut) {
					sb.append("[io]");
				} else if (param.getIoType() == DatabaseMetaData.functionColumnOut) {
					sb.append("[o]");
				}
				sb.append("*/");
			}
		}
		sb.append(")");
		return sb.toString();
	}

	public String buildDropStatement(SQLProcedure p, boolean fullName) {
    	setupEnclosureChar(p);
		final StringBuilder sb = new StringBuilder();
		sb.append("drop ");
		if (p.isFunction()) {
			sb.append("function ");
		} else {
			sb.append("procedure ");
		}
		if (fullName) {
			sb.append(p.getAbsoluteName());
		} else {
			sb.append(p.getName());
		}
		sb.append("(");
		for (int i = 0; i < p.getParameterCount(); i++) {
			if (i > 0) {
				sb.append(",");
			}
			Parameter pa = p.getParameterAt(i);
			sb.append(pa.getTypeName());
		}
		sb.append(")");
		return sb.toString();
	}

	public String buildDropStatement(SQLSequence p, boolean fullName) {
    	setupEnclosureChar(p);
		final StringBuilder sb = new StringBuilder();
		sb.append("drop sequence ");
		if (fullName) {
			sb.append(p.getAbsoluteName());
		} else {
			sb.append(p.getName());
		}
		return sb.toString();
	}

	public static void completeTableAndColumns(SQLSchema schema) {
		SQLTable sqlTable = null;
		for (int x = 0; x < schema.getTableCount(); x++) {
			sqlTable = schema.getTableAt(x);
			// sicherstellen, dass die Felder geladen sind, sonst
			// funktioniert der Vergleich der Tabellen nicht
			if (sqlTable.isFieldsLoaded() == false) {
				sqlTable.loadColumns();
			}
		}
	}

    public static List<SQLTable> getTablesSortedByReference(SQLSchema schema) {
		completeTableAndColumns(schema);
		List<SQLTable> schemaTables = new ArrayList<SQLTable>();
		for (int i = 0; i < schema.getTableCount(); i++) {
            SQLTable table = schema.getTableAt(i);
			schemaTables.add(table);
		}
		return sortByForeignKeys(schemaTables);
    }

    public static String convertJavaToSqlCode(String javaCode) {
        final StringReplacer sr = new StringReplacer(javaCode);
        sr.replace("\r", "");
        sr.replace("\t", " ");
        sr.replace("\\n", "");
        javaCode = sr.getResultText();
        final StringBuilder sb = new StringBuilder(javaCode.length());
        // Zeile für Zeile verpacken in doppelte Hochkomma
        char c;
        char c1 = ' ';
        boolean inString = false;
        boolean inSqlCode = false;
        boolean masked = false;
        for (int i = 0; i < javaCode.length(); i++) {
            c = javaCode.charAt(i);
            if (i < javaCode.length() - 1) {
                c1 = javaCode.charAt(i + 1);
            }
            if (inSqlCode) {
                // innerhalb des SQL-Codes darf ein Hochkomma nicht als Java-Code interpretiert werden
                if (inString) {
                    if (c == '\'') {
                        inString = false;
                    }
                } else {
                    if (c == '\'') {
                        inString = true;
                    }
                }
                if (c == '\"') {
                    if (!masked) {
                        // nur wenn KEINE Maskierung
                        inSqlCode = false;
                        if (inString) {
                            inString = false;
                        } else {
                            // den Zeilenumbruch nicht innerhalb eines Strings
                            sb.append('\n');
                        }
                    } else {
                        masked = false;
                        sb.append(c);
                    }
                } else if (c == '\\') {
                    // maskierung gefunden
                    // wenn nachfolgend ein Doppeltes Hochkomma oder Backslash,
                    // dann die maskierung nicht schreiben
                    if (c1 == '\"' || c1 == '\\') {
                        // maskiertes hochkomma gefunden
                        masked = true;
                    } else {
                        masked = false;
                        sb.append(c);
                    }
                } else {
                    masked = false;
                    sb.append(c);
                }
            } else {
                // wenn nicht in SQL-Code dann Zeichen verwerfen
                // und auf Beginn des SQL-Codes testen
                if (c == '\"') {
                    // ok gefunden
                    inSqlCode = true;
                }
            }
        }
        return sb.toString();
    }

    public static String convertSqlToJavaString(String sql) {
        // alle bereits enthaltenen doppelten Hochkomma als Escape-Sequenz umsetzen
        final StringReplacer sr = new StringReplacer(sql);
        sr.replace("\"", "\\\"");
        sql = sr.getResultText().trim();
        final StringBuilder sb = new StringBuilder(sql.length());
        // Zeile für Zeile verpacken in doppelte Hochkomma
        sb.append("\""); // erstes Hochkomma
        char c = ' ';
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            c = sql.charAt(i);
            if (!inString) {
                if (c == '\'') {
                    sb.append(c);
                    inString = true;
                } else if (c == '\n') {
                    if (i < sql.length() - 1) {
                        // nach dem Zeilenumbruch ein Hochkomma einfügen
                        sb.append("\\n\"\n    + \"");
                    } else {
                        sb.append("\"\n");
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '\\') {
                    sb.append('\\'); // Backslash maskieren
                } else if (c == '\'') {
                    inString = false;
                }
                sb.append(c);
            }
            if (i == sql.length() - 1) {
                sb.append("\"");
            }
        }
        return sb.toString();
    }

    public static String convertSQLToDynamicSQLString(String sql) {
        final StringReplacer sr = new StringReplacer(sql);
        sql = sr.getResultText().trim();
        final StringBuilder sb = new StringBuilder(sql.length());
        sb.append("'");
        char c = ' ';
        char c1 = ' ';
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            c = sql.charAt(i);
            if (i < sql.length() - 1) {
                c1 = sql.charAt(i + 1);
            }
            if (inString == false) {
                if (c == '\'') {
                    sb.append("''");
                    inString = true;
                } else if (c == '\n') {
                    if (i < sql.length() - 1) {
                        sb.append("'\n ||' ");
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '\'') {
                    sb.append("''");
                    if (c1 != '\'') {
                    	inString = false;
                    }
                } else {
                    sb.append(c);
                }
            }
        }
        sb.append("'");
        return sb.toString();
    }

    public static String convertSqlToJavaStringBuffer(String sql, String stringBufferVarName) {
        // alle bereits enthaltenen doppelten Hochkomma als Escape-Sequenz umsetzen
        final StringReplacer sr = new StringReplacer(sql);
        sr.replace("\"", "\\\"");
        sql = sr.getResultText().trim();
        final StringBuilder sb = new StringBuilder(sql.length());
        // Zeile für Zeile verpacken in doppelte Hochkomma
        sb.append("StringBuilder ");
        sb.append(stringBufferVarName);
        sb.append(" = new StringBuilder();\n");
        sb.append(stringBufferVarName);
        sb.append(".append(\"");
        char c = ' ';
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            c = sql.charAt(i);
            if (!inString) {
                if (c == '\'') {
                    sb.append(c);
                    inString = true;
                } else if (c == '\n') {
                    if (i < sql.length() - 1) {
                        // nach dem Zeilenumbruch ein Hochkomma einfügen
                        sb.append("\\n\");\n");
                        sb.append(stringBufferVarName);
                        sb.append(".append(\"");
                    } else {
                        sb.append("\");\n");
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '\\') {
                    sb.append('\\'); // Backslash maskieren
                } else if (c == '\'') {
                    inString = false;
                }
                sb.append(c);
            }
            if (i == sql.length() - 1) {
                sb.append("\");\n");
            }
        }
        return sb.toString();
    }
    
    public String buildSchemaUpdateStatements(ModelComparator comparator) {
    	String alternativeSchemaName = comparator.getTargetSchema().getName();
    	StringBuilder sb = new StringBuilder(1024);
    	// tables
    	List<SQLTable> listTablesToAdd = comparator.getTablesToAdd();
    	if (listTablesToAdd.size() > 0) {
    		sb.append("-- ############ Tables to add ############\n");
        	listTablesToAdd = sortByForeignKeys(listTablesToAdd);
        	for (SQLTable table : listTablesToAdd) {
        		String sql = buildCreateStatement(table, true, alternativeSchemaName, false, false, false);
        		sb.append(sql);
        		if (sql.endsWith(";\n") || sql.endsWith(";")) {
            		sb.append("\n");
        		} else {
            		sb.append(";\n\n");
        		}
        	}
    	}
    	List<SQLTable> listTablesToRemove = comparator.getTablesToRemove();
    	if (listTablesToRemove.size() > 0) {
    		sb.append("\n\n-- ########## Tables to remove ############\n");
    		listTablesToRemove = sortByForeignKeys(listTablesToRemove);
        	for (int i = listTablesToRemove.size() - 1; i >= 0; i--) {
        		SQLTable table = listTablesToRemove.get(i);
        		sb.append(buildDropStatement(table, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	// fields
    	List<SQLField> listFieldsToAdd = comparator.getFieldsToAdd();
    	if (listFieldsToAdd.size() > 0) {
    		sb.append("\n\n-- ############ Columns to add ############\n");
        	for (SQLField field : listFieldsToAdd) {
        		sb.append(buildCreateStatement(field, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLField> listFieldsToRemove = comparator.getFieldsToRemove();
    	if (listFieldsToRemove.size() > 0) {
    		sb.append("\n\n-- ############ Columns to remove ############\n");
        	for (int i = listFieldsToRemove.size() - 1; i >= 0; i--) {
        		SQLField field = listFieldsToRemove.get(i);
        		sb.append(buildDropStatement(field, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLField> listFieldsToChange = comparator.getFieldsToChange();
    	if (listFieldsToChange.size() > 0) {
    		sb.append("\n\n-- ############ Columns to change ############\n");
        	for (SQLField field : listFieldsToChange) {
        		sb.append(buildAlterStatement(field, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLFieldNotNullConstraint> listNncToAdd = comparator.getNotNullsToAdd();
    	if (listNncToAdd.size() > 0) {
    		sb.append("\n\n-- ############ Columns Not Null Constraints to add ############\n");
        	for (SQLFieldNotNullConstraint nnc : listNncToAdd) {
        		sb.append(buildSetNotNullConstraint(nnc.getField(), true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLFieldNotNullConstraint> listNncToRemove = comparator.getNotNullsToRemove();
    	if (listNncToRemove.size() > 0) {
    		sb.append("\n\n-- ############ Columns Not Null Constraints to remove ############\n");
        	for (SQLFieldNotNullConstraint nnc : listNncToRemove) {
        		sb.append(buildDropNotNullConstraint(nnc.getField(), true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLConstraint> listConstraintsToAdd = comparator.getConstraintsToAdd();
    	if (listConstraintsToAdd.size() > 0) {
    		sb.append("\n\n-- ############ Constraints to add ############\n");
        	for (SQLConstraint constr : listConstraintsToAdd) {
        		sb.append(buildAddToTableStatement(constr, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLConstraint> listConstraintsToRemove = comparator.getConstraintsToRemove();
    	if (listConstraintsToRemove.size() > 0) {
    		sb.append("\n\n-- ############ Constraints to remove ############\n");
        	for (int i = listConstraintsToRemove.size() - 1; i >= 0; i--) {
        		SQLConstraint constr = listConstraintsToRemove.get(i);
        		sb.append(buildDropStatement(constr, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLConstraint> listConstraintsToChange = comparator.getConstraintsToChange();
    	if (listConstraintsToChange.size() > 0) {
    		sb.append("\n\n-- ############ Constraints to change ############\n");
        	for (SQLConstraint constr : listConstraintsToChange) {
        		sb.append(buildDropStatement(constr, true, alternativeSchemaName));
        		sb.append(";\n");
        		sb.append(buildAddToTableStatement(constr, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLIndex> listIndicesToAdd = comparator.getIndicesToAdd();
    	if (listIndicesToAdd.size() > 0) {
    		sb.append("\n\n-- ############ Indices to add ############\n");
        	for (SQLIndex index : listIndicesToAdd) {
        		sb.append(buildCreateStatement(index, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLIndex> listIndicesToRemove = comparator.getIndicesToRemove();
		if (listIndicesToRemove.size() > 0) {
			sb.append("\n\n-- ############ Indices to remove ############\n");
	    	for (SQLIndex index : listIndicesToRemove) {
	    		sb.append(buildDropStatement(index, true, alternativeSchemaName));
	    		sb.append(";\n");
	    	}
		}
    	List<SQLIndex> listIndicesToChange = comparator.getIndicesToChange();
    	if (listIndicesToChange.size() > 0) {
    		sb.append("\n\n-- ############ Indices to change ############\n");
        	for (SQLIndex index : listIndicesToChange) {
        		sb.append(buildDropStatement(index, true, alternativeSchemaName));
        		sb.append(";\n");
        		sb.append(buildCreateStatement(index, true, alternativeSchemaName));
        		sb.append(";\n");
        	}
    	}
    	List<SQLProcedure> listProceduresToRemove = comparator.getProceduresToRemove();
    	if (listProceduresToRemove.isEmpty() == false) {
    		sb.append("\n\n-- ############ Procedures to remove ############\n");
    		for (SQLProcedure p : listProceduresToRemove) {
    			sb.append(buildDropStatement(p, true));
        		sb.append(";");
        		sb.append("\n/\n\n");
    		}
    	}
    	List<SQLProcedure> listProceduresToAdd = comparator.getProceduresToAdd();
    	if (listProceduresToAdd.isEmpty() == false) {
    		sb.append("\n\n-- ############ Procedures to add ############\n");
    		for (SQLProcedure p : listProceduresToAdd) {
    			sb.append(buildCreateStatement(p, true, alternativeSchemaName));
        		sb.append(";");
        		sb.append("\n/\n\n");
    		}
    	}
    	List<SQLProcedure> listProceduresToChange = comparator.getProceduresToChange();
    	if (listProceduresToChange.isEmpty() == false) {
    		sb.append("\n\n-- ############ Procedures to change ############\n");
    		for (SQLProcedure p : listProceduresToChange) {
    			sb.append(buildDropStatement(p, true));
        		sb.append(";\n");
    			sb.append(buildCreateStatement(p, true, alternativeSchemaName));
        		sb.append(";");
        		sb.append("\n/\n\n");
    		}
    	}
    	List<SQLSequence> listSequencesToRemove = comparator.getSequencesToRemove();
    	if (listSequencesToRemove.isEmpty() == false) {
    		sb.append("\n\n-- ############ Sequences to remove ############\n");
    		for (SQLSequence seq : listSequencesToRemove) {
    			sb.append(buildDropStatement(seq, true));
        		sb.append(";\n");
    		}
    	}
    	List<SQLSequence> listSequencesToAdd = comparator.getSequencesToAdd();
    	if (listSequencesToAdd.isEmpty() == false) {
    		sb.append("\n\n-- ############ Sequences to add ############\n");
    		for (SQLSequence seq : listSequencesToAdd) {
    			sb.append(buildCreateStatement(seq, true, alternativeSchemaName));
        		sb.append(";\n");
    		}
    	}
    	return sb.toString();
    }
    
    public static List<SQLTable> sortByForeignKeys(List<SQLTable> unsortedTables) {
    	List<SQLTable> sortedList = new ArrayList<SQLTable>();
    	for (SQLTable t : unsortedTables) {
    		sortByForeignKeys(unsortedTables, t, sortedList);
    	}
    	return sortedList;
    }

    protected static void sortByForeignKeys(List<SQLTable> unsortedTables, SQLTable table, List<SQLTable> sortedList) {
    	List<SQLTable> currentList = new ArrayList<SQLTable>();
    	sortByForeignKeys(unsortedTables, table, sortedList, currentList);
    }
    
    protected static void sortByForeignKeys(List<SQLTable> unsortedTables, SQLTable table, List<SQLTable> sortedList, List<SQLTable> currentList) {
    	if (currentList.contains(table) == false) {
    		currentList.add(table);
        	List<SQLTable> referencedTables = table.getReferencedTables();
        	for (SQLTable rt : referencedTables) {
        		if (table.equals(rt) == false && unsortedTables.contains(table)) {
        			// ignore self referencing tables
    	    		sortByForeignKeys(unsortedTables, rt, sortedList, currentList);
        		}
        	}
    	}
    	if (sortedList.contains(table) == false && unsortedTables.contains(table)) {
        	sortedList.add(table);
    	}
    }

}
