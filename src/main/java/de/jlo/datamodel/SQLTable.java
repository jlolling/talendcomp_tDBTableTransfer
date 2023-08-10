package de.jlo.datamodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Represents a table within a database schema
 * 
 * @author <a href="jan.lolling@gmail.com>Jan Lolling</a>
 *
 */
public final class SQLTable extends SQLObject {

	private final List<SQLField> listColumns = new ArrayList<SQLField>();
	private List<SQLIndex> listIndexes = new ArrayList<SQLIndex>();
    private final Map<String, SQLField> fieldMap = new HashMap<String, SQLField>();
    private SQLSchema schema;
    private String type;
    private String comment;
    private String sourceCode;
    static final int IS_REFERENCED_FROM_TABLE = -1;
    static final int HAS_REFERENCE_TO_TABLE   = 1;
    static final int NO_REFERENCES            = 0;
    static public final String TYPE_TABLE               = "TABLE";
    static public final String TYPE_VIEW                = "VIEW";
    static public final String TYPE_MAT_VIEW            = "MATERIALIZED VIEW";
    static public final String TYPE_SYSTEM_TABLE        = "SYSTEM TABLE";
    static public final String TYPE_SYSTEM_VIEW         = "SYSTEM VIEW";
    static public final String TYPE_GLOBAL_TEMPORARY    = "GLOBAL TEMPORARY";
    static public final String TYPE_LOCAL_TEMPORARY     = "LOCAL TEMPORARY";
    static public final String TYPE_ALIAS               = "ALIAS";
    static public final String TYPE_SYNONYM             = "SYNONYM";
    static public final String TYPE_SYSTEM              = "SYSTEM";
    private boolean fieldsLoaded = false;
    private boolean constraintsLoaded = false;
    private boolean indexesLoaded = false;
    private Map<String, SQLConstraint> constraintMap = new HashMap<String, SQLConstraint>();
    private transient SQLConstraint primaryKeyConstraint = null;
    private boolean loadingColumns = false;
    private boolean loadingConstraints = false;
    private boolean loadingIndexes = false;
    private boolean inheritated = false;
    private int countPartitions = 0;

    public SQLTable(SQLDataModel model, SQLSchema schema, String name) {
    	super(model, name);
    	this.schema = schema;
    }
    
    public SQLTable clone() {
    	SQLTable clone = new SQLTable(this.getModel(), this.schema, this.getName());
    	clone.comment = this.comment;
    	clone.constraintMap = this.constraintMap;
    	clone.constraintsLoaded = this.constraintsLoaded;
    	clone.countPartitions = this.countPartitions;
    	// clone fields
    	for (SQLField field : this.listColumns) {
    		SQLField clonedField = field.clone();
    		addField(clonedField);
    	}
    	clone.listIndexes = this.listIndexes; 
    	clone.fieldsLoaded = this.fieldsLoaded;
    	clone.primaryKeyConstraint = this.primaryKeyConstraint;
    	clone.sourceCode = this.sourceCode;
    	clone.type = this.type;
    	return clone;
    }
    
    public void clear() {
    	clearConstraints();
    	clearIndexes();
    	clearFields();
    }

    public Map<String, SQLConstraint> getConstraintMap() {
        return constraintMap;
    }

    public void setConstraintMap(HashMap<String, SQLConstraint> constraintMap) {
        this.constraintMap = constraintMap;
    }

    public SQLConstraint getPrimaryKeyConstraint() {
        return primaryKeyConstraint;
    }

    public void setPrimaryKeyConstraint(SQLConstraint primaryKeyConstraint) {
        this.primaryKeyConstraint = primaryKeyConstraint;
        addConstraint(primaryKeyConstraint);
    }
    
    public void clearConstraints() {
        constraintMap.clear();
        primaryKeyConstraint = null;
        constraintsLoaded = false;
        for (SQLField f : listColumns) {
        	f.setPrimaryKey(false);
        }
    }
    
    public int getCountConstraints() {
    	return constraintMap.size();
    }
    
    public List<SQLConstraint> getConstraints() {
        List<SQLConstraint> list = new ArrayList<SQLConstraint>();
        for (Iterator<Map.Entry<String, SQLConstraint>> it = constraintMap.entrySet().iterator(); it.hasNext(); ) {
            list.add(it.next().getValue());
        }
        return list;
    }
    
    public List<SQLIndex> getIndexes() {
        List<SQLIndex> list = new ArrayList<SQLIndex>();
        for (SQLIndex index : listIndexes) {
            list.add(index);
        }
        return list;
    }
    
    public int getCountIndexes() {
    	return listIndexes.size();
    }

    public int countConstraints() {
        return constraintMap.size();
    }

    public void addConstraint(SQLConstraint constraint) {
        constraintMap.put(constraint.getName(), constraint);
    }
    
    public void removeConstraint(SQLConstraint constraint) {
        constraintMap.remove(constraint.getName());
    }
    
    public SQLConstraint getConstraint(String name) {
        return constraintMap.get(name);
    }
    
    public boolean isConstraintsLoadFinished() {
        return constraintsLoaded;
    }
    
    public boolean isSourceCodeLoaded() {
    	return sourceCode != null;
    }
    
    void setConstraintsLoaded() {
        constraintsLoaded = true;
    }

    public boolean isFieldsLoaded() {
        return fieldsLoaded;
    }
    
    public void setFieldsLoaded() {
        fieldsLoaded = true;
    }
    
    void setIndexesLoaded() {
    	indexesLoaded = true;
    }
    
    public boolean isIndexesLoadFinished() {
    	return indexesLoaded;
    }
    
    public String getAbsoluteName() {
    	if (schema.getName().isEmpty() == false) {
            return schema.getName() + "." + getName();
    	} else {
            return getName();
    	}
    }

    public void setSchema(SQLSchema schema_loc) {
        this.schema = schema_loc;
    }

    public SQLSchema getSchema() {
        return schema;
    }

    public void addIndex(SQLIndex index) {
    	if (listIndexes.contains(index) == false) {
    		listIndexes.add(index);
    	}
    }
    
    public int countIndexes() {
    	return listIndexes.size();
    }
    
    public SQLIndex getIndexAt(int listPos) {
    	return listIndexes.get(listPos);
    }
    
    public SQLIndex getIndexByName(String indexName) {
    	for (SQLIndex index : listIndexes) {
    		if (index.getName().equalsIgnoreCase(indexName)) {
    			return index;
    		}
    	}
    	return null;
    }
    
    public void addField(SQLField field) {
        listColumns.add(field);
        fieldMap.put(field.getName().toLowerCase(), field);
    }
    
    public void removeSQLField(SQLField field) {
        listColumns.remove(field);
        fieldMap.remove(field.getName().toLowerCase());
    }
    
    public void clearFields() {
        listColumns.clear();
        fieldMap.clear();
        fieldsLoaded = false;
    }
    
    public void clearIndexes() {
    	listIndexes.clear();
    	indexesLoaded = false;
    }
    
    public int getFieldCount() {
    	if (loadingColumns) {
    		return 0;
    	}
    	if (fieldsLoaded == false) {
            loadColumns();
    	}
        return listColumns.size();
    }
    
    public SQLField getFieldAt(int index) {
        if (fieldsLoaded == false) {
            loadColumns();
        }
        if (loadingColumns) {
        	return null;
        } else {
            return listColumns.get(index);
        }
    }

    public SQLField getField(String name) {
        if (fieldsLoaded == false) {
            loadColumns();
        }
        SQLField field = null;
        for (int i = 0; i < listColumns.size(); i++) {
            field = listColumns.get(i);
            if (name.equalsIgnoreCase(field.getName())) {
                break;
            } else {
                field = null;
            }
        }
        return field;
    }
    
    public List<String> getPrimaryKeyFieldNames() {
        if (fieldsLoaded == false) {
            loadColumns();
        }
    	List<String> names = new ArrayList<>();
    	for (SQLField field : listColumns) {
    		if (field.isPrimaryKey()) {
        		names.add(field.getName());
    		}
    	}
    	return names;
    }
    
    public List<String> getNonPrimaryKeyFieldNames() {
        if (fieldsLoaded == false) {
            loadColumns();
        }
    	List<String> names = new ArrayList<>();
    	for (SQLField field : listColumns) {
    		if (field.isPrimaryKey() == false) {
        		names.add(field.getName());
    		}
    	}
    	return names;
    }

    public List<String> getFieldNames() {
        if (fieldsLoaded == false) {
            loadColumns();
        }
    	List<String> names = new ArrayList<String>();
    	for (SQLField field : listColumns) {
    		names.add(field.getName());
    	}
    	return names;
    }
    
    public boolean isReferencingByForeignKeys() {
    	if (isTable()) {
        	if (constraintMap.isEmpty() == false) {
                for (SQLConstraint constraint : constraintMap.values()) {
                	if (constraint.getType() == SQLConstraint.FOREIGN_KEY) {
            			return true;
                	}
                }
        	}
    	}
    	return false;
    }
    
    public List<SQLTable> getReferencedTables() {
    	List<SQLTable> referencedTables = new ArrayList<SQLTable>();
    	if (isTable()) {
        	if (constraintMap.isEmpty() == false) {
                for (SQLConstraint constraint : constraintMap.values()) {
                	if (constraint.getType() == SQLConstraint.FOREIGN_KEY) {
                		SQLTable rt = constraint.getReferencedTable();
                		referencedTables.add(rt);
                	}
                }
        	}
    	}
    	return referencedTables;
    }

    public void setupPrimaryKeyFieldsByUniqueIndex() {
    	if (hasPrimaryKeyFields() == false) {
    		// we do not have primary key fields
    		// let us check if we have an unique index and use it to mark primary keys
    		if (indexesLoaded == false) {
    			throw new IllegalStateException("Cannot check unique key indices because indices are not loaded.");
    		} else {
    			SQLIndex uniqueIndex = null;
    			for (SQLIndex index : listIndexes) {
    				if (index.isUnique()) {
    					uniqueIndex = index;
    					break;
    				}
    			}
    			if (uniqueIndex != null) {
    				for (int i = 0; i < uniqueIndex.getCountFields(); i++) {
    					String indexFieldName = uniqueIndex.getFieldAt(i).getName();
    					SQLField tableField = getField(indexFieldName);
    					if (tableField == null) {
    						throw new IllegalStateException("Found a field name: " + indexFieldName + " in unique index: " + uniqueIndex.getName() + " which is not known as field of the table!");
    					} else {
    						tableField.setPrimaryKey(true);
    					}
    				}
    			}
    		}
    	}
    }
    
    /**
     * Check if a table is referencing a given table
     * @param table table for testing
     * @return true table is referenced
     */
    public boolean isReferencedTable(SQLTable table) {
    	if (isTable() && table.isTable()) {
        	if (constraintMap.isEmpty() == false) {
                for (SQLConstraint constraint : constraintMap.values()) {
                	if (constraint.getType() == SQLConstraint.FOREIGN_KEY) {
                		SQLTable rt = constraint.getReferencedTable();
                		System.out.print("isReferenced: " + getName() + " rt:" + rt.getName() + " ? " + table.getName());
                		if (rt.equals(table)) {
                			System.out.println(" = true");
                			return true;
                		} else {
                			System.out.println(" = false");
                		}
                	}
                }
        	}
    	} else {
    		if (isView()) {
    			if (sourceCode != null) {
    				if (sourceCode.toLowerCase().indexOf(table.getName().toLowerCase()) > 0) {
    					return true;
    				}
    			}
    		}
    	}
        return false;
    }

    /**
     * testet ob übergebenen Tabelle einen primary key besitzt
     * @return true table is referenced
     */
    public boolean hasPrimaryKeyFields() {
        SQLField field;
        boolean hasPrimaryKeyField = false;
        for (int i = 0; i < listColumns.size(); i++) {
            field = listColumns.get(i);
            if (field.isPrimaryKey()) {
                hasPrimaryKeyField = true;
                break;
            }
        }
        return hasPrimaryKeyField;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (o instanceof SQLTable) {
            isEqual = ((SQLTable) o).getAbsoluteName().equalsIgnoreCase(getAbsoluteName());
        }
        return isEqual;
    }

    /**
     * @return type
     */
    public String getType() {
        return type;
    }

    public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	/**
     * @param string
     */
    public void setType(String string) {
        type = string;
    }

    public void loadColumns() {
        fieldsLoaded = getModel().loadColumns(this);
    }

    public void loadColumns(boolean onlyColumns) {
        fieldsLoaded = getModel().loadColumns(this, onlyColumns);
    }

    @Override
    public int hashCode() {
        return getAbsoluteName().hashCode();
    }
    
    public boolean isTable() {
    	return TYPE_TABLE.equals(type) 
    	|| TYPE_SYSTEM_TABLE.equals(type) 
    	|| TYPE_GLOBAL_TEMPORARY.equals(type) 
    	|| TYPE_LOCAL_TEMPORARY.equals(type) 
    	|| TYPE_SYSTEM.equals(type) 
    	|| TYPE_SYSTEM_TABLE.equals(type) 
    	|| TYPE_ALIAS.equals(type);
    }

    public boolean isView() {
    	return TYPE_VIEW.equals(type) || TYPE_MAT_VIEW.equals(type) || TYPE_SYSTEM_VIEW.equals(type);
    }

    public boolean isMaterializedView() {
    	return TYPE_MAT_VIEW.equals(type);
    }

    public String getSourceCode() {
		return sourceCode;
	}

	public void setSourceCode(String sourceCode) {
		if (sourceCode != null) {
			sourceCode = sourceCode.trim();
		}
		this.sourceCode = sourceCode;
	}

	public boolean isLoadingColumns() {
		return loadingColumns;
	}

	public void setLoadingColumns(boolean loadingColumns) {
		this.loadingColumns = loadingColumns;
	}

	public boolean isLoadingConstraints() {
		return loadingConstraints;
	}

	public void setLoadingConstraints(boolean loadingConstraints) {
		this.loadingConstraints = loadingConstraints;
	}

	public boolean isLoadingIndexes() {
		return loadingIndexes;
	}

	public void setLoadingIndexes(boolean loadingIndexes) {
		this.loadingIndexes = loadingIndexes;
	}
    
	public boolean isInheritated() {
		return inheritated;
	}

	public void setInheritated(boolean inheritated) {
		this.inheritated = inheritated;
	}

	public int getCountPartitions() {
		return countPartitions;
	}

	public void setCountPartitions(int countPartitions) {
		this.countPartitions = countPartitions;
	}
	
}
