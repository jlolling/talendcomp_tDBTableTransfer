package de.jlo.datamodel;

import java.util.ArrayList;

public class SQLConstraint extends SQLObject {
	
	public static final int PRIMARY_KEY = 1;
	public static final int FOREIGN_KEY = 2;
	public static final int UNIQUE_KEY = 4;
	public static final int NOT_NULL = 5;
	private int type = 0;
    // for primary keys
	private ArrayList<PkColumn> pkColumnList = new ArrayList<PkColumn>();
    // for foreign keys
	private ArrayList<FkPkColumnPair> fkColumnPairList = new ArrayList<FkPkColumnPair>();
	private String referencedTableName = null;
	private SQLTable sqlTable = null;

    public SQLTable getTable() {
        return sqlTable;
    }

	public SQLConstraint(SQLDataModel model, SQLTable sqlTable, int type, String name) {
		super(model, name);
        this.sqlTable = sqlTable;
		this.type = type;
	}
	
	public void setReferencedTableName(String tableName) {
		referencedTableName = tableName;
	}
	
	public SQLTable getReferencedTable() {
		return getModel().getSQLTable(referencedTableName);
	}
	
	public void addForeignKeyColumnNamePair(String fkColumnName, String pkColumnName, int index) {
        FkPkColumnPair pair = new FkPkColumnPair();
        pair.setFkColumnName(fkColumnName);
        pair.setPkColumnName(pkColumnName);
        pair.setIndex(index);
		if (fkColumnPairList.contains(pair) == false) {
			fkColumnPairList.add(pair);
		}
	}
	
	public int getImportedKeyFieldListSize() {
		return fkColumnPairList.size();
	}
	
	public String getImportedKeyFieldNameAt(int index) {
        if (index >= fkColumnPairList.size()) {
            throw new IllegalArgumentException("index greater than size=" + fkColumnPairList.size());
        }
		return fkColumnPairList.get(index).getPkColumnName();
	}
	
	public void addPrimaryKeyFieldName(String name, int index) {
        PkColumn col = new PkColumn();
        col.setPkColumnName(name);
        col.setIndex(index);
		if (pkColumnList.contains(col) == false) {
			pkColumnList.add(col);
		}
		SQLField field = sqlTable.getField(name);
		if (field == null) {
			throw new IllegalStateException("column " + name + " does not exists.");
		}
		field.setPrimaryKey(true);
	}
	
	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
    
    public int getFkColumnPairCount() {
        return fkColumnPairList.size();
    }
    
    public int getColumnCount() {
    	if (type == PRIMARY_KEY || type == UNIQUE_KEY) {
            return pkColumnList.size();
    	} else {
    		return fkColumnPairList.size();
    	}
    }
    
    public Object getColumnAt(int index) {
    	if (type == PRIMARY_KEY || type == UNIQUE_KEY) {
        	return pkColumnList.get(index);
    	} else {
    		return fkColumnPairList.get(index);
    	}
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SQLConstraint) {
            SQLConstraint c = (SQLConstraint) o;
            if (c.getType() != type) {
                return false;
            }
            if (getName().equalsIgnoreCase(c.getName())) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 13 * hash + this.type;
        hash = 13 * hash + (this.getName() != null ? this.getName().toLowerCase().hashCode() : 0);
        return hash;
    }
    
    public static class FkPkColumnPair implements Comparable<FkPkColumnPair> {
        
        private String fkColumnName;
        private String pkColumnName;
        private int index = 0;

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }
        
        public String getFkColumnName() {
            return fkColumnName;
        }

        public void setFkColumnName(String fkColumnName) {
            this.fkColumnName = fkColumnName;
        }

        public String getPkColumnName() {
            return pkColumnName;
        }

        public void setPkColumnName(String pkColumnName) {
            this.pkColumnName = pkColumnName;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o instanceof FkPkColumnPair) {
                FkPkColumnPair pair = (FkPkColumnPair) o;
                if (pair.fkColumnName != null && pair.fkColumnName.equalsIgnoreCase(fkColumnName)
                    && pair.pkColumnName != null && pair.pkColumnName.equalsIgnoreCase(pkColumnName)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 47 * hash + (this.fkColumnName != null ? this.fkColumnName.hashCode() : 0);
            hash = 47 * hash + (this.pkColumnName != null ? this.pkColumnName.hashCode() : 0);
            return hash;
        }

        public int compareTo(FkPkColumnPair o) {
        	if (o != null) {
                return o.getIndex() - index;
        	} else {
        		return 0;
        	}
        }
        
        @Override
        public String toString() {
            return fkColumnName + "->" + pkColumnName + "(" + index + ")";
        } 
        
    }

    public static class PkColumn implements Comparable<PkColumn> {
        
        private String pkColumnName;
        private int index = 0;

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }
        
        public String getPkColumnName() {
            return pkColumnName;
        }

        public void setPkColumnName(String pkColumnName) {
            this.pkColumnName = pkColumnName;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o instanceof PkColumn) {
                PkColumn pair = (PkColumn) o;
                if (pair.pkColumnName != null && pair.pkColumnName.equalsIgnoreCase(pkColumnName)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 47 * hash + (this.pkColumnName != null ? this.pkColumnName.hashCode() : 0);
            return hash;
        }

        public int compareTo(PkColumn o) {
            if (o != null) {
                return o.getIndex() - index;
            } else {
                return 0;
            }
        }
        
        @Override
        public String toString() {
            return pkColumnName + "(" + index + ")";
        }
    }
    
    public String getDropStatement(boolean withSchemaName) {
        StringBuilder sb = new StringBuilder();
        sb.append("alter table ");
        if (withSchemaName) {
            sb.append(sqlTable.getAbsoluteName());
        } else {
            sb.append(sqlTable.getName());
        }
        sb.append(" drop constraint ");
        sb.append(getName());
        return sb.toString();
    }
    
    @Override
    public String toString() {
        if (type == PRIMARY_KEY) {
            StringBuilder sb = new StringBuilder();
            sb.append("PK <");
            sb.append(getName());
            sb.append("> (");
            for (int i = 0; i < pkColumnList.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(pkColumnList.get(i).toString());
            }
            sb.append(')');
            return sb.toString();
        } else if (type == FOREIGN_KEY) {
            StringBuilder sb = new StringBuilder();
            sb.append("FK <");
            sb.append(getName());
            sb.append("> to table ");
            sb.append(referencedTableName);
            sb.append(" (");
            for (int i = 0; i < fkColumnPairList.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(fkColumnPairList.get(i).toString());
            }
            sb.append(')');
            return sb.toString();
        } else if (type == UNIQUE_KEY) {
            StringBuilder sb = new StringBuilder();
            sb.append("UN <");
            sb.append(getName());
            sb.append("> (");
            for (int i = 0; i < pkColumnList.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(pkColumnList.get(i).toString());
            }
            sb.append(')');
            return sb.toString();
        } else {
            return "constraint " + getName() + " has unknown type=" + type;
        }
    }

	public ArrayList<FkPkColumnPair> getFkColumnPairList() {
		return fkColumnPairList;
	}

	public ArrayList<PkColumn> getPkColumnList() {
		return pkColumnList;
	}

}
