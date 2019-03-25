package de.jlo.datamodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

public class SQLSchema extends SQLObject {

    private final Vector<SQLTable> tableList = new Vector<SQLTable>();
    private final HashMap<String, SQLTable> tableMap = new HashMap<String, SQLTable>();
    private final ArrayList<SQLProcedure> procedureList = new ArrayList<SQLProcedure>();
    private final ArrayList<SQLSequence> sequenceList = new ArrayList<SQLSequence>();
    private boolean loadingTables = false;
    private boolean loadingProcedures = false;
    private boolean tablesLoaded = false;
    private boolean procedureLoaded = false;
    private boolean loadingSequences = false;
    private boolean sequencesLoaded = false;
    private SQLCatalog catalog;

    public void setProcedureLoaded() {
        this.procedureLoaded = true;
    }
    
    public boolean isProceduresLoaded() {
        return procedureLoaded;
    } 
    
    public void addSequence(SQLSequence sequence) {
    	sequenceList.add(sequence);
    }
    
    public List<SQLSequence> getSequences() {
    	if (sequencesLoaded == false && loadingSequences == false) {
    		getModel().loadSequences(this);
    	}
    	return sequenceList;
    }
    
    public int getSequenceCount() {
        if (sequencesLoaded == false) {
            getModel().loadSequences(this);
        }
        return sequenceList.size();
    }

    public void addProcedure(SQLProcedure procedure) {
        procedureList.add(procedure);
    }
    
    public void sortProcedureList() {
    	Collections.sort(procedureList);
    }
    
    public List<SQLProcedure> getProcedures(String name) {
    	if (procedureLoaded == false && loadingProcedures == false) {
    		loadProcedures();
    	}
    	List<SQLProcedure> list = new ArrayList<SQLProcedure>();
        SQLProcedure p = null;
        for (int i = 0; i < procedureList.size(); i++) {
            p = procedureList.get(i);
            if (p.getName() != null && p.getName().equalsIgnoreCase(name)) {
            	list.add(p);
            }
        }
        return list;
    }
    
    public List<SQLProcedure> getProcedures() {
    	if (procedureLoaded == false && loadingProcedures == false) {
    		loadProcedures();
    	}
    	List<SQLProcedure> list = new ArrayList<SQLProcedure>();
        SQLProcedure p = null;
        for (int i = 0; i < procedureList.size(); i++) {
            p = procedureList.get(i);
        	list.add(p);
        }
        return list;
    }

    public void removeProcedure(SQLProcedure procedure) {
        procedureList.remove(procedure);
    }
    
    public int getProcedureCount() {
        if (procedureLoaded == false) {
            getModel().loadProcedures(this);
        }
        return procedureList.size();
    } 
    
    public SQLProcedure getProcedureAt(int index) {
        if (procedureLoaded == false) {
            getModel().loadProcedures(this);
        }
        return procedureList.get(index);
    }
    
    public SQLSequence getSequenceAt(int index) {
        if (sequencesLoaded == false) {
            getModel().loadSequences(this);
        }
        return sequenceList.get(index);
    }
    
    public void clearProcedures() {
        procedureList.clear();
        procedureLoaded = false;
    }
    
    public boolean isTablesLoaded() {
        return tablesLoaded;
    }
    
    public void setSequencesLoaded() {
        sequencesLoaded = true;
    }

    public boolean isSequencesLoaded() {
        return sequencesLoaded;
    }
    
    public void setTablesLoaded() {
        tablesLoaded = true;
    }

    public SQLSchema(SQLDataModel model, String name) {
    	super(model, name);
    }

    public void addTable(SQLTable table) {
        tableList.addElement(table);
        tableMap.put(table.getName().toLowerCase(), table);
    }
    
    void removeSQLTable(SQLTable table) {
        tableList.remove(table);
        tableMap.remove(table.getName().toLowerCase());
    }
    
    public void clearTables() {
        tableList.clear();
        tableMap.clear();
        tablesLoaded = false;
        sequencesLoaded = false;
    }

    public int getTableCount() {
        if (tablesLoaded == false) {
            loadTables();
        }
        return tableList.size();
    }
    
    public SQLTable getTableAt(int index) {
        if (tablesLoaded == false) {
            loadTables();
        }
        return tableList.get(index);
    }
    
    public List<SQLTable> getTables() {
    	List<SQLTable> list = new ArrayList<SQLTable>();
    	for (int i = 0; i < getTableCount(); i++) {
    		list.add(getTableAt(i));
    	}
    	Collections.sort(list, new Comparator<SQLTable>() {

			@Override
			public int compare(SQLTable o1, SQLTable o2) {
				return o1.getName().toLowerCase().compareToIgnoreCase(o2.getName());
			}
    		
		});
    	return list;
    }
    
    public SQLTable getTable(String name) {
    	if (isValidIdentifier(name)) {
            if (tablesLoaded == false) {
                loadTables();
            }
            return tableMap.get(name.toLowerCase());
    	} else {
    		return null;
    	}
    }
    
    public boolean loadTables() {
        tablesLoaded = getModel().loadTables(this);
        return tablesLoaded;
    }
    
    public boolean loadProcedures() {
        return getModel().loadProcedures(this);
    }
    
    public boolean loadSequences() {
        return getModel().loadSequences(this);
    }

    @Override
    public int hashCode() {
    	return getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
    	if (o instanceof SQLSchema) {
    		SQLSchema so = (SQLSchema) o;
			return so.getName().equalsIgnoreCase(getName());
    	}
		return false;
    }

	public boolean isLoadingTables() {
		return loadingTables;
	}

	public void setLoadingTables(boolean loadingTables) {
		this.loadingTables = loadingTables;
	}

	public boolean isLoadingProcedures() {
		return loadingProcedures;
	}

	public void setLoadingSequences(boolean loading) {
		this.loadingSequences = loading;
	}

	public boolean isLoadingSequences() {
		return loadingSequences;
	}

	public void setLoadingProcedures(boolean loadingProcedures) {
		this.loadingProcedures = loadingProcedures;
	}

	public SQLCatalog getCatalog() {
		return catalog;
	}

	public void setCatalog(SQLCatalog catalog) {
		this.catalog = catalog;
		catalog.addSQLSchema(this);
	}

}