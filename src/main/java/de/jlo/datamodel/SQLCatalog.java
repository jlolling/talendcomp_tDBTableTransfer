package de.jlo.datamodel;

import java.util.ArrayList;
import java.util.List;

public class SQLCatalog extends SQLObject {
	
	private List<SQLSchema> schemas = new ArrayList<SQLSchema>();
	private boolean schemasLoaded = false;
	private boolean loadingSchemas = false;
	
	public SQLCatalog(SQLDataModel model, String name) {
    	super(model, name);
    }
	
	public List<SQLSchema> getSchemas() {
		return schemas;
	}

	public int getCountSchemas() {
		return schemas.size();
	}
	
	public SQLSchema getSchemaAt(int index) {
		return schemas.get(index);
	}
	
	public void addSQLSchema(SQLSchema schema) {
		if (schemas.contains(schema) == false) {
			schemas.add(schema);
			schema.setCatalog(this);
		}
	}
	
	public void clear() {
		schemas.clear();
		schemasLoaded = false;
	}
	
	public SQLSchema getSQLSchema(String name) {
		if (name == null) return null;
		for (SQLSchema s : schemas) {
			if (name.equalsIgnoreCase(s.getName())) {
				return s;
			}
		}
		return null;
	}
	
	public void setLoadingSchemas(boolean loading) {
		this.loadingSchemas = loading;
	}

	public boolean isSchemasLoaded() {
		return schemasLoaded;
	}
	
	public boolean loadSchemas() {
		schemasLoaded = getModel().loadSchemas(this);
		return schemasLoaded;
	}

	public boolean isLoadingSchemas() {
		return loadingSchemas;
	}
	
}
