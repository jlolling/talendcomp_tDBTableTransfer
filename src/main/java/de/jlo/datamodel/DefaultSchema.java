package de.jlo.datamodel;

public class DefaultSchema extends SQLSchema {

	public DefaultSchema(SQLDataModel model) {
		super(model, "default");
	}

	@Override
	public String getKey() {
		return null;
	}

	@Override
	public String toString() {
		return getCatalog().getName();
	}
	
	@Override
	public String getName() {
		return getCatalog().getName();
	}
	
}
