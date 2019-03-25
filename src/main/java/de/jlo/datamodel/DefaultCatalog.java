package de.jlo.datamodel;

public class DefaultCatalog extends SQLCatalog {
	
	public static final String DEFAULT = "Database";

	public DefaultCatalog(SQLDataModel model) {
		super(model, DEFAULT);
	}

	@Override
	public String getKey() {
		return null;
	}
	
}
