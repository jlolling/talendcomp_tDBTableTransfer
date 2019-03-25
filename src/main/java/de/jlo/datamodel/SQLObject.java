package de.jlo.datamodel;

public class SQLObject {

	private String name;
	private transient SQLDataModel model = null;

	public SQLObject(SQLDataModel model, String name) {
		if (model == null) {
			if (this instanceof SQLDataModel) {
				this.model = (SQLDataModel) this;
			} else {
				throw new IllegalArgumentException("model cannot be null");
			}
		} else {
			this.model = model;
		}
		if (name == null) {
			throw new IllegalArgumentException("name cannot be null");
		}
		this.name = name;
	}
	
	public String getKey() {
		return name;
	}
	
	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return name;
	}

	public SQLDataModel getModel() {
		return model;
	}
	
    public static boolean isValidIdentifier(String name) {
    	boolean valid = true;
    	if (name == null) {
    		return false;
    	}
    	if (name.isEmpty()) {
    		return false;
    	}
    	char c;
    	for (int i = 0, n = name.length(); i < n; i++) {
    		c = name.charAt(i);
    		if (i == 0) {
    			if ((Character.isJavaIdentifierStart(c) || Character.isUpperCase(c)) == false) {
    				return false;
    			}
    		} else {
        		if ((Character.isUpperCase(c) || Character.isJavaIdentifierPart(c) || c == '-') == false) {
        			return false;
        		}
    		}
    	}
    	return valid;
    }
	
}
