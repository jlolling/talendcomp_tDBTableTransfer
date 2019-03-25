package de.jlo.datamodel;

public class SQLTrigger extends SQLObject {
	
	private SQLTable table;
	private String code;
	
	public SQLTrigger(SQLDataModel model, String name, SQLTable table) {
		super(model, name);
		this.table = table;
	}

	public SQLTable getTable() {
		return table;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		if (code != null) {
			this.code = code.trim();
		} else {
			this.code = null;
		}
	}
	
}
