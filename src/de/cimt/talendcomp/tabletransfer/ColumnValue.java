package de.cimt.talendcomp.tabletransfer;

public class ColumnValue {

	private String columnName;
	private Object value;
	
	public ColumnValue(String name) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("name cannot be null or empty!");
		}
		this.columnName = name;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public Object getValue() {
		return value;
	}
	
	public void setValue(Object value) {
		this.value = value;
	}
	
}
