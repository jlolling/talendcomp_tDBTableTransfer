package de.jlo.datamodel;

import java.util.ArrayList;
import java.util.List;

public class SQLIndex extends SQLObject {
	
	private SQLTable table;
	private int type;
	private boolean unique;
	private int cardinality;
	private String filterCondition;
	
	private List<IndexField> listIndexedFields = new ArrayList<IndexField>();

	public SQLIndex(SQLDataModel model, String name, SQLTable table) {
		super(model, name);
		this.table = table;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}

	public int getCardinality() {
		return cardinality;
	}

	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}
	
	public String getFilterCondition() {
		return filterCondition;
	}
	
	public void setFilterCondition(String filterCondition) {
		this.filterCondition = filterCondition;
	}

	public SQLTable getTable() {
		return table;
	}
	
	public void addIndexField(String name, int ordinalPosition, String sortOrder) {
		IndexField field = new IndexField(name, ordinalPosition, sortOrder);
		int pos = listIndexedFields.indexOf(field);
		if (pos != -1) {
			IndexField existingField = listIndexedFields.get(pos);
			existingField.updateFrom(field);
		} else {
			listIndexedFields.add(field);
		}
	}
	
	public void clearFields() {
		listIndexedFields.clear();
	}
	
	public int getCountFields() {
		return listIndexedFields.size();
	}
	
	public IndexField getFieldByName(String fieldName) {
		for (IndexField f : listIndexedFields) {
			if (f.name.equalsIgnoreCase(fieldName)) {
				return f;
			}
		}
		return null;
	}
	
	public IndexField getFieldByOrdinalPosition(int ordinalPos) {
		for (IndexField f : listIndexedFields) {
			if (f.ordinalPosition == ordinalPos) {
				return f;
			}
		}
		return null;
	}
	
	public IndexField getFieldAt(int listPos) {
		return listIndexedFields.get(listPos);
	}
	
	public static class IndexField {
		
		public IndexField(String name, int ordinalPosition, String sortOrder) {
			this.name = name;
			this.ordinalPosition = ordinalPosition;
			this.sortOrder = sortOrder;
		}
		
		private String name;
		private int ordinalPosition;
		private String sortOrder;
		
		public String getName() {
			return name;
		}
		
		public int getOrdinalPosition() {
			return ordinalPosition;
		}
		
		public String getSortOrder() {
			return sortOrder;
		}
		
		public void updateFrom(IndexField field) {
			if (field != null) {
				this.ordinalPosition = field.ordinalPosition;
				this.sortOrder = field.sortOrder;
			}
		}
		
		@Override
		public int hashCode() {
			return name.hashCode();
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof IndexField) {
				return name.equalsIgnoreCase(((IndexField) o).name);
			} else {
				return false;
			}
		}
		
		@Override
		public String toString() {
			return name;
		}
		
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof SQLIndex) {
			return super.equals(o) && this.table.equals(((SQLIndex) o).table);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return super.toString() + (unique ? " (unique)" : "");
	}
	
}
