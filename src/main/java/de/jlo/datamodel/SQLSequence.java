package de.jlo.datamodel;

public class SQLSequence extends SQLObject {
	
	private long startsWith;
	private long endsWith;
	private long stepWith;
	private long currentValue;
	private String createCode;
	private String nextvalCode;
    private SQLSchema schema;

	public SQLSequence(SQLSchema schema, String name) {
		super(schema.getModel(), name);
		this.schema = schema;
	}
	
    public String getAbsoluteName() {
    	return schema.getName() + "." + getName();
    }

    public SQLSchema getSchema() {
		return schema;
	}

	public long getStartsWith() {
		return startsWith;
	}

	public void setStartsWith(long startsWith) {
		this.startsWith = startsWith;
	}

	public long getEndsWith() {
		return endsWith;
	}

	public void setEndsWith(long endsWith) {
		this.endsWith = endsWith;
	}

	public long getStepWith() {
		return stepWith;
	}

	public void setStepWith(long stepWith) {
		this.stepWith = stepWith;
	}

	public String getCreateCode() {
		return createCode;
	}

	public void setCreateCode(String code) {
		this.createCode = code;
	}

	public long getCurrentValue() {
		return currentValue;
	}

	public void setCurrentValue(long currentValue) {
		this.currentValue = currentValue;
	}

	public String getNextvalCode() {
		return nextvalCode;
	}

	public void setNextvalCode(String nextvalCode) {
		this.nextvalCode = nextvalCode;
	}

    @Override
    public boolean equals(Object o) {
    	if (o instanceof SQLSequence) {
    		SQLSequence so = (SQLSequence) o;
			return so.getName().equalsIgnoreCase(getName());
    	}
		return false;
    }

}
