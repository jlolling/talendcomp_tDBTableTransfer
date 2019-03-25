package de.jlo.datamodel;

public class DatamodelEvent {
	
	private SQLDataModel model;
	private String message;
	public static final int COMPARE_EVENT = 1;
	public static final int ACTION_MESSAGE_EVENT = 2;
	public static final int SELECTION_EVENT = 3;

	private int type;
	
	public DatamodelEvent(SQLDataModel model, String message, int type) {
		this.model = model;
		this.message = message;
	}

	public int getType() {
		return type;
	}
	
	public SQLDataModel getModel() {
		return model;
	}

	public String getMessage() {
		return message;
	}

}
