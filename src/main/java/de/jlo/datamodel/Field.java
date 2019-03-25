package de.jlo.datamodel;

public interface Field {
	
	void setTypeName(String name);
	
	void setTypeSQLCode(String code);
	
	void setBasicType(int type);
	
	String getTypeName();
	
	String getTypeSQLCode();
	
	int getLength();
	
	void setLength(Integer length);
	
	int getDecimalDigits();
	
	void setDecimalDigits(Integer digits);
	
	void setJavaClass(Class<?> clazz);
	
	Class<?> getJavaClass();

}
