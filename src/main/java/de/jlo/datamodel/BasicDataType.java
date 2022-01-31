package de.jlo.datamodel;

import java.sql.Types;
import java.util.HashMap;

/**
 *
 * @author jan
 */
public class BasicDataType {

    private int id;
    private String name;
    private static HashMap<Integer, BasicDataType> list = new HashMap<Integer, BasicDataType>();
    public static final BasicDataType CHARACTER = new BasicDataType(0, "Character");
    public static final BasicDataType DATE = new BasicDataType(1, "Date");
    public static final BasicDataType DOUBLE = new BasicDataType(2, "Double");
    public static final BasicDataType BINARY = new BasicDataType(3, "Binary");
    public static final BasicDataType INTEGER = new BasicDataType(5, "Integer");
    public static final BasicDataType LONG = new BasicDataType(6, "Long");
    public static final BasicDataType CLOB = new BasicDataType(4, "CLOB");
    public static final BasicDataType ROWID = new BasicDataType(-100, "RowId");
    public static final BasicDataType SQLEXP = new BasicDataType(7, "SQL expression");
    public static final BasicDataType BOOLEAN = new BasicDataType(8, "Boolean");
    public static final BasicDataType UNKNOWN = new BasicDataType(99999, "Unknown");
    
    private BasicDataType(int id, String name) {
        this.id = id;
        this.name = name;
        if (list.containsKey(id)) {
            throw new IllegalArgumentException("id=" + id + " already exists");
        }
        list.put(id, this);
    }
    
    public static boolean isNumberType(int type) {
    	return type == DOUBLE.id || type == INTEGER.id || type == LONG.id;
    }

    public static boolean isNumberType(BasicDataType type) {
    	return type == DOUBLE || type == INTEGER || type == LONG;
    }

    public static boolean isStringType(int type) {
    	return type == CLOB.id || type == CHARACTER.id;
    }

    public static boolean isStringType(BasicDataType type) {
    	return type == CLOB || type == CHARACTER;
    }

    public static boolean isDateType(int type) {
    	return type == DATE.id;
    }

    public static boolean isDateType(BasicDataType type) {
    	return type == DATE;
    }

    public static boolean isBooleanType(int type) {
    	return type == BOOLEAN.id;
    }

    public static boolean isBooleanType(BasicDataType type) {
    	return type == BOOLEAN;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public static BasicDataType getBasicDataType(int id) {
        return list.get(id);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BasicDataType) {
            return id == ((BasicDataType) o).getId();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + this.id;
        return hash;
    }

    @Override
    public String toString() {
        return getName();
    }

    public static int getBasicTypeByTypes(int type) {
        if (type == Types.VARCHAR || type == Types.CHAR || type == Types.NVARCHAR || type == Types.NCHAR || type == Types.ROWID) {
            return BasicDataType.CHARACTER.getId();
        } else if (type == Types.NUMERIC || type == Types.FLOAT || type == Types.REAL || type == Types.DOUBLE || type == Types.DECIMAL) {
            return BasicDataType.DOUBLE.getId();
        } else if (type == Types.INTEGER || type == Types.TINYINT || type == Types.SMALLINT) {
            return BasicDataType.INTEGER.getId();
        } else if (type == Types.BIGINT) {
            return BasicDataType.LONG.getId();
        } else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
            return BasicDataType.DATE.getId();
        } else if (type == Types.CLOB || type == Types.NCLOB || type == Types.LONGNVARCHAR || type == Types.LONGVARCHAR) {
            return BasicDataType.CLOB.getId();
        } else if (type == Types.BINARY || type == Types.VARBINARY || type == Types.LONGVARBINARY || type == Types.BLOB) {
            return BasicDataType.BINARY.getId();
        } else if (type == SQLField.ORACLE_ROWID) {
            return BasicDataType.ROWID.getId();
        } else if (type == Types.BOOLEAN || type == Types.BIT) {
        	return BasicDataType.BOOLEAN.getId();
        } else {
            return 0;
        }
    }

    public static BasicDataType getBasicTypeByTypeObjects(int type) {
        if (type == Types.VARCHAR || type == Types.CHAR || type == Types.NVARCHAR || type == Types.NCHAR || type == Types.ROWID) {
            return BasicDataType.CHARACTER;
        } else if (type == Types.NUMERIC || type == Types.FLOAT || type == Types.REAL || type == Types.DOUBLE || type == Types.DECIMAL) {
            return BasicDataType.DOUBLE;
        } else if (type == Types.INTEGER || type == Types.TINYINT || type == Types.SMALLINT) {
            return BasicDataType.INTEGER;
        } else if (type == Types.BIGINT) {
            return BasicDataType.LONG;
        } else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
            return BasicDataType.DATE;
        } else if (type == Types.CLOB || type == Types.NCLOB || type == Types.LONGNVARCHAR || type == Types.LONGVARCHAR) {
            return BasicDataType.CLOB;
        } else if (type == Types.BINARY || type == Types.VARBINARY || type == Types.LONGVARBINARY || type == Types.BLOB) {
            return BasicDataType.BINARY;
        } else if (type == SQLField.ORACLE_ROWID) {
            return BasicDataType.ROWID;
        } else if (type == Types.BOOLEAN || type == Types.BIT) {
        	return BasicDataType.BOOLEAN;
        } else {
            return BasicDataType.UNKNOWN;
        }
    }

    /**
	 * ordnet die unterschiedlichen Datentypen zu Basic-Typen und gibt für die
	 * ausgewählte Spalte den Basictyp zurück
	 * 
	 * @param typeName
	 *            Name des Types
	 * @return Basic-Type
	 */
	public static int getBasicTypeByName(String typeName) {
		typeName = typeName.toUpperCase();
		if (typeName.equals("CLOB")) {
			return BasicDataType.CLOB.getId();
		} else if ((typeName.indexOf("CHAR") != -1) 
				|| typeName.equals("TEXT")) {
			return BasicDataType.CHARACTER.getId();
		} else if ((typeName.indexOf("NUM") != -1)
				|| typeName.equals("FLOAT")
				|| typeName.equals("REAL")
				|| typeName.equals("DOUBLE")
				|| typeName.equals("DECIMAL")) {
			return BasicDataType.DOUBLE.getId();
		} else if (typeName.equals("INT8")
				|| typeName.equals("BIGINT")) {
			return BasicDataType.LONG.getId();
		} else if (typeName.equals("INTEGER")
				|| typeName.equals("TINYINT")
				|| typeName.equals("INT4")
				|| typeName.equals("INT2")
				|| typeName.equals("SMALLINT")) {
			return BasicDataType.INTEGER.getId();
		} else if (typeName.equals("DATE")
				|| typeName.equals("TIME")
				|| typeName.equals("TIMESTAMP")) {
			return BasicDataType.DATE.getId();
		} else if (typeName.equals("BOOL")
				|| typeName.equals("BOOLEAN")
				|| typeName.equals("BIT")) {
			return BasicDataType.BOOLEAN.getId();
		} else if (typeName.indexOf("BIN") != -1) {
			return BasicDataType.BINARY.getId();
		} else if (typeName.equals("BLOB")) {
			return BasicDataType.BINARY.getId();
		} else if (typeName.equals("ROWID")) {
			return BasicDataType.CHARACTER.getId();
		} else {
			return 0;
		}
	}

}
