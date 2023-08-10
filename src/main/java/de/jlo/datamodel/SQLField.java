package de.jlo.datamodel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public final class SQLField extends SQLObject implements Comparable<SQLField>, Field {

	private int type; // Constanten von java.sql.Types anwenden
	private String dbTypeName;
	private int basicType; // laut Konstanten in Database
	private SQLTable ownSQLTable;
	private boolean nullEnabled = true;
	private SQLFieldNotNullConstraint nnc = null;
	private boolean isPrimaryKey = false;
	private String usedInIndex = null;
	private int length = 0;
	private int decimalDigits = 0;
	private int ordinalPosition;
	private String comment;
	private String typeSQLCode;
	private String defaultValue;
	private boolean isSerial = false;
	private Class<?> javaClass;
	public static final int USAGE_INS_UPD = 0;
	public static final int USAGE_INS_ONLY = 1;
	public static final int USAGE_UPD_ONLY = 2;
	private int usageType = USAGE_INS_UPD;
	private boolean isFixedValue = false;
	static public final int ORACLE_ROWID = -100;
	static final int[] arrayBasicTypes = new int [] {
		BasicDataType.CHARACTER.getId(),
		BasicDataType.DATE.getId(),
		BasicDataType.DOUBLE.getId(),
		BasicDataType.INTEGER.getId(),
		BasicDataType.LONG.getId(),
		BasicDataType.BINARY.getId(),
		BasicDataType.CLOB.getId(),
		BasicDataType.BOOLEAN.getId(),
	};
	private static final HashMap<Integer, Integer> customTypeMap = new HashMap<Integer, Integer>();

	public SQLField(SQLDataModel model, SQLTable sqlTable, String name) {
		super(model, name);
		this.ownSQLTable = sqlTable;
	}

	public SQLField clone() {
		SQLField clone = new SQLField(getModel(), this.ownSQLTable, getName());
		clone.basicType = this.basicType;
		clone.comment = this.comment;
		clone.dbTypeName = this.dbTypeName;
		clone.decimalDigits = this.decimalDigits;
		clone.defaultValue = this.defaultValue;
		clone.isFixedValue = this.isFixedValue;
		clone.isPrimaryKey = this.isPrimaryKey;
		clone.isSerial = this.isSerial;
		clone.javaClass = this.javaClass;
		clone.length = this.length;
		clone.nnc = this.nnc;
		clone.nullEnabled = this.nullEnabled;
		clone.ordinalPosition = this.ordinalPosition;
		clone.type = this.type;
		clone.typeSQLCode = this.typeSQLCode;
		clone.usageType = this.usageType;
		clone.usedInIndex = this.usedInIndex;
		return clone;
	}
	
	public String getTableName() {
		return ownSQLTable.getName();
	}

	public void setPrimaryKey(boolean isPrimaryKey_loc) {
		this.isPrimaryKey = isPrimaryKey_loc;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setNullValueAllowed(boolean nullEnabled_loc) {
		this.nullEnabled = nullEnabled_loc;
	}

	public SQLFieldNotNullConstraint getNotNullConstraint() {
		if (nullEnabled) {
			return null;
		} else {
			if (nnc == null) {
				nnc = new SQLFieldNotNullConstraint(this);
			}
			return nnc;
		}
	}
	
	public boolean isNullValueAllowed() {
		return nullEnabled && isPrimaryKey == false;
	}

	public static void addCustomTypeBasictypePair(int sqlType, int basicType) {
		customTypeMap.put(sqlType, basicType);
	}
	
	public static Set<Map.Entry<Integer, Integer>> getCustomTypeMap() {
		return customTypeMap.entrySet();
	}

	/**
	 * this sets the datatyp of this field
	 * @param type this value should be one of the constants of the class java.sql.Type
	 */
	public void setType(int type) {
		this.type = type;
		basicType = BasicDataType.getBasicTypeByTypes(type);
	}

	public int getType() {
		return type;
	}

	@Override
	public void setTypeName(String name_loc) {
		this.dbTypeName = name_loc;
	}

	@Override
	public String getTypeName() {
		return dbTypeName;
	}

	@Override
	public void setLength(Integer length_loc) {
		if (length_loc != null) {
			this.length = length_loc;
		}
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public void setDecimalDigits(Integer digits) {
		if (digits != null) {
			this.decimalDigits = digits;
		}
	}

	@Override
	public int getDecimalDigits() {
		return this.decimalDigits;
	}

	public void setOrdinalPosition(int pos) {
		this.ordinalPosition = pos;
	}

	public int getOrdinalPosition() {
		return this.ordinalPosition;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		if (comment != null && comment.trim().isEmpty() == false) {
			this.comment = comment.trim();
		} else {
			this.comment = null;
		}
	}
	
	public String getAbsoluteName() {
		return ownSQLTable.getName() + "." + getName();
	}

	@Override
	public int compareTo(SQLField field) {
		int compvalue = 0;
		if (field != null) {
			compvalue = ownSQLTable.getName().compareTo(field.getTableName());
			if (compvalue == 0) {
				compvalue = (this.ordinalPosition > field.getOrdinalPosition()) ? 1 : -1;
			}
		}
		return compvalue;
	}

    @Override
	public boolean equals(Object object) {
		if (object instanceof SQLField) {
			return getAbsoluteName().equalsIgnoreCase(((SQLField) object).getAbsoluteName());
		} else {
			return false;
		}
	}

    @Override
	public int hashCode() {
		return getAbsoluteName().hashCode();
	}

	/**
	 * @return
	 */
	public SQLTable getSQLTable() {
		return ownSQLTable;
	}

	public int getBasicType() {
		return basicType;
	}
	
	@Override
	public void setBasicType(int basicType) {
		this.basicType = basicType;
	}

	public final String getUsedInIndex() {
		return usedInIndex;
	}

	public final void setUsedInIndex(String usedInIndex) {
		this.usedInIndex = usedInIndex;
	}
	
	/**
	 * sets the mapping with a string which contains all pairs of sql type and basic type
	 * @param parameterString in form "sqlType1=basicType1,sqltype2=basicType2.....sqltypeN=basicTypeN"
	 * @throws Exception if the parameterString contains illegal values or a has wrong format.
	 */
	public static void setCustomSqlTypeMapping(String parameterString) throws Exception {
		StringTokenizer st = new StringTokenizer(parameterString, ",;|");
		while (st.hasMoreTokens()) {
			String pair = st.nextToken();
			try {
				int posEqual = pair.indexOf('=');
				if (posEqual > 0 && posEqual < pair.length() - 1) {
					int sqlType = Integer.parseInt(pair.substring(0, posEqual));
					int basicType = Integer.parseInt(pair.substring(posEqual + 1));
					if (isValidBasicType(basicType)) {
						addCustomTypeBasictypePair(sqlType, basicType);
					} else {
						throw new Exception("Unknown basicType " + basicType);
					}
				} else {
					throw new Exception("invalid pair");
				}
			} catch (Exception e) {
				throw new Exception("Using customTypMap pair <" + pair + "> failed: " + e.getMessage(), e);
			}
		}
	}
	
	private static boolean isValidBasicType(int basicType) {
		for (int type : arrayBasicTypes) {
			if (type == basicType) {
				return true;
			}
		}
		return false;
	}
	
	public static String getBasicTypeName(int id) {
		BasicDataType type = BasicDataType.getBasicDataType(id);
		if (type != null) {
			return type.getName();
		} else {
			return "Unknown Type"; 
		}
	}

	@Override
	public String getTypeSQLCode() {
		return typeSQLCode;
	}

	@Override
	public void setTypeSQLCode(String typeSQLCode) {
		this.typeSQLCode = typeSQLCode;
	}

	public String getDefaultValue() {
		return defaultValue;
	}
	
	public String getDefaultValueSQL() {
		String sql = "";
		if (defaultValue != null) {
			sql = " default " + defaultValue;
		}
		return sql;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public boolean isSerial() {
		return isSerial;
	}

	public void setSerial(Boolean isSerial) {
		if (isSerial != null) {
			this.isSerial = isSerial;
		}
	}

	@Override
	public void setJavaClass(Class<?> clazz) {
		javaClass = clazz;
	}

	@Override
	public Class<?> getJavaClass() {
		return javaClass;
	}

	public int getUsageType() {
		return usageType;
	}

	public void setUsageType(int usageType) {
		if (usageType == USAGE_INS_UPD || usageType == USAGE_INS_ONLY || usageType == USAGE_UPD_ONLY) {
			this.usageType = usageType;
		} else {
			throw new IllegalArgumentException("Invalid usageType: " + usageType + " for column: " + getName() + " in table: " + getTableName());
		}
	}

	public boolean isFixedValue() {
		return isFixedValue;
	}

	public void setIsFixedValue(boolean isFixedValue) {
		this.isFixedValue = isFixedValue;
	}
	
}
