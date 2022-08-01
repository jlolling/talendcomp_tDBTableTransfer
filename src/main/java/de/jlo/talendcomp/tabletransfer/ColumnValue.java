/**
 * Copyright 2022 Jan Lolling jan.lolling@gmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.jlo.talendcomp.tabletransfer;

import de.jlo.datamodel.SQLField;

public class ColumnValue {

	private String columnName;
	private Object value;
	private int usageType = 0;
	
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

	public int getUsageType() {
		return usageType;
	}

	/**
	 * set the usage of this value
	 * 0 = insert+update
	 * 1 = insert only
	 * 2 = update only
	 * @param usageType
	 */
	public void setUsageType(Integer usageType) {
		if (usageType == null) {
			throw new IllegalArgumentException("usageType of column: " + columnName + " cannot be null and must be 0 or 1 or 2");
		}
		if (usageType == SQLField.USAGE_INS_UPD || usageType == SQLField.USAGE_INS_ONLY || usageType == SQLField.USAGE_UPD_ONLY) {
			this.usageType = usageType;
		} else {
			throw new IllegalArgumentException("Invalid usageType: " + usageType + " for column: " + columnName);
		}
	}	
}
