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
package de.jlo.datamodel.ext.impl;

import java.util.ArrayList;
import java.util.List;

import de.jlo.datamodel.BasicDataType;
import de.jlo.datamodel.Field;
import de.jlo.datamodel.ext.GenericDatabaseExtension;

public class MSSqlExtension extends GenericDatabaseExtension {
	
	public MSSqlExtension() {
		addDriverClassName("net.sourceforge.jtds.jdbc.Driver");
		addDriverClassName("com.microsoft.jdbc.sqlserver.SQLServerDriver");
		addDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	}
	
	@Override
	public void setupDataType(Field field) {
		String typeName = field.getTypeName().toLowerCase();
        if (typeName.indexOf("int") != -1) {
        	if (typeName.indexOf("identity") != -1) {
        		field.setTypeSQLCode("integer indentity(1,1)");
        	} else {
        		field.setTypeSQLCode("integer");
        	}
    		field.setBasicType(BasicDataType.INTEGER.getId());
        } else if (typeName.indexOf("double") != -1) {
            field.setTypeSQLCode("double");
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if (typeName.indexOf("float") != -1) {
            field.setTypeSQLCode("float");
    		field.setBasicType(BasicDataType.DOUBLE.getId());
        } else if ("bool".equals(typeName)) {
        	field.setTypeName("boolean");
            field.setLength(0);
    		field.setBasicType(BasicDataType.DATE.getId());
        }
	}

	@Override
	public List<String> getAdditionalSQLKeywords() {
		List<String> list = new ArrayList<String>();
		list.add("identity");
		list.add("coalesce");
		list.add("len");
		return list;
	}

}
