/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.jlo.datamodel;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jan
 */
public class SQLProcedure extends SQLObject implements Comparable<SQLProcedure> {
    
    private SQLSchema schema;
    private String comment = null;
    private boolean function = false;
    private Parameter returnParam;
    private List<Parameter> returnTable = null; 
    private String code;
    private ArrayList<Parameter> listParameters = new ArrayList<Parameter>();
    
    public SQLProcedure(SQLDataModel model, SQLSchema schema, String name) {
    	super(model, name);
    	this.schema = schema;
    }

    public String getComment() {
        return comment;
    }
    
    public String getAbsoluteName() {
    	return schema.getName() + "." + getName();
    }

    public void setComment(String remark) {
        this.comment = remark;
    }
    
    public SQLSchema getSchema() {
        return schema;
    }
    
    public Parameter addParameter(String name, int type, String typeName, int length, int precision, int ioType) {
        Parameter parameter = new Parameter(this);
        parameter.setName(name);
        parameter.setLength(length);
        parameter.setType(type);
        parameter.setTypeName(typeName);
        parameter.setPrecision(precision);
        parameter.setIoType(ioType);
        if (parameter.isSingleReturnValue()) {
            setFunction(true);
            returnParam = parameter;
        } else if (parameter.isResultsetReturnValue()) {
            setFunction(true);
            if (returnTable == null) {
            	returnTable = new ArrayList<>();
            }
        	returnTable.add(parameter);
        } else {
            listParameters.add(parameter);
        }
        return parameter;
    }
    
    public Parameter getReturnParameter() {
    	return returnParam;
    }
    
    public List<Parameter> getResultsetParameters() {
    	return returnTable;
    }

    public int getParameterCount() {
        return listParameters.size();
    }
    
    public Parameter getParameterAt(int index) {
        return listParameters.get(index);
    }
    
    public void clearParameters() {
        listParameters.clear();
    }
    
    public boolean isFunction() {
        return function;
    }

    private void setFunction(boolean function) {
        this.function = function;
    }
    
    public String getProcedureCallCode() {
    	StringBuilder sb = new StringBuilder(getName());
    	sb.append("(");
    	boolean firstLoop = true;
    	for (Parameter p : listParameters) {
    		if (p.name != null) {
        		if (firstLoop) {
        			firstLoop = false;
        		} else {
        			sb.append(",");
        		}
        		sb.append(p.name);
    		}
    	}
    	sb.append(")");
    	return sb.toString();
    }
    
    @Override
    public int hashCode() {
    	return getProcedureCallCode().hashCode();
    }
    
    @Override
    public boolean equals(Object object) {
    	if (object instanceof SQLProcedure) {
    		SQLProcedure other = (SQLProcedure) object;
    		return getProcedureCallCode().equalsIgnoreCase(other.getProcedureCallCode());
   		} else {
    		return false;
    	}
    }
    
    public static class Parameter {
        
        private String name;
        private int type;
        private String typeName;
        private int length;
        private int precision;
        private int ioType; // use constants in DatabaseMetaData
        private SQLProcedure procedure;
        
        public Parameter(SQLProcedure procedure) {
            this.procedure = procedure;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public int getIoType() {
            return ioType;
        }

        public boolean isSingleReturnValue() {
        	return ioType == DatabaseMetaData.procedureColumnReturn;
        }
        
        public boolean isResultsetReturnValue() {
        	return ioType == DatabaseMetaData.procedureColumnResult;
        }

        public boolean isOutputParameter() {
        	return ioType == DatabaseMetaData.procedureColumnOut || ioType == DatabaseMetaData.procedureColumnInOut;
        }
        
        public void setIoType(int ioType) {
            this.ioType = ioType;
        }
        
        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public String getName() {
        	if (name != null) {
        		return name;
        	} else {
        		return "";
        	}
        }
        
        public SQLProcedure getProcedure() {
        	return procedure;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getPrecision() {
            return precision;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
        
        @Override
        public boolean equals(Object o) {
            if (o instanceof Parameter) {
                return ((Parameter) o).getName().equalsIgnoreCase(name);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 31 * hash + (this.name != null ? this.name.hashCode() : 0);
            return hash;
        }
        
        @Override
        public String toString() {
            if (name != null && name.length() > 0) {
                return name;
            } else {
                return "return";
            }
        }
        
    }

	@Override
	public int compareTo(SQLProcedure o) {
		if (o.getName().equals(getName())) {
			if (getParameterCount() > o.getParameterCount()) {
				return 1;
			} else {
				return -1;
			}
		} else {
			return getName().compareTo(o.getName());
		}
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}
    
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(getName());
    	boolean firstLoop = true;
		sb.append("(");
    	for (Parameter p : listParameters) {
    		if (p.isSingleReturnValue() == false) {
        		if (firstLoop) {
        			firstLoop = false;
        		} else {
        			sb.append(",");
        		}
        		sb.append(p.typeName);
    		}
    	}
		sb.append(")");
    	return sb.toString();
    }
    
}
