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
package de.jlo.datamodel;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.jlo.datamodel.generator.SQLCodeGenerator;


/**
 * @author lolling.jan
 */
public final class ModelComparator {

	private static final Logger logger = LogManager.getLogger(ModelComparator.class);
    private List<SQLTable> tablesToAdd = new ArrayList<SQLTable>();
    private List<SQLField> fieldsToAdd = new ArrayList<SQLField>();
    private List<SQLIndex> indicesToAdd = new ArrayList<SQLIndex>();
    private List<SQLConstraint> constraintsToAdd = new ArrayList<SQLConstraint>();
    private List<SQLFieldNotNullConstraint> notNullsToAdd = new ArrayList<SQLFieldNotNullConstraint>();
    private List<SQLTable> tablesToRemove = new ArrayList<SQLTable>();
    private List<SQLField> fieldsToRemove = new ArrayList<SQLField>();
    private List<SQLIndex> indicesToRemove = new ArrayList<SQLIndex>();
    private List<SQLConstraint> constraintsToRemove = new ArrayList<SQLConstraint>();
    private List<SQLFieldNotNullConstraint> notNullsToRemove = new ArrayList<SQLFieldNotNullConstraint>();
    private List<SQLField> fieldsToChange = new ArrayList<SQLField>();
    private List<SQLConstraint> constraintsToChange = new ArrayList<SQLConstraint>();
    private List<SQLIndex> indicesToChange = new ArrayList<SQLIndex>();
    private List<SQLProcedure> proceduresToAdd = new ArrayList<SQLProcedure>();
    private List<SQLProcedure> proceduresToRemove = new ArrayList<SQLProcedure>();
    private List<SQLProcedure> proceduresToChange = new ArrayList<SQLProcedure>();
    private List<SQLSequence> sequencesToAdd = new ArrayList<SQLSequence>();
    private List<SQLSequence> sequencesToRemove = new ArrayList<SQLSequence>();
//    private List<SQLSequence> sequencesToChange = new ArrayList<SQLSequence>();
    private DatamodelListener dml;
    private SQLSchema targetSchema;
    
    public void clear() {
    	tablesToAdd.clear();
    	fieldsToAdd.clear();
    	indicesToAdd.clear();
    	constraintsToAdd.clear();
    	notNullsToAdd.clear();
    	tablesToRemove.clear();
    	fieldsToRemove.clear();
    	indicesToRemove.clear();
    	constraintsToRemove.clear();
    	notNullsToRemove.clear();
    	fieldsToChange.clear();
    	constraintsToChange.clear();
    	indicesToChange.clear();
    	proceduresToAdd.clear();
    	proceduresToRemove.clear();
    	proceduresToChange.clear();
    }
    
    public void compare(SQLSchema schemaOne, SQLSchema schemaTwo) {
    	targetSchema = schemaTwo;
        // find missing tables in target
        for (int i = 0; i < schemaOne.getTableCount(); i++) {
        	if (Thread.currentThread().isInterrupted()) {
        		return;
        	}
        	SQLTable table1 = schemaOne.getTableAt(i);
        	if (table1.isInheritated()) {
        		continue;
        	}
        	if (logger.isDebugEnabled()) {
        		logger.debug("check table to add/change " + table1.getAbsoluteName());
        	}
        	SQLTable table2 = schemaTwo.getTable(table1.getName());
            if (table2 != null) {
                compare(table1, table2);
            } else {
            	tablesToAdd.add(table1);
            	if (table1.isMaterializedView()) {
                	fireEvent("+ Materialized View " + table1 + " must be added");
            	} else if (table1.isView()) {
                	fireEvent("+ View " + table1 + " must be added");
            	} else {
                	fireEvent("+ Table " + table1 + " must be added");
            	}
            }
        }
        // find obsolete tables in target
        for (int i = 0; i < schemaTwo.getTableCount(); i++) {
        	if (Thread.currentThread().isInterrupted()) { 
        		return;
        	}
        	SQLTable table2 = schemaTwo.getTableAt(i);
        	if (table2.isInheritated()) {
        		continue;
        	}
        	if (logger.isDebugEnabled()) {
        		logger.debug("check table to remove " + table2.getAbsoluteName());
        	}
        	SQLTable table1 = schemaOne.getTable(table2.getName());
            if (table1 == null) {
            	tablesToRemove.add(table2);
            	if (table2.isMaterializedView()) {
                	fireEvent("- Materialized View " + table2 + " must be removed");
            	} else if (table2.isView()) {
                	fireEvent("- View " + table2 + " must be removed");
            	} else {
                	fireEvent("- Table " + table2 + " must be removed");
            	}
            }
        }
        // check missing procedures
        for (int i = 0; i < schemaOne.getProcedureCount(); i++) {
        	if (Thread.currentThread().isInterrupted()) {
        		return;
        	}
        	SQLProcedure proc1 = schemaOne.getProcedureAt(i);
        	List<SQLProcedure> listProc2 = schemaTwo.getProcedures(proc1.getName());
        	if (listProc2.isEmpty()) {
        		proceduresToAdd.add(proc1);
        		fireEvent("+ Procedure " + proc1.getName() + " count parameters:" + proc1.getParameterCount() + " must be added");
        	} else {
        		// check for same signature
        		boolean exists = false;
        		for (SQLProcedure p2 : listProc2) {
        			if (proc1.equals(p2)) {
        				exists = true;
        				compareProcedures(proc1, p2);
        				break;
        			}
        		}
        		if (exists == false) {
            		proceduresToAdd.add(proc1);
            		fireEvent("+ Procedure " + proc1.getName() + " count parameters:" + proc1.getParameterCount() + " must be added");
        		}
        	}
        }
        // check procedures to remove
        for (int i = 0; i < schemaTwo.getProcedureCount(); i++) {
        	if (Thread.currentThread().isInterrupted()) {
        		return;
        	}
        	SQLProcedure proc2 = schemaTwo.getProcedureAt(i);
        	List<SQLProcedure> listProc2 = schemaOne.getProcedures(proc2.getName());
        	if (listProc2.isEmpty()) {
        		proceduresToRemove.add(proc2);
        		fireEvent("- Procedure " + proc2.getName() + " count parameters:" + proc2.getParameterCount() + " must be removed");
        	} else {
        		// check for same signature
        		boolean exists = false;
        		for (SQLProcedure p1 : listProc2) {
        			if (proc2.equals(p1)) {
        				exists = true;
        				break;
        			}
        		}
        		if (exists == false) {
            		proceduresToRemove.add(proc2);
            		fireEvent("- Procedure " + proc2.getName() + " count parameters:" + proc2.getParameterCount() + " must be removed");
        		}
        	}
        }
        // check missing sequences
    	List<SQLSequence> listSeq2 = schemaTwo.getSequences();
        for (int i = 0; i < schemaOne.getSequenceCount(); i++) {
        	if (Thread.currentThread().isInterrupted()) {
        		return;
        	}
        	SQLSequence s1 = schemaOne.getSequenceAt(i);
        	if (listSeq2.isEmpty()) {
        		sequencesToAdd.add(s1);
        		fireEvent("+ Sequence " + s1.getName() + " must be added");
        	} else {
        		boolean exists = false;
        		for (SQLSequence s2 : listSeq2) {
        			if (s1.equals(s2)) {
        				exists = true;
        				break;
        			}
        		}
        		if (exists == false) {
        			sequencesToAdd.add(s1);
            		fireEvent("+ Sequence " + s1.getName() + " must be added");
        		}
        	}
        }
        // check sequences to remove
    	List<SQLSequence> listSeq1 = schemaOne.getSequences();
        for (int i = 0; i < schemaTwo.getSequenceCount(); i++) {
        	if (Thread.currentThread().isInterrupted()) {
        		return;
        	}
        	SQLSequence s2 = schemaTwo.getSequenceAt(i);
        	if (listSeq1.isEmpty()) {
        		sequencesToRemove.add(s2);
        		fireEvent("- Sequence " + s2.getName() + " must be removed");
        	} else {
        		boolean exists = false;
        		for (SQLSequence s1 : listSeq1) {
        			if (s1.equals(s2)) {
        				exists = true;
        				break;
        			}
        		}
        		if (exists == false) {
        			sequencesToRemove.add(s2);
            		fireEvent("- Sequence " + s2.getName() + " must be removed");
        		}
        	}
        }
    }
    
    public void compareProcedures(SQLProcedure procOne, SQLProcedure procTwo) {
    	if (procOne.getCode() != null && procTwo.getCode() != null) {
    		if (procOne.getCode().equals(procTwo.getCode()) == false) {
    			proceduresToChange.add(procOne);
        		fireEvent("* Procedure " + procOne.getName() + " must be changed");
    		}
    	}
    }
    
    public void compare(SQLTable tableOne, SQLTable tableTwo) {
    	if (logger.isDebugEnabled()) {
    		logger.debug("compare " + tableOne.getAbsoluteName() + " with " + tableTwo.getAbsoluteName());
    	}
        if (tableOne.isTable() && tableTwo.isTable()) {
            SQLField fieldOne = null;
            SQLField fieldTwo = null;
            // tableOne
            for (int i = 0; i < tableOne.getFieldCount(); i++) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
                fieldOne = tableOne.getFieldAt(i);
            	if (logger.isDebugEnabled()) {
            		logger.debug("check column to add/change " + fieldOne.getAbsoluteName());
            	}
                fieldTwo = tableTwo.getField(fieldOne.getName());
                if (fieldTwo != null) {
                    compare(fieldOne, fieldTwo);
                } else {
                	fieldsToAdd.add(fieldOne);
                	fireEvent("+ Column " + fieldOne.getAbsoluteName() + " must be added");
                }
            }
            for (int i = 0; i < tableTwo.getFieldCount(); i++) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
                fieldTwo = tableTwo.getFieldAt(i);
            	if (logger.isDebugEnabled()) {
            		logger.debug("check column to remove " + fieldTwo.getAbsoluteName());
            	}
                fieldOne = tableOne.getField(fieldTwo.getName());
                if (fieldOne == null) {
                	fieldsToRemove.add(fieldTwo);
                	fireEvent("- Column " + fieldTwo.getAbsoluteName() + " must be removed");
                }
            }
            List<SQLConstraint> listConstraints1 = tableOne.getConstraints();
            for (SQLConstraint c1 : listConstraints1) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
            	SQLConstraint c2 = tableTwo.getConstraint(c1.getName());
            	if (c2 != null) {
            		compare(c1, c2);
            	} else {
            		constraintsToAdd.add(c1);
                	fireEvent("+ Constraint on " + c1.getTable().getName() + "->" + c1.getName() + " must be added");
            	}
            }
            List<SQLConstraint> listConstraints2 = tableTwo.getConstraints();
            for (SQLConstraint c2 : listConstraints2) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
            	SQLConstraint c1 = tableOne.getConstraint(c2.getName());
            	if (c1 == null) {
            		constraintsToRemove.add(c2);
                	fireEvent("- Constraint on " + c2.getTable().getName() + "->" + c2.getName() + " must be removed");
            	}
            }
            List<SQLIndex> listIndices1 = tableOne.getIndexes();
            for (SQLIndex i1 : listIndices1) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
            	SQLIndex i2 = tableTwo.getIndexByName(i1.getName());
            	if (i2 != null) {
            		compare(i1, i2);
            	} else {
            		if (findConstraint(listConstraints1, i1) == null) {
                		indicesToAdd.add(i1);
                    	fireEvent("+ Index on " + i1.getTable().getName() + "->" + i1.getName() + " must be added");
            		}
            	}
            }
            List<SQLIndex> listIndices2 = tableTwo.getIndexes();
            for (SQLIndex i2 : listIndices2) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
            	SQLIndex i1 = tableOne.getIndexByName(i2.getName());
            	if (i1 == null) {
            		if (findConstraint(listConstraints2, i2) == null) {
                		indicesToRemove.add(i2);
                    	fireEvent("- Index on " + i2.getTable().getName() + "->" + i2.getName() + " must be removed");
            		}
            	}
            }
        } else if (tableOne.isView() && tableTwo.isView()) {
            SQLField fieldOne = null;
            SQLField fieldTwo = null;
            // tableOne
            for (int i = 0; i < tableOne.getFieldCount(); i++) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
                fieldOne = tableOne.getFieldAt(i);
            	if (logger.isDebugEnabled()) {
            		logger.debug("check column to add/change " + fieldOne.getAbsoluteName());
            	}
                fieldTwo = tableTwo.getField(fieldOne.getName());
                if (fieldTwo != null) {
                    if (equals(fieldOne, fieldTwo) == false) {
                    	if (tablesToAdd.contains(tableOne) == false) {
                        	tablesToAdd.add(tableOne);
                        	tablesToRemove.add(tableTwo);
                        	if (tableOne.isMaterializedView()) {
                            	fireEvent("* Materialized View " + tableOne + " must changed");
                        	} else {
                            	fireEvent("* View " + tableOne + " must changed");
                        	}
                    	}
                    	break;
                    }
                } else {
                	if (tablesToAdd.contains(tableOne) == false) {
                    	tablesToAdd.add(tableOne);
                    	tablesToRemove.add(tableTwo);
                    	if (tableOne.isMaterializedView()) {
                        	fireEvent("* Materialized View " + tableOne + " must changed");
                    	} else {
                        	fireEvent("* View " + tableOne + " must changed");
                    	}
                	}
                	break;
                }
            }
            for (int i = 0; i < tableTwo.getFieldCount(); i++) {
            	if (Thread.currentThread().isInterrupted()) {
            		return;
            	}
                fieldTwo = tableTwo.getFieldAt(i);
            	if (logger.isDebugEnabled()) {
            		logger.debug("check superfluous view column " + fieldTwo.getAbsoluteName());
            	}
                fieldOne = tableOne.getField(fieldTwo.getName());
                if (fieldOne == null) {
                	if (tablesToAdd.contains(tableOne) == false) {
                    	tablesToAdd.add(tableOne);
                    	tablesToRemove.add(tableTwo);
                    	if (tableOne.isMaterializedView()) {
                        	fireEvent("* Materialized View " + tableOne + " must changed");
                    	} else {
                        	fireEvent("* View " + tableOne + " must changed");
                    	}
                	}
                	break;
                }
            }
        }
    }
    
    private SQLConstraint findConstraint(List<SQLConstraint> listConstraints, SQLIndex index) {
    	for (SQLConstraint c : listConstraints) {
    		if (c.getName().equalsIgnoreCase(index.getName())) {
    			return c;
    		}
    	}
    	return null;
    }

    private boolean equals(SQLField field1, SQLField field2) {
    	if (field1.getName().equalsIgnoreCase(field2.getName()) == false) {
    		return false;
    	}
    	if (field1.getBasicType() != field2.getBasicType()) {
    		return false;
    	}
    	if (field1.getLength() != field2.getLength()) {
    		return false;
    	}
    	if (field1.getDecimalDigits() != field2.getDecimalDigits()) {
    		return false;
    	}
    	if (field1.getNotNullConstraint() != null && field2.getNotNullConstraint() == null) {
    		return false;
    	}
    	if (field1.getNotNullConstraint() == null && field2.getNotNullConstraint() != null) {
    		return false;
    	}
    	return true;
    }
    
    private void compare(SQLField field1, SQLField field2) {
    	String fd1 = SQLCodeGenerator.getInstance().buildFieldDeclaration(field1);
    	String fd2 = SQLCodeGenerator.getInstance().buildFieldDeclaration(field2);
    	if (fd1.equalsIgnoreCase(fd2) == false) {
    		fieldsToChange.add(field1);
        	fireEvent("* Column " + field1.getAbsoluteName() + " must be changed");
    	} else if (field1.isNullValueAllowed() == false) {
    		if (field2.isNullValueAllowed()) {
    			notNullsToAdd.add(field1.getNotNullConstraint());
            	fireEvent("+ Not Null Constraint for " + field1.getAbsoluteName() + " must be added");
    		}
    	} else if (field2.isNullValueAllowed() == false) {
    		if (field1.isNullValueAllowed()) {
    			notNullsToRemove.add(field2.getNotNullConstraint());
            	fireEvent("- Not Null Constraint for " + field1.getAbsoluteName() + " must be removed");
    		}
    	}
    }
    
    private void compare(SQLConstraint c1, SQLConstraint c2) {
    	String cd1 = SQLCodeGenerator.getInstance().buildAddToTableStatement(c1, false);
    	String cd2 = SQLCodeGenerator.getInstance().buildAddToTableStatement(c2, false);
    	if (cd1.equalsIgnoreCase(cd2) == false) {
    		constraintsToChange.add(c1);
        	fireEvent("* Constraint " + c1.getTable().getName() + "->" + c1.getName() + " must be changed");
    	}
    }
    
    private void compare(SQLIndex i1, SQLIndex i2) {
    	String id1 = SQLCodeGenerator.getInstance().buildCreateStatement(i1, false);
    	String id2 = SQLCodeGenerator.getInstance().buildCreateStatement(i2, false);
    	if (id1.equalsIgnoreCase(id2) == false) {
    		indicesToChange.add(i1);
        	fireEvent("* Index " + i1.getTable().getName() + "->" + i1.getName() + " must be changed");
    	}
    }
    
    public void setDatamodelListener(DatamodelListener l) {
    	this.dml = l;
    }
    
    private void fireEvent(String message) {
    	DatamodelEvent e = new DatamodelEvent(null, message, DatamodelEvent.COMPARE_EVENT);
    	dml.eventHappend(e);
    }

	public List<SQLTable> getTablesToAdd() {
		return tablesToAdd;
	}

	public List<SQLField> getFieldsToAdd() {
		return fieldsToAdd;
	}

	public List<SQLIndex> getIndicesToAdd() {
		return indicesToAdd;
	}

	public List<SQLConstraint> getConstraintsToAdd() {
		return constraintsToAdd;
	}

	public List<SQLFieldNotNullConstraint> getNotNullsToAdd() {
		return notNullsToAdd;
	}

	public List<SQLTable> getTablesToRemove() {
		return tablesToRemove;
	}

	public List<SQLField> getFieldsToRemove() {
		return fieldsToRemove;
	}

	public List<SQLIndex> getIndicesToRemove() {
		return indicesToRemove;
	}

	public List<SQLConstraint> getConstraintsToRemove() {
		return constraintsToRemove;
	}

	public List<SQLFieldNotNullConstraint> getNotNullsToRemove() {
		return notNullsToRemove;
	}

	public List<SQLField> getFieldsToChange() {
		return fieldsToChange;
	}

	public List<SQLConstraint> getConstraintsToChange() {
		return constraintsToChange;
	}

	public List<SQLIndex> getIndicesToChange() {
		return indicesToChange;
	}
	
	public SQLSchema getTargetSchema() {
		return targetSchema;
	}

	public List<SQLProcedure> getProceduresToAdd() {
		return proceduresToAdd;
	}

	public List<SQLProcedure> getProceduresToRemove() {
		return proceduresToRemove;
	}

	public List<SQLProcedure> getProceduresToChange() {
		return proceduresToChange;
	}

	public List<SQLSequence> getSequencesToAdd() {
		return sequencesToAdd;
	}

	public List<SQLSequence> getSequencesToRemove() {
		return sequencesToRemove;
	}
    
}
