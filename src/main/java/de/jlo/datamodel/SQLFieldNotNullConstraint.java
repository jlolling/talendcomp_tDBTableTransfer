package de.jlo.datamodel;
/**
 * This call covers a not null constraint.
 * Instances of this class will be used mainly in schema comparison
 * @author jlolling
 *
 */
public class SQLFieldNotNullConstraint extends SQLObject {
	
	private SQLField f;
	
	public SQLFieldNotNullConstraint(SQLField f) {
		super(f.getModel(), f.getName());
		this.f = f;
	}
	
	public SQLField getField() {
		return f;
	}
	
}
