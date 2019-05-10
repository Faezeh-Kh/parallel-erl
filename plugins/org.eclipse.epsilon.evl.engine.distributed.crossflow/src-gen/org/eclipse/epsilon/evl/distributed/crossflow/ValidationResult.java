package org.eclipse.epsilon.evl.distributed.crossflow;

import java.io.Serializable;
import java.util.UUID;
import org.eclipse.scava.crossflow.runtime.Job;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

public class ValidationResult extends Job  {
	
	public ValidationResult() {}
	
	public ValidationResult(java.util.Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms) {
		this.atoms = atoms;
	}
	
	public ValidationResult(java.util.Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms, Job correlation) {
		this.atoms = atoms;
		this.correlationId = correlation.getId();
	}
		
	protected java.util.Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms;
	
	public void setAtoms(java.util.Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> atoms) {
		this.atoms = atoms;
	}
	
	public java.util.Collection<org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlResultAtom> getAtoms() {
		return atoms;
	}
	
	
	public Object[] toObjectArray(){
		Object[] ret = new Object[1];
	 	ret[0] = getAtoms();
		return ret;
	}
	
	public String toString() {
		return "ValidationResult (" + " atoms=" + atoms + " id=" + id + " correlationId=" + correlationId + " destination=" + destination + ")";
	}
	
}

