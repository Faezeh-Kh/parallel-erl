package org.eclipse.epsilon.evl.distributed.crossflow;

import java.io.Serializable;
import java.util.UUID;
import org.eclipse.scava.crossflow.runtime.Job;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

public class ValidationResult extends Job  {
	
	public ValidationResult() {}
	
	public ValidationResult(org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom atom) {
		this.atom = atom;
	}
	
	public ValidationResult(org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom atom, Job correlation) {
		this.atom = atom;
		this.correlationId = correlation.getId();
	}
		
	protected org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom atom;
	
	public void setAtom(org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom atom) {
		this.atom = atom;
	}
	
	public org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom getAtom() {
		return atom;
	}
	
	
	public Object[] toObjectArray(){
		Object[] ret = new Object[1];
	 	ret[0] = getAtom();
		return ret;
	}
	
	public String toString() {
		return "ValidationResult (" + " atom=" + atom + " id=" + id + " correlationId=" + correlationId + " destination=" + destination + ")";
	}
	
}

