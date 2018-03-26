/*******************************************************************************
 * Copyright (c) 2008 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Dimitrios Kolovos - initial API and implementation
 ******************************************************************************/
package org.eclipse.epsilon.evl.execute.context;

import java.util.*;
import org.eclipse.epsilon.eol.execute.context.EolContext;
import org.eclipse.epsilon.evl.IEvlModule;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.trace.ConstraintTrace;

public class EvlContext extends EolContext implements IEvlContext, Cloneable {

	protected Set<UnsatisfiedConstraint> unsatisfiedConstraints = new HashSet<>();
	protected Set<Constraint> constraintsDependedOn = new HashSet<>();
	protected ConstraintTrace constraintTrace = new ConstraintTrace();
	
	@Override
	public ConstraintTrace getConstraintTrace() {
		return constraintTrace;
	}
	
	@Override
	public Set<UnsatisfiedConstraint> getUnsatisfiedConstraints() {
		return unsatisfiedConstraints;
	}
	
	@Override
	public IEvlModule getModule() {
		return (IEvlModule) module;
	}
	
	@Override
	public Set<Constraint> getConstraintsDependedOn() {
		return constraintsDependedOn;
	}

	@Override
	public void setConstraintsDependedOn(Set<Constraint> constraints) {
		constraintsDependedOn = constraints;
	}
}
