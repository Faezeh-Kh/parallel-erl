/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.exceptions.EolTypeNotFoundException;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;

/**
 * A worker EVL module, intended to be spawned during distributed processing.
 * The execution of this module is performed element by element rather than in
 * bulk. That is, the equivalent of the {@link #checkConstraints()} method is
 * {@link #evaluateElement(SerializableEvlInputAtom)}, which is called by the
 * distributed processing framework.
 * 
 * @see {@link EvlModuleDistributedMaster}
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedSlave extends EvlModuleParallel {

	public EvlModuleDistributedSlave() {
		super();
	}

	public EvlModuleDistributedSlave(int parallelism) {
		super(parallelism);
	}
	
	/**
	 * Evaluates all applicable constraints for the given model element.
	 * 
	 * @param atom The input to this worker.
	 * @return A collection of serializable UnsatisfiedConstraints. If all constraints for
	 * the given element are satisfied, an empty collection is returned.
	 * @throws EolRuntimeException If anything in Epsilon goes wrong (e.g. problems with the user's code).
	 */
	public Collection<SerializableEvlResultAtom> evaluateElement(final SerializableEvlInputAtom inputAtom) throws EolRuntimeException {
		IEvlContext context = getContext();
		Object modelElement = inputAtom.findElement(context);
		ConstraintContext constraintContext = getConstraintContextByTypeName(inputAtom.contextName);
		Collection<SerializableEvlResultAtom> unsatisfied;
		
		if (!constraintContext.shouldBeChecked(modelElement, context)) {
			return Collections.emptyList();
		}
		
		Collection<Constraint> constraintsToCheck = constraintContext.getConstraints();
		unsatisfied = new ArrayList<>(constraintsToCheck.size());
		
		for (Constraint constraint : constraintsToCheck) {
			constraint.execute(modelElement, context)
				.map(unsatisfiedConstraint -> {
					SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
					outputAtom.contextName = inputAtom.contextName;
					outputAtom.modelName = inputAtom.modelName;
					outputAtom.constraintName = unsatisfiedConstraint.getConstraint().getName();
					outputAtom.modelElementID = inputAtom.modelElementID;
					outputAtom.message = unsatisfiedConstraint.getMessage();
					return outputAtom;
				})
				.ifPresent(unsatisfied::add);
		}
		
		return unsatisfied;
	}
	
	protected ConstraintContext getConstraintContextByTypeName(String typeName) throws EolTypeNotFoundException {
		return getConstraintContexts()
			.stream()
			.filter(cc -> cc.getTypeName().equals(typeName))
			.findAny()
			.orElseThrow(() -> new EolTypeNotFoundException("No ConstraintContext of type '"+typeName+"' found", this));
	}
	
	public Collection<SerializableEvlResultAtom> serializeResults() {
		return getContext().getUnsatisfiedConstraints()
			.parallelStream()
			.map(this::serializeResult)
			.collect(Collectors.toList());
	}
	
	/**
	 * Transform the {@linkplain UnsatisfiedConstraint} into a serializable form.
	 * 
	 * @param uc The unsatisfied constraint.
	 * @return The serialized form of the unsatisfied constraint.
	 */
	public SerializableEvlResultAtom serializeResult(UnsatisfiedConstraint uc) {
		SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
		Object modelElement = uc.getInstance();
		IModel owningModel = getContext().getModelRepository().getOwningModel(modelElement);
		outputAtom.contextName = uc.getConstraint().getConstraintContext().getTypeName();
		outputAtom.modelName = owningModel.getName();
		outputAtom.modelElementID = owningModel.getElementId(modelElement);
		outputAtom.constraintName = uc.getConstraint().getName();
		outputAtom.message = uc.getMessage();
		return outputAtom;
	}
	
	@Override
	public Set<UnsatisfiedConstraint> executeImpl() throws EolRuntimeException {
		throw new UnsupportedOperationException("This method should only be called by the master!");
	}
	
	@Override
	protected void checkConstraints() throws EolRuntimeException {
		throw new IllegalStateException("This method should only be called by the master!");
	}
	
	// METHOD VISIBILITY
	
	@Override
	public void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
	}
	@Override
	public void postExecution() throws EolRuntimeException {
		super.postExecution();
	}
}
