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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.EvlModule;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;
import org.eclipse.epsilon.evl.execute.exceptions.EvlConstraintNotFoundException;

/**
 * Base implementation of EVL with distributed execution semantics.
 * Splitting is supported at the element-level granularity. This class
 * can partition the data through the {@link #createJobs()} method, which can be
 * used as input to the distribution framework. The {@link #checkConstraints()} method
 * initiates the distributed processing; which in turn should spawn instances of
 * {@link EvlModuleDistributedSlave}. If a data sink is used (i.e.the results can be acquired
 * by this module as they appear), the {@link #deserializeResult(SerializableEvlResultAtom)} method
 * can be used to rebuild the unsatisfied constraints and apply them to the context. Otherwise if
 * the processing is blocking (i.e. the master must wait for all results to become available), then
 * {@link #assignDeserializedResults(Stream)} can be used.
 * 
 * @see {@link EvlModuleDistributedSlave}
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributedMaster extends EvlModule {

	public EvlModuleDistributedMaster(int parallelism) {
		context = new EvlContextDistributedMaster(0, parallelism);
	}

	@Override
	protected void prepareContext() {
		getContext().storeInitialVariables();
		super.prepareContext();
	}
	
	@Override
	protected abstract void checkConstraints() throws EolRuntimeException;
	
	/**
	 * Performs a batch collection of serialized unsatisfied constraints and
	 * adds them to the context's UnsatisfiedConstraints.
	 * 
	 * @param results The serialized {@linkplain UnsatisfiedConstraint}s
	 */
	protected void assignDeserializedResults(Stream<SerializableEvlResultAtom> results) {
		getContext().setUnsatisfiedConstraints(
			results.map(this::deserializeResult).collect(Collectors.toSet())
		);
	}
	
	/**
	 * Splits the workload into a collection of model elements. The order can be randomised
	 * (shuffled) to ensure a balanced workload. Subclasses may override this method to
	 * define an optimal split based on static analysis.
	 * 
	 * @param shuffle Whether to randomise the list order.
	 * @return The data to be distributed.
	 * @throws EolRuntimeException
	 */
	public List<SerializableEvlInputAtom> createJobs(boolean shuffle) throws EolRuntimeException {
		IEvlContext context = getContext();
		Collection<ConstraintContext> constraintContexts = getConstraintContexts();
		ArrayList<SerializableEvlInputAtom> problems = new ArrayList<>();
		
		for (ConstraintContext constraintContext : constraintContexts) {
			EolModelElementType modelElementType = constraintContext.getType(context);
			IModel model = modelElementType.getModel();
			Collection<?> allOfKind = model.getAllOfKind(modelElementType.getTypeName());
			
			problems.ensureCapacity(problems.size()+allOfKind.size());
			
			for (Object modelElement : allOfKind) {
				SerializableEvlInputAtom problem = new SerializableEvlInputAtom();
				problem.modelElementID = model.getElementId(modelElement);
				problem.modelName = model.getName(); //modelElementType.getModelName();
				problem.contextName = constraintContext.getTypeName();
				problems.add(problem);
			}
		}
		
		if (shuffle)
			Collections.shuffle(problems);
		
		return problems;
	}
	
	// TODO: support fixes and 'extras'
	/**
	 * Transforms the serialized UnsatisfiedConstraint into a native UnsatisfiedConstraint.
	 * @param sr The unsatisfied constraint information.
	 * @return The derived {@link UnsatisfiedConstraint}.
	 */
	public UnsatisfiedConstraint deserializeResult(SerializableEvlResultAtom sr) {
		UnsatisfiedConstraint uc = new UnsatisfiedConstraint();
		try {
			Object modelElement = sr.findElement(context);
			uc.setInstance(modelElement);
			uc.setMessage(sr.message);
			Constraint constraint = constraints
				.getConstraint(sr.constraintName, getConstraintContext(sr.contextName), modelElement, getContext(), false)
				.orElseThrow(() -> new EvlConstraintNotFoundException(sr.constraintName, this));
			uc.setConstraint(constraint);
		}
		catch (EolRuntimeException ex) {
			System.err.println(ex);
			throw new RuntimeException(ex);
		}
		
		return uc;
	}
	
	@Override
	public EvlContextDistributedMaster getContext() {
		return (EvlContextDistributedMaster) context;
	}
	
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EvlContextDistributedMaster) {
			super.setContext(context);
		}
	}
}
