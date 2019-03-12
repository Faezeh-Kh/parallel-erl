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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.function.CheckedEolFunction;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * Base implementation of EVL with distributed execution semantics.
 * Splitting is supported at the element-level granularity. This class
 * can partition the data through the {@link #createJobs()} method, which can be
 * used as input to the distribution framework. The {@link #checkConstraints()} method
 * initiates the distributed processing; which in turn should spawn instances of
 * {@link EvlModuleDistributedSlave}. If a data sink is used (i.e.the results can be
 * acquired by this module as they appear), the 
 * {@link SerializableEvlResultAtom#deserializeResult(org.eclipse.epsilon.evl.IEvlModule)} 
 * method can be used to rebuild the unsatisfied constraints and apply them to the context. Otherwise if
 * the processing is blocking (i.e. the master must wait for all results to become available), then
 * {@linkplain #assignDeserializedResults(Stream)} can be used.
 * 
 * @see {@link EvlModuleDistributedSlave}
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributedMaster extends EvlModuleParallel {

	public EvlModuleDistributedMaster(int parallelism) {
		setContext(new EvlContextDistributedMaster(0, parallelism));
	}

	@Override
	protected void prepareContext() {
		getContext().storeInitialVariables();
		super.prepareContext();
	}
	
	@Override
	protected abstract void checkConstraints() throws EolRuntimeException;
	
	protected List<SerializableEvlInputAtom> createJobs(boolean shuffle) throws EolRuntimeException {
		return SerializableEvlInputAtom.createJobs(getConstraintContexts(), getContext(), shuffle);
	}
	
	protected void addToResults(Iterable<SerializableEvlResultAtom> serializedResults) throws EolRuntimeException {
		Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		for (SerializableEvlResultAtom sr : serializedResults) {
			unsatisfiedConstraints.add(sr.deserializeResult(this));
		}
	}
	
	/**
	 * Performs a batch collection of serialized unsatisfied constraints and
	 * adds them to the context's UnsatisfiedConstraints.
	 * 
	 * @param results The serialized {@linkplain UnsatisfiedConstraint}s
	 */
	protected void assignDeserializedResults(Stream<SerializableEvlResultAtom> results) {
		getContext().setUnsatisfiedConstraints(
			results.map((CheckedEolFunction<SerializableEvlResultAtom, UnsatisfiedConstraint>)
				sr -> sr.deserializeResult(this)
			)
			.collect(Collectors.toSet())
		);
	}
	
	@Override
	public EvlContextDistributedMaster getContext() {
		return (EvlContextDistributedMaster) super.getContext();
	}
	
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EvlContextDistributedMaster) {
			super.setContext(context);
		}
	}
}
