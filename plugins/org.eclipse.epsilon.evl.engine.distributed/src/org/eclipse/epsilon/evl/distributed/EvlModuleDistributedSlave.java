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
import java.util.Set;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

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
		this(0);
	}
	
	public EvlModuleDistributedSlave(int parallelism) {
		this.context = new EvlContextDistributedSlave(parallelism);
	}
	
	List<ConstraintContextAtom> contextJobsCache;
	/**
	 * Creates data-parallel jobs (i.e. model elements from constraint contexts) and evaluates them based on the
	 * specified indices. Since both the master and slave modules create the same jobs from the same inputs (i.e.
	 * the process is deterministic), this approach can only fail if the supplied batch numbers are out of range.
	 * 
	 * @param batch The chunk of the problem this module should solve.
	 * @return A collection of serializable UnsatisfiedConstraints. If all constraints for
	 * the given element are satisfied, an empty collection is returned.
	 * @throws EolRuntimeException If anything in Epsilon goes wrong (e.g. problems with the user's code).
	 */
	public Collection<SerializableEvlResultAtom> evaluateBatch(final DistributedEvlBatch batch) throws EolRuntimeException {
		if (contextJobsCache == null) {
			contextJobsCache = ConstraintContextAtom.getContextJobs(this);
		}
		
		return batch.evaluate(contextJobsCache, getContext());
	}

	@Override
	public EvlContextDistributedSlave getContext() {
		return (EvlContextDistributedSlave) context;
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
