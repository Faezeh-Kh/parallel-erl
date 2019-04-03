/*********************************************************************
 * Copyright (c) 2019 The University of York.
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
import java.util.Iterator;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributed;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributed extends EvlModuleParallel {

	public EvlModuleDistributed(int parallelism) {
		super(parallelism);
		setContext(new EvlContextDistributed(parallelism));
	}

	@Override
	protected abstract void checkConstraints() throws EolRuntimeException;
	
	
	/**
	 * Executes the provided Serializable job(s) and returns the Serializable result.
	 * 
	 * @param job The Serializable input job(s).
	 * @return A Serializable Collection containing zero or more {@link SerializableEvlResultAtom}s.
	 * @throws EolRuntimeException If an exception occurs when executing the job using this module.
	 * @throws IllegalArgumentException If the job type was not recognised.
	 */
	public Collection<SerializableEvlResultAtom> evaluateJob(Object job) throws EolRuntimeException {
		if (job instanceof SerializableEvlInputAtom) {
			return ((SerializableEvlInputAtom) job).evaluate(this);
		}
		else if (job instanceof DistributedEvlBatch) {
			return evaluateBatch((DistributedEvlBatch) job);
		}
		else if (job instanceof Iterable) {
			return evaluateJob(((Iterable<?>) job).iterator());
		}
		else if (job instanceof Iterator) {
			ArrayList<SerializableEvlResultAtom> resultsCol = new ArrayList<>();
			
			for (Iterator<?> iter = (Iterator<?>) job; iter.hasNext();) {
				Object obj = iter.next();
				final Collection<SerializableEvlResultAtom> nested;
				
				if (obj instanceof SerializableEvlInputAtom) {
					nested = evaluateAtom((SerializableEvlInputAtom) job);
				}
				else if (obj instanceof DistributedEvlBatch) {
					nested = evaluateBatch((DistributedEvlBatch) obj);
				}
				else {
					nested = evaluateJob(obj);
				}
				
				resultsCol.addAll(nested);
			}
			return resultsCol;
		}
		else if (job instanceof java.util.stream.BaseStream) {
			return evaluateJob(((java.util.stream.BaseStream<?,?>)job).iterator());
		}
		else {
			throw new IllegalArgumentException("Received unexpected object of type "+job.getClass().getName());
		}
	}
	
	// TODO implement parallel
	protected Collection<SerializableEvlResultAtom> evaluateAtom(final SerializableEvlInputAtom atom) throws EolRuntimeException {
		return atom.evaluate(this);
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
	public EvlContextDistributed getContext() {
		return (EvlContextDistributed) super.getContext();
	}
	
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EvlContextDistributed) {
			super.setContext(context);
		}
	}
}
