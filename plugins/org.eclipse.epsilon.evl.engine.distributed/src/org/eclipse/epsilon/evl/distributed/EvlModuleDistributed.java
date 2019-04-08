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
import org.eclipse.epsilon.eol.exceptions.models.EolModelElementTypeNotFoundException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelNotFoundException;
import org.eclipse.epsilon.eol.execute.concurrent.ThreadLocalBatchData;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributed;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.concurrent.*;

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
	
	/**
	 * Executes the given jobs in parallel.
	 * 
	 * @param jobs The Serializable instances to forward to {@link #executeJob(Object)}
	 * @throws EolRuntimeException
	 */
	protected void executeParallel(Iterable<?> jobs) throws EolRuntimeException {
		if (jobs == null) return;
		EvlContextDistributed context = getContext();
		Collection<Runnable> executorJobs = jobs instanceof Collection ?
			new ArrayList<>(((Collection<?>) jobs).size()) : new ArrayList<>();
		
		for (Object job : jobs) {
			executorJobs.add(() -> {
				try {
					executeJob(job);
				}
				catch (EolRuntimeException eox) {
					context.handleException(eox);
				}
			});
		}

		context.executeParallel(null, executorJobs);
	}
	
	/**
	 * Executes the provided Serializable job(s) and returns the Serializable result.
	 * 
	 * @param job The Serializable input job(s).
	 * @return A Serializable Collection containing zero or more {@link SerializableEvlResultAtom}s.
	 * @throws EolRuntimeException If an exception occurs when executing the job using this module.
	 * @throws IllegalArgumentException If the job type was not recognised.
	 */
	public Collection<SerializableEvlResultAtom> executeJob(Object job) throws EolRuntimeException {
		if (job instanceof SerializableEvlInputAtom) {
			return execute((SerializableEvlInputAtom) job);
		}
		else if (job instanceof DistributedEvlBatch) {
			return execute((DistributedEvlBatch) job);
		}
		else if (job instanceof ConstraintContextAtom) {
			return execute((ConstraintContextAtom) job);
		}
		else if (job instanceof ConstraintAtom) {
			return execute((ConstraintAtom) job);
		}
		else if (job instanceof Iterable) {
			return executeJob(((Iterable<?>) job).iterator());
		}
		else if (job instanceof Iterator) {
			Iterator<?> iter = (Iterator<?>) job;
			ArrayList<SerializableEvlResultAtom> resultsCol = null;
			
			while (iter.hasNext()) {
				Collection<SerializableEvlResultAtom> result = executeJob(iter.next());
				if (result != null) {
					if (resultsCol != null) resultsCol.addAll(result);
					else resultsCol = new ArrayList<>(result);
				}
			}
			return resultsCol;
		}
		else if (job instanceof java.util.stream.BaseStream) {
			return executeJob(((java.util.stream.BaseStream<?,?>)job).iterator());
		}
		else {
			throw new IllegalArgumentException("Received unexpected object of type "+job.getClass().getName());
		}
	}
	
	List<ConstraintContextAtom> contextJobsCache;
	
	/**
	 * Calls {@link ConstraintContextAtom#getContextJobs(org.eclipse.epsilon.evl.IEvlModule)}
	 * 
	 * @return A cached (re-usable) deterministicly ordered List of jobs.
	 * @throws EolRuntimeException
	 */
	public final List<ConstraintContextAtom> getContextJobs() throws EolRuntimeException {
		if (contextJobsCache == null) {
			contextJobsCache = getContextJobsImpl();
		}
		return contextJobsCache;
	}
	
	public List<DistributedEvlBatch> getBatches(int numBatches) throws EolRuntimeException {
		return DistributedEvlBatch.getBatches(getContextJobs().size(), numBatches);
	}
	
	protected ArrayList<ConstraintContextAtom> getContextJobsImpl() throws EolModelElementTypeNotFoundException, EolModelNotFoundException {
		ArrayList<ConstraintContextAtom> atoms = new ArrayList<>();
		EvlContextDistributed context = getContext();
		
		for (ConstraintContext constraintContext : getConstraintContexts()) {
			Collection<?> allOfKind = constraintContext.getAllOfSourceKind(context);
			atoms.ensureCapacity(atoms.size()+allOfKind.size());
			for (Object element : allOfKind) {
				atoms.add(new ConstraintContextAtom(constraintContext, element));
			}
		}
		
		return atoms;
	}
	
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
	public Collection<SerializableEvlResultAtom> execute(final DistributedEvlBatch batch) throws EolRuntimeException {
		EvlContextDistributed context = getContext();
		EolExecutorService executor = context.beginParallelTask(null);
		ThreadLocalBatchData<SerializableEvlResultAtom> results = new ThreadLocalBatchData<>(context.getParallelism());
		
		for (ConstraintContextAtom job : batch.split(getContextJobs())) {
			executor.execute(() -> {
				try {
					for (UnsatisfiedConstraint uc : job.executeWithResults(context)) {
						results.addElement(SerializableEvlResultAtom.serializeResult(uc, context));
					}
				}
				catch (EolRuntimeException ex) {
					context.handleException(ex, executor);
				}
			});
		}
		
		executor.awaitCompletion();
		context.endParallelTask(null);
		return results.getBatch();
	}
	
	protected Collection<SerializableEvlResultAtom> execute(final SerializableEvlInputAtom atom) throws EolRuntimeException {
		return atom.execute(this);
	}
	
	protected Collection<SerializableEvlResultAtom> execute(final ConstraintContextAtom atom) throws EolRuntimeException {
		atom.execute(getContext());
		return null;
	}
	
	protected Collection<SerializableEvlResultAtom> execute(final ConstraintAtom atom) throws EolRuntimeException {
		atom.execute(getContext());
		return null;
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
