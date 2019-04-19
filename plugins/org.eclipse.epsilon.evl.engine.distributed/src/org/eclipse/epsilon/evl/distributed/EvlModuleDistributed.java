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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelElementTypeNotFoundException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelNotFoundException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributed;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.*;

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
	 * Executes the provided Serializable job(s) and returns the Serializable result.
	 * 
	 * @param job The Serializable input job(s).
	 * @return A Serializable Collection containing zero or more {@link SerializableEvlResultAtom}s,
	 * or <code>null</code> if this module is the master.
	 * @throws EolRuntimeException If an exception occurs when executing the job using this module.
	 * @throws IllegalArgumentException If the job type was not recognised.
	 */
	public Collection<SerializableEvlResultAtom> executeJob(Object job) throws EolRuntimeException {
		if (job == null) {
			return null;
		}
		else if (job instanceof SerializableEvlInputAtom) {
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
			return executeJob(((Iterable<?>) job).spliterator());
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
		else if (job instanceof Spliterator) {
			return executeJob(StreamSupport.stream((Spliterator<?>) job, true));
		}
		else if (job instanceof Stream) {
			return ((Stream<?>)job).map(t -> {
					try {
						return executeJob(t);
					}
					catch (EolRuntimeException ex) {
						getContext().handleException(ex, null);
						throw new RuntimeException(ex);
					}
				})
				.filter(c -> c != null)
				.flatMap(Collection::stream)
				.collect(Collectors.toList());
		}
		else if (job instanceof BaseStream) {
			return executeJob(((BaseStream<?,?>)job).iterator());
		}
		else if (job instanceof SerializableEvlResultAtom) {
			return Collections.singletonList((SerializableEvlResultAtom) job);
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
	
	public List<DistributedEvlBatch> getBatches(double batchPercent) throws EolRuntimeException {
		final int numTotalJobs = getContextJobs().size();
		return DistributedEvlBatch.getBatches(numTotalJobs, (int) (numTotalJobs * batchPercent));
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
	protected Collection<SerializableEvlResultAtom> execute(final DistributedEvlBatch batch) throws EolRuntimeException {
		return executeJob(batch.split(getContextJobs()));
	}
	
	protected Collection<SerializableEvlResultAtom> execute(final SerializableEvlInputAtom atom) throws EolRuntimeException {
		return atom.execute(this);
	}
	
	protected Collection<SerializableEvlResultAtom> execute(final ConstraintContextAtom atom) throws EolRuntimeException {
		return serializeResults(atom.executeWithResults(getContext()));
	}
	
	protected Collection<SerializableEvlResultAtom> execute(final ConstraintAtom atom) throws EolRuntimeException {
		atom.execute(getContext());
		return null;
	}
	
	protected Collection<SerializableEvlResultAtom> serializeResults(Collection<UnsatisfiedConstraint> unsatisfiedConstraints) {
		EvlContextDistributed context = getContext();
		return unsatisfiedConstraints.parallelStream()
			.map(uc -> SerializableEvlResultAtom.serializeResult(uc, context))
			.collect(Collectors.toCollection(ArrayList::new));
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
