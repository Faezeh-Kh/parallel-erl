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
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * Base implementation of EVL with distributed execution semantics.
 * Splitting is supported at the element-level granularity. The {@link #checkConstraints()}
 * method initiates the distributed processing; which in turn should spawn instances of
 * {@link EvlModuleDistributedSlave}. If a data sink is used (i.e.the results can be
 * acquired by this module as they appear), the 
 * {@link SerializableEvlResultAtom#deserializeEager(org.eclipse.epsilon.evl.IEvlModule)} 
 * method can be used to rebuild the unsatisfied constraints and apply them to the context. Otherwise if
 * the processing is blocking (i.e. the master must wait for all results to become available), then
 * {@linkplain #assignDeserializedResults(Stream)} can be used.
 * 
 * @see {@link EvlModuleDistributedSlave}
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributedMaster extends EvlModuleDistributed {

	public EvlModuleDistributedMaster(int parallelism) {
		super(parallelism);
		setContext(new EvlContextDistributedMaster(0, parallelism));
	}

	@Override
	protected void prepareContext() {
		getContext().storeInitialVariables();
		super.prepareContext();
	}
	
	@Override
	protected abstract void checkConstraints() throws EolRuntimeException;
	
	/**
	 * Resolves the serialized unsatisfied constraints lazily.
	 * 
	 * @param serializedResults The serialized UnsatisfiedConstraint instances.
	 * @return A Collection of lazily resolved UnsatisfiedConstraints.
	 */
	protected Collection<LazyUnsatisfiedConstraint> deserializeLazy(Iterable<SerializableEvlResultAtom> serializedResults) {
		Collection<LazyUnsatisfiedConstraint> results = serializedResults instanceof Collection ?
			new ArrayList<>(((Collection<?>) serializedResults).size()) : new ArrayList<>();
		
		for (SerializableEvlResultAtom sr : serializedResults) {
			results.add(sr.deserializeLazy(this));
		}
		
		return results;
	}
	
	// TODO refactor to use CompletableFuture
	/**
	 * Executes this worker's jobs in parallel and adds the deserialized results to the
	 * unsatisfied constraints. Execution is done in two stages: first by calling
	 * {@link #evaluateLocal(Object)} and then {@link #deserializeLazy(Iterable)}, both
	 * in parallel.
	 * 
	 * @param jobs The Serializable instances to forward to {@link #evaluateLocal(Object)}
	 * @throws EolRuntimeException
	 */
	protected void executeParallel(Iterable<?> jobs) throws EolRuntimeException {
		EvlContextDistributedMaster context = getContext();
		EolExecutorService executor = context.beginParallelTask(this);
		Collection<Future<?>> evalFutures = jobs instanceof Collection ?
			new ArrayList<>(((Collection<?>) jobs).size()) : new ArrayList<>();
		
		for (Object job : jobs) {
			evalFutures.add(executor.submit(() -> evaluateJob(job)));
		}
		
		Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		
		for (Object intermediate : evalFutures) try {
			@SuppressWarnings("unchecked")
			Collection<SerializableEvlResultAtom> resValues = (Collection<SerializableEvlResultAtom>)
				((Future<Collection<SerializableEvlResultAtom>>) intermediate).get();

			unsatisfiedConstraints.addAll(deserializeLazy(resValues));
		}
		catch (ExecutionException | CancellationException | InterruptedException ex) {
			throw new EolRuntimeException(ex);
		}
		
		context.endParallelTask(this);
	}
	
	/**
	 * Deserializes the results eagerly in parallel using this context's ExecutorService.
	 * @param results The serialized results.
	 * @param eager Whether to fully resolve each UnsatisfiedConstraint.
	 * @return The deserialized UnsatisfiedConstraints.
	 * @throws EolRuntimeException
	 */
	protected Collection<UnsatisfiedConstraint> deserializeEager(Iterable<? extends SerializableEvlResultAtom> results) throws EolRuntimeException {
		EvlContextDistributedMaster context = getContext();
		ArrayList<Callable<UnsatisfiedConstraint>> jobs = results instanceof Collection ?
			new ArrayList<>(((Collection<?>)results).size()) : new ArrayList<>();
		
		for (SerializableEvlResultAtom sera : results) {
			jobs.add(() -> sera.deserializeEager(this));
		}
		
		return context.executeParallelTyped(null, jobs);
	}
	
	/**
	 * Deserializes the object if it is a valid result type and adds it to
	 * the unsatisfied constraints.
	 * 
	 * @param reponse The serializable result object.
	 * @return Whether the object was a valid result
	 * @throws EolRuntimeException
	 */
	@SuppressWarnings("unchecked")
	protected boolean deserializeResults(Object response) throws EolRuntimeException {
		if (response instanceof Iterable) {
			Iterable<SerializableEvlResultAtom> srIter;
			try {
				srIter = (Iterable<SerializableEvlResultAtom>) response;
			}
			catch (ClassCastException ccx) {
				return false;
			}
			getContext().getUnsatisfiedConstraints().addAll(deserializeLazy(srIter));
			return true;
		}
		else if (response instanceof Iterator) {
			java.util.function.Supplier<Iterator<Object>> iterSup = () -> (Iterator<Object>) response;
			return deserializeResults((Iterable<Object>) iterSup::get);
		}
		else if (response instanceof SerializableEvlResultAtom) {
			getContext().getUnsatisfiedConstraints().add(((SerializableEvlResultAtom) response).deserializeEager(this));
			return true;
		}
		else if (response instanceof java.util.stream.BaseStream<?,?>) {
			return deserializeResults(((java.util.stream.BaseStream<?,?>) response).iterator());
		}
		else return false;
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
