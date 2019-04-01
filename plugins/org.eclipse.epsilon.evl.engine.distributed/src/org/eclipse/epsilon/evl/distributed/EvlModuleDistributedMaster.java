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
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.function.CheckedEolFunction;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * Base implementation of EVL with distributed execution semantics.
 * Splitting is supported at the element-level granularity. The {@link #checkConstraints()}
 * method initiates the distributed processing; which in turn should spawn instances of
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
	
	protected void addToResults(Iterable<SerializableEvlResultAtom> serializedResults) throws EolRuntimeException {
		Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		for (SerializableEvlResultAtom sr : serializedResults) {
			unsatisfiedConstraints.add(sr.deserializeResult(this));
		}
	}
	
	/**
	 * Executes this worker's jobs in parallel and adds the deserialized results to the
	 * unsatisfied constraints. Execution is done in two stages: first by calling
	 * {@link #evaluateLocal(Object)} and then {@link #addToResults(Iterable)}, both
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
			evalFutures.add(executor.submit(() -> evaluateLocal(job)));
		}
		
		Collection<Future<?>> resultFutures = new ArrayList<>(evalFutures.size());
		for (Object intermediate : evalFutures) try {
			@SuppressWarnings("unchecked")
			Collection<SerializableEvlResultAtom> resValues = (Collection<SerializableEvlResultAtom>)
				((Future<Collection<SerializableEvlResultAtom>>) intermediate).get();
			
			resultFutures.add(executor.submit(() -> {
				try {
					addToResults(resValues);
				}
				catch (EolRuntimeException exception) {
					context.handleException(exception);
				}
			}));
		}
		catch (ExecutionException | CancellationException | InterruptedException ex) {
			throw new EolRuntimeException(ex);
		}
		
		executor.awaitCompletion(resultFutures);
		context.endParallelTask(this);
	}
	
	/**
	 * Deserializes the results in parallel using this context's ExecutorService.
	 * @param results The serialized results.
	 * @return The deserialized UnsatisfiedConstraints.
	 * @throws EolRuntimeException
	 */
	protected Collection<UnsatisfiedConstraint> deserializeParallel(Iterable<? extends SerializableEvlResultAtom> results) throws EolRuntimeException {
		EvlContextDistributedMaster context = getContext();
		ArrayList<Callable<UnsatisfiedConstraint>> jobs = results instanceof Collection ?
			new ArrayList<>(((Collection<?>)results).size()) : new ArrayList<>();
		
		for (SerializableEvlResultAtom sera : results) {
			jobs.add(() -> sera.deserializeResult(this));
		}
		
		return context.executeParallelTyped(null, jobs);
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
	
	/**
	 * Deserializes the object if it is a valid result type and adds it to
	 * the unsatisfied constraints.
	 * 
	 * @param reponse The serializable result object.
	 * @return Whether the object was a valid result
	 * 
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
			getContext().getUnsatisfiedConstraints().addAll(deserializeParallel(srIter));
			return true;
		}
		else if (response instanceof Iterator) {
			java.util.function.Supplier<Iterator<Object>> iterSup = () -> (Iterator<Object>) response;
			return deserializeResults((Iterable<Object>) iterSup::get);
		}
		else if (response instanceof SerializableEvlResultAtom) {
			getContext().getUnsatisfiedConstraints().add(((SerializableEvlResultAtom) response).deserializeResult(this));
			return true;
		}
		else if (response instanceof java.util.stream.BaseStream<?,?>) {
			return deserializeResults(((java.util.stream.BaseStream<?,?>) response).iterator());
		}
		else return false;
	}
	
	/**
	 * Processes the serialized jobs using this module.
	 * 
	 * @param job
	 * @throws EolRuntimeException
	 * @return The serialized results
	 */
	protected Collection<SerializableEvlResultAtom> evaluateLocal(Object job) throws EolRuntimeException {
		if (job instanceof Iterable) {
			return evaluateLocal(((Iterable<?>) job).iterator());
		}
		else if (job instanceof Iterator) {
			Collection<SerializableEvlResultAtom> results = new ArrayList<>();
			for (Iterator<?> it = (Iterator<?>) job; it.hasNext();) {
				results.addAll(evaluateLocal(it.next()));
			}
			return results;
		}
		if (job instanceof SerializableEvlInputAtom) {
			return ((SerializableEvlInputAtom) job).evaluate(this);
		}
		else if (job instanceof DistributedEvlBatch) {
			return ((DistributedEvlBatch) job).evaluate(this);
		}
		else if (job instanceof BaseStream<?,?>) {
			return evaluateLocal(((BaseStream<?,?>) job).iterator());
		}
		else return Collections.emptyList();
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
