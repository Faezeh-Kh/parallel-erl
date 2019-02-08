/*********************************************************************
 * Copyright (c) 2018-2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.data;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.concurrent.ThreadLocalBatchData;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;

/**
 * Simple over-the-wire input for telling each node the start and end indexes
 * of their batch to process.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlBatch implements java.io.Serializable, Cloneable {

	private static final long serialVersionUID = 7612999545641549495L;

	public int from, to;
	
	@Override
	protected DistributedEvlBatch clone() {
		DistributedEvlBatch clone = new DistributedEvlBatch();
		clone.from = this.from;
		clone.to = this.to;
		return clone;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(from, to);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof DistributedEvlBatch)) return false;
		DistributedEvlBatch other = (DistributedEvlBatch) obj;
		return this.from == other.from && this.to == other.to;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+": from="+from+", to="+to;
	}
	
	/**
	 * Splits the jobs into batches based on the parallelism.
	 * @param context
	 * @return The serializable start and end indexes for the batches.
	 * @throws EolRuntimeException
	 */
	public static List<DistributedEvlBatch> getBatches(EvlModuleDistributedMaster module) throws EolRuntimeException {
		// Plus one because master itself is also included as a worker
		final int batchSize = module.getContext().getDistributedParallelism()+1,
			totalJobs = ConstraintContextAtom.getContextJobs(module).size(),
			increments = totalJobs / batchSize;
		
		return IntStream.range(0, batchSize)
			.mapToObj(i -> {
				DistributedEvlBatch batch = new DistributedEvlBatch();
				batch.from = i*increments;
				batch.to = (i+1)*increments;
				return batch;
			})
			.collect(Collectors.toList());
	}
	
	public Collection<SerializableEvlResultAtom> evaluate(List<ConstraintContextAtom> jobList, EvlContextParallel context) throws EolRuntimeException {
		EolExecutorService executor = context.beginParallelTask(null);
		ThreadLocalBatchData<SerializableEvlResultAtom> results = new ThreadLocalBatchData<>(context.getParallelism());
		
		for (ConstraintContextAtom job : jobList.subList(from, to)) {
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
}
