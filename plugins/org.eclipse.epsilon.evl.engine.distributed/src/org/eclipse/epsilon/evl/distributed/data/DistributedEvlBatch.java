/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.data;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * Simple over-the-wire input for telling each node the start and end indexes
 * of their batch to process.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlBatch implements java.io.Serializable, Cloneable {
	
	private static final long serialVersionUID = -6635227506532865240L;
	
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
	public static List<DistributedEvlBatch> getBatches(EvlContextDistributedMaster context) throws EolRuntimeException {
		final int batchSize = context.getDistributedParallelism(),
			totalJobs = ConstraintContextAtom.getContextJobs(context).size(),
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
}
