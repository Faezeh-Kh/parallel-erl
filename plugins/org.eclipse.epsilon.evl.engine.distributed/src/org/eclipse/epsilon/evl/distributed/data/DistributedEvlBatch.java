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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Simple over-the-wire input for telling each node the start and end indexes
 * of their batch to process.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlBatch implements java.io.Serializable, Cloneable {
	
	private static final long serialVersionUID = 6660450310143565940L;
	
	public int from, to;
	
	@Override
	protected DistributedEvlBatch clone() {
		DistributedEvlBatch clone;
		try {
			clone = (DistributedEvlBatch) super.clone();
		}
		catch (CloneNotSupportedException cnsx) {
			throw new UnsupportedOperationException(cnsx);
		}
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
	 * Provides a List of indices based on the desired split size.
	 * 
	 * @param totalJobs The size of the source List being split
	 * @param batchSize The number of batches (i.e. the size of the returned List).
	 * @return A List of indexes with {@code totalJobs/batches} increments.
	 */
	public static List<DistributedEvlBatch> getBatches(int totalJobs, int batches) {
		if (batches <= 1) {
			DistributedEvlBatch batch = new DistributedEvlBatch();
			batch.from = 0;
			batch.to = totalJobs;
			return Collections.singletonList(batch);
		}
		final int increments = batches < totalJobs ? totalJobs / batches : 1;
		
		return IntStream.range(0, batches)
			.mapToObj(i -> {
				DistributedEvlBatch batch = new DistributedEvlBatch();
				batch.from = i*increments;
				batch.to = (i+1)*increments;
				return batch;
			})
			.collect(Collectors.toList());
	}

	public <T> List<T> splitToList(T[] arr) {
		return Arrays.asList(split(arr));
	}
	public <T> T[] split(T[] arr) {
		return Arrays.copyOfRange(arr, from, to);
	}
	
	/**
	 * Splits the given list based on this class's indices.
	 * @param <T> The type of the List
	 * @param list The list to call {@link List#subList(int, int)} on
	 * @return The split list.
	 */
	public <T> List<T> split(List<T> list) {
		return list.subList(from, to);
	}
}
