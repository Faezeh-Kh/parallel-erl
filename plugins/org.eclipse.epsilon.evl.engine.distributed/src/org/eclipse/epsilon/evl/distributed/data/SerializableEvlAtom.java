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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.epsilon.eol.exceptions.models.EolModelNotFoundException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class SerializableEvlAtom implements java.io.Serializable, Cloneable {
	
	private static final long serialVersionUID = 7643563897928949494L;
	
	public String modelElementID, modelName, contextName;

	@Override
	protected abstract SerializableEvlAtom clone();
	
	@Override
	public int hashCode() {
		return Objects.hash(getClass(), modelElementID, modelName, contextName);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		
		SerializableEvlAtom other = (SerializableEvlAtom) obj;
		return
			Objects.equals(this.modelElementID, other.modelElementID) &&
			Objects.equals(this.modelName, other.modelName) &&
			Objects.equals(this.contextName, other.contextName);
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+": modelElementID="
				+ modelElementID + ", modelName=" + modelName
				+ ", contextType = " + contextName;
	}
	
	public Object findElement(IEolContext context) throws EolModelNotFoundException {
		return context.getModelRepository().getModelByName(modelName).getElementById(modelElementID);
	}
	
	/**
	 * Splits the list into the specified number of partitions.
	 * @param jobs The jobs to split.
	 * @param numPartitions How many splits.
	 * @return The batches.
	 */
	public static <S extends SerializableEvlAtom> Collection<List<S>> split(final List<S> jobs, final int numPartitions) {
		int jobSize = jobs.size();
		
		if (jobSize <= 0) {
			return Collections.emptyList();
		}
		else if (numPartitions < 2) {
			return Collections.singleton(jobs);
		}
		else if (numPartitions >= jobSize) {
			return jobs.stream()
				.map(Collections::singletonList)
				.collect(Collectors.toList());
		}
		
		final int fullChunks = jobSize / numPartitions;
		
		return IntStream.range(0, numPartitions)
			.mapToObj(i -> jobs.subList(
				i * fullChunks, 
				i == numPartitions - 1 ? jobSize : (i + 1) * fullChunks)
			)
			.collect(Collectors.toList());
	}

}
