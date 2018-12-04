/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.batch;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleDistributedFlink;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedFlinkSubset extends EvlModuleDistributedFlink {

	public EvlModuleDistributedFlinkSubset() {
		super();
	}
	public EvlModuleDistributedFlinkSubset(int parallelism) {
		super(parallelism);
	}

	@Override
	protected void processDistributed(ExecutionEnvironment execEnv, Configuration jobConfig) throws Exception {
		assignConstraintsFromResults(
			execEnv.fromCollection(getBatches())
			.flatMap(new EvlFlinkSubsetFlatMapFunction())
			.collect()
			.parallelStream()
		);
	}
	private Collection<DistributedEvlBatch> getBatches() throws EolRuntimeException {
		final int batchSize = getContext().getDistributedParallelism(),
			totalJobs = ConstraintContextAtom.getContextJobs(getContext()).size(),
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
