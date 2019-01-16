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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleDistributedFlink;

/**
 * This distribution strategy splits the model elements into a preset
 * number of batches based on the parallelism, so that each worker knows
 * exactly which elements to evaluate in advance. The only data sent
 * over the wire as input is the start and end index of the jobs for
 * each node. It is expected that the distribution runtime algorithm
 * sends each batch to a different node.
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
	protected DataSet<SerializableEvlResultAtom> getProcessingPipeline(ExecutionEnvironment execEnv) throws Exception {
		return execEnv.fromCollection(DistributedEvlBatch.getBatches(getContext())).flatMap(new EvlFlinkSubsetFlatMapFunction());
	}
}
