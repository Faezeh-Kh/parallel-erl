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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlFlinkRichFunction;

/**
 * Executes this worker's batch of jobs and collects the results.
 * 
 * @author Sina Madani
 * @since 1.6
 */
class EvlFlinkSubsetFlatMapFunction extends EvlFlinkRichFunction implements FlatMapFunction<DistributedEvlBatch, SerializableEvlResultAtom> {

	private static final long serialVersionUID = 8491311327811474665L;

	@Override
	public void flatMap(DistributedEvlBatch batch, Collector<SerializableEvlResultAtom> out) throws Exception {
		localModule.execute(batch).forEach(out::collect);
	}
}
