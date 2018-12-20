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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlFlinkRichFunction;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;
import org.eclipse.epsilon.evl.execute.context.concurrent.IEvlContextParallel;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
class EvlFlinkSubsetFlatMapFunction extends EvlFlinkRichFunction implements FlatMapFunction<DistributedEvlBatch, SerializableEvlResultAtom> {

	private static final long serialVersionUID = 8491311327811474665L;

	@Override
	public void flatMap(DistributedEvlBatch batch, Collector<SerializableEvlResultAtom> out) throws Exception {
		IEvlContextParallel context = localModule.getContext();
		Collection<ConstraintContextAtom> jobs = ConstraintContextAtom.getContextJobs(context).subList(batch.from, batch.to);
		Collection<Callable<Collection<SerializableEvlResultAtom>>> executorJobs = new ArrayList<>(jobs.size());
		
		for (ConstraintContextAtom job : jobs) {
			executorJobs.add(() -> {
				return job.executeWithResults(context)
					.stream()
					.map(localModule::serializeResult)
					.collect(Collectors.toList());
			});
		}
		
		for (Collection<SerializableEvlResultAtom> resultBatch : context.executeParallelTyped(localModule, executorJobs)) {
			for (SerializableEvlResultAtom resultElement : resultBatch) {
				out.collect(resultElement);
			}
		}
	}
}
