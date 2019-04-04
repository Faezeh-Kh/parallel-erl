/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.batch;

import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.JMSException;
import javax.jms.Message;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * Sends jobs to workers as soon as they are available, instead of waiting
 * for all workers to be configured before processing.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedMasterJMSBatchAsync extends EvlModuleDistributedMasterJMSBatch {

	List<ConstraintContextAtom> jobs;
	List<DistributedEvlBatch> batches;
	
	public EvlModuleDistributedMasterJMSBatchAsync(int expectedWorkers, int bpw, String host, int sessionID) throws URISyntaxException {
		super(expectedWorkers, bpw, host, sessionID);
	}

	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		final int batchSize = 1 + (expectedSlaves * batchesPerWorker);
		jobs = ConstraintContextAtom.getContextJobs(this);
		batches = DistributedEvlBatch.getBatches(jobs.size(), batchSize);
	}
	
	@Override
	protected void confirmWorker(Message confirmation, AtomicInteger workersReady) throws JMSException {
		int batchNum = workersReady.incrementAndGet();
		sendJob(batches.get(batchNum));
		if (batchNum >= expectedSlaves * batchesPerWorker) {
			signalCompletion();
		}
	}
	
	@Override
	protected void processJobs(AtomicInteger workersReady) throws Exception {
		log("Began processing own jobs");
		executeParallel(batches.get(0).split(jobs));
		log("Finished processing own jobs");
	}
}
