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
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * Batch-based approach
 * 
 * @see DistributedEvlBatch
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedMasterJMSBatch extends EvlModuleDistributedMasterJMS {
	
	protected final int batchesPerWorker;
	
	public EvlModuleDistributedMasterJMSBatch(int expectedWorkers, int bpw, String host, int sessionID) throws URISyntaxException {
		super(expectedWorkers, host, sessionID);
		this.batchesPerWorker = bpw > 0 ? bpw : 1;
	}
	
	@Override
	protected void processJobs(AtomicInteger workersReady) throws Exception {
		waitForWorkersToConnect(workersReady);
		
		final EvlContextDistributedMaster evlContext = getContext();
		final int batchSize = 1 + (expectedSlaves * batchesPerWorker);
		final List<ConstraintContextAtom> ccJobs = ConstraintContextAtom.getContextJobs(this);
		final List<DistributedEvlBatch> batches = DistributedEvlBatch.getBatches(ccJobs.size(), batchSize);
		
		assert expectedSlaves == evlContext.getDistributedParallelism();
		assert slaveWorkers.size() == expectedSlaves;
		
		for (DistributedEvlBatch batch : batches.subList(0, batchSize-1)) {
			sendJob(batch);
		}
		signalCompletion();
		
		log("Began processing own jobs");
		
		batches.get(batchSize-1).split(ccJobs)
			.parallelStream()
			.forEach(cca -> {
				try {
					cca.execute(evlContext);
				}
				catch (EolRuntimeException ex) {
					evlContext.handleException(ex);
				}
			});

		log("Finished processing own jobs");
	}
}
