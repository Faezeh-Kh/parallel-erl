/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms;

import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.JMSContext;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * Waits for all workers to be configured before processing.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedMasterJMSSynced extends EvlModuleDistributedMasterJMS {

	public static void main(String... args) throws Exception {
		EvlModuleDistributedMasterJMS.extensibleMain(EvlModuleDistributedMasterJMSSynced.class, args);
	}
	
	public EvlModuleDistributedMasterJMSSynced(int expectedWorkers, String host) throws URISyntaxException {
		super(expectedWorkers, host);
	}
	
	@Override
	protected void processJobs(AtomicInteger readyWorkers, JMSContext jobContext) throws Exception {
		// Await workers
		while (readyWorkers.get() < expectedSlaves) synchronized (readyWorkers) {
			readyWorkers.wait();
		}
		log("All workers connected");
		
		final EvlContextDistributedMaster evlContext = getContext();
		final int parallelism = evlContext.getDistributedParallelism()+1;
		final List<ConstraintContextAtom> ccJobs = ConstraintContextAtom.getContextJobs(this);
		final List<DistributedEvlBatch> batches = DistributedEvlBatch.getBatches(ccJobs, parallelism);
		assert slaveWorkers.size() == expectedSlaves;
		assert slaveWorkers.size() == batches.size()-1;
		
		Iterator<WorkerView> workersIter = slaveWorkers.iterator();
		Iterator<DistributedEvlBatch> batchesIter = batches.iterator();
		
		while (workersIter.hasNext()) {
			WorkerView worker = workersIter.next();
			worker.sendJob(batchesIter.next(), true);
			log("Finished submitting to "+worker);
		}
		
		log("Began processing own jobs");
		
		assert batchesIter.hasNext();
		for (ConstraintContextAtom cca : batchesIter.next().split(ccJobs)) {
			cca.execute(evlContext);
		}
		
		log("Finished processing own jobs");
	}
}
