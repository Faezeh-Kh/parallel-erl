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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * Sends jobs to workers as soon as they are available, instead of waiting
 * for all workers to be configured before processing.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedMasterJMSAsync extends EvlModuleDistributedMasterJMS {

	public static void main(String... args) throws Exception {
		EvlModuleDistributedMasterJMS.extensibleMain(EvlModuleDistributedMasterJMSAsync.class, args);
	}
	
	List<ConstraintContextAtom> jobs;
	List<DistributedEvlBatch> batches;
	
	public EvlModuleDistributedMasterJMSAsync(int expectedWorkers, String host) throws URISyntaxException {
		super(expectedWorkers, host);
	}

	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		final EvlContextDistributedMaster evlContext = getContext();
		final int parallelism = evlContext.getDistributedParallelism()+1;
		jobs = ConstraintContextAtom.getContextJobs(this);
		batches = DistributedEvlBatch.getBatches(jobs, parallelism);
	}
	
	@Override
	protected void confirmWorker(WorkerView worker, JMSContext session, AtomicInteger workersReady) throws JMSException {
		worker.confirm(session);
		worker.sendJob(batches.get(workersReady.incrementAndGet()), true);
		log("Sent job to "+worker);
	}
	
	@Override
	protected void processJobs(AtomicInteger readyWorkers, JMSContext jobContext) throws Exception {
		EvlContextDistributedMaster evlContext = getContext();
		log("Began processing own jobs");
		for (ConstraintContextAtom cca : batches.get(0).split(jobs)) {
			cca.execute(evlContext);
		}
		log("Finished processing own jobs");
	}
}
