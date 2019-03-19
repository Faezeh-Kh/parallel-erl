/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.atomic;

import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.JMSContext;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedMasterJMSAtomicSynch extends EvlModuleDistributedMasterJMS {
	
	public EvlModuleDistributedMasterJMSAtomicSynch(int expectedWorkers, String host) throws URISyntaxException {
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
		final List<SerializableEvlInputAtom> jobs = SerializableEvlInputAtom.createJobs(
			getConstraintContexts(), evlContext, false
		);
		final int parallelism = evlContext.getDistributedParallelism()+1;
		final int selfBatch = jobs.size() / parallelism;
		
		assert slaveWorkers.size() == expectedSlaves;
		assert expectedSlaves == parallelism-1;
		
		for (SerializableEvlInputAtom jobAtom : jobs.subList(selfBatch, jobs.size())) {
			sendJob(jobAtom);
		}
		signalCompletion();
		
		log("Began processing own jobs");
		
		for (SerializableEvlInputAtom jobAtom : jobs.subList(0, selfBatch)) {
			addToResults(jobAtom.evaluate(this));
		}
		
		log("Finished processing own jobs");
	}
}
