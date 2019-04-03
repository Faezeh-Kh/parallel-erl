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
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedMasterJMSAtomic extends EvlModuleDistributedMasterJMS {

	public EvlModuleDistributedMasterJMSAtomic(int expectedWorkers, String host, int sessionID) throws URISyntaxException {
		super(expectedWorkers, host, sessionID);
	}

	@Override
	protected void processJobs(AtomicInteger workersReady) throws Exception {
		waitForWorkersToConnect(workersReady);
		
		final List<SerializableEvlInputAtom> jobs = SerializableEvlInputAtom.createJobs(this, false);
		final int parallelism = expectedSlaves + 1;
		final int selfBatch = jobs.size() / parallelism;
		
		assert slaveWorkers.size() == expectedSlaves;
		assert expectedSlaves == getContext().getDistributedParallelism();
		
		sendAllJobsAsync(jobs.subList(selfBatch, jobs.size()));
		
		log("Began processing own jobs");
		executeParallel(jobs.subList(0, selfBatch));
		log("Finished processing own jobs");
	}
}
