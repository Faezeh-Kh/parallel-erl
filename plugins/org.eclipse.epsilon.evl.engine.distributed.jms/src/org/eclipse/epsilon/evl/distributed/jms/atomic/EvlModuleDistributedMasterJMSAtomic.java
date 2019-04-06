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
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

/**
 * Atom-based approach, sending the Serializable ConstraintContext and model element
 * pairs to workers.
 * 
 * @see SerializableEvlInputAtom
 * @see ConstraintContextAtom
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
		
		AtomicJobSplitter splitter = new AtomicJobSplitter(1 / (1 + expectedSlaves), true);
		
		sendAllJobsAsync(splitter.getWorkerJobs()).throwIfPresent();
		
		log("Began processing own jobs");
		executeParallel(splitter.getMasterJobs());
		log("Finished processing own jobs");
	}
}
