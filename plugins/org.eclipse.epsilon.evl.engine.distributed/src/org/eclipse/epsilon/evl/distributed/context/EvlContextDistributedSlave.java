/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.context;

import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolThreadPoolExecutor;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedSlave extends EvlContextParallel {
	
	public EvlContextDistributedSlave(int localParallelism) {
		super(localParallelism, false);
	}
	
	@Override
	public EolExecutorService newExecutorService() {
		return EolThreadPoolExecutor.adaptiveExecutor(numThreads);
	}
}
