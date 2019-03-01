/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.epl.execute.context.concurrent;

import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolForkJoinExecutor;
import org.eclipse.epsilon.epl.IEplModule;
import org.eclipse.epsilon.erl.execute.context.concurrent.ErlContextParallel;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EplContextParallel extends ErlContextParallel {

	public EplContextParallel() {
		this(0);
	}

	public EplContextParallel(int parallelism) {
		super(parallelism);
	}

	@Override
	public EolForkJoinExecutor newExecutorService() {
		return new EolForkJoinExecutor(getParallelism());
	}
	
	@Override
	public IEplModule getModule() {
		return (IEplModule) super.getModule();
	}
	
	@Override
	public void setModule(IModule module) {
		if (module instanceof IEplModule) {
			super.setModule(module);
		}
	}
}
