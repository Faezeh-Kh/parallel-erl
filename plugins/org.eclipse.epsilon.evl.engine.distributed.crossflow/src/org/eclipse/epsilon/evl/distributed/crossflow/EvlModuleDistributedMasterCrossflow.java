/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.scava.crossflow.runtime.Mode;

public class EvlModuleDistributedMasterCrossflow extends EvlModuleDistributedMaster {

	public EvlModuleDistributedMasterCrossflow() {
		super(-1);
	}

	@Override
	protected void checkConstraints() throws EolRuntimeException {
		try {
			DistributedEVL crossflow = new DistributedEVL(Mode.MASTER_BARE);
			crossflow.setInstanceId("DistributedEVL");
			//
			crossflow.getConfigurationSource().masterModule = this;
			//
			crossflow.run();
			
			// TODO proper condition
			while (!crossflow.hasTerminated()) {
				Thread.sleep(1000);
			}

			//
			crossflow.getResultSink();
		}
		catch (Exception ex) {
			throw new EolRuntimeException(ex);
		}
	}
}
