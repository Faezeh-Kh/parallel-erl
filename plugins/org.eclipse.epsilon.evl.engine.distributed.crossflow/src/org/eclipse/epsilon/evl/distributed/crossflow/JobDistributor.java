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

import java.util.List;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;

public class JobDistributor extends JobDistributorBase {
	
	@Override
	public void consumeConfigTopic(Config config) throws Exception {
		final EvlModuleDistributedMaster module = workflow.getConfigurationSource().masterModule;
		final int batchSize = 100;
		final List<DistributedEvlBatch> batches = module.getBatches(batchSize);
		
		for (DistributedEvlBatch batch : batches.subList(0, batchSize-1)) {
			sendToValidationDataQueue(new ValidationData(batch));
		}
	}
}