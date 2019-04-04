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

import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;

/**
 * Master-bare only
 * 
 * @author Sina Madani
 */
public class Processing extends ProcessingBase {
	
	DistributedEvlRunConfiguration configuration;
	EvlModuleDistributedSlave slaveModule;
	
	@SuppressWarnings("unchecked")
	@Override
	public void consumeConfigTopic(Config config) throws Exception {
		while (configuration == null) synchronized (this) {
			configuration = EvlContextDistributedSlave.parseJobParameters(
				config.data,
				"C:/Users/Sina-/Google Drive/PhD/Experiments/"
			);
			slaveModule = (EvlModuleDistributedSlave) configuration.getModule();
			notify();
		}
	}
	
	@Override
	public void consumeValidationDataQueue(ValidationData validationData) throws Exception {
		while (configuration == null) synchronized (this) {
			wait();
		}
		
		slaveModule.executeJob(validationData.data)
			.stream()
			.map(ValidationResult::new)
			.forEach(this::sendToValidationOutput);
	}
}