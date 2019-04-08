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

import java.util.Collection;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationSlave;

/**
 * Master-bare only
 * 
 * @author Sina Madani
 */
public class Processing extends ProcessingBase {
	
	DistributedEvlRunConfigurationSlave configuration;
	EvlModuleDistributedSlave slaveModule;
	
	@SuppressWarnings("unchecked")
	@Override
	public void consumeConfigTopic(Config config) throws Exception {
		while (configuration == null) synchronized (this) {
			configuration = EvlContextDistributedSlave.parseJobParameters(
				config.data,
				"C:/Users/Sina-/Google Drive/PhD/Experiments/"
			);
			slaveModule = configuration.getModule();
			notify();
		}
	}
	
	@Override
	public void consumeValidationDataQueue(ValidationData validationData) throws Exception {
		while (configuration == null) synchronized (this) {
			wait();
		}
		
		Collection<SerializableEvlResultAtom> results = slaveModule.executeJob(validationData.data);
		if (results != null && !workflow.isMaster()) for (SerializableEvlResultAtom resultAtom : results) {
			sendToValidationOutput(new ValidationResult(resultAtom));
		}
	}
}