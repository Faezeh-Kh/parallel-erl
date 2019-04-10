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
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class Processing extends ProcessingBase {
	
	final boolean isMaster = workflow.isMaster();
	DistributedEvlRunConfigurationSlave configuration;
	EvlModuleDistributedSlave slaveModule;
	
	@SuppressWarnings("unchecked")
	@Override
	public void consumeConfigTopic(Config config) throws Exception {
		if (isMaster) return;
		
		assert workflow.isWorker();
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
		while (!isMaster && configuration == null) synchronized (this) {
			wait();
		}
		
		final java.io.Serializable job = validationData.data;
		
		if (isMaster) {
			workflow.configurationSource.masterModule.executeJob(job);
		}
		else {
			assert workflow.isWorker() && slaveModule != null;
			Collection<SerializableEvlResultAtom> results = slaveModule.executeJob(job);
			if (results != null) {
				sendToValidationOutput(new ValidationResult(results));
			}
		}
	}
}