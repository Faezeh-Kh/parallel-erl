package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;

public class ConfigurationSource extends ConfigurationSourceBase {
	
	public EvlModuleDistributedMaster masterModule;
	
	@Override
	public void produce() throws Exception {
		sendToConfigTopic(new Config(masterModule.getContext().getJobParameters()));
	}

}