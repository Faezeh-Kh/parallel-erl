package org.eclipse.epsilon.evl.distributed.crossflow;

public class ConfigurationSource extends ConfigurationSourceBase {
	
	EvlModuleDistributedMasterCrossflow masterModule;
	
	@Override
	public void produce() throws Exception {
		sendToConfigTopic(new Config(masterModule.getContext().getJobParameters()));
	}

}