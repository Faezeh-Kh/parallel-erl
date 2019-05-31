package org.eclipse.epsilon.evl.distributed.crossflow;

public class ConfigConfigSource extends ConfigConfigSourceBase {
	
	public EvlModuleDistributedMasterCrossflow masterModule;

	@Override
	public void produce() throws Exception {
		sendToConfigTopic(new Config(masterModule.getContext().getJobParameters(false)));
	}


}
