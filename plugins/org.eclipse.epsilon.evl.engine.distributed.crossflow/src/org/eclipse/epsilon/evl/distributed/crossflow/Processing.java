package org.eclipse.epsilon.evl.distributed.crossflow;

import java.io.Serializable;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;

/**
 * Master-bare only
 * 
 * @author Sina Madani
 */
public class Processing extends ProcessingBase {
	
	public DistributedEvlRunConfiguration configuration;
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
		
		Serializable result = slaveModule.evaluateJob(validationData.data);
		if (result instanceof Iterable) {
			for (Object obj : (Iterable<?>) result) {
				if (obj instanceof SerializableEvlResultAtom) {
					sendToValidationOutput(new ValidationResult((SerializableEvlResultAtom) obj));
				}
			}
		}
		else if (result instanceof SerializableEvlResultAtom) {
			sendToValidationOutput((ValidationResult) result);
		}
	}
}