package org.eclipse.epsilon.evl.distributed.crossflow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
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
		
		Serializable result = evaluateJob(validationData.data);
		if (result instanceof Iterable) {
			for (SerializableEvlResultAtom obj : (Iterable<SerializableEvlResultAtom>) result) {
				sendToValidationOutput(new ValidationResult(obj));
			}
		}
	}

	Serializable evaluateJob(Object msgObj) throws EolRuntimeException {
		if (msgObj instanceof Iterable) {
			return evaluateJob(((Iterable<?>) msgObj).iterator());
		}
		else if (msgObj instanceof Iterator) {
			ArrayList<SerializableEvlResultAtom> resultsCol = new ArrayList<>();
			for (Iterator<?> iter = (Iterator<?>) msgObj; iter.hasNext();) {
				Object obj = iter.next();
				if (obj instanceof SerializableEvlInputAtom) {
					resultsCol.addAll(((SerializableEvlInputAtom) obj).evaluate(slaveModule));
				}
				else if (obj instanceof DistributedEvlBatch) {
					resultsCol.addAll(slaveModule.evaluateBatch((DistributedEvlBatch) obj));
				}
			}
			return resultsCol;
		}
		if (msgObj instanceof SerializableEvlInputAtom) {
			return (Serializable)((SerializableEvlInputAtom) msgObj).evaluate(slaveModule);
		}
		else if (msgObj instanceof DistributedEvlBatch) {
			return (Serializable) slaveModule.evaluateBatch((DistributedEvlBatch) msgObj);
		}
		else if (msgObj instanceof java.util.stream.BaseStream) {
			return evaluateJob(((java.util.stream.BaseStream<?,?>)msgObj).iterator());
		}
		else {
			throw new IllegalArgumentException("Received unexpected object of type "+msgObj.getClass().getName());
		}
	}
}