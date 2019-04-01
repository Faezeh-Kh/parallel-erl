package org.eclipse.epsilon.evl.distributed.crossflow;

import java.util.List;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

public class JobDistributor extends JobDistributorBase {
	
	@Override
	public void consumeConfigTopic(Config config) throws Exception {
		final EvlModuleDistributedMaster module = workflow.getConfigurationSource().masterModule;
		final int batchSize = 100;
		final List<ConstraintContextAtom> ccJobs = ConstraintContextAtom.getContextJobs(module);
		final List<DistributedEvlBatch> batches = DistributedEvlBatch.getBatches(ccJobs.size(), batchSize);
		
		for (DistributedEvlBatch batch : batches.subList(0, batchSize-1)) {
			sendToValidationDataQueue(new ValidationData(batch));
		}
	}
}