package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.scava.crossflow.runtime.FailedJob;
import org.eclipse.scava.crossflow.runtime.Task;
import org.eclipse.scava.crossflow.runtime.Workflow;

public abstract class JobDistributorBase extends Task  implements ConfigTopicConsumer{
		
	protected DistributedEVL workflow;
	
	public void setWorkflow(DistributedEVL workflow) {
		this.workflow = workflow;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	
	public String getId(){
		return "JobDistributor:"+workflow.getName();
	}
	
	protected ValidationDataQueue validationDataQueue;
	
	protected void setValidationDataQueue(ValidationDataQueue validationDataQueue) {
		this.validationDataQueue = validationDataQueue;
	}
	
	protected ValidationDataQueue getValidationDataQueue() {
		return validationDataQueue;
	}
	
	public void sendToValidationDataQueue(ValidationData validationData) {
		validationData.setCacheable(this.cacheable);
		hasSentToValidationDataQueue = true;
		getValidationDataQueue().send(validationData, this.getClass().getName());
	}
	
	boolean hasSentToValidationDataQueue = false;
	
	@Override
	public final void consumeConfigTopicWithNotifications(Config config) {
		
		try {
			workflow.getJobDistributors().getSemaphore().acquire();
		} catch (Exception e) {
			workflow.reportInternalException(e);
		}
				
		hasSentToValidationDataQueue = false;
				
		Runnable consumer = () -> {		
			try {
				workflow.setTaskInProgess(this);

				consumeConfigTopic(config);

				ValidationData conf = new ValidationData();
				conf.setCorrelationId(config.getId());
				conf.setIsTransactionSuccessMessage(true);
				conf.setTotalOutputs((hasSentToValidationDataQueue ? 1 : 0));
				if (hasSentToValidationDataQueue) {
					sendToValidationDataQueue(conf);
				}
		


			} catch (Exception ex) {
				try {
					config.setFailures(config.getFailures()+1);
					workflow.getFailedJobsQueue().send(new FailedJob(config, ex, workflow.getName(), "JobDistributor"));
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			} finally {
				try {
					workflow.getJobDistributors().getSemaphore().release();
					workflow.setTaskWaiting(this);
				} catch (Exception e) {
					workflow.reportInternalException(e);
				}
			}
		
		};

		workflow.getJobDistributors().getExecutor().submit(consumer);
	}
	
	public abstract void consumeConfigTopic(Config config) throws Exception;
	

	
	
}

