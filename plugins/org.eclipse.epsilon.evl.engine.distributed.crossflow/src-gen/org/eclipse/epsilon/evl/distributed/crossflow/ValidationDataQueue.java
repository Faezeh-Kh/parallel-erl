package org.eclipse.epsilon.evl.distributed.crossflow;

import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQDestination;
import org.eclipse.scava.crossflow.runtime.Workflow;
import org.eclipse.scava.crossflow.runtime.Job;
import org.eclipse.scava.crossflow.runtime.JobStream;

public class ValidationDataQueue extends JobStream<ValidationData> {
		
	public ValidationDataQueue(Workflow workflow, boolean enablePrefetch) throws Exception {
		super(workflow);
		
		ActiveMQDestination postQ;
			pre.put("Processing", (ActiveMQDestination) session.createQueue("ValidationDataQueuePre.Processing." + workflow.getInstanceId()));
			destination.put("Processing", (ActiveMQDestination) session.createQueue("ValidationDataQueueDestination.Processing." + workflow.getInstanceId()));
			postQ = (ActiveMQDestination) session.createQueue("ValidationDataQueuePost.Processing." + workflow.getInstanceId()
					+ (enablePrefetch?"":"?consumer.prefetchSize=1"));		
			post.put("Processing", postQ);			
		
		for (String consumerId : pre.keySet()) {
			ActiveMQDestination preQueue = pre.get(consumerId);
			ActiveMQDestination destQueue = destination.get(consumerId);
			ActiveMQDestination postQueue = post.get(consumerId);
			
			if (workflow.isMaster()) {
				MessageConsumer preConsumer = session.createConsumer(preQueue);
				consumers.add(preConsumer);
				preConsumer.setMessageListener(new MessageListener() {
	
					@Override
					public void onMessage(Message message) {
						try {
							workflow.cancelTermination();
							Job job = (Job) workflow.getSerializer().toObject(((TextMessage) message).getText());
							
							if (workflow.getCache() != null && workflow.getCache().hasCachedOutputs(job)) {
								
								workflow.setTaskInProgess(cacheManagerTask);
								List<Job> cachedOutputs = workflow.getCache().getCachedOutputs(job);
								workflow.setTaskWaiting(cacheManagerTask);
								
								for (Job output : cachedOutputs) {
									if (output.getDestination().equals("ValidationOutput")) {
										workflow.cancelTermination();
										((DistributedEVL) workflow).getValidationOutput().send((ValidationResult) output, consumerId);
									}
									
								}
							} else {
								MessageProducer producer = session.createProducer(destQueue);
								producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
								producer.send(message);
								producer.close();
							}
							
						} catch (Exception ex) {
							workflow.reportInternalException(ex);
						} finally { 
							try {
								message.acknowledge();
							} catch (Exception ex) {
								workflow.reportInternalException(ex);
							} 
						}
					}					
				});
				
				MessageConsumer destinationConsumer = session.createConsumer(destQueue);
				consumers.add(destinationConsumer);
				destinationConsumer.setMessageListener(new MessageListener() {
	
					@Override
					public void onMessage(Message message) {
						try {
							workflow.cancelTermination();
							TextMessage textMessage = (TextMessage) message;
							Job job = (Job) workflow.getSerializer().toObject(textMessage.getText());
							if (workflow.getCache() != null && !job.isCached())
								if(job.isTransactional())
									workflow.getCache().cacheTransactionally(job);
								else
									workflow.getCache().cache(job);
							if(job.isTransactionSuccessMessage())
								return;
							MessageProducer producer = session.createProducer(postQueue);
							producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
							producer.send(message);
							producer.close();
						}
						catch (Exception ex) {
							workflow.reportInternalException(ex);
						} finally { 
							try {
								message.acknowledge();
							} catch (Exception ex) {
								workflow.reportInternalException(ex);
							} 
						}
					}					
				});
			}
		}
	}
	
	public void addConsumer(ValidationDataQueueConsumer consumer, String consumerId) throws Exception {
	
		ActiveMQDestination postQueue = post.get(consumerId);
		
		//only connect if the consumer exists (for example it will not in a master_bare situation)
		if(consumer!=null) {
		
			MessageConsumer messageConsumer = session.createConsumer(postQueue);
			consumers.add(messageConsumer);
			messageConsumer.setMessageListener(new MessageListener() {
		
				@Override
				public void onMessage(Message message) {
					TextMessage textMessage = (TextMessage) message;
					try {
						ValidationData validationData = (ValidationData) workflow.getSerializer().toObject(textMessage.getText());
						consumer.consumeValidationDataQueueWithNotifications(validationData);
					} catch (Exception ex) {
						workflow.reportInternalException(ex);
					} finally { 
						try {
							message.acknowledge();
						} catch (Exception ex) {
							workflow.reportInternalException(ex);
						} 
					}
				}	
			});
		}
	
	}

}

