/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms;

import java.io.Serializable;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.Map;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.epsilon.common.function.CheckedConsumer;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;

public class EvlJMSWorker implements Runnable {

	public static void main(String[] args) throws UnknownHostException {
		new EvlJMSWorker(
			args[0],
			args.length > 1 ? args[1] : Inet6Address.getLocalHost().toString()
		)
		.run();
	}
	
	@Override
	public void run() {
		try {
			register();
			configContainer.preExecute();
			confirmReady();
			processJobs();
			teardown();
			//container.postExecute();
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	final String workerID;
	final ConnectionFactory connectionFactory;
	DistributedRunner configContainer;

	public EvlJMSWorker(String jmsAddr, String id) {
		this.workerID = id;
		connectionFactory = new ActiveMQJMSConnectionFactory(jmsAddr);
	}
	
	void register() throws Exception {
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Announce our presence to the master
			Destination registered = regContext.createQueue(EvlModuleDistributedMasterJMS.REGISTRATION_NAME);
			Message ack = regContext.createMessage();
			ack.setJMSCorrelationID(workerID);
			regContext.createProducer().send(registered, ack);
			
			// Here we receive the actual configuration from the master
			Destination configReceive = regContext.createTopic(EvlModuleDistributedMasterJMS.CONFIG_BROADCAST_NAME);
			// Block until received
			Map<String, ? extends Serializable> config = regContext.createConsumer(configReceive).receiveBody(Map.class);
			configContainer = EvlContextDistributedSlave.parseJobParameters(config);
		}
	}
	
	void confirmReady() throws JMSException {
		try (JMSContext confirmContext = connectionFactory.createContext()) {
			// This is to acknowledge when we have completed loading the script(s) and model(s)
			Destination configComplete = confirmContext.createQueue(EvlModuleDistributedMasterJMS.READY_QUEUE_NAME);
			Message configuredAckMsg = confirmContext.createMessage();
			configuredAckMsg.setJMSCorrelationID(workerID);
			// Tell the master we're setup and ready to work
			confirmContext.createProducer().send(configComplete, configuredAckMsg);
		}
	}
	
	void processJobs() throws JMSException {
		try (JMSContext jobContext = connectionFactory.createContext()) {
			// Job processing, requires destinations for inputs (jobs) and outputs (results)
			Topic jobTopic = jobContext.createTopic(workerID + EvlModuleDistributedMasterJMS.JOB_SUFFIX);
			Destination resultsQueue = jobContext.createQueue(EvlModuleDistributedMasterJMS.RESULTS_QUEUE_NAME);
			JMSProducer resultsSender = jobContext.createProducer();
			// This is to allow for load-balancing
			JMSConsumer jobConsumer = jobContext.createSharedConsumer(jobTopic, jobTopic.getTopicName()+"-subscription");
			
			CheckedConsumer<Serializable, JMSException> resultProcessor = obj -> {
				ObjectMessage resultsMessage = jobContext.createObjectMessage(obj);
				resultsMessage.setJMSCorrelationID(workerID);
				resultsSender.send(resultsQueue, resultsMessage);
			};
			
			jobConsumer.setMessageListener(getJobProcessor(resultProcessor, (EvlModuleDistributedSlave) configContainer.getModule()));
			
			awaitCompletion();
		}
	}
	
	void awaitCompletion() {
		// TODO more intelligent blocking
		try {
			Thread.sleep(Integer.MAX_VALUE);
		}
		catch (InterruptedException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
	}
	
	protected MessageListener getJobProcessor(final CheckedConsumer<Serializable, ? extends JMSException> resultProcessor, final EvlModuleDistributedSlave module) {
		return msg -> {
			try {
				final Serializable objMsg = ((ObjectMessage)msg).getObject();
				final Object resultObj;
				
				if (objMsg instanceof SerializableEvlInputAtom) {
					resultObj  = module.evaluateElement((SerializableEvlInputAtom) objMsg);
				}
				else if (objMsg instanceof DistributedEvlBatch) {
					resultObj = module.evaluateBatch((DistributedEvlBatch) objMsg);
				}
				else {
					resultObj = null;
					System.err.println("["+workerID+"] Received unexpected object of type "+objMsg.getClass().getName());
				}
				
				if (resultObj instanceof Serializable) {
					resultProcessor.acceptThrows((Serializable) resultObj);
				}
			}
			catch (JMSException | EolRuntimeException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		};
	}

	void teardown() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
}
