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

import static org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS.*;
import java.io.Serializable;
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
		new EvlJMSWorker(args[0]).run();
	}
	
	@Override
	public void run() {
		try {
			setup();
			processJobs();
			//container.postExecute();
			teardown();
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	final ConnectionFactory connectionFactory;
	DistributedRunner configContainer;
	String workerID;

	public EvlJMSWorker(String jmsAddr) {
		connectionFactory = new ActiveMQJMSConnectionFactory(jmsAddr);
	}
	
	void setup() throws Exception {
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Announce our presence to the master
			Destination regQueue = regContext.createQueue(REGISTRATION_QUEUE_NAME);
			Destination tempQueue = regContext.createTemporaryQueue();
			JMSProducer regProducer = regContext.createProducer();
			Message initMsg = regContext.createMessage();
			initMsg.setJMSReplyTo(tempQueue);
			regProducer.send(regQueue, initMsg);
			
			// Get the configuration and our ID from the reply
			Message configMsg = regContext.createConsumer(tempQueue).receive();
			this.workerID = configMsg.getJMSCorrelationID();
			System.out.println(workerID + " config and ID received at "+System.currentTimeMillis());
			
			configContainer = EvlContextDistributedSlave.parseJobParameters(configMsg.getBody(Map.class));
			configContainer.preExecute();
			((EvlModuleDistributedSlave)configContainer.getModule()).prepareExecution();
			
			// This is to acknowledge when we have completed loading the script(s) and model(s)
			Message configuredAckMsg = regContext.createMessage();
			configuredAckMsg.setJMSCorrelationID(workerID);
			configuredAckMsg.setJMSReplyTo(tempQueue);
			// Tell the master we're setup and ready to work
			regProducer.send(configMsg.getJMSReplyTo(), configuredAckMsg);
		}
	}
	
	void processJobs() throws JMSException {
		try (JMSContext jobContext = connectionFactory.createContext()) {
			// Job processing, requires destinations for inputs (jobs) and outputs (results)
			Destination jobDest = jobContext.createTopic(workerID + JOB_SUFFIX);
			Destination resultsDest = jobContext.createQueue(RESULTS_QUEUE_NAME);
			JMSProducer resultsSender = jobContext.createProducer();
			
			CheckedConsumer<Serializable, JMSException> resultProcessor = obj -> {
				ObjectMessage resultsMessage = jobContext.createObjectMessage(obj);
				resultsMessage.setJMSCorrelationID(workerID);
				resultsSender.send(resultsDest, resultsMessage);
			};
			
			jobContext.createConsumer(jobDest).setMessageListener(
				getJobProcessor(resultProcessor, (EvlModuleDistributedSlave) configContainer.getModule())
			);
			
			awaitCompletion();
			
			// Tell the master we've finished
			Message finishedMsg = jobContext.createMessage();
			finishedMsg.setJMSCorrelationID(workerID);
			resultsSender.send(resultsDest, finishedMsg);
		}
	}
	
	void awaitCompletion() {
		// TODO more intelligent blocking
		System.out.println(workerID+" awaiting jobs since "+System.currentTimeMillis());
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
