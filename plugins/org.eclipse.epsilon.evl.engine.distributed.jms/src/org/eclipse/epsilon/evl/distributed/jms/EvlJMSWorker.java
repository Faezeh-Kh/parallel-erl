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
import java.util.Collection;
import java.util.Map;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.epsilon.common.function.CheckedConsumer;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;

public class EvlJMSWorker {

	public static void main(String[] args) throws Exception {
		EvlJMSWorker worker = new EvlJMSWorker(
			args[0],
			args.length > 1 ? args[1] : Inet6Address.getLocalHost().toString()
		);
		
		DistributedRunner runner = worker.setup();
		Thread.sleep(Integer.MAX_VALUE);
		// TODO wait it out
		runner.postExecute();
		worker.teardown();
	}
	
	final String workerID;
	final ConnectionFactory connectionFactory;

	public EvlJMSWorker(String jmsAddr, String id) {
		this.workerID = id;
		connectionFactory = new ActiveMQJMSConnectionFactory(jmsAddr);
	}
	
	DistributedRunner setup() throws Exception {

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
			DistributedRunner configContainer = EvlContextDistributedSlave.parseJobParameters(config);
			configContainer.preExecute();
			
			// Job processing, requires destinations for inputs (jobs) and outputs (results)
			Destination jobQueue = regContext.createQueue(workerID + EvlModuleDistributedMasterJMS.JOB_SUFFIX);
			Destination resultsQueue = regContext.createQueue(EvlModuleDistributedMasterJMS.RESULTS_QUEUE_NAME);
			JMSProducer resultsSender = regContext.createProducer();
			JMSConsumer jobConsumer = regContext.createConsumer(jobQueue);
			
			CheckedConsumer<Serializable, JMSException> resultProcessor = obj -> {
				ObjectMessage resultsMessage = regContext.createObjectMessage(obj);
				resultsMessage.setJMSCorrelationID(workerID);
				resultsSender.send(resultsQueue, resultsMessage);
			};
			
			jobConsumer.setMessageListener(getJobProcessor(resultProcessor, (EvlModuleDistributedSlave) configContainer.getModule()));
			
			// This is to acknowledge when we have completed loading the script(s) and model(s)
			Destination configComplete = regContext.createQueue(EvlModuleDistributedMasterJMS.READY_QUEUE_NAME);
			Message configuredAckMsg = regContext.createMessage();
			configuredAckMsg.setJMSCorrelationID(workerID);
			// Tell the master we're setup and ready to work
			regContext.createProducer().send(configComplete, configuredAckMsg);
			
			return configContainer;
		}
	}
	
	protected MessageListener getJobProcessor(final CheckedConsumer<Serializable, ? extends JMSException> resultProcessor, final EvlModuleDistributedSlave module) {
		return msg -> {
			try {
				Serializable objMsg = ((ObjectMessage)msg).getObject();
				if (objMsg instanceof SerializableEvlInputAtom) {
					Collection<? extends SerializableEvlResultAtom> resultObj = module.evaluateElement((SerializableEvlInputAtom) objMsg);
					resultProcessor.acceptThrows((Serializable) resultObj);
				}
				else {
					System.err.println("["+workerID+"] Received unexpected object of type "+objMsg.getClass().getName());
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
