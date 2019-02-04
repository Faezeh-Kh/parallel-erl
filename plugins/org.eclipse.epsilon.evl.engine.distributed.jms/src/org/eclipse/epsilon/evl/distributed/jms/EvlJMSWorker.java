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
import org.eclipse.epsilon.common.function.CheckedSupplier;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;

public class EvlJMSWorker {

	public static void main(String[] args) throws Exception {
		System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "*");
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
	
	final String jmsHost;
	final String workerID;
	ConnectionFactory connectionFactory;

	public EvlJMSWorker(String jmsAddr, String id) {
		this.jmsHost = jmsAddr;
		this.workerID = id;
	}
	
	@SuppressWarnings("unchecked")
	DistributedRunner setup() throws Exception {
		// Connect to the broker
		connectionFactory = new ActiveMQJMSConnectionFactory(jmsHost);
		Connection brokerConnection = connectionFactory.createConnection();
		brokerConnection.start();
		
		// Announce our presence to the master
		Session regSession = brokerConnection.createSession();
		Destination registered = regSession.createQueue(EvlModuleDistributedMasterJMS.REGISTRATION_NAME);
		MessageProducer regSender = regSession.createProducer(registered);
		Message ack = regSession.createMessage();
		ack.setJMSCorrelationID(workerID);
		regSender.send(ack);
		
		// Here we receive the actual configuration from the master
		Destination configReceive = regSession.createTopic(EvlModuleDistributedMasterJMS.CONFIG_BROADCAST_NAME);
		MessageConsumer configConsumer = regSession.createConsumer(configReceive);
		// Block until received
		ObjectMessage configMsg = (ObjectMessage) configConsumer.receive();
		Map<String, ? extends Serializable> config = (Map<String, ? extends Serializable>) configMsg.getObject();
		DistributedRunner configContainer = EvlContextDistributedSlave.parseJobParameters(config);
		configContainer.preExecute();
		
		// Set up the session and queue for job processing. This involves receiving jobs and sending results, hence two destinations.
		Session jobSession = brokerConnection.createSession();
		Destination jobQueue = jobSession.createQueue(workerID + EvlModuleDistributedMasterJMS.JOB_SUFFIX);
		Destination resultsQueue = jobSession.createQueue(EvlModuleDistributedMasterJMS.RESULTS_QUEUE_NAME);
		MessageProducer resultsSender = jobSession.createProducer(resultsQueue);
		CheckedSupplier<? extends ObjectMessage, JMSException> resultsMsgFactory = jobSession::createObjectMessage;
		MessageConsumer jobConsumer = jobSession.createConsumer(jobQueue);
		jobConsumer.setMessageListener(getJobProcessor(resultsSender, resultsMsgFactory, (EvlModuleDistributedSlave) configContainer.getModule()));
		
		// This is to acknowledge when we have completed loading the script(s) and model(s)
		Destination configComplete = regSession.createQueue(EvlModuleDistributedMasterJMS.READY_QUEUE_NAME);
		MessageProducer configSender = regSession.createProducer(configComplete);
		Message configuredAckMsg = regSession.createMessage();
		configuredAckMsg.setJMSCorrelationID(workerID);
		// Tell the master we're setup and ready to work
		configSender.send(configuredAckMsg);
		
		return configContainer;
	}
	
	protected MessageListener getJobProcessor(final MessageProducer resultsSender, final CheckedSupplier<? extends ObjectMessage, JMSException> messageFactory, final EvlModuleDistributedSlave module) {
		return msg -> {
			try {
				Serializable objMsg = ((ObjectMessage)msg).getObject();
				if (objMsg instanceof SerializableEvlInputAtom) {
					ObjectMessage resultsMessage = messageFactory.getThrows();
					Collection<? extends SerializableEvlResultAtom> resultObj = module.evaluateElement((SerializableEvlInputAtom) objMsg);
					resultsMessage.setObject((Serializable) resultObj);
					resultsMessage.setJMSCorrelationID(workerID);
					resultsSender.send(resultsMessage);
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
		connectionFactory = null;
	}
}
