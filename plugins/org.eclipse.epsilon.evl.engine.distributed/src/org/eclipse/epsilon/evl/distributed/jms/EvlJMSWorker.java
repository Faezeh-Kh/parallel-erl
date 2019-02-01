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
import java.net.URI;
import java.util.Map;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;

public class EvlJMSWorker {

	public static void main(String[] args) throws Exception {
		EvlJMSWorker worker = new EvlJMSWorker(
			URI.create(args[0]),
			args.length > 2 ? args[2] : Inet6Address.getLocalHost().toString(),
			Integer.parseInt(args[1])
		);
		
		DistributedRunner runner = worker.setup();
		runner.execute();
		runner.postExecute();
		worker.teardown();
	}
	
	final int parallelism;
	final URI jmsHost;
	final String workerID;
	Connection brokerConnection;
	
	// TODO: configure listener for job processing
	
	public EvlJMSWorker(URI jmsAddr, String id, int parallelism) {
		this.jmsHost = jmsAddr;
		this.workerID = id;
		this.parallelism = parallelism;
	}
	
	@SuppressWarnings("unchecked")
	DistributedRunner setup() throws Exception {
		brokerConnection = new ActiveMQConnectionFactory(jmsHost).createConnection();
		brokerConnection.start();
		Session regSession = brokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
		Destination registered = regSession.createQueue(EvlModuleDistributedComposer.REGISTRATION_NAME);
		MessageProducer regSender = regSession.createProducer(registered);
		Message ack = regSession.createMessage();
		ack.setJMSCorrelationID(workerID);
		regSender.send(ack);
		
		Destination configReceive = regSession.createTopic(EvlModuleDistributedComposer.CONFIG_BROADCAST_NAME);
		Destination configComplete = regSession.createQueue(EvlModuleDistributedComposer.READY_QUEUE_NAME);
		MessageProducer configSender = regSession.createProducer(configComplete);
		
		ObjectMessage configMsg = (ObjectMessage) regSession.createConsumer(configReceive).receive();
		configMsg.acknowledge();
		Map<String, ? extends Serializable> config = (Map<String, ? extends Serializable>) configMsg.getObject();
		DistributedRunner configContainer = EvlContextDistributedSlave.parseJobParameters(config);
		configContainer.preExecute();
		
		Message configuredAckMsg = regSession.createMessage();
		configuredAckMsg.setJMSCorrelationID(workerID);
		configSender.send(configuredAckMsg);
		
		return configContainer;
	}
	
	void teardown() throws Exception {
		brokerConnection.close();
		brokerConnection = null;
	}
}
