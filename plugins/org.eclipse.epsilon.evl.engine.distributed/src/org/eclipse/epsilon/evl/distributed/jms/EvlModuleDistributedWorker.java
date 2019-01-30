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

import java.net.URI;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;

public class EvlModuleDistributedWorker extends EvlModuleDistributedSlave {

	final URI jmsHost;
	String workerID = "my unique ID";	// TODO: set based on IP?
	Connection brokerConnection;
	Queue jobInbox;
	
	public EvlModuleDistributedWorker(int parallelism, URI jmsAddr) {
		super(parallelism);
		this.jmsHost = jmsAddr;
	}
	
	void setup() throws Exception {
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
		Runnable messageSender = () -> {
			try {
				Message msg = regSession.createMessage();
				msg.setJMSCorrelationID(workerID);
				configSender.send(msg);
			}
			catch (JMSException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		};
		MessageConsumer receiver = regSession.createConsumer(configReceive);
		receiver.setMessageListener(getConfigMessageListener(messageSender));
		
	}
	
	MessageListener getConfigMessageListener(Runnable messageSender) {
		return msg -> {
			try {
				msg.acknowledge();
				// TODO: get the config and use it
				messageSender.run();
			}
			catch (JMSException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		};
	}
	
	void teardown() throws Exception {
		brokerConnection.close();
		brokerConnection = null;
	}
	
	@Override
	public void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		try {
			setup();
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}

	@Override
	public void postExecution() throws EolRuntimeException {
		super.postExecution();
		try {
			teardown();
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
	
}
