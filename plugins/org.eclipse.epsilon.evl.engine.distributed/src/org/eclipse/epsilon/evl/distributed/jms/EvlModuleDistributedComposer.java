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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

public class EvlModuleDistributedComposer extends EvlModuleDistributedMaster {
	
	public static final String
		REGISTRATION_NAME = "registration",
		CONFIG_BROADCAST_NAME = "configuration",
		READY_QUEUE_NAME = "confirm-configured",
		RESULTS_QUEUE_NAME = "results";
	
	final URI host;
	BrokerService broker;
	Connection brokerConnection;
	Session regSession;
	Map<String, Destination> workers;
	
	public EvlModuleDistributedComposer(int expectedWorkers, String addr, int port) throws URISyntaxException {
		super(expectedWorkers);
		
		if (port <= 80 || port == 8080) {
			port = Integer.parseInt (BrokerService.DEFAULT_PORT);
		}
		if (addr == null || addr.length() < 5) {
			try {
				addr = "tcp://"+InetAddress.getLocalHost().getHostAddress();
			}
			catch (UnknownHostException uhe) {
				addr = "tcp://"+BrokerService.DEFAULT_BROKER_NAME;
			}
		}
		host = new URI(addr+":"+port);
	}
	
	void setup() throws Exception {
		broker = new BrokerService();
		broker.setUseJmx(true);
		broker.addConnector(host);
		broker.start();
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(host);
		brokerConnection = connectionFactory.createConnection();
		brokerConnection.start();
	}
	
	void awaitWorkers(final int expectedWorkers, final Serializable config) throws JMSException {
		workers = new HashMap<>(expectedWorkers+1, 1f);
		regSession = brokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
		Destination registered = regSession.createQueue(REGISTRATION_NAME);
		MessageConsumer regConsumer = regSession.createConsumer(registered);
		Object lock = new Object();
		AtomicInteger connectedWorkers = new AtomicInteger();
		
		regConsumer.setMessageListener(msg -> {
			try {
				synchronized (lock) {
					msg.acknowledge();
					String workerID = msg.getJMSCorrelationID();
					workers.put(workerID, null);
					if (connectedWorkers.incrementAndGet() == expectedWorkers) {
						lock.notify();
					}
				}
			}
			catch (JMSException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		});
		
		while (connectedWorkers.get() < expectedWorkers) {
			synchronized (lock) {
				try {
					lock.wait();
				}
				catch (InterruptedException ie) {}
			}
		}
		
		Destination configSent = regSession.createTopic(CONFIG_BROADCAST_NAME);
		Destination configComplete = regSession.createQueue(READY_QUEUE_NAME);
		MessageConsumer confirmer = regSession.createConsumer(configComplete);
		
		confirmer.setMessageListener(msg -> {
			try {
				msg.acknowledge();
				String workerID = msg.getJMSCorrelationID();
				synchronized (lock) {
					if (workers.containsKey(workerID)) {
						Session workerSession = brokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
						Destination workerBox = workerSession.createQueue(workerID);
						workerSession.createConsumer(workerBox).setMessageListener(getResultsMessageListener(workerBox));
						workers.put(workerID, workerBox);
					}
					if (connectedWorkers.incrementAndGet() == expectedWorkers) {
						assert connectedWorkers.get() == workers.size();
						lock.notify();
					}
				}
			}
			catch (JMSException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		});
		
		ObjectMessage configMessage = regSession.createObjectMessage(config);
		MessageProducer sender = regSession.createProducer(configSent);
		sender.send(configMessage);
		
		connectedWorkers.set(0);
		while (connectedWorkers.get() < expectedWorkers) {
			synchronized (lock) {
				try {
					lock.wait();
				}
				catch (InterruptedException ie) {}
			}
		}
		
		regSession.close();
	}
	
	protected MessageListener getResultsMessageListener(final Object client) {
		final Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		return msg -> {
			try {
				SerializableEvlResultAtom result;
				synchronized (client) {
					msg.acknowledge();
					result = ((SerializableEvlResultAtom)((ObjectMessage)msg).getObject());
				}
				unsatisfiedConstraints.add(deserializeResult(result));
			}
			catch (JMSException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		};
	}

	void teardown() throws Exception {
		brokerConnection.close();
		broker.deleteAllMessages();
		broker = null;
		brokerConnection = null;
		workers = null;
	}
	
	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		try {
			setup();
			EvlContextDistributedMaster context = getContext();
			awaitWorkers(context.getDistributedParallelism(), context.getJobParameters());
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
	
	@Override
	protected void checkConstraints() throws EolRuntimeException {
		// TODO Auto-generated method stub
	}
	
	@Override
	protected void postExecution() throws EolRuntimeException {
		super.postExecution();
		try {
			teardown();
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
}
