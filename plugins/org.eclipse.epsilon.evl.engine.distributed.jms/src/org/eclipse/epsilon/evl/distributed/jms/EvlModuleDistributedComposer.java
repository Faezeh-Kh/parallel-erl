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
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.epsilon.eol.cli.EolConfigParser;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlAtom;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

public class EvlModuleDistributedComposer extends EvlModuleDistributedMaster {
	
	public static void main(String[] args) throws ClassNotFoundException {
		String modelPath = args[1].contains("://") ? args[1] : "file:///"+args[1];
		String metamodelPath = args[2].contains("://") ? args[2] : "file:///"+args[2];
		String expectedWorkers = args[3];
		String addr = args[4];
		String port = args[5];
		
		EolConfigParser.main(new String[] {
			"CONFIG:"+DistributedRunner.class.getName(),
			args[0],
			"-models",
				"\"emf.DistributableEmfModel#"
				+ "concurrent=true,cached=true,readOnLoad=true,storeOnDisposal=false,"
				+ "modelUri="+modelPath+",fileBasedMetamodelUri="+metamodelPath+"\"",
			"-module", EvlModuleDistributedComposer.class.getName().substring(20),
				"int="+expectedWorkers,
				"String="+addr,
				"int="+port
		});
	}
	
	public static final String
		REGISTRATION_NAME = "registration",
		CONFIG_BROADCAST_NAME = "configuration",
		READY_QUEUE_NAME = "confirm-configured",
		RESULTS_QUEUE_NAME = "results",
		JOB_SUFFIX = "-jobs";
	
	final URI host;
	BrokerService broker;
	Connection brokerConnection;
	Collection<Worker> workers;
	
	class Worker {
		public Worker(String id) {
			this.id = id;
		}
		public final String id;
		public Session session;
		public Destination jobs, results;
		public MessageProducer jobSender;
		
		public void sendJob(SerializableEvlAtom input) throws JMSException {
			ObjectMessage jobMsg = session.createObjectMessage();
			jobMsg.setObject(input);
			jobMsg.setJMSCorrelationID(id);
			jobSender.send(jobMsg);
		}
	}
	
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
		workers = new ArrayList<>(expectedWorkers);
		Session regSession = brokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		Destination registered = regSession.createQueue(REGISTRATION_NAME);
		MessageConsumer regConsumer = regSession.createConsumer(registered);
		Object lock = new Object();
		AtomicInteger connectedWorkers = new AtomicInteger();
		
		regConsumer.setMessageListener(msg -> {
			try {
				synchronized (lock) {
					msg.acknowledge();
					String workerID = msg.getJMSCorrelationID();
					workers.add(new Worker(workerID));
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
					lock.wait(30_000);
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
					Worker worker = workers.stream().filter(w -> w.id.equals(workerID)).findAny().orElse(null);
					if (worker != null) {
						worker.session = brokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
						worker.jobs = worker.session.createQueue(workerID + JOB_SUFFIX);
						worker.results = worker.session.createQueue(RESULTS_QUEUE_NAME);
						worker.session.createConsumer(worker.jobs).setMessageListener(getResultsMessageListener(worker.results));
						worker.jobSender = worker.session.createProducer(worker.jobs);
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
		// TODO load balancing
		Collection<? extends SerializableEvlAtom> jobs = createJobs(false);
		Worker worker = workers.iterator().next();
		for (SerializableEvlAtom job : jobs) {
			try {
				worker.sendJob(job);
			}
			catch (JMSException ex) {
				throw new EolRuntimeException(ex);
			}
		}
	}
	
	@Override
	protected void postExecution() throws EolRuntimeException {
		//super.postExecution();
		try {
			//teardown();
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
}
