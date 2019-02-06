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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.epsilon.eol.cli.EolConfigParser;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

public class EvlModuleDistributedMasterJMS extends EvlModuleDistributedMaster {
	
	public static void main(String[] args) throws ClassNotFoundException, UnknownHostException, URISyntaxException {
		String modelPath = args[1].contains("://") ? args[1] : "file:///"+args[1];
		String metamodelPath = args[2].contains("://") ? args[2] : "file:///"+args[2];
		String expectedWorkers = args[3];
		String addr = args[4];
		
		if (addr == null || addr.length() < 5) {
			addr = "tcp://"+InetAddress.getLocalHost().getHostAddress()+":61616";
		}
		
		// For validation purposes
		new URI(addr);
		
		EolConfigParser.main(new String[] {
			"CONFIG:"+DistributedRunner.class.getName(),
			args[0],
			"-models",
				"\"emf.DistributableEmfModel#"
				+ "concurrent=true,cached=false,readOnLoad=true,storeOnDisposal=false,"
				+ "modelUri="+modelPath+",fileBasedMetamodelUri="+metamodelPath+"\"",
			"-module", EvlModuleDistributedMasterJMS.class.getName().substring(20),
				"int="+expectedWorkers,
				"String="+addr
		});
	}
	
	public static final String
		REGISTRATION_NAME = "registration",
		CONFIG_BROADCAST_NAME = "configuration",
		READY_QUEUE_NAME = "confirm-configured",
		RESULTS_QUEUE_NAME = "results",
		JOB_SUFFIX = "-jobs";
	
	final String host;
	final ConnectionFactory connectionFactory;
	final List<Worker> workers;
	final AtomicInteger receivedResults = new AtomicInteger();
	int expectedResults;
	JMSContext resultsContext;
	Destination resultsDest;
	
	class Worker implements AutoCloseable {
		public final String id;
		JMSContext session;
		Topic jobsTopic;
		JMSProducer jobSender;
		
		public Worker(String id) {
			this.id = id;
		}
		
		public void confirm(JMSContext session) {
			jobsTopic = (this.session = session).createTopic(id + JOB_SUFFIX);
			jobSender = session.createProducer();
		}
		
		public void sendJob(Serializable input) throws JMSException {
			try (JMSContext jobContext = session.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				ObjectMessage jobMsg = jobContext.createObjectMessage(input);
				jobMsg.setJMSCorrelationID(id);
				jobSender.send(jobsTopic, jobMsg);
			}
		}
		
		@Override
		public void close() throws Exception {
			session.close();
			session = null;
			jobsTopic = null;
			jobSender = null;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof Worker)) return false;
			return Objects.equals(this.id, ((Worker)obj).id);
		}
		@Override
		public int hashCode() {
			return Objects.hashCode(id);
		}
		@Override
		public String toString() {
			return getClass().getName()+"-"+id;
		}
	}
	
	public EvlModuleDistributedMasterJMS(int expectedWorkers, String host) throws URISyntaxException {
		super(expectedWorkers);
		connectionFactory = new ActiveMQJMSConnectionFactory(this.host = host);
		workers = new ArrayList<>(expectedWorkers);
	}
	
	void awaitWorkers(final int expectedWorkers, final Serializable config) throws JMSException {
		AtomicInteger connectedWorkers = new AtomicInteger();
		
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Initial registration of workers
			regContext.createConsumer(regContext.createQueue(REGISTRATION_NAME)).setMessageListener(msg -> {
				try {
					synchronized (connectedWorkers) {
						workers.add(new Worker(msg.getJMSCorrelationID()));
						if (connectedWorkers.incrementAndGet() == expectedWorkers) {
							connectedWorkers.notify();
						}
					}
				}
				catch (JMSException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
			});
			
			// Wait for workers to connect
			while (connectedWorkers.get() < expectedWorkers) {
				synchronized (connectedWorkers) {
					try {
						connectedWorkers.wait(30_000);
					}
					catch (InterruptedException ie) {}
				}
			}
			
			// Send the configuration
			regContext.createProducer().send(regContext.createTopic(CONFIG_BROADCAST_NAME), config);
			
			// Add confirmed workers once they've loaded the configuration
			regContext.createConsumer(regContext.createQueue(READY_QUEUE_NAME)).setMessageListener(msg -> {
				try {
					String workerID = msg.getJMSCorrelationID();
					synchronized (connectedWorkers) {
						workers.stream()
							.filter(w -> w.id.equals(workerID))
							.findAny()
							.ifPresent(w -> w.confirm(regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)));
						
						if (connectedWorkers.incrementAndGet() == expectedWorkers) {
							assert connectedWorkers.get() == workers.size();
							connectedWorkers.notify();
						}
					}
				}
				catch (JMSException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
			});
			
			// Wait for workers to confirm that they've completed configuration
			connectedWorkers.set(0);
			do {
				synchronized (connectedWorkers) {
					try {
						connectedWorkers.wait();
					}
					catch (InterruptedException ie) {}
				}
			}
			while (connectedWorkers.get() < expectedWorkers);
		}
	}
	
	@SuppressWarnings("unchecked")
	protected MessageListener getResultsMessageListener() {
		final Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		return msg -> {
			try {
				if (msg instanceof ObjectMessage) {
					if (receivedResults.incrementAndGet() >= expectedResults) {
						synchronized (receivedResults) {
							receivedResults.notify();
						}
					}
					
					Serializable contents = ((ObjectMessage)msg).getObject();
					
					if (contents instanceof Iterable) {
						((Iterable<? extends SerializableEvlResultAtom>)contents).forEach(this::deserializeResult);
					}
					else if (contents instanceof SerializableEvlResultAtom) {
						unsatisfiedConstraints.add(deserializeResult((SerializableEvlResultAtom) contents));
					}
					else {
						System.err.println("[MASTER] Received unexpected object of type "+contents.getClass().getSimpleName());
					}
				}
				else {
					System.err.println("[MASTER] Received non-object message.");
				}
			}
			catch (JMSException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		};
	}
	
	void teardown() throws Exception {
		resultsContext.close();
		resultsContext = null;
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
	
	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		try {
			EvlContextDistributedMaster context = getContext();
			awaitWorkers(context.getDistributedParallelism(), context.getJobParameters());
			resultsContext = connectionFactory.createContext();
			resultsDest = resultsContext.createQueue(RESULTS_QUEUE_NAME);
			resultsContext.createConsumer(resultsDest).setMessageListener(getResultsMessageListener());
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
	
	@Override
	protected void checkConstraints() throws EolRuntimeException {	
		List<DistributedEvlBatch> batches = DistributedEvlBatch.getBatches(this);
		expectedResults = batches.get(batches.size()-1).to;
		Iterator<Worker> workersIter = workers.iterator();
		Iterator<? extends Serializable> batchesIter = batches.iterator();
		
		try {
			while (batchesIter.hasNext() && workersIter.hasNext()) {
				workersIter.next().sendJob(batchesIter.next());
			}
			
			while (receivedResults.get() < expectedResults) {
				synchronized (receivedResults) {
					receivedResults.wait(120_000);
				}
			}
		}
		catch (JMSException | InterruptedException ex) {
			throw new EolRuntimeException(ex);
		}
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
