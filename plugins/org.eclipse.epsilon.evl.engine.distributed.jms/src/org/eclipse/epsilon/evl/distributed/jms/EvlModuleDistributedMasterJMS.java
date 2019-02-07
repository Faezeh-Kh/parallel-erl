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
				+ "concurrent=true,cached=true,readOnLoad=true,storeOnDisposal=false,"
				+ "modelUri="+modelPath+",fileBasedMetamodelUri="+metamodelPath+"\"",
			"-module", EvlModuleDistributedMasterJMS.class.getName().substring(20),
				"int="+expectedWorkers,
				"String="+addr
		});
	}
	
	public static final String
		REGISTRATION_QUEUE_NAME = "registration",
		RESULTS_QUEUE_NAME = "results",
		JOB_SUFFIX = "-jobs",
		WORKER_ID_PREFIX = "EVL-jms-";
	
	private static final String LOG_PREFIX = "[MASTER] ";
	
	final String host;
	final ConnectionFactory connectionFactory;
	final List<Worker> workers;
	JMSContext resultsContext;
	Destination resultsDest;
	
	class Worker implements AutoCloseable {
		public final String id;
		public Destination localBox = null;
		public boolean finished = false;
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
		
		public void sendJob(Serializable input, JMSContext jobContext) throws JMSException {
			ObjectMessage jobMsg = jobContext.createObjectMessage(input);
			jobMsg.setJMSCorrelationID(id);
			jobSender.send(jobsTopic, jobMsg);
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
			Destination regQueue = regContext.createQueue(REGISTRATION_QUEUE_NAME);
			JMSConsumer regConsumer = regContext.createConsumer(regQueue);
			JMSProducer regProducer = regContext.createProducer();
			
			regConsumer.setMessageListener(msg -> {
				try {
					synchronized (connectedWorkers) {
						// Assign worker ID
						Destination tempQueue = regContext.createTemporaryQueue();
						Worker worker = new Worker(WORKER_ID_PREFIX+connectedWorkers.get());
						worker.localBox = msg.getJMSReplyTo();
						workers.add(worker);
						// Tell the worker what their ID is along with the configuration parameters
						Message configMsg = regContext.createObjectMessage(config);
						configMsg.setJMSReplyTo(tempQueue);
						configMsg.setJMSCorrelationID(worker.id);
						regProducer.send(worker.localBox, configMsg);
						
						// Wait for the worker to load the configuration and get back to us
						Message ack = regContext.createConsumer(tempQueue).receive();
						worker.confirm(regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE));
						
						// Stop waiting once all workers are connected
						if (connectedWorkers.incrementAndGet() == expectedWorkers) {
							assert expectedWorkers == workers.size();
							connectedWorkers.notify();
						}
					}
				}
				catch (JMSException jmx) {
					throw new JMSRuntimeException(jmx.getMessage());
				}
			});
			
			// Wait for workers to confirm that they've completed configuration
			while (connectedWorkers.get() < expectedWorkers) {
				synchronized (connectedWorkers) {
					try {
						connectedWorkers.wait(30_000);
					}
					catch (InterruptedException ie) {}
				}
			}
			
			System.out.println(LOG_PREFIX+"Workers ready at "+System.currentTimeMillis());
		}
	}
	
	@SuppressWarnings("unchecked")
	protected MessageListener getResultsMessageListener() {
		final Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		return msg -> {
			try {
				if (msg instanceof ObjectMessage) {
					Serializable contents = ((ObjectMessage)msg).getObject();
					if (contents instanceof Iterable) {
						((Iterable<? extends SerializableEvlResultAtom>)contents).forEach(this::deserializeResult);
					}
					else if (contents instanceof SerializableEvlResultAtom) {
						unsatisfiedConstraints.add(deserializeResult((SerializableEvlResultAtom) contents));
					}
					else {
						System.err.println(LOG_PREFIX+"Received unexpected object of type "+contents.getClass().getSimpleName());
					}
				}
				else {
					String workerID = msg.getJMSCorrelationID();
					workers.stream().filter(w -> w.id.equals(workerID)).findAny().ifPresent(w1 -> {
						w1.finished = true;
						if (workers.stream().allMatch(w2 -> w2.finished)) {
							synchronized (workers) {
								workers.notify();
							}
						}
					});
					//System.err.println(LOG_PREFIX+"Received non-object message.");
				}
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
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
		Iterator<Worker> workersIter = workers.iterator();
		Iterator<? extends Serializable> batchesIter = batches.iterator();
		
		try (JMSContext jobContext = connectionFactory.createContext()) {
			while (batchesIter.hasNext() && workersIter.hasNext()) {
				workersIter.next().sendJob(batchesIter.next(), jobContext);
			}
			
			while (workers.stream().anyMatch(w -> !w.finished)) {
				synchronized (workers) {
					workers.wait(120_000);
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
