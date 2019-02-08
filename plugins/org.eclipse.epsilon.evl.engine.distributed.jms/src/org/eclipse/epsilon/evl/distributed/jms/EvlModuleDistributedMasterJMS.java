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
import org.eclipse.epsilon.evl.execute.concurrent.ConstraintContextAtom;

public class EvlModuleDistributedMasterJMS extends EvlModuleDistributedMaster {
	
	public static void main(String[] args) throws ClassNotFoundException, UnknownHostException, URISyntaxException {
		String modelPath = args[1].contains("://") ? args[1] : "file:///"+args[1];
		String metamodelPath = args[2].contains("://") ? args[2] : "file:///"+args[2];
		String expectedWorkers = args[3];
		String addr = args.length > 4 ? args[4] : null;
		
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
	final List<WorkerView> slaveWorkers;
	final AtomicInteger workersFinished = new AtomicInteger();
	JMSContext resultsContext;
	Destination resultsDest;
	
	class WorkerView extends AbstractWorker {
		public Destination localBox = null;
		JMSContext session;
		Topic jobsTopic;
		JMSProducer jobSender;
		
		public WorkerView(String id) {
			this.workerID = id;
		}
		
		public void confirm(JMSContext session) {
			jobsTopic = (this.session = session).createTopic(workerID + JOB_SUFFIX);
			jobSender = session.createProducer();
		}
		
		public void sendJob(Serializable input, JMSContext jobContext) throws JMSException {
			ObjectMessage jobMsg = jobContext.createObjectMessage(input);
			jobMsg.setJMSCorrelationID(workerID);
			jobSender.send(jobsTopic, jobMsg);
		}
		
		public void signalEnd(JMSContext jobContext) throws JMSException {
			Message endMsg = jobContext.createMessage();
			endMsg.setJMSCorrelationID(workerID);
			jobSender.send(jobsTopic, endMsg);
		}
		
		@Override
		public void close() throws Exception {
			session.close();
			session = null;
			jobsTopic = null;
			jobSender = null;
		}
	}
	
	public EvlModuleDistributedMasterJMS(int expectedWorkers, String host) throws URISyntaxException {
		super(expectedWorkers);
		connectionFactory = new ActiveMQJMSConnectionFactory(this.host = host);
		System.out.println(LOG_PREFIX+"connected to "+host+" at "+System.currentTimeMillis());
		slaveWorkers = new ArrayList<>(expectedWorkers);
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
						WorkerView worker = new WorkerView(WORKER_ID_PREFIX+connectedWorkers.get());
						worker.localBox = msg.getJMSReplyTo();
						slaveWorkers.add(worker);
						// Tell the worker what their ID is along with the configuration parameters
						Message configMsg = regContext.createObjectMessage(config);
						configMsg.setJMSReplyTo(tempQueue);
						configMsg.setJMSCorrelationID(worker.workerID);
						regProducer.send(worker.localBox, configMsg);
						
						// Wait for the worker to load the configuration and get back to us
						Message ack = regContext.createConsumer(tempQueue).receive();
						worker.confirm(regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE));
						
						// Stop waiting once all workers are connected
						if (connectedWorkers.incrementAndGet() == expectedWorkers) {
							assert expectedWorkers == slaveWorkers.size();
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
						connectedWorkers.wait();
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
						for (SerializableEvlResultAtom sra : (Iterable<? extends SerializableEvlResultAtom>)contents) {
							unsatisfiedConstraints.add(deserializeResult(sra));
						}
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
					slaveWorkers.stream().filter(w -> w.workerID.equals(workerID)).findAny().ifPresent(w -> {
						w.finished = true;
						if (workersFinished.incrementAndGet() >= slaveWorkers.size()) {
							synchronized (workersFinished) {
								workersFinished.notify();
							}
						}
					});
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
		for (AutoCloseable worker : slaveWorkers) {
			worker.close();
			worker = null;
		}
		slaveWorkers.clear();
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
		assert slaveWorkers.size() == batches.size()+1;
		Iterator<WorkerView> workersIter = slaveWorkers.iterator();
		Iterator<DistributedEvlBatch> batchesIter = batches.iterator();
		
		try (JMSContext jobContext = connectionFactory.createContext()) {
			while (workersIter.hasNext()) {
				WorkerView worker = workersIter.next();
				worker.sendJob(batchesIter.next(), jobContext);
				worker.signalEnd(jobContext);
				System.out.println(LOG_PREFIX+" finished submitting to "+worker+" at "+System.currentTimeMillis());
			}
			
			assert batchesIter.hasNext();
			System.out.println(LOG_PREFIX+" began processing own jobs at "+System.currentTimeMillis());
			batchesIter.next().evaluate(ConstraintContextAtom.getContextJobs(this), getContext());
			System.out.println(LOG_PREFIX+" finished processing own jobs at "+System.currentTimeMillis());
			
			System.out.println(LOG_PREFIX+" awaiting completion...");
			while (workersFinished.get() < slaveWorkers.size()) {
				synchronized (workersFinished) {
					workersFinished.wait();
				}
			}
			System.out.println(LOG_PREFIX+" completed.");
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
