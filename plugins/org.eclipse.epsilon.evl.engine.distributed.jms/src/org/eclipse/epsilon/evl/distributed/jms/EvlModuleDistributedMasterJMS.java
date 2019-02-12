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

/**
 * This module co-ordinates a message-based architecture. The workflow is as follows: <br/>
 * 
 * - Master is invoked in usual way, given the usual data (script, models etc.)
 *  + URI of the broker + expected number of workers <br/>
 *  
 * - Master waits on a registration queue for workers to confirm presence <br/>
 * 
 * - Master sends each worker their unique ID and the confirguration parameters
 * obtained from {@linkplain EvlContextDistributedMaster#getJobParameters()} <br/>
 * 
 * - Workers send back a message when they've loaded the configuration <br/>
 * 
 * - Jobs are sent to the workers (either as batches or individual model elements to evaluate) <br/>
 * 
 * - Workers send back results to results queue, which are then deserialized. <br/>
 * 
 * Note that each worker is processed independently and asynchronously. That is, once a worker has connected,
 * it need not wait for other workers to connect or be in the same stage of registration. This module is
 * designed such that it is possible for a fast worker to start sending results back before another has even
 * registered. This class also tries to abstract away from the handshaking / messaging code by invoking methods
 * at key points in the back-and-forth messaging process within listeners which can be used to control the
 * execution strategy. See the {@link #checkConstraints()} method for where these "checkpoint" methods are.
 * 
 * @see EvlJMSWorker
 * @author Sina Madani
 * @since 1.6
 */
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
		REGISTRATION_QUEUE = "registration",
		RESULTS_QUEUE_NAME = "results",
		JOB_SUFFIX = "-jobs",
		WORKER_ID_PREFIX = "EVL-jms-";
	
	private static final String LOG_PREFIX = "[MASTER] ";
	
	protected final String host;
	protected final ConnectionFactory connectionFactory;
	protected final List<WorkerView> slaveWorkers;
	protected final int expectedSlaves;
	final AtomicInteger workersFinished = new AtomicInteger();
	
	protected class WorkerView extends AbstractWorker {
		public Destination localBox = null;
		JMSContext session;
		Topic jobsTopic;
		JMSProducer jobSender;
		
		public WorkerView(String id) {
			this.workerID = id;
		}
		
		public void confirm() {
			session = connectionFactory.createContext();
			jobsTopic = session.createTopic(workerID + JOB_SUFFIX);
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
		slaveWorkers = new ArrayList<>(this.expectedSlaves = expectedWorkers);
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
							unsatisfiedConstraints.add(sra.deserializeResult(this));
						}
					}
					else if (contents instanceof SerializableEvlResultAtom) {
						unsatisfiedConstraints.add(((SerializableEvlResultAtom) contents).deserializeResult(this));
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
			catch (EolRuntimeException eox) {
				throw new RuntimeException(eox);
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
		};
	}
	
	protected void waitForWorkersToFinishJobs(JMSContext jobContext) {
		System.out.println(LOG_PREFIX+" awaiting completion...");
		while (workersFinished.get() < slaveWorkers.size()) {
			synchronized (workersFinished) {
				try {
					workersFinished.wait();
				}
				catch (InterruptedException ie) {}
			}
		}
		System.out.println(LOG_PREFIX+" completed.");
	}
	
	protected void teardown() throws Exception {
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
	protected final void checkConstraints() throws EolRuntimeException {
		final Serializable config = getContext().getJobParameters();
		
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Initial registration of workers
			Destination tempQueue = regContext.createTemporaryQueue();
			JMSProducer regProducer = regContext.createProducer();
			regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			System.out.println(LOG_PREFIX+" awaiting workers since "+System.currentTimeMillis());
			
			AtomicInteger registeredWorkers = new AtomicInteger();
			// Triggered when a worker announces itself to the registration queue
			regContext.createConsumer(regContext.createQueue(REGISTRATION_QUEUE)).setMessageListener(msg -> {
				try {
					// Assign worker ID
					WorkerView worker = createWorker(registeredWorkers.incrementAndGet(), msg.getJMSReplyTo());
					slaveWorkers.add(worker);
					// Tell the worker what their ID is along with the configuration parameters
					Message configMsg = regContext.createObjectMessage(config);
					configMsg.setJMSReplyTo(tempQueue);
					configMsg.setJMSCorrelationID(worker.workerID);
					regProducer.send(worker.localBox, configMsg);
				}
				catch (JMSException jmx) {
					throw new JMSRuntimeException(jmx.getMessage());
				}
			});
			
			try (JMSContext resultsContext = regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				resultsContext.createConsumer(resultsContext.createQueue(RESULTS_QUEUE_NAME))
					.setMessageListener(getResultsMessageListener());
				
				AtomicInteger readyWorkers = new AtomicInteger();
				// Triggered when a worker has completed loading the configuration
				regContext.createConsumer(tempQueue).setMessageListener(response -> {
					String wid;
					try {
						wid = response.getJMSCorrelationID();
					}
					catch (JMSException jmx) {
						throw new JMSRuntimeException(jmx.getMessage());
					}
					
					WorkerView worker = slaveWorkers.stream()
						.filter(w -> w.workerID.equals(wid))
						.findAny()
						.orElseThrow(() -> new JMSRuntimeException("Could not find worker with id "+wid));
					
					int workersReady = readyWorkers.incrementAndGet();
					confirmWorker(worker, workersReady);
					
					if (workersReady >= expectedSlaves) {
						synchronized (readyWorkers) {
							readyWorkers.notify();
						}
					}
				});
				
				beforeEndRegistrationContext(readyWorkers, resultsContext);
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
			
			try (JMSContext jobContext = regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				processJobs(jobContext);
				waitForWorkersToFinishJobs(jobContext);
			}
			catch (Exception ex) {
				throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
			}
		}
	}
	
	/**
	 * This method can be used to perform any final steps, such as waiting, before the main JMSContext
	 * used for registration is destroyed.
	 * 
	 * @param readyWorkers Convenience handle which may be used for synchronization, e.g.
	 * to wait on the workers to be ready.
	 * @param session The inner-most JMSContext, used by the results processor.
	 */
	protected void beforeEndRegistrationContext(AtomicInteger readyWorkers, JMSContext session) throws EolRuntimeException, JMSException {
		while (readyWorkers.get() < expectedSlaves) {
			try {
				synchronized (readyWorkers) {
					readyWorkers.wait();
				}
			}
			catch (InterruptedException ie) {}
		}
	}

	protected void processJobs(JMSContext jobContext) throws Exception {
		final EvlContextDistributedMaster evlContext = getContext();
		final int parallelism = evlContext.getDistributedParallelism()+1;
		final List<ConstraintContextAtom> ccJobs = ConstraintContextAtom.getContextJobs(this);
		List<DistributedEvlBatch> batches = DistributedEvlBatch.getBatches(ccJobs, parallelism);
		assert slaveWorkers.size() == batches.size()-1;
		
		Iterator<WorkerView> workersIter = slaveWorkers.iterator();
		Iterator<DistributedEvlBatch> batchesIter = batches.iterator();
		
		while (workersIter.hasNext()) {
			WorkerView worker = workersIter.next();
			worker.sendJob(batchesIter.next(), jobContext);
			worker.signalEnd(jobContext);
			System.out.println(LOG_PREFIX+" finished submitting to "+worker+" at "+System.currentTimeMillis());
		}
		
		assert batchesIter.hasNext();
		System.out.println(LOG_PREFIX+" began processing own jobs at "+System.currentTimeMillis());
		addToResults(batchesIter.next().evaluate(ccJobs, evlContext));
		System.out.println(LOG_PREFIX+" finished processing own jobs at "+System.currentTimeMillis());
	}

	/**
	 * Called when a worker has registered.
	 * @param currentWorkers The number of currently registered workers.
	 * @param outbox The channel used to contact this work.
	 * @return The created worker.
	 */
	protected WorkerView createWorker(int currentWorkers, Destination outbox) {
		WorkerView worker = new WorkerView(WORKER_ID_PREFIX + currentWorkers);
		worker.localBox = outbox;
		return worker;
	}

	/**
	 * Called when a worker has completed loading its configuration.
	 * @param worker The worker that has been configured.
	 * @param workerReady The number of workers that have currently been configured, including this one.
	 * That is, the value will never be less than 1.
	 */
	protected void confirmWorker(WorkerView worker, int workerReady) {
		System.out.println(LOG_PREFIX + worker+" ready at "+System.currentTimeMillis());
		worker.confirm();
	}

	@Override
	protected final void postExecution() throws EolRuntimeException {
		super.postExecution();
		try {
			teardown();
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
}
