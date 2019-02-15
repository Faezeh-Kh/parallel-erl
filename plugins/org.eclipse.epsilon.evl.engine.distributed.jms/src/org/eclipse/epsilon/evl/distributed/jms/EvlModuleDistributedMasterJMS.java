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
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.epsilon.eol.cli.EolConfigParser;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

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
public abstract class EvlModuleDistributedMasterJMS extends EvlModuleDistributedMaster {
	
	public static void extensibleMain(Class<? extends EvlModuleDistributedMasterJMS> moduleClass, String... args) throws Exception {
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
			"-module", moduleClass.getName().substring(20),
				"int="+expectedWorkers,
				"String="+addr
		});
	}
	
	public static final String
		REGISTRATION_QUEUE = "registration",
		RESULTS_QUEUE_NAME = "results",
		JOB_SUFFIX = "-jobs",
		WORKER_ID_PREFIX = "EVL-jms-";
	
	protected final String host;
	protected final List<WorkerView> slaveWorkers;
	protected final int expectedSlaves;
	protected final AtomicInteger workersFinished = new AtomicInteger();
	protected ConnectionFactory connectionFactory;
	// Set this to false for unbounded scalability
	protected boolean refuseAdditionalWorkers = true;
	
	protected class WorkerView extends AbstractWorker {
		public Destination localBox = null;
		JMSContext session;
		Topic jobsTopic;
		JMSProducer jobSender;
		
		public WorkerView(String id) {
			this.workerID = id;
		}
		
		public void confirm() {
			confirm(null);
		}
		
		public void confirm(JMSContext parentSession) {
			session = parentSession != null ?
				parentSession.createContext(JMSContext.AUTO_ACKNOWLEDGE) :
				connectionFactory.createContext();
			jobsTopic = session.createTopic(workerID + JOB_SUFFIX);
			jobSender = session.createProducer();
		}
		
		/**
		 * Sends an {@linkplain ObjectMessage} to this worker's destination.
		 * 
		 * @param input The message contents
		 * @param last Whether this is the last message that will be sent.
		 * @throws JMSRuntimeException
		 */
		public void sendJob(Serializable input, boolean last) throws JMSRuntimeException {
			ObjectMessage jobMsg = session.createObjectMessage(input);
			setMessageParameters(jobMsg, last);
			jobSender.send(jobsTopic, jobMsg);
		}
		
		protected void setMessageParameters(Message msg, boolean last) throws JMSRuntimeException {
			try {
				msg.setStringProperty(ID_PROPERTY, workerID);
				msg.setBooleanProperty(LAST_MESSAGE_PROPERTY, last);
			}
			catch (JMSException jmx) {
				jmx.printStackTrace();
			}
		}
		
		@Override
		public void close() throws Exception {
			session.close();
			session = null;
			jobsTopic = null;
			jobSender = null;
		}

		/**
		 * Called to signal that a worker has completed all of its jobs.
		 * @param msg The last message received
		 * @throws JMSRuntimeException
		 */
		public void onCompletion(Message msg) throws JMSRuntimeException {
			finished.set(true);
			try {
				assert msg.getBooleanProperty(LAST_MESSAGE_PROPERTY);
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			} 
		}
	}
	
	public EvlModuleDistributedMasterJMS(int expectedWorkers, String host) throws URISyntaxException {
		super(expectedWorkers);
		this.host = host;
		slaveWorkers = new ArrayList<>(this.expectedSlaves = expectedWorkers);
	}
	
	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		connectionFactory = new ActiveMQJMSConnectionFactory(host);
		log("Connected to "+host);
	}
	
	@Override
	protected final void checkConstraints() throws EolRuntimeException {
		final Serializable config = getContext().getJobParameters();
		
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Initial registration of workers
			Destination tempQueue = regContext.createTemporaryQueue();
			JMSProducer regProducer = regContext.createProducer();
			regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			log("Awaiting workers");
			
			AtomicInteger registeredWorkers = new AtomicInteger();
			// Triggered when a worker announces itself to the registration queue
			regContext.createConsumer(regContext.createQueue(REGISTRATION_QUEUE)).setMessageListener(msg -> {
				// For security / load purposes, stop additional workers from being picked up.
				if (refuseAdditionalWorkers && registeredWorkers.get() >= expectedSlaves) return;
				try {
					// Assign worker ID
					WorkerView worker = createWorker(registeredWorkers.incrementAndGet(), msg.getJMSReplyTo());
					slaveWorkers.add(worker);
					// Tell the worker what their ID is along with the configuration parameters
					Message configMsg = regContext.createObjectMessage(config);
					configMsg.setJMSReplyTo(tempQueue);
					configMsg.setStringProperty(AbstractWorker.ID_PROPERTY, worker.workerID);
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
						wid = response.getStringProperty(AbstractWorker.ID_PROPERTY);
					}
					catch (JMSException jmx) {
						throw new JMSRuntimeException(jmx.getMessage());
					}
					
					WorkerView worker = slaveWorkers.stream()
						.filter(w -> w.workerID.equals(wid))
						.findAny()
						.orElseThrow(() -> new JMSRuntimeException("Could not find worker with id "+wid));
					
					try {
						confirmWorker(worker, resultsContext, readyWorkers);
					}
					catch (JMSException jmx) {
						throw new JMSRuntimeException(jmx.getMessage());
					}
				});
				
				try (JMSContext jobContext = resultsContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
					processJobs(readyWorkers, jobContext);
					waitForWorkersToFinishJobs(jobContext);
				}
			}
			catch (Exception ex) {
				if (ex instanceof JMSException) throw new JMSRuntimeException(ex.getMessage());
				else if (ex instanceof EolRuntimeException) throw (EolRuntimeException) ex;
				else throw new EolRuntimeException(ex);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	protected MessageListener getResultsMessageListener() {
		final Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		final AtomicInteger resultsInProgress = new AtomicInteger();
		
		return msg -> {
			try {
				resultsInProgress.incrementAndGet();
				
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
						log("Received unexpected object of type "+contents.getClass().getSimpleName());
					}
				}
				
				if (!(msg instanceof ObjectMessage) || msg.getBooleanProperty(AbstractWorker.LAST_MESSAGE_PROPERTY)) {
					String workerID = msg.getStringProperty(AbstractWorker.ID_PROPERTY);
					WorkerView worker = slaveWorkers.stream()
						.filter(w -> w.workerID.equals(workerID))
						.findAny()
						.orElseThrow(() -> new java.lang.IllegalStateException("Could not find worker with ID "+workerID));
					
					workerCompleted(worker, msg);
					
					if (workersFinished.incrementAndGet() >= expectedSlaves) {
						// Before signalling, we need to wait for all received results to be processed
						while (resultsInProgress.get() > 1) synchronized (resultsInProgress) {
							try {
								resultsInProgress.wait();
							}
							catch (InterruptedException ie) {}
						}
						synchronized (workersFinished) {
							workersFinished.notify();
						}
					}
				}
			}
			catch (EolRuntimeException eox) {
				throw new RuntimeException(eox);
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
			finally {
				if (resultsInProgress.decrementAndGet() <= 1) synchronized (resultsInProgress) {
					resultsInProgress.notify();
				}
			}
		};
	}
	
	/**
	 * This method is called in the body of {@link #checkConstraints()}, and is intended
	 * to be where the main processing logic goes. Immediately after this method, the
	 * {@link #waitForWorkersToFinishJobs(JMSContext)} is called.
	 * 
	 * @param readyWorkers  Convenience handle which may be used for synchronization, e.g.
	 * to wait on the workers to be ready.
	 * @param jobContext The inner-most JMSContext.
	 * @throws Exception
	 */
	abstract protected void processJobs(AtomicInteger readyWorkers, JMSContext jobContext) throws Exception;

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
	 * Called when a worker has signalled its completion status. This method
	 * can be used to perform additional tasks, but should always call the
	 * {@link WorkerView#onCompletion(Message)} method.
	 * 
	 * @param worker The worker that has finished.
	 * @param msg The message received from the worker to signal this.
	 */
	protected void workerCompleted(WorkerView worker, Message msg) {
		worker.onCompletion(msg);
		log(worker.workerID + " finished");
	}

	protected void waitForWorkersToFinishJobs(JMSContext jobContext) {
		log("Awaiting workers to signal completion...");
		while (workersFinished.get() < expectedSlaves) synchronized (workersFinished) {
			try {
				workersFinished.wait();
			}
			catch (InterruptedException ie) {}
		}
		log("All workers finished");
	}
	
	/**
	 * Called when a worker has completed loading its configuration. This method can be used to perform
	 * additional tasks, but should always call {@link WorkerView#confirm(JMSContext)}.
	 * 
	 * @param worker The worker that has been configured.
	 * @param session The context in which the listener was invoked.
	 * @param workerReady The number of workers that have currently been configured, excluding this one.
	 * Implementations are expected to increment this number and can use the object's lock to signal
	 * when all workers are connected by comparing this number to {@linkplain #expectedSlaves}.
	 * @throws JMSException 
	 */
	protected void confirmWorker(WorkerView worker, JMSContext session, AtomicInteger workersReady) throws JMSException {
		log(worker+" ready");
		worker.confirm(session);
		
		if (workersReady.incrementAndGet() >= expectedSlaves) synchronized (workersReady) {
			workersReady.notify();
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
	
	protected void log(String message) {
		System.out.println("[MASTER] "+LocalTime.now()+" "+message);
	}
}
