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
import java.time.LocalDateTime;
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
		
		public void sendJob(Serializable input) throws JMSRuntimeException {
			ObjectMessage jobMsg = session.createObjectMessage(input);
			setMessageID(jobMsg);
			jobSender.send(jobsTopic, jobMsg);
		}
		
		public void signalEnd() throws JMSException {
			Message endMsg = session.createMessage();
			setMessageID(endMsg);
			jobSender.send(jobsTopic, endMsg);
		}
		
		protected void setMessageID(Message msg) throws JMSRuntimeException {
			try {
				msg.setJMSCorrelationID(workerID);
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
	}
	
	public EvlModuleDistributedMasterJMS(int expectedWorkers, String host) throws URISyntaxException {
		super(expectedWorkers);
		this.host = host;
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
						log("Received unexpected object of type "+contents.getClass().getSimpleName());
					}
				}
				else {
					String workerID = msg.getJMSCorrelationID();
					slaveWorkers.stream().filter(w -> w.workerID.equals(workerID)).findAny().ifPresent(w -> {
						w.finished.set(true);
						if (workersFinished.incrementAndGet() >= expectedSlaves) {
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
		log("awaiting completion...");
		while (workersFinished.get() < slaveWorkers.size()) {
			synchronized (workersFinished) {
				try {
					workersFinished.wait();
				}
				catch (InterruptedException ie) {}
			}
		}
		log("All workers finished.");
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
					try {
						confirmWorker(worker, resultsContext, workersReady);
					}
					catch (JMSException jmx) {
						throw new JMSRuntimeException(jmx.getMessage());
					}
					
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
	 * used for registration is destroyed. The base implementation does nothing.
	 * It is up to the subclass to decide how to use this method.
	 * 
	 * @param readyWorkers Convenience handle which may be used for synchronization, e.g.
	 * to wait on the workers to be ready.
	 * @param session The inner-most JMSContext, used by the results processor.
	 */
	protected void beforeEndRegistrationContext(AtomicInteger readyWorkers, JMSContext session) throws EolRuntimeException, JMSException {
		// Optional abstract method
	}

	/**
	 * This method is called in the body of {@link #checkConstraints()}, and is intended
	 * to be where the main processing logic goes.
	 * 
	 * @param jobContext
	 * @throws Exception
	 */
	abstract protected void processJobs(JMSContext jobContext) throws Exception;

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
	 * @param session The context in which the listener was invoked.
	 * @param workerReady The number of workers that have currently been configured, including this one.
	 * That is, the value will never be less than 1.
	 * @throws JMSException 
	 */
	protected void confirmWorker(WorkerView worker, JMSContext session, int workersReady) throws JMSException {
		log(worker+" ready");
		worker.confirm(session);
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
		System.out.println("[MASTER] "+LocalDateTime.now()+" "+message);
	}
}
