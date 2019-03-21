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
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.epsilon.common.function.CheckedConsumer;
import org.eclipse.epsilon.common.function.CheckedRunnable;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.*;
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
 * - Workers send back results to results queue, which are then deserialized. <br/><br/>
 * 
 * Note that each worker is processed independently and asynchronously. That is, once a worker has connected,
 * it need not wait for other workers to connect or be in the same stage of registration. This module is
 * designed such that it is possible for a fast worker to start sending results back before another has even
 * registered. This class also tries to abstract away from the handshaking / messaging code by invoking methods
 * at key points in the back-and-forth messaging process within listeners which can be used to control the
 * execution strategy. See the {@link #checkConstraints()} method for where these "checkpoint" methods are.
 * <br/><br/>
 * 
 * It is the responsibility of subclasses to handled failed jobs sent from workers. The {@link #failedJobs}
 * collection gets appended to every time a failure message is received. This message will usually be the job that
 * was sent to the worker. Every time a failure is added, the collection object's monitor is notified.
 * Implementations can use this to listen for failures and take appropriate action, such as re-scheduling the jobs
 * or processing them directly. Although this handling can happen at any stage (e.g. either during execution or once
 * all workers have finished), the {@link #processFailedJobs(JMSContext)} method is guaranteed to be called after
 * {@linkplain #waitForWorkersToFinishJobs(AtomicInteger, JMSContext)} so any remaining jobs will be processed
 * if they have not been handled. This therefore requires that implementations should remove jobs if they process
 * them during execution to avoid unnecessary duplicate processing. <br/>
 * It should also be noted that the {@link #failedJobs} is not thread-safe, so manual synchronization is required.
 * 
 * @see EvlJMSWorker
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributedMasterJMS extends EvlModuleDistributedMaster {
	
	public static final String
		JOBS_QUEUE = "worker-jobs",
		CONFIG_TOPIC = "configuration",
		END_JOBS_TOPIC = "no-more-jobs",
		REGISTRATION_QUEUE = "registration",
		RESULTS_QUEUE_NAME = "results",
		WORKER_ID_PREFIX = "EVL-jms-",
		LAST_MESSAGE_PROPERTY = "lastMsg",
		ID_PROPERTY = "wid",
		CONFIG_HASH = "configChecksum";
	
	protected final String host;
	protected final int expectedSlaves;
	protected final ConcurrentMap<String, Map<String, Duration>> slaveWorkers;
	protected final Collection<Serializable> failedJobs;
	protected ConnectionFactory connectionFactory;
	// Set this to false for unbounded scalability
	protected boolean refuseAdditionalWorkers = true;
	private CheckedConsumer<Serializable, JMSException> jobSender;
	private CheckedRunnable<JMSException> completionSender;
	
	public EvlModuleDistributedMasterJMS(int expectedWorkers, String host) throws URISyntaxException {
		super(expectedWorkers);
		this.host = host;
		slaveWorkers = new ConcurrentHashMap<>(this.expectedSlaves = expectedWorkers);
		failedJobs = new java.util.HashSet<>();
	}
	
	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		connectionFactory = new ActiveMQJMSConnectionFactory(host);
		log("Connected to "+host);
	}
	
	@Override
	protected final void checkConstraints() throws EolRuntimeException {
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Initial registration of workers
			final Destination tempDest = regContext.createTemporaryQueue();
			final JMSProducer regProducer = regContext.createProducer();
			regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			final Serializable config = getContext().getJobParameters();
			final int configHash = config.hashCode();
			final AtomicInteger registeredWorkers = new AtomicInteger();
			
			log("Awaiting workers");
			// Triggered when a worker announces itself to the registration queue
			regContext.createConsumer(regContext.createQueue(REGISTRATION_QUEUE)).setMessageListener(msg -> {
				// For security / load purposes, stop additional workers from being picked up.
				if (refuseAdditionalWorkers && registeredWorkers.get() >= expectedSlaves) return;
				try {
					// Assign worker ID
					int currentWorkers = registeredWorkers.incrementAndGet();
					String workerID = createWorker(currentWorkers, msg);
					slaveWorkers.put(workerID, Collections.emptyMap());
					// Tell the worker what their ID is along with the configuration parameters
					Message configMsg = regContext.createObjectMessage(config);
					configMsg.setJMSReplyTo(tempDest);
					configMsg.setStringProperty(ID_PROPERTY, workerID);
					configMsg.setIntProperty(CONFIG_HASH, configHash);
					regProducer.send(msg.getJMSReplyTo(), configMsg);
				}
				catch (JMSException jmx) {
					throw new JMSRuntimeException(jmx.getMessage());
				}
			});
			
			try (JMSContext resultsContext = regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				final JMSProducer jobsProducer = resultsContext.createProducer();
				final Queue jobsQueue = resultsContext.createQueue(JOBS_QUEUE);
				jobSender = obj -> jobsProducer.send(jobsQueue, obj);
				
				final Topic completionTopic = resultsContext.createTopic(END_JOBS_TOPIC);
				completionSender = () -> jobsProducer.send(completionTopic, resultsContext.createMessage());
				
				final AtomicInteger workersFinished = new AtomicInteger();
				
				resultsContext.createConsumer(resultsContext.createQueue(RESULTS_QUEUE_NAME))
					.setMessageListener(getResultsMessageListener(workersFinished));
				
				final AtomicInteger readyWorkers = new AtomicInteger();
				// Triggered when a worker has completed loading the configuration
				regContext.createConsumer(tempDest).setMessageListener(response -> {
					try {
						final int receivedHash = response.getIntProperty(CONFIG_HASH);
						if (receivedHash != configHash) {
							throw new java.lang.IllegalStateException("Received invalid configuration checksum!");
						}
						confirmWorker(response, resultsContext, readyWorkers);
					}
					catch (JMSException jmx) {
						throw new JMSRuntimeException(jmx.getMessage());
					}
				});
				
				try (JMSContext jobContext = resultsContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
					processJobs(readyWorkers, jobContext);
					waitForWorkersToFinishJobs(workersFinished, jobContext);
					processFailedJobs(jobContext);
				}
			}
			catch (Exception ex) {
				if (ex instanceof EolRuntimeException) throw (EolRuntimeException) ex;
				if (ex instanceof JMSException) throw new JMSRuntimeException(ex.getMessage());
				else throw new EolRuntimeException(ex);
			}
		}
	}
	
	protected void processFailedJobs(JMSContext jobContext) throws EolRuntimeException {
		for (Iterator<? extends Serializable> it = failedJobs.iterator(); it.hasNext(); it.remove()) {
			evaluateLocal(it.next());
		}
	}
	
	protected final void sendJob(Serializable msgBody) throws JMSException {
		jobSender.acceptThrows(msgBody);
	}
	
	protected final void signalCompletion() throws JMSException {
		completionSender.runThrows();
	}

	/**
	 * Main results processing listener. Implementations are expected to handle both results processing and
	 * signalling of terminal waiting condition once all workers have indicated all results have been
	 * processed. Due to the complexity of the implementation, it is not recommended that subclasses override
	 * this method. It is non-final for completeness / extensibility only. Incomplete / incorrect implementations
	 * will break the entire class, so overriding methods should be extremely careful and fully understand
	 * the inner workings / implementation of the base class if overriding this method.
	 * 
	 * @param workersFinished Mutable number of workers which have signalled completion status.
	 * @return A callback which can handle the semantics of results processing (i.e. deserialization and
	 * assignment) as well as co-ordination (signalling of completion etc.)
	 */
	@SuppressWarnings("unchecked")
	protected MessageListener getResultsMessageListener(final AtomicInteger workersFinished) {
		final Collection<UnsatisfiedConstraint> unsatisfiedConstraints = getContext().getUnsatisfiedConstraints();
		final AtomicInteger resultsInProgress = new AtomicInteger();
		
		return msg -> {
			try {
				resultsInProgress.incrementAndGet();
				
				if (msg instanceof ObjectMessage) {
					Serializable contents = ((ObjectMessage)msg).getObject();
					if (contents instanceof Iterable) {
						for (Serializable obj : (Iterable<? extends Serializable>)contents) {
							if (obj instanceof SerializableEvlResultAtom) {
								SerializableEvlResultAtom sra = (SerializableEvlResultAtom) obj;
								unsatisfiedConstraints.add(sra.deserializeResult(this));
							}
						}
					}
					else if (contents instanceof SerializableEvlResultAtom) {
						unsatisfiedConstraints.add(((SerializableEvlResultAtom) contents).deserializeResult(this));
					}
					else synchronized (failedJobs) {
						// Treat anything else (e.g. SerializableEvlInputAtom, DistributedEvlBatch) as a failure
						if (failedJobs.add(contents)) {
							failedJobs.notify();
						}
					}
				}
				
				if (msg.getBooleanProperty(LAST_MESSAGE_PROPERTY)) {
					String workerID = msg.getStringProperty(ID_PROPERTY);
					if (!slaveWorkers.containsKey(workerID)) {
						throw new java.lang.IllegalStateException("Could not find worker with ID "+workerID);
					}
					
					workerCompleted(workerID, msg);
					
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
	 * @param jobContext The inner-most JMSContext  from {@linkplain #checkConstraints()}.
	 * @throws Exception
	 */
	abstract protected void processJobs(final AtomicInteger workersReady, final JMSContext jobContext) throws Exception;

	/**
	 * Called when a worker has registered.
	 * @param currentWorkers The number of currently registered workers.
	 * @param outbox The channel used to contact this work.
	 * @return The created worker.
	 */
	protected String createWorker(int currentWorkers, Message regMsg) {
		return WORKER_ID_PREFIX + currentWorkers;
	}

	/**
	 * Called when a worker has signalled its completion status. This method
	 * can be used to perform additional tasks.
	 * 
	 * @param worker The worker that has finished.
	 * @param msg The message received from the worker to signal this.
	 */
	@SuppressWarnings("unchecked")
	protected void workerCompleted(String worker, Message msg) throws JMSException {
		if (msg instanceof ObjectMessage) {
			Serializable body = ((ObjectMessage) msg).getObject();
			if (body instanceof Map) {
				slaveWorkers.put(worker, (Map<String, Duration>) body);
			}
		}
		log(worker + " finished");
	}

	/**
	 * Waits for the critical condition <code>workersFinished.get() >= expectedSlaves</code>
	 * to be signalled from the results processor as returned from {@linkplain #getResultsMessageListener()}.
	 * 
	 * @param workersFinished The number of workers that have signalled completion status. The value
	 * should not be mutated by this method, and only used for synchronising on the condition.
	 * @param jobContext The inner-most JMSContext from {@linkplain #checkConstraints()}.
	 */
	protected void waitForWorkersToFinishJobs(AtomicInteger workersFinished, JMSContext jobContext) {
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
	protected void confirmWorker(final Message response, final JMSContext session, final AtomicInteger workersReady) throws JMSException {
		String worker = response.getStringProperty(ID_PROPERTY);
		if (!slaveWorkers.containsKey(worker)) {
			throw new JMSRuntimeException("Could not find worker with id "+worker);
		}
		
		log(worker+" ready");
		
		if (workersReady.incrementAndGet() >= expectedSlaves) synchronized (workersReady) {
			workersReady.notify();
		}
	}
	
	@Override
	protected void postExecution() throws EolRuntimeException {
		// Merge the workers' execution times with this one
		getContext().getExecutorFactory().getRuleProfiler().mergeExecutionTimes(
			slaveWorkers.values().stream()
				.flatMap(execTimes -> execTimes.entrySet().stream())
				.collect(Collectors.toMap(
					e -> this.constraints.stream()
						.filter(c -> c.getName().equals(e.getKey()))
						.findAny().get(),
					Map.Entry::getValue,
					(t1, t2) -> t1.plus(t2)
				))
		);
		
		super.postExecution();
		try {	
			teardown();
		}
		catch (Exception ex) {
			throw ex instanceof EolRuntimeException ? (EolRuntimeException) ex : new EolRuntimeException(ex);
		}
	}
	
	/**
	 * Cleanup method used to free resources once execution has completed.
	 * 
	 * @throws Exception
	 */
	protected void teardown() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
	
	/**
	 * Convenience method used for diagnostic purposes.
	 * 
	 * @param message The message to output.
	 */
	protected void log(Object message) {
		System.out.println("[MASTER] "+LocalTime.now()+" "+message);
	}
}
