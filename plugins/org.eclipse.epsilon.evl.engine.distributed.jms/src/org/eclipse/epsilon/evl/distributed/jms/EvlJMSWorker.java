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

import static org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS.*;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.epsilon.common.function.CheckedRunnable;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;

/**
 * Reactive slave worker.
 * 
 * @see EvlModuleDistributedSlave
 * @see EvlModuleDistributedMasterJMS
 * @author Sina Madani
 * @since 1.6
 */
public final class EvlJMSWorker implements Runnable, AutoCloseable {

	public static void main(String... args) throws Exception {
		if (args.length < 2) throw new java.lang.IllegalStateException(
			"Must provide base path and session ID!"
		);
		String basePath = args[0];
		int sessionID = Integer.valueOf(args[1]);
		
		String host = "tcp://localhost:61616";
		if (args.length > 2) try {
			host = new URI(args[2]).toString();
		}
		catch (URISyntaxException urx) {
			System.err.println(urx);
			System.err.println("Using default "+host);
		}
		
		try (EvlJMSWorker worker = new EvlJMSWorker(host, basePath, sessionID)) {
			System.out.println("Worker started for session "+sessionID);
			worker.run();
		}
	}
	
	final java.util.concurrent.BlockingQueue<Serializable> jobsInProgress = new LinkedBlockingQueue<>();
	final AtomicBoolean finished = new AtomicBoolean(false);
	final ConnectionFactory connectionFactory;
	final String basePath;
	final int sessionID;
	String workerID;
	DistributedEvlRunConfiguration configContainer;
	EvlModuleDistributedSlave module;
	volatile Serializable stopBody;

	public EvlJMSWorker(String host, String basePath, int sessionID) {
		connectionFactory = new ActiveMQJMSConnectionFactory(host);
		this.basePath = basePath;
		this.sessionID = sessionID;
	}
	
	@Override
	public void run() {
		try (JMSContext regContext = connectionFactory.createContext()) {
			Runnable ackSender = setup(regContext);
			
			try (JMSContext resultContext = regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				try (JMSContext jobContext = resultContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
					CheckedRunnable<?> completionSignaller = prepareToProcessJobs(jobContext, resultContext);
					
					// Tell the master we're setup and ready to work. We need to send the message here
					// because if the master is fast we may receive jobs before we have even created the listener!
					ackSender.run();
					
					awaitCompletion();
			
					// Tell the master we've finished
					completionSignaller.runThrows();
					onCompletion();
				}
			}
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	Runnable setup(JMSContext regContext) throws Exception {
		// Announce our presence to the master
		Queue regQueue = regContext.createQueue(REGISTRATION_QUEUE+sessionID);
		JMSProducer regProducer = regContext.createProducer();
		regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		Message initMsg = regContext.createMessage();
		Queue tempQueue = regContext.createTemporaryQueue();
		initMsg.setJMSReplyTo(tempQueue);
		regProducer.send(regQueue, initMsg);
		
		// Get the configuration and our ID
		Message configMsg = regContext.createConsumer(tempQueue).receive();
		Destination configAckDest = configMsg.getJMSReplyTo();
		Message configuredAckMsg = regContext.createMessage();
		Runnable ackSender = () -> regProducer.send(configAckDest, configuredAckMsg);
		
		try {
			this.workerID = configMsg.getStringProperty(WORKER_ID_PROPERTY);
			log("Configuration and ID received");
			configuredAckMsg.setStringProperty(WORKER_ID_PROPERTY, workerID);
			
			Map<String, ? extends Serializable> configMap = configMsg.getBody(Map.class);
			configContainer = EvlContextDistributedSlave.parseJobParameters(configMap, basePath);
			configContainer.preExecute();
			(module = (EvlModuleDistributedSlave) configContainer.getModule()).prepareExecution();

			// This is to acknowledge when we have completed loading the script(s) and model(s) successfully
			configuredAckMsg.setIntProperty(CONFIG_HASH, configMap.hashCode());
			return ackSender;
		}
		catch (Exception ex) {
			// Tell the master we failed
			ackSender.run();
			throw ex;
		}
	}
	
	CheckedRunnable<JMSException> prepareToProcessJobs(JMSContext jobContext, JMSContext resultContext) throws JMSException {
		
		Thread resultsProcessor = new Thread(
			getJopProcessor(resultContext.createContext(JMSContext.AUTO_ACKNOWLEDGE))
		);
		resultsProcessor.setName("job-processor");
		
		resultContext.createConsumer(resultContext.createTopic(STOP_TOPIC+sessionID))
			.setMessageListener(getTerminalProcessor());
		
		jobContext.createConsumer(jobContext.createQueue(JOBS_QUEUE+sessionID))
			.setMessageListener(getJobProcessor());
		
		jobContext.createConsumer(jobContext.createTopic(END_JOBS_TOPIC+sessionID)).setMessageListener(msg -> {
			finished.set(true);
			if (jobsInProgress.isEmpty()) synchronized (finished) {
				finished.notify();
			}
			log("Acknowledged end of jobs");
		});
		
		resultsProcessor.start();
		
		return () -> {
			resultsProcessor.interrupt();
			ObjectMessage finishedMsg = jobContext.createObjectMessage();
			finishedMsg.setStringProperty(WORKER_ID_PROPERTY, workerID);
			finishedMsg.setBooleanProperty(LAST_MESSAGE_PROPERTY, true);
			finishedMsg.setObject((Serializable) configContainer.getSerializableRuleExecutionTimes());
			jobContext.createProducer().send(jobContext.createQueue(RESULTS_QUEUE_NAME+sessionID), finishedMsg);
		};
	}
	
	void onCompletion() throws Exception {
		log("Signalled completion");
	}
	
	void awaitCompletion() {
		log("Awaiting completion");
		while (!finished.get()) synchronized (finished) {
			try {
				finished.wait();
			}
			catch (InterruptedException ie) {}
		}
		if (stopBody == null) {
			log("Finished all jobs");
		}
		else {
			// Exception!
			log(stopBody);
		}
	}
	
	void onFail(Exception ex, Message msg) {
		System.err.println("Failed job '"+msg+"': "+ex);
	}
	
	Runnable getJopProcessor(JMSContext replyContext) {
		return () -> {
			JMSProducer resultSender = replyContext.createProducer();
			Queue resultQueue = replyContext.createQueue(RESULTS_QUEUE_NAME+sessionID);
			
			while (stopBody == null && (!jobsInProgress.isEmpty() || !finished.get())) try {
				Serializable currentJob = null;
				try {
					currentJob = jobsInProgress.take();
				}
				catch (InterruptedException ie) {
					if (stopBody != null) break;
				}
				
				ObjectMessage resultsMsg = null;
				try {
					Serializable resultObj = evaluateJob(currentJob);
					resultsMsg = replyContext.createObjectMessage(resultObj);
				}
				catch (EolRuntimeException eox) {
					resultsMsg = replyContext.createObjectMessage(currentJob);
					resultsMsg.setObjectProperty(EXCEPTION_PROPERTY, eox);
				}
				
				resultsMsg.setStringProperty(WORKER_ID_PROPERTY, workerID);
				resultSender.send(resultQueue, resultsMsg);
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
			
			replyContext.close();
			if (finished.get()) synchronized (finished) {
				finished.notify();
			}
		};
	}

	Serializable evaluateJob(Object msgObj) throws EolRuntimeException {
		if (msgObj instanceof Iterable) {
			return evaluateJob(((Iterable<?>) msgObj).iterator());
		}
		else if (msgObj instanceof Iterator) {
			ArrayList<SerializableEvlResultAtom> resultsCol = new ArrayList<>();
			for (Iterator<?> iter = (Iterator<?>) msgObj; iter.hasNext();) {
				Object obj = iter.next();
				if (obj instanceof SerializableEvlInputAtom) {
					resultsCol.addAll(((SerializableEvlInputAtom) obj).evaluate(module));
				}
				else if (obj instanceof DistributedEvlBatch) {
					resultsCol.addAll(module.evaluateBatch((DistributedEvlBatch) obj));
				}
			}
			return resultsCol;
		}
		if (msgObj instanceof SerializableEvlInputAtom) {
			return (Serializable)((SerializableEvlInputAtom) msgObj).evaluate(module);
		}
		else if (msgObj instanceof DistributedEvlBatch) {
			return (Serializable) module.evaluateBatch((DistributedEvlBatch) msgObj);
		}
		else if (msgObj instanceof java.util.stream.BaseStream) {
			return evaluateJob(((java.util.stream.BaseStream<?,?>)msgObj).iterator());
		}
		else {
			throw new IllegalArgumentException("Received unexpected object of type "+msgObj.getClass().getName());
		}
	}
	
	MessageListener getJobProcessor() {
		return msg -> {
			if (stopBody != null) return;
			try {
				if (msg instanceof ObjectMessage)  {
					jobsInProgress.add(((ObjectMessage)msg).getObject());
				}
				
				if (msg.getBooleanProperty(LAST_MESSAGE_PROPERTY)) {
					finished.set(true);
				}
				else if (!(msg instanceof ObjectMessage)) {
					log("Received unexpected message of type "+msg.getClass().getName());
				}
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
			// Wake up the main thread once all jobs have been processed.
			if (finished.get() && jobsInProgress.isEmpty()) synchronized (finished) {
				finished.notify();
			}
		};
	}

	MessageListener getTerminalProcessor() {
		return msg -> {
			log("Stopping execution!");
			synchronized (finished) {
				try {
					stopBody = msg.getBody(Serializable.class);
				}
				catch (JMSException ex) {
					stopBody = ex;
				}
				finished.set(true);
				finished.notify();
			}
		};
	}
	
	void log(Object message) {
		System.out.println("["+workerID+"] "+LocalTime.now()+" "+message);
	}
	
	@Override
	public void close() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof EvlJMSWorker)) return false;
		EvlJMSWorker other = (EvlJMSWorker) obj;
		return
			Objects.equals(this.workerID, other.workerID) &&
			Objects.equals(this.sessionID, other.sessionID);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(workerID, sessionID);
	}
	
	@Override
	public String toString() {
		return getClass().getName()+"-"+workerID+" (session "+sessionID+")";
	}
}
