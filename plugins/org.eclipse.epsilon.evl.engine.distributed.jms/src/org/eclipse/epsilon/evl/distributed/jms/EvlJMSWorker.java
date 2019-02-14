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
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.eclipse.epsilon.common.function.CheckedConsumer;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.data.DistributedEvlBatch;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;

/**
 * Reactive slave worker.
 * 
 * @see EvlModuleDistributedSlave
 * @see EvlModuleDistributedMasterJMS
 * @author Sina Madani
 * @since 1.6
 */
public final class EvlJMSWorker extends AbstractWorker implements Runnable {

	public static void main(String[] args) throws Exception {
		String host = args[0];
		
		if (args.length > 0) try {
			host = new URI(args[0]).toString();
		}
		catch (URISyntaxException urx) {
			System.err.println(urx);
			host = "tcp://127.0.0.1:61616";
			System.err.println("Using default "+host);
		}
		
		try (EvlJMSWorker worker = new EvlJMSWorker(host)) {
			worker.run();
		}
	}
	
	final ConnectionFactory connectionFactory;
	DistributedRunner configContainer;
	String workerID;
	final AtomicInteger jobsInProgress = new AtomicInteger();

	public EvlJMSWorker(String host) {
		connectionFactory = new ActiveMQJMSConnectionFactory(host);
	}
	
	void setup() throws Exception {
		try (JMSContext regContext = connectionFactory.createContext()) {
			// Announce our presence to the master
			Destination regQueue = regContext.createQueue(REGISTRATION_QUEUE);
			Destination tempQueue = regContext.createTemporaryQueue();
			JMSProducer regProducer = regContext.createProducer();
			regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			Message initMsg = regContext.createMessage();
			initMsg.setJMSReplyTo(tempQueue);
			regProducer.send(regQueue, initMsg);
			
			// Get the configuration and our ID from the reply
			Message configMsg = regContext.createConsumer(tempQueue).receive();
			this.workerID = configMsg.getStringProperty(ID_PROPERTY);
			log("Configuration and ID received");
			
			configContainer = EvlContextDistributedSlave.parseJobParameters(configMsg.getBody(Map.class));
			configContainer.preExecute();
			((EvlModuleDistributedSlave)configContainer.getModule()).prepareExecution();
			
			// This is to acknowledge when we have completed loading the script(s) and model(s)
			Message configuredAckMsg = regContext.createMessage();
			configuredAckMsg.setStringProperty(ID_PROPERTY, workerID);
			configuredAckMsg.setJMSReplyTo(tempQueue);
			// Tell the master we're setup and ready to work
			regProducer.send(configMsg.getJMSReplyTo(), configuredAckMsg);
		}
	}
	
	void processJobs() throws JMSException {
		try (JMSContext jobContext = connectionFactory.createContext()) {
			// Job processing, requires destinations for inputs (jobs) and outputs (results)
			Destination jobDest = jobContext.createTopic(workerID + JOB_SUFFIX);
			Queue resultsQueue = jobContext.createQueue(RESULTS_QUEUE_NAME);
			JMSProducer resultsSender = jobContext.createProducer();
			
			CheckedConsumer<Serializable, JMSException> resultProcessor = obj -> {
				ObjectMessage resultsMessage = jobContext.createObjectMessage(obj);
				resultsMessage.setStringProperty(ID_PROPERTY, workerID);
				resultsSender.send(resultsQueue, resultsMessage);
			};
			
			jobContext.createConsumer(jobDest).setMessageListener(
				getJobProcessor(resultProcessor, (EvlModuleDistributedSlave) configContainer.getModule())
			);
			
			awaitCompletion();
	
			// For some silly reason apparently re-using the jobContext is invalid due to concurrency.
			try (JMSContext finishedContext = jobContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				// Tell the master we've finished
				Message finishedMsg = finishedContext.createMessage();
				finishedMsg.setStringProperty(ID_PROPERTY, workerID);
				finishedMsg.setBooleanProperty(LAST_MESSAGE_PROPERTY, true);
				finishedContext.createProducer().send(finishedContext.createQueue(RESULTS_QUEUE_NAME), finishedMsg);
			}
		}
	}
	
	void awaitCompletion() {
		log("Awaiting completion");
		while (jobsInProgress.get() > 0 || !finished.get()) {
			try {
				synchronized (jobsInProgress) {
					jobsInProgress.wait();
				}
			}
			catch (InterruptedException ie) {}
		}
		log("Finished all jobs");
	}
	
	MessageListener getJobProcessor(final CheckedConsumer<Serializable, ? extends JMSException> resultProcessor, final EvlModuleDistributedSlave module) {
		return msg -> {
			try {
				if (msg instanceof ObjectMessage) {
					jobsInProgress.incrementAndGet();
					
					final Serializable objMsg = ((ObjectMessage)msg).getObject();
					final Object resultObj;
					
					if (objMsg instanceof SerializableEvlInputAtom) {
						resultObj  = ((SerializableEvlInputAtom) objMsg).evaluate(module);
					}
					else if (objMsg instanceof DistributedEvlBatch) {
						resultObj = module.evaluateBatch((DistributedEvlBatch) objMsg);
					}
					else {
						resultObj = null;
						log("Received unexpected object of type "+objMsg.getClass().getName());
					}
					
					if (resultObj instanceof Serializable) {
						resultProcessor.acceptThrows((Serializable) resultObj);
					}
					
					jobsInProgress.decrementAndGet();
				}
				
				if (!(msg instanceof ObjectMessage) || msg.getBooleanProperty(LAST_MESSAGE_PROPERTY)) {
					finished.set(true);
				}
				
				if (finished.get() && jobsInProgress.get() <= 0) {
					// Wake up the main thread once all jobs have been processed.
					synchronized (jobsInProgress) {
						jobsInProgress.notify();
					}
				}
			}
			catch (JMSException | EolRuntimeException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		};
	}

	@Override
	public void run() {
		try {
			setup();
			processJobs();
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	@Override
	public void close() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
	
	void log(String message) {
		System.out.println("["+workerID+"] "+LocalDateTime.now()+" "+message);
	}
}
