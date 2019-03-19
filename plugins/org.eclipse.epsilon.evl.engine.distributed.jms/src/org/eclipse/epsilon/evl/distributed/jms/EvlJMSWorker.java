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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
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
		if (args.length < 1) throw new java.lang.IllegalStateException(
			"Must provide base path!"
		);
		
		String basePath = args[0];
		
		String host = "tcp://localhost:61616";
		if (args.length > 1) try {
			host = new URI(args[1]).toString();
		}
		catch (URISyntaxException urx) {
			System.err.println(urx);
			System.err.println("Using default "+host);
		}
		
		try (EvlJMSWorker worker = new EvlJMSWorker(host, basePath)) {
			worker.run();
		}
	}
	
	final AtomicBoolean finished = new AtomicBoolean(false);
	final ConnectionFactory connectionFactory;
	String id, basePath;
	DistributedEvlRunConfiguration configContainer;
	EvlModuleDistributedSlave module;

	public EvlJMSWorker(String host, String basePath) {
		connectionFactory = new ActiveMQJMSConnectionFactory(host);
	}
	
	@Override
	public void run() {
		try (JMSContext regContext = connectionFactory.createContext()) {
			Runnable ackSender = setup(regContext);
			
			try (JMSContext jobContext = regContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				prepareToProcessJobs(jobContext);
				
				// Tell the master we're setup and ready to work. We need to send the message here
				// because if the master is fast we may receive jobs before we have even created the listener!
				ackSender.run();
				
				awaitCompletion();
		
				// Tell the master we've finished
				try (JMSContext endContext = jobContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
					signalCompletion(endContext);
				}
			}
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	Runnable setup(JMSContext regContext) throws Exception {
		// Announce our presence to the master
		Queue regQueue = regContext.createQueue(REGISTRATION_QUEUE);
		JMSProducer regProducer = regContext.createProducer();
		regProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		Message initMsg = regContext.createMessage();
		Queue tempQueue = regContext.createTemporaryQueue();
		initMsg.setJMSReplyTo(tempQueue);
		regProducer.send(regQueue, initMsg);
		
		// Get the configuration and our ID
		Message configMsg = regContext.createConsumer(tempQueue).receive();
		this.id = configMsg.getStringProperty(ID_PROPERTY);
		log("Configuration and ID received");
		
		configContainer = EvlContextDistributedSlave.parseJobParameters(configMsg.getBody(Map.class), basePath);
		configContainer.preExecute();
		(module = (EvlModuleDistributedSlave) configContainer.getModule()).prepareExecution();
		
		// This is to acknowledge when we have completed loading the script(s) and model(s)
		Message configuredAckMsg = regContext.createMessage();
		configuredAckMsg.setStringProperty(ID_PROPERTY, id);
		Destination configAckDest = configMsg.getJMSReplyTo();
		return () -> regProducer.send(configAckDest, configuredAckMsg);
	}
	
	void prepareToProcessJobs(JMSContext jobContext) throws JMSException {
		// Job processing, requires destinations for inputs (jobs) and outputs (results)
		Queue resultsQueue = jobContext.createQueue(RESULTS_QUEUE_NAME);
		JMSProducer resultsSender = jobContext.createProducer();
		
		Consumer<Serializable> resultProcessor = obj -> {
			ObjectMessage resultsMessage = jobContext.createObjectMessage(obj);
			try {
				resultsMessage.setStringProperty(ID_PROPERTY, id);
			}
			catch (JMSException jmx) {
				throw new JMSRuntimeException(jmx.getMessage());
			}
			resultsSender.send(resultsQueue, resultsMessage);
		};
		
		jobContext.createConsumer(jobContext.createQueue(JOBS_QUEUE)).setMessageListener(
			getJobProcessor(resultProcessor, module)
		);
		
		jobContext.createConsumer(jobContext.createTopic(END_JOBS_TOPIC)).setMessageListener(msg -> {
			//assert msg.getBooleanProperty(LAST_MESSAGE_PROPERTY);
			synchronized (finished) {
				finished.set(true);
				finished.notify();
			}
			log("Acknowledged end of jobs");
		});
	}
	
	void signalCompletion(JMSContext endContext) throws JMSException {
		ObjectMessage finishedMsg = endContext.createObjectMessage();
		finishedMsg.setStringProperty(ID_PROPERTY, id);
		finishedMsg.setBooleanProperty(LAST_MESSAGE_PROPERTY, true);
		finishedMsg.setObject((Serializable) configContainer.getSerializableRuleExecutionTimes());
		endContext.createProducer().send(endContext.createQueue(RESULTS_QUEUE_NAME), finishedMsg);
	}
	
	void awaitCompletion() {
		log("Awaiting completion");
		while (!finished.get()) synchronized (finished) {
			try {
				finished.wait();
			}
			catch (InterruptedException ie) {}
		}
		log("Finished all jobs");
	}
	
	MessageListener getJobProcessor(final Consumer<Serializable> resultProcessor, final EvlModuleDistributedSlave module) {
		final AtomicInteger jobsInProgress = new AtomicInteger();
		return msg -> {
			try {
				jobsInProgress.incrementAndGet();
				
				if (msg instanceof ObjectMessage) {
					final Serializable msgObj = ((ObjectMessage)msg).getObject();
					final Object resultObj;
					
					if (msgObj instanceof SerializableEvlInputAtom) {
						resultObj  = ((SerializableEvlInputAtom) msgObj).evaluate(module);
					}
					else if (msgObj instanceof Iterable) {
						ArrayList<SerializableEvlResultAtom> resultsCol = new ArrayList<>();
						
						for (Object obj : (Iterable<?>) msgObj) {
							if (obj instanceof SerializableEvlInputAtom) {
								resultsCol.addAll(((SerializableEvlInputAtom) obj).evaluate(module));
							}
							else if (obj instanceof DistributedEvlBatch) {
								resultsCol.addAll(module.evaluateBatch((DistributedEvlBatch) obj));
							}
						}
						
						resultObj = resultsCol;
					}
					else if (msgObj instanceof DistributedEvlBatch) {
						resultObj = module.evaluateBatch((DistributedEvlBatch) msgObj);
					}
					else {
						resultObj = null;
						log("Received unexpected object of type "+msgObj.getClass().getName());
					}
					
					if (resultObj instanceof Serializable) {
						resultProcessor.accept((Serializable) resultObj);
					}
					
					jobsInProgress.decrementAndGet();
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
			catch (EolRuntimeException eox) {
				throw new RuntimeException(eox);
			}
			finally {
				// Wake up the main thread once all jobs have been processed.
				if (jobsInProgress.decrementAndGet() <= 0 && finished.get()) synchronized (finished) {
					finished.notify();
				}
			}
		};
	}
	
	@Override
	public void close() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
	
	void log(Object message) {
		System.out.println("["+id+"] "+LocalTime.now()+" "+message);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof EvlJMSWorker)) return false;
		return Objects.equals(this.id, ((EvlJMSWorker)obj).id);
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
