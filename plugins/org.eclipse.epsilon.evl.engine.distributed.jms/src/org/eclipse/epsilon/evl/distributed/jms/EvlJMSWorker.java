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
import java.util.Map;
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
 *
 * @see EvlModuleDistributedMasterJMS
 * @author Sina Madani
 * @since 1.6
 */
public class EvlJMSWorker extends AbstractWorker implements Runnable {

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
	
	@Override
	public void run() {
		try {
			setup();
			processJobs();
			//container.postExecute();
			close();
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	final ConnectionFactory connectionFactory;
	DistributedRunner configContainer;
	String workerID;
	volatile boolean finished;
	final Object completionLock = new Object();

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
			this.workerID = configMsg.getJMSCorrelationID();
			System.out.println(workerID + " config and ID received at "+System.currentTimeMillis());
			
			configContainer = EvlContextDistributedSlave.parseJobParameters(configMsg.getBody(Map.class));
			configContainer.preExecute();
			((EvlModuleDistributedSlave)configContainer.getModule()).prepareExecution();
			
			// This is to acknowledge when we have completed loading the script(s) and model(s)
			Message configuredAckMsg = regContext.createMessage();
			configuredAckMsg.setJMSCorrelationID(workerID);
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
				resultsMessage.setJMSCorrelationID(workerID);
				resultsSender.send(resultsQueue, resultsMessage);
			};
			
			jobContext.createConsumer(jobDest).setMessageListener(
				getJobProcessor(resultProcessor, (EvlModuleDistributedSlave) configContainer.getModule())
			);
			
			awaitCompletion();
			
			try (JMSContext finishedContext = jobContext.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
				// Tell the master we've finished
				Message finishedMsg = finishedContext.createMessage();
				finishedMsg.setJMSCorrelationID(workerID);
				Destination finishedDest = finishedContext.createQueue(RESULTS_QUEUE_NAME);
				finishedContext.createProducer().send(finishedDest, finishedMsg);
			}
		}
	}
	
	void awaitCompletion() {
		System.out.println(workerID+" awaiting jobs since "+System.currentTimeMillis());
		while (!finished) {
			try {
				synchronized (completionLock) {
					completionLock.wait();
				}
			}
			catch (InterruptedException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			}
		}
		System.out.println(workerID+" finished jobs at "+System.currentTimeMillis());
	}
	
	protected MessageListener getJobProcessor(final CheckedConsumer<Serializable, ? extends JMSException> resultProcessor, final EvlModuleDistributedSlave module) {
		return msg -> {
			try {
				if (!(msg instanceof ObjectMessage)) {
					// End of jobs
					finished = true;
					System.out.println(workerID+" received all jobs by "+System.currentTimeMillis());
					synchronized (completionLock) {
						completionLock.notify();
					}
					return;
				}
				
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
					System.err.println("["+workerID+"] Received unexpected object of type "+objMsg.getClass().getName());
				}
				
				if (resultObj instanceof Serializable) {
					resultProcessor.acceptThrows((Serializable) resultObj);
					if (finished) {
						// Wake up the main thread once the last job has been processed and sent
						synchronized (completionLock) {
							completionLock.notify();
						}
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
	public void close() throws Exception {
		if (connectionFactory instanceof AutoCloseable) {
			((AutoCloseable) connectionFactory).close();
		}
	}
}
