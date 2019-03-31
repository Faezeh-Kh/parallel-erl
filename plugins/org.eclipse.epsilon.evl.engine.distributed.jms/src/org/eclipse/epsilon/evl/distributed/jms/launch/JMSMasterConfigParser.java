/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.launch;

import org.apache.commons.cli.Option;
import org.eclipse.epsilon.eol.cli.EolConfigParser;
import org.eclipse.epsilon.evl.distributed.jms.atomic.EvlModuleDistributedMasterJMSAtomic;
import org.eclipse.epsilon.evl.distributed.jms.batch.EvlModuleDistributedMasterJMSBatchAsync;
import org.eclipse.epsilon.evl.distributed.jms.batch.EvlModuleDistributedMasterJMSBatch;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 * @param <J>
 * @param <B>
 */
public class JMSMasterConfigParser<J extends JMSMasterRunner, B extends JMSMasterBuilder<J, B>> extends EolConfigParser<J, B> {

	public static void main(String... args) {
		new JMSMasterConfigParser<>().parseAndRun(args);
	}
	
	private final String
		brokerHostOpt = "broker",
		basePathOpt = "basePath",
		expectedWorkersOpt = "workers",
		batchesOpt = "batches",
		asyncOpt = "async",
		sessionIdOpt = "session";
	
	@SuppressWarnings("unchecked")
	public JMSMasterConfigParser() {
		this((B) new JMSMasterBuilder<>());
	}
	
	public JMSMasterConfigParser(B builder) {
		super(builder);		
		options.addOption(Option.builder("bpw")
			.longOpt(batchesOpt)
			.desc("Number of batches per worker (sets the module to batch-based)")
			.hasArg()
			.build()
		).addOption(Option.builder()
			.longOpt(asyncOpt)
			.desc("Whether to use asynchronous module (skips waiting for workers to connect before processing)")
			.build()
		).addOption(Option.builder()
			.longOpt(expectedWorkersOpt)
			.desc("The expected number of slave workers")
			.hasArg()
			.build()
		).addOption(Option.builder()
			.longOpt(brokerHostOpt)
			.desc("Address of the JMS broker host")
			.hasArg()
			.build()
		).addOption(Option.builder()
			.longOpt(basePathOpt)
			.desc("Base directory to start looking for resources from")
			.hasArg()
			.build()
		).addOption(Option.builder()
			.longOpt(sessionIdOpt)
			.desc("Identifier for the execution session")
			.hasArg()
			.build()
		);
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		builder.brokerHost = cmdLine.getOptionValue(brokerHostOpt);
		builder.basePath = cmdLine.getOptionValue(basePathOpt);
		builder.sessionID = tryParse(sessionIdOpt, builder.sessionID);
		builder.async = cmdLine.hasOption(asyncOpt);
		builder.expectedWorkers = tryParse(expectedWorkersOpt, builder.expectedWorkers);
		builder.bpw = tryParse(batchesOpt, builder.bpw);
		
		if (builder.async && builder.bpw > 0) {
			builder.module = new EvlModuleDistributedMasterJMSBatchAsync(
				builder.expectedWorkers, builder.bpw, builder.brokerHost, builder.sessionID
			);
		}
		else if (builder.async) {
			// TODO implement
		}
		else if (builder.bpw > 0) {
			builder.module = new EvlModuleDistributedMasterJMSBatch(
				builder.expectedWorkers, builder.bpw, builder.brokerHost, builder.sessionID
			);
		}
		else {
			builder.module = new EvlModuleDistributedMasterJMSAtomic(
				builder.expectedWorkers, builder.brokerHost, builder.sessionID
			);
		}
	}

	protected int tryParse(String opt, int absentDefault) throws IllegalArgumentException {
		if (cmdLine.hasOption(opt)) {
			String value = cmdLine.getOptionValue(opt);
			try {
				return Integer.valueOf(value);
			}
			catch (NumberFormatException nan) {
				throw new IllegalArgumentException(
					"Invalid value for option '"+opt
					+ "': expected int but got "+value
				);
			}
		}
		else return absentDefault;
	}
	
}
