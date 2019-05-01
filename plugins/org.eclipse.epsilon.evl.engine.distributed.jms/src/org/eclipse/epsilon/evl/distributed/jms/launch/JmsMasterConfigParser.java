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
import org.eclipse.epsilon.evl.distributed.jms.atomic.EvlModuleJmsMasterAtomic;
import org.eclipse.epsilon.evl.distributed.jms.batch.EvlModuleJmsMasterBatch;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 * @param <J>
 * @param <B>
 */
public class JmsMasterConfigParser<J extends JmsMasterRunner, B extends JmsMasterBuilder<J, B>> extends EolConfigParser<J, B> {

	public static void main(String... args) {
		new JmsMasterConfigParser<>().parseAndRun(args);
	}
	
	private final String
		brokerHostOpt = "broker",
		basePathOpt = "basePath",
		expectedWorkersOpt = "workers",
		shuffleOpt = "shuffle",
		batchesOpt = "batches",
		sessionIdOpt = "session";
	
	@SuppressWarnings("unchecked")
	public JmsMasterConfigParser() {
		this((B) new JmsMasterBuilder<>());
	}
	
	public JmsMasterConfigParser(B builder) {
		super(builder);		
		options.addOption(Option.builder("bf")
			.longOpt(batchesOpt)
			.desc("Granularity of job batches, between 0 and 1 (sets the module to batch-based)")
			.hasArg()
			.build()
		).addOption(Option.builder()
			.longOpt(shuffleOpt)
			.desc("Whether to randomise the order of jobs")
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
		builder.shuffle = cmdLine.hasOption(shuffleOpt);
		builder.expectedWorkers = tryParse(expectedWorkersOpt, builder.expectedWorkers);
		builder.batchFactor = tryParse(batchesOpt, builder.batchFactor);
		
		if (builder.batchFactor > 0) {
			builder.module = new EvlModuleJmsMasterBatch(
				builder.expectedWorkers, builder.batchFactor, builder.shuffle, builder.brokerHost, builder.sessionID
			);
		}
		else {
			builder.module = new EvlModuleJmsMasterAtomic(
				builder.expectedWorkers, builder.shuffle, builder.brokerHost, builder.sessionID
			);
		}
	}

	protected double tryParse(String opt, double absentDefault) throws IllegalArgumentException {
		if (cmdLine.hasOption(opt)) {
			String value = cmdLine.getOptionValue(opt);
			try {
				return Double.valueOf(value);
			}
			catch (NumberFormatException nan) {
				throw new IllegalArgumentException(
					"Invalid value for option '"+opt
					+ "': expected double but got "+value
				);
			}
		}
		else return absentDefault;
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
