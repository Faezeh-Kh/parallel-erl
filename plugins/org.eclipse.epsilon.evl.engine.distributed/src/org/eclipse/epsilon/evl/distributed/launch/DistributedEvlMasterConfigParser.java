/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.launch;

import org.apache.commons.cli.Option;

public class DistributedEvlMasterConfigParser<R extends DistributedEvlRunConfigurationMaster, B extends DistributedEvlRunConfigurationMaster.Builder<R, B>> extends DistributedEvlConfigParser<R, B> {
	
	private final String
		expectedWorkersOpt = "workers",
		shuffleOpt = "shuffle",
		batchesOpt = "batches";
	
	public DistributedEvlMasterConfigParser(B builder) {
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
		);
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		builder.shuffle = cmdLine.hasOption(shuffleOpt);
		builder.distributedParallelism = tryParse(expectedWorkersOpt, builder.distributedParallelism);
		builder.batchFactor = tryParse(batchesOpt, builder.batchFactor);
	}
}
