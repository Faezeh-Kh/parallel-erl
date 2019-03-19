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
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS;
import org.eclipse.epsilon.evl.distributed.jms.atomic.EvlModuleDistributedMasterJMSAtomicSynch;

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
		masterModuleOpt = "master";
	
	@SuppressWarnings("unchecked")
	public JMSMasterConfigParser() {
		this((B) new JMSMasterBuilder<>());
	}
	
	public JMSMasterConfigParser(B builder) {
		super(builder);
		
		options.addOption(Option.builder("m")
			.longOpt(masterModuleOpt)
			.desc("The module to use (must be subclass of "+EvlModuleDistributedMasterJMS.class.getSimpleName()+")")
			.hasArg()
			.build()
		);
		
		options.addOption(Option.builder()
			.longOpt(expectedWorkersOpt)
			.desc("The expected number of slave workers")
			.hasArg()
			.build()
		);
		options.addOption(Option.builder()
			.longOpt(brokerHostOpt)
			.desc("Address of the JMS broker host")
			.hasArg()
			.build()
		);
		options.addOption(Option.builder()
			.longOpt(basePathOpt)
			.desc("Base directory to start looking for resources from")
			.hasArg()
			.build()
		);
	}

	@Override
	protected void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		builder.brokerHost = cmdLine.getOptionValue(brokerHostOpt);
		builder.basePath = cmdLine.getOptionValue(basePathOpt);
		if (cmdLine.hasOption(expectedWorkersOpt)) {
			builder.expectedWorkers = Integer.parseInt(cmdLine.getOptionValue(expectedWorkersOpt));
		}
		String masterModule = cmdLine.getOptionValue(masterModuleOpt);
		
		if (masterModule != null && !masterModule.trim().isEmpty()) {
			String pkg = getClass().getPackage().getName();
			pkg = pkg.substring(0, pkg.lastIndexOf('.')+1);
			builder.module = (EvlModuleDistributedMasterJMS) Class.forName(pkg + masterModule)
				.getConstructor(int.class, String.class)
				.newInstance(builder.expectedWorkers, builder.brokerHost);
		}
		else {
			builder.module = new EvlModuleDistributedMasterJMSAtomicSynch(builder.expectedWorkers, builder.brokerHost);
		}
	}

}
