/*********************************************************************
 * Copyright (c) 2020 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.egl.cli;

import org.apache.commons.cli.Option;
import org.eclipse.epsilon.egl.launch.EgxRunConfiguration;
import org.eclipse.epsilon.eol.cli.EolConfigParser;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 * @param <C>
 * @param <B>
 */
public class EgxConfigParser<C extends EgxRunConfiguration, B extends EgxRunConfiguration.Builder<C, B>> extends EolConfigParser<C, B> {
	
	public static void main(String... args) {
		new EgxConfigParser<>().parseAndRun(args);
	}
	
	@SuppressWarnings("unchecked")
	public EgxConfigParser() {
		this((B) EgxRunConfiguration.Builder());
	}
	
	protected final String
		tfRootOpt = "factoryRoot",
		inMemoryOutput = "nopersist",
		deleteFiles = "deleteBeforeGen";
	
	public EgxConfigParser(B builder) {
		super(builder);
		optionalUsage += "  [no generated files]"+nL
			+"  -"+tfRootOpt+" [TemplateFactory root path]"+nL;
		
		options.addOption(Option.builder("tfr")
			.longOpt(tfRootOpt)
			.hasArg()
			.desc("Root path of the EglTemplateFactory")
			.build()
		)
		.addOption(Option.builder("imo")
			.longOpt(inMemoryOutput)
			.desc("Disables file generation")
			.build()
		)
		.addOption(Option.builder("dbg")
			.longOpt(deleteFiles)
			.desc("Deletes all files in the output directory before each run")
			.build()
		);
	}
	
	@Override
	protected void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		builder.deleteOutputDirBefore = cmdLine.hasOption(deleteFiles);
		builder.persistOutput = !cmdLine.hasOption(inMemoryOutput);
		if (cmdLine.hasOption(tfRootOpt)) {
			builder.withOutputRoot(cmdLine.getOptionValue(tfRootOpt));
		}
	}
}
