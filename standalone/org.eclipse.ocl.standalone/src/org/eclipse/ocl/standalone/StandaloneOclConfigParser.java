/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.ocl.standalone;

import org.eclipse.epsilon.common.cli.ConfigParser;

public class StandaloneOclConfigParser extends ConfigParser<StandaloneOcl, StandaloneOclBuilder> {

	public static void main(String[] args) {
		new StandaloneOclConfigParser(true).apply(args).run();
	}
	
	final boolean checkArguments;
	
	protected StandaloneOclConfigParser(boolean checkArgs) {
		super(new StandaloneOclBuilder());
		this.checkArguments = checkArgs;
		
		requiredUsage = "Must provide absolute path to "+nL
		  + "  [Complete OCL Document] (if metamodel doesn't contain constraints) "+nL
		  + "  [XMI model file] "+nL
		  + "  [Ecore metamodel file] ";
	}
	
	@Override
	protected void parseArgs(String[] args) throws Exception {
		if (checkArguments && args.length < 3) {
			throw new IllegalArgumentException();
		}
		
		super.parseArgs(args);
		if (args.length > 1) builder.withModel(args[1]);
		if (args.length > 2) builder.withMetamodel(args[2]);
	}
}
