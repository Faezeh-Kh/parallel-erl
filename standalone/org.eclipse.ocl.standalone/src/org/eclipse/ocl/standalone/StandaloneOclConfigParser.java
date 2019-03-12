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

import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EValidator;
import org.eclipse.epsilon.common.cli.ConfigParser;

public class StandaloneOclConfigParser extends ConfigParser<StandaloneOcl, StandaloneOclBuilder> {

	public static void main(String[] args) {
		new StandaloneOclConfigParser(true).apply(args).run();
	}
	
	final boolean isInterpreted;
	
	protected StandaloneOclConfigParser(boolean checkArgs) {
		super(new StandaloneOclBuilder());
		this.isInterpreted = checkArgs;
		
		requiredUsage = "Must provide absolute path to "+nL
		  + "  [Complete OCL Document] (if metamodel doesn't contain constraints) "+nL
		  + "  [XMI model file] "+nL
		  + "  [Ecore metamodel file] ";
	}
	
	@Override
	protected void parseArgs(String[] args) throws Exception {
		if (isInterpreted) {
			if (args.length < 2) throw new IllegalArgumentException();
			if (args.length > 1) builder.withModel(args[1]);
			if (args.length > 2) builder.withMetamodel(args[2]);
		}
		super.parseArgs(args);
		if (!isInterpreted) {
			builder.script = null;
		}
	}
	
	public static StandaloneOclBuilder compiledInstanceBuilder(EPackage rootPackage, String... args) throws Exception {
		StandaloneOclConfigParser parser = new StandaloneOclConfigParser(false);
		parser.parseArgs(args);
		
		if (args.length == 0 ||
			(args.length == 1 && (args[0].length() < 5 || !args[0].endsWith(".xmi"))) ||
			(args.length >= 3 && args[1].length() < 5)
		) {
			throw new IllegalArgumentException("Must provide absolute path to EMF model!");
		}
		
		StandaloneOclBuilder builder = parser.builder;
		
		if (args.length >= 1)
			builder = builder.withModel(args[0]);
		
		return builder.withPackage(rootPackage);
	}
	
	public static StandaloneOcl newCompiledInstance(EPackage rootPackage, EValidator customValidator, String... args) throws Exception {
		return compiledInstanceBuilder(rootPackage, args).withValidator(customValidator).build();
	}
}
