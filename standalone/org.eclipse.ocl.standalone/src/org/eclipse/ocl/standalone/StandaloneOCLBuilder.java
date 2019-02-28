/*********************************************************************
 * Copyright (c) 2017 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.ocl.standalone;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EValidator;
import org.eclipse.epsilon.common.cli.ConfigParser;
import org.eclipse.epsilon.common.launch.ProfilableRunConfiguration;

/**
 * Utility class for creating OCL run configuration. Effectively a container for
 * script, model, metamodel etc. Can be invoked from the command-line.
 * 
 * @author Sina Madani
 */
public class StandaloneOCLBuilder extends ProfilableRunConfiguration.Builder<StandaloneOCL, StandaloneOCLBuilder> {

	public static void main(String[] args) throws Exception {
		try {
			new OCLConfigParser(true).parseAndRun(args);
		}
		catch (IllegalArgumentException iax) {
			System.err.println(iax.getMessage());
		}
	}
	
	static class OCLConfigParser extends ConfigParser<StandaloneOCL, StandaloneOCLBuilder> {
		final boolean checkArguments;
		
		OCLConfigParser(boolean checkArgs) {
			super(new StandaloneOCLBuilder());
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
	
	public URI modelUri, metamodelUri;
	public EPackage rootPackage;
	public EValidator customValidator;
	
	public StandaloneOCLBuilder withModel(URI uri) {
		this.modelUri = uri;
		return this;
	}
	public StandaloneOCLBuilder withModel(String path) {
		return withModel(URI.createFileURI(path));
	}
	public StandaloneOCLBuilder withMetamodel(URI uri) {
		this.metamodelUri = uri;
		return this;
	} 
	public StandaloneOCLBuilder withMetamodel(String path) {
		return withMetamodel(URI.createFileURI(path));
	}
	public StandaloneOCLBuilder withPackage(EPackage root) {
		this.rootPackage = root;
		return this;
	}
	public StandaloneOCLBuilder withValidator(EValidator validator) {
		this.customValidator = validator;
		return this;
	}
	public StandaloneOCLBuilder withURIs(String[] uris) {
		if (uris != null) {
			if (uris.length > 0) withScript(uris[0]);
			if (uris.length > 1) withModel(uris[1]);
			if (uris.length > 2) withMetamodel(uris[2]);
		}
		return this;
	}
	
	@Override
	public StandaloneOCL build() throws IllegalArgumentException, IllegalStateException {
		return new StandaloneOCL(this);
	}
	
	public static StandaloneOCLBuilder compiledInstanceBuilder(EPackage rootPackage, String... args) {
		if (args.length == 0 ||
			(args.length == 1 && (args[0].length() < 5 || !args[0].endsWith(".xmi"))) ||
			(args.length >= 3 && args[1].length() < 5)
		) {
			throw new IllegalArgumentException("Must provide absolute path to EMF model!");
		}
		
		StandaloneOCLBuilder builder = new StandaloneOCLBuilder().withPackage(rootPackage);
		
		if (args.length >= 1)
			builder = builder.withModel(args[0]);
		
		return builder;
	}
	
	public static StandaloneOCL newCompiledInstance(EPackage rootPackage, EValidator customValidator, String... args) {
		return compiledInstanceBuilder(rootPackage, args).withValidator(customValidator).withProfiling().build();
	}
}
