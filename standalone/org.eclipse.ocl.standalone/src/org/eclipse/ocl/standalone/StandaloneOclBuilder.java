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
import org.eclipse.epsilon.common.launch.ProfilableRunConfiguration;

/**
 * Utility class for creating OCL run configuration. Effectively a container for
 * script, model, metamodel etc. Can be invoked from the command-line.
 * 
 * @author Sina Madani
 */
public class StandaloneOclBuilder extends ProfilableRunConfiguration.Builder<StandaloneOcl, StandaloneOclBuilder> {
	
	public URI modelUri, metamodelUri;
	public EPackage rootPackage;
	public EValidator customValidator;
	
	public StandaloneOclBuilder withModel(URI uri) {
		this.modelUri = uri;
		return this;
	}
	public StandaloneOclBuilder withModel(String path) {
		return withModel(URI.createFileURI(path));
	}
	public StandaloneOclBuilder withMetamodel(URI uri) {
		this.metamodelUri = uri;
		return this;
	} 
	public StandaloneOclBuilder withMetamodel(String path) {
		return withMetamodel(URI.createFileURI(path));
	}
	public StandaloneOclBuilder withPackage(EPackage root) {
		this.rootPackage = root;
		return this;
	}
	public StandaloneOclBuilder withValidator(EValidator validator) {
		this.customValidator = validator;
		return this;
	}
	public StandaloneOclBuilder withURIs(String[] uris) {
		if (uris != null) {
			if (uris.length > 0) withScript(uris[0]);
			if (uris.length > 1) withModel(uris[1]);
			if (uris.length > 2) withMetamodel(uris[2]);
		}
		return this;
	}
	
	@Override
	public StandaloneOcl build() throws IllegalArgumentException, IllegalStateException {
		return new StandaloneOcl(this);
	}
	
	public static StandaloneOclBuilder compiledInstanceBuilder(EPackage rootPackage, String... args) {
		if (args.length == 0 ||
			(args.length == 1 && (args[0].length() < 5 || !args[0].endsWith(".xmi"))) ||
			(args.length >= 3 && args[1].length() < 5)
		) {
			throw new IllegalArgumentException("Must provide absolute path to EMF model!");
		}
		
		StandaloneOclBuilder builder = new StandaloneOclBuilder().withPackage(rootPackage);
		
		if (args.length >= 1)
			builder = builder.withModel(args[0]);
		
		return builder;
	}
	
	public static StandaloneOcl newCompiledInstance(EPackage rootPackage, EValidator customValidator, String... args) {
		return compiledInstanceBuilder(rootPackage, args).withValidator(customValidator).build();
	}
}
