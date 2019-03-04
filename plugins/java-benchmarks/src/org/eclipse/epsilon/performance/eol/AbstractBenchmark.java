/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.performance.eol;

import java.lang.reflect.InvocationTargetException;
import org.eclipse.epsilon.common.launch.ProfilableRunConfiguration;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.common.util.profiling.BenchmarkUtils;
import org.eclipse.epsilon.eol.execute.introspection.IPropertyGetter;
import org.eclipse.epsilon.eol.models.IModel;

public abstract class AbstractBenchmark extends ProfilableRunConfiguration {

	public static abstract class Builder<BENCH extends AbstractBenchmark> extends ProfilableRunConfiguration.Builder<BENCH, Builder<BENCH>> {
		static final String fileProtocol = "file:///";
		
		IModel model;
		boolean parallel;
		StringProperties modelProperties = new StringProperties();
		
		public Builder<BENCH> parallel(boolean p) {
			this.parallel = p;
			return this;
		}
		
		public Builder<BENCH> withModel(IModel model, String modelPath, String metamodelPath) {
			this.model = model;
			modelProperties.put("cached", true);
			modelProperties.put("concurrent", true);
			modelProperties.put("fileBasedMetamodelUri", fileProtocol+metamodelPath);
			modelProperties.put("modelUri", fileProtocol+metamodelPath);
			return this;
		}
		
		protected boolean isParallelJar() {
			return new java.io.File(getClass()
				.getProtectionDomain()
				.getCodeSource()
				.getLocation()
				.getPath()
			)
			.getName().toLowerCase().contains("parallel");
		}
	}
	
	
	
	protected static <B extends AbstractBenchmark> void extensibleMain(Class<B> clazz, IModel model, String... args) throws Exception {
		if (args.length < 3) throw new IllegalArgumentException(
			"Must include path to model, metamodel and output file!"
		);
		
		var modelPath = args[0];
		var metamodelPath = args[1];
		var resultsFile = args[2];
		
		var builder = new Builder<B>() {
			@SuppressWarnings("unchecked")
			@Override
			public B build() throws IllegalArgumentException, IllegalStateException {
				try {
					return (B) clazz.getConstructors()[0].newInstance(this);
				}
				catch (InstantiationException | IllegalAccessException | InvocationTargetException | SecurityException ex) {
					throw new IllegalStateException(ex);
				}
			}
		};
		builder
			.withOutputFile(resultsFile)
			.withProfiling().withResults()
			.withModel(model, modelPath, metamodelPath)
			.parallel(builder.isParallelJar())
			.build()
			.run();
	}
	
	protected AbstractBenchmark(Builder<?> builder) {
		super(builder);
		this.propertyGetter = (this.model = builder.model).getPropertyGetter();
		this.parallel = builder.parallel;
		this.modelProperties = builder.modelProperties;
	}
	
	protected final IModel model;
	protected final StringProperties modelProperties;
	protected final IPropertyGetter propertyGetter;
	protected final boolean parallel;
	
	@Override
	protected void preExecute() throws Exception {
		super.preExecute();
		BenchmarkUtils.profileExecutionStage(profiledStages, "Model loading", () -> model.load(modelProperties));
	}
	
}
