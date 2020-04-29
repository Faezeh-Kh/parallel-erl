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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import org.eclipse.epsilon.common.util.FileUtil;
import org.eclipse.epsilon.eol.EolModule;
import org.eclipse.epsilon.eol.IEolModule;
import org.eclipse.epsilon.eol.cli.EolConfigParser;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.introspection.IPropertyGetter;
import org.eclipse.epsilon.eol.launch.EolRunConfiguration;
import org.eclipse.epsilon.eol.models.IModel;

public abstract class AbstractBenchmark extends EolRunConfiguration {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String... args) throws Exception {
		extensibleMain(new Builder(AbstractBenchmark.class), args);
	}
	
	protected static <C extends AbstractBenchmark, B extends Builder<C, B>> void extensibleMain(B builder, String... args) throws Exception {
		new BenchmarkConfigParser<>(builder).apply(args).run();
	}
	
	public static class Builder<C extends AbstractBenchmark, B extends Builder<C, B>> extends EolRunConfiguration.Builder<C, B> {
		public Builder(Class<C> runConfigClass) {
			super(runConfigClass);
		}

		boolean parallel = false;
		
		@SuppressWarnings("unchecked")
		@Override
		public C build() {
			try {
				if (module == null) module = createModule();
				String scriptName = FileUtil.getFileName(script.getFileName().toString(), false);
				String className = AbstractBenchmark.class.getPackage().getName()+"."+scriptName.split("_")[0]+"."+scriptName;
				return (C) Class.forName(className).getDeclaredConstructors()[0].newInstance(this);
			}
			catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | SecurityException | ClassNotFoundException ex) {
				throw new IllegalStateException(ex);
			}
		}

		@Override
		protected IEolModule createModule() {
			return new EolModule() {
				@Override
				public boolean parse(File file) {
					return true;
				}
				@Override
				public Object execute() throws EolRuntimeException {
					throw new UnsupportedOperationException();
				}
			};
		}
	}
	
	protected static class BenchmarkConfigParser<C extends AbstractBenchmark, B extends Builder<C, B>> extends EolConfigParser<C, B> {
		public BenchmarkConfigParser(B builder) {
			super(builder);
			options.addOption("parallel", false, "Whether to parallelise");
		}

		@Override
		protected void parseArgs(String[] args) throws Exception {
			super.parseArgs(args);
			builder.parallel = cmdLine.hasOption("parallel");
		}
	}
	
	protected AbstractBenchmark(Builder<?, ?> builder) {
		super(builder);
		this.parallel = builder.parallel;
		this.propertyGetter =
			(model = modelsAndProperties.keySet().iterator().next())
			.getPropertyGetter();
	}
	
	protected final IModel model;
	protected final IPropertyGetter propertyGetter;
	protected final boolean parallel;
	
	@Override
	protected void postExecute() throws Exception {
		profiledStages.removeIf(pd -> "Parsing script".equals(pd.stageName));
		super.postExecute();
	}
}
