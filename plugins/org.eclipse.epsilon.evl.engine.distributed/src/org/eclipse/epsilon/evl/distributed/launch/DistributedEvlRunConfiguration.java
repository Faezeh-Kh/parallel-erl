/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.launch;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.erl.execute.RuleProfiler;
import org.eclipse.epsilon.evl.distributed.*;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.launch.EvlRunConfiguration;

/**
 * Run configuration container which holds the program arguments in the slave
 * nodes (i.e. the path to the script, the models, additional
 * parameters and arguments etc.).
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlRunConfiguration extends EvlRunConfiguration {
	
	public static Builder<DistributedEvlRunConfiguration, ?> Builder() {
		return Builder(DistributedEvlRunConfiguration.class);
	}
	
	public DistributedEvlRunConfiguration(EvlRunConfiguration other) {
		super(other);
	}
	
	public DistributedEvlRunConfiguration(Builder<DistributedEvlRunConfiguration, ?> builder) {
		super(builder.withProfiling());
		EvlContextDistributedMaster context = (EvlContextDistributedMaster) getModule().getContext();
		context.setModelProperties(this.modelsAndProperties.values());
		if (outputFile != null) {
			context.setOutputPath(outputFile.toString());
		}
	}
	
	/**
	 * This constructor is to be called by workers as a convenient
	 * data holder for initializing Epsilon.
	 * @param evlFile
	 * @param modelsAndProperties
	 * @param evlModule
	 * @param parameters
	 */
	public DistributedEvlRunConfiguration(
		Path evlFile,
		Map<IModel, StringProperties> modelsAndProperties,
		EvlModuleDistributedSlave evlModule,
		Map<String, Object> parameters) {
			super(Builder(DistributedEvlRunConfiguration.class)
				.withScript(evlFile)
				.withModels(modelsAndProperties)
				.withModule(evlModule)
				.withParameters(parameters)
				//.withProfiling()
			);
	}
	
	/**
	 * Convenience method for serializing the profiling information of a
	 * slave worker to be sent to the master.
	 * 
	 * @return A serializable representation of {@link RuleProfiler}.
	 */
	public HashMap<String, java.time.Duration> getSerializableRuleExecutionTimes() {
		return getModule().getContext()
			.getExecutorFactory().getRuleProfiler()
			.getExecutionTimes()
			.entrySet().stream()
			.collect(Collectors.toMap(
				e -> e.getKey().getName(),
				Map.Entry::getValue,
				(t1, t2) -> t1.plus(t2),
				HashMap::new
			));
	}
	
	@Override
	protected EvlModuleDistributedMaster getDefaultModule() {
		return new EvlModuleDistributedMaster(1) {
			@Override
			protected void checkConstraints() throws EolRuntimeException {
				throw new UnsupportedOperationException("This is a no-op module.");
			}
		};
	}
}
