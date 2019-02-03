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
import java.util.Map;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.models.IModel;
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
public class DistributedRunner extends EvlRunConfiguration {
	
	public static Builder<DistributedRunner, ?> Builder() {
		return Builder(DistributedRunner.class);
	}
	
	public DistributedRunner(EvlRunConfiguration other) {
		super(other);
	}
	
	DistributedRunner(Builder<DistributedRunner, ?> builder) {
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
	public DistributedRunner(
		Path evlFile,
		Map<IModel, StringProperties> modelsAndProperties,
		EvlModuleDistributedSlave evlModule,
		Map<String, Object> parameters) {
			super(Builder(DistributedRunner.class)
				.withScript(evlFile)
				.withModels(modelsAndProperties)
				.withModule(evlModule)
				.withParameters(parameters)
			);
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
