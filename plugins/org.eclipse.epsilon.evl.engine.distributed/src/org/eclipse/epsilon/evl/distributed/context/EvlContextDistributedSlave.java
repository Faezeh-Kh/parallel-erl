/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.context;

import static org.eclipse.epsilon.eol.cli.EolConfigParser.parseModelParameters;
import static org.eclipse.epsilon.eol.cli.EolConfigParser.parseScriptParameters;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolThreadPoolExecutor;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedSlave extends EvlContextParallel {
	
	public EvlContextDistributedSlave(int localParallelism) {
		super(localParallelism, false);
	}
	
	@Override
	public EolExecutorService newExecutorService() {
		return EolThreadPoolExecutor.adaptiveExecutor(numThreads);
	}
	
	public static DistributedRunner parseJobParameters(Map<String, ? extends Serializable> config) throws Exception {
		Path evlScriptPath = Paths.get(Objects.toString(config.get("evlScript"), null));
		
		int numModels = Integer.parseInt(Objects.toString(config.get("numberOfModels"), null));
		String[] modelsConfig = new String[numModels];
		for (int i = 0; i < numModels; i++) {
			modelsConfig[i] = Objects.toString(config.get("model"+i), null);
		}
		Map<IModel, StringProperties> localModelsAndProperties = parseModelParameters(modelsConfig);
		
		Map<String, Object> scriptVariables = parseScriptParameters(
			Objects.toString(config.get("scriptParameters"), "").split(",")
		);
		
		EvlModuleDistributedSlave localModule = new EvlModuleDistributedSlave(
			Integer.parseInt(Objects.toString(config.get("localParallelism"), "0"))
		);
		
		return new DistributedRunner(
			evlScriptPath,
			localModelsAndProperties,
			localModule,
			scriptVariables
		);
	}
	
	@Override
	public EvlModuleDistributedSlave getModule() {
		return (EvlModuleDistributedSlave) super.getModule();
	}
}
