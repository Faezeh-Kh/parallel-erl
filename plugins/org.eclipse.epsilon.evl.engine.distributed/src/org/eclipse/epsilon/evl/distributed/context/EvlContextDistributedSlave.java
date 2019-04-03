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

import static org.eclipse.epsilon.eol.cli.EolConfigParser.*;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolExecutorService;
import org.eclipse.epsilon.eol.execute.concurrent.executors.EolThreadPoolExecutor;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedSlave extends EvlContextDistributed {
	
	public EvlContextDistributedSlave(int localParallelism) {
		super(localParallelism);
	}
	
	@Override
	public EolExecutorService newExecutorService() {
		return EolThreadPoolExecutor.adaptiveExecutor(numThreads);
	}
	
	public static DistributedEvlRunConfiguration parseJobParameters(Map<String, ? extends Serializable> config, String basePath) throws Exception {
		String normBasePath = basePath.replace("\\", "/");
		if (!normBasePath.endsWith("/")) normBasePath += "/";
		String masterBasePath = Objects.toString(config.get(BASE_PATH), null);
		String evlScriptPath = Objects.toString(config.get(EVL_SCRIPT), null);
		if (evlScriptPath == null) throw new IllegalStateException("No script path!");
		evlScriptPath = evlScriptPath.replace(masterBasePath, normBasePath);
		
		Map<IModel, StringProperties> localModelsAndProperties;
		if (config.containsKey(IGNORE_MODELS)) {
			localModelsAndProperties = Collections.emptyMap();
		}
		else {
			int numModels = Integer.parseInt(Objects.toString(config.get(NUM_MODELS), "0"));
			String[] modelsConfig = new String[numModels];
			for (int i = 0; i < numModels; i++) {
				String modelConfig = Objects.toString(config.get(MODEL_PREFIX+i), null);
				if (modelConfig != null) {
					modelsConfig[i] = modelConfig.replace(masterBasePath, normBasePath);
				}
			}
			localModelsAndProperties = parseModelParameters(modelsConfig);
		}
		
		Map<String, Object> scriptVariables = parseScriptParameters(
			Objects.toString(config.get(SCRIPT_PARAMS), "").split(",")
		);
		
		EvlModuleDistributedSlave localModule = new EvlModuleDistributedSlave(
			Integer.parseInt(Objects.toString(config.get(LOCAL_PARALLELISM), "0"))
		);
		
		return new DistributedEvlRunConfiguration(
			normBasePath,
			Paths.get(evlScriptPath),
			localModelsAndProperties,
			localModule,
			scriptVariables
		);
	}
	
	@Override
	public EvlModuleDistributedSlave getModule() {
		return (EvlModuleDistributedSlave) super.getModule();
	}
	
	@Override
	public void setModule(IModule module) {
		if (module instanceof EvlModuleDistributedSlave) {
			super.setModule(module);
		}
	}
}
