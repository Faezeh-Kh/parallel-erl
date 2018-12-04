/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import static org.eclipse.epsilon.eol.cli.EolConfigParser.*;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedSlave;
import org.eclipse.epsilon.evl.distributed.launch.DistributedRunner;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlFlinkRichFunction extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;
	
	protected transient EvlModuleDistributedSlave localModule;
	protected transient DistributedRunner configContainer;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		Path evlScriptPath = Paths.get(parameters.getString("evlScript", null));
		
		int numModels = parameters.getInteger("numberOfModels", 0);
		String[] modelsConfig = new String[numModels];
		for (int i = 0; i < numModels; i++) {
			modelsConfig[i] = parameters.getString("model"+i, "");
		}
		Map<IModel, StringProperties> localModelsAndProperties = parseModelParameters(modelsConfig);
		
		Map<String, Object> scriptVariables = parseScriptParameters(
			parameters.getString("scriptParameters", null).split(",")
		);
		
		localModule = new EvlModuleDistributedSlave(parameters.getInteger("localParallelism", 0));
		
		configContainer = new DistributedRunner(
			evlScriptPath,
			localModelsAndProperties,
			localModule,
			scriptVariables
		);
		
		configContainer.preExecute();
		localModule.prepareExecution();
	}
	
	@Override
	public void close() throws Exception {
		localModule.postExecution();
		configContainer.postExecute();
	}
}
