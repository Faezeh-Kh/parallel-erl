/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.launch;

import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.emc.emf.DistributableEmfModel;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.atomic.EvlModuleFlinkAtoms;
import org.eclipse.epsilon.evl.distributed.flink.batch.EvlModuleFlinkSubset;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfigurationMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class FlinkRunner extends DistributedEvlRunConfigurationMaster {

	public static Builder<FlinkRunner, ?> Builder() {
		return new Builder<>(FlinkRunner.class);
	}
	
	public static void main(String[] args) throws ClassNotFoundException {
		String basePath = args[0];
		String scriptPath = args[1];
		String modelPath = args[2].contains("://") ? args[1] : "file:///"+args[2];
		String metamodelPath = args[3].contains("://") ? args[2] : "file:///"+args[3];
		int parallelism = args.length > 4 ? Integer.valueOf(args[4]) : -1;
		
		StringProperties modelProperties = new StringProperties();
		modelProperties.put(DistributableEmfModel.PROPERTY_CONCURRENT, true);
		modelProperties.put(DistributableEmfModel.PROPERTY_CACHED, true);
		modelProperties.put(DistributableEmfModel.PROPERTY_MODEL_URI, modelPath);
		modelProperties.put(DistributableEmfModel.PROPERTY_FILE_BASED_METAMODEL_URI, metamodelPath);
		
		Builder<FlinkRunner, ?> builder = (Builder<FlinkRunner, ?>) Builder()
			.withProfiling()
			.withBasePath(basePath)
			.withScript(scriptPath)
			.withModel(new DistributableEmfModel(), modelProperties);
		
		EvlModuleFlinkMaster<?> module = null;
		
		if (args.length > 5) {
			String moduleImplName = args[5].toLowerCase();
			if (moduleImplName.contains("batch") || moduleImplName.contains("subset")) {
				module = new EvlModuleFlinkSubset(parallelism);
			}
		}
		if (module == null) {
			module = new EvlModuleFlinkAtoms(parallelism);
		}
		if (args.length > 6) {
			builder = builder.withOutputFile(args[6]);
		}
		
		builder.withModule(module).build().run();
	}
	
	public FlinkRunner(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
	}

	@Override
	protected EvlModuleFlinkMaster<?> getDefaultModule() {
		return new EvlModuleFlinkAtoms();
	}
}
