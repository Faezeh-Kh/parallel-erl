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
import java.nio.file.Paths;
import java.util.Map;
import static org.eclipse.epsilon.emc.emf.EmfModel.*;
import org.eclipse.epsilon.common.util.FileUtil;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.emc.emf.EmfModel;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.*;
import org.eclipse.epsilon.evl.distributed.flink.atomic.EvlModuleDistributedFlinkAtoms;
import org.eclipse.epsilon.evl.distributed.flink.batch.EvlModuleDistributedFlinkSubset;
import org.eclipse.epsilon.evl.launch.EvlRunConfiguration;

/**
 * Run configuration container which serves two purposes: to
 * launch the master through command-line arguments by calling the
 * main method, and to hold the program arguments in the slave
 * nodes (i.e. the path to the script, the models, additional
 * parameters and arguments etc.).
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedRunner extends EvlRunConfiguration {
	
	public static void main(String[] args) {
		String fileProtocol = "file:///",
			scriptPath = args[0],
			modelPath = fileProtocol + args[1],
			metamodelPath = fileProtocol + args[2];
		
		IModel model = new EmfModel() {
			protected org.eclipse.emf.ecore.resource.ResourceSet createResourceSet() {
				return new org.eclipse.emf.ecore.resource.impl.ResourceSetImpl();
			}
		};
		
		StringProperties properties = new StringProperties();
		properties.put(PROPERTY_CONCURRENT, true);
		properties.put(PROPERTY_CACHED, true);
		properties.put(PROPERTY_READONLOAD, true);
		properties.put(PROPERTY_STOREONDISPOSAL, false);
		properties.put(PROPERTY_FILE_BASED_METAMODEL_URI, metamodelPath);
		properties.put(PROPERTY_MODEL_URI, modelPath);
		properties.put(PROPERTY_NAME, FileUtil.getFileName(modelPath));

		int parallelism = args.length > 3 ? Integer.parseInt(args[3]) : -1;
		
		EvlModuleDistributedMaster module = args.length > 4 && args[4].toLowerCase().contains("batch") ?
			new EvlModuleDistributedFlinkSubset(parallelism) :
			new EvlModuleDistributedFlinkAtoms(parallelism);
		
		String outputFile = args.length > 5 ? args[5] : null;
		
		System.out.println("Using "+module.getClass().getSimpleName()+'\n');
		new DistributedRunner(scriptPath, model, properties, module, outputFile).run();
	}
	
	
	public DistributedRunner(EvlRunConfiguration other) {
		super(other);
	}
	
	private static Builder<DistributedRunner, ?> builderForParams(
		String evlFile,
		IModel model,
		StringProperties modelProperties,
		EvlModuleDistributedMaster evlModule,
		String outputFile
	) {
		Builder<DistributedRunner, ?> builder = Builder(DistributedRunner.class)
				.withScript(Paths.get(evlFile))
				.withModel(model, modelProperties)
				.withModule(evlModule)
				.withProfiling();
		if (outputFile != null && !outputFile.trim().isEmpty()) {
			builder = builder.withOutputFile(outputFile);
		}
		return builder;
	}
	
	DistributedRunner(
		String evlFile,
		IModel model,
		StringProperties modelProperties,
		EvlModuleDistributedMaster evlModule,
		String outputFile
	) {
		super(builderForParams(evlFile, model, modelProperties, evlModule, outputFile));
		evlModule.getContext().setModelProperties(this.modelsAndProperties.values());
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
		return new EvlModuleDistributedFlinkAtoms();
	}
}
