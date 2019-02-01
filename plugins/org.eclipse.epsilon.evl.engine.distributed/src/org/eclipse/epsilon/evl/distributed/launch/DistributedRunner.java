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
import org.eclipse.epsilon.eol.cli.EolConfigParser;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.*;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
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
	
	public static void main(String[] args) throws ClassNotFoundException {
		String modelPath = args[1].contains("://") ? args[1] : "file:///"+args[1];
		String metamodelPath = args[2].contains("://") ? args[2] : "file:///"+args[2];
		
		EolConfigParser.main(new String[] {
			"CONFIG:"+DistributedRunner.class.getName(),
			args[0],
			"-models",
				"\"emf.DistributableEmfModel#"
				+ "concurrent=true,cached=true,readOnLoad=true,storeOnDisposal=false,"
				+ "modelUri="+modelPath+",fileBasedMetamodelUri="+metamodelPath+"\"",
			args.length > 5 ? "-outfile" : "",
			args.length > 5 ? args[5] : "",
			"-module",
				(args.length > 4 && (
					args[4].toLowerCase().contains("batch")  || args[4].toLowerCase().contains("subset")
				) ? EvlModuleDistributedFlinkSubset.class : EvlModuleDistributedFlinkSubset.class).getName().substring(20),
			"int="+(args.length > 3 ? args[3] : "-1")
		});
	}
	
	public DistributedRunner(EvlRunConfiguration other) {
		super(other);
	}
	
	DistributedRunner(Builder<DistributedRunner, ?> builder) {
		super(builder.withProfiling());
		EvlContextDistributedMaster context = getModule().getContext();
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
	public EvlModuleDistributedMaster getModule() {
		return (EvlModuleDistributedMaster) super.getModule();
	}
	
	@Override
	protected EvlModuleDistributedMaster getDefaultModule() {
		return new EvlModuleDistributedFlinkAtoms();
	}
}
