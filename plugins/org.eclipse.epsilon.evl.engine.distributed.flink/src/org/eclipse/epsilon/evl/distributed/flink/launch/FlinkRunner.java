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

import org.eclipse.epsilon.eol.cli.EolConfigParser;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.flink.atomic.EvlModuleDistributedFlinkAtoms;
import org.eclipse.epsilon.evl.distributed.flink.batch.EvlModuleDistributedFlinkSubset;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlRunConfiguration;
import org.eclipse.epsilon.evl.launch.EvlRunConfiguration;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class FlinkRunner extends DistributedEvlRunConfiguration {

	public static void main(String[] args) throws ClassNotFoundException {
		String modelPath = args[1].contains("://") ? args[1] : "file:///"+args[1];
		String metamodelPath = args[2].contains("://") ? args[2] : "file:///"+args[2];
		
		EolConfigParser.main(new String[] {
			"CONFIG:"+DistributedEvlRunConfiguration.class.getName(),
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
	
	public FlinkRunner(EvlRunConfiguration other) {
		super(other);
	}

	@Override
	protected EvlModuleDistributedMaster getDefaultModule() {
		return new EvlModuleDistributedFlinkAtoms();
	}
}
