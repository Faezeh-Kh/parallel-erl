/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.atomic;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleDistributedFlink;
import org.eclipse.epsilon.evl.distributed.flink.format.EvlFlinkInputFormat;

/**
 * Data-parallel evaluation strategy which works over elements.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedFlinkAtoms extends EvlModuleDistributedFlink {

	public EvlModuleDistributedFlinkAtoms() {
		super();
	}
	
	public EvlModuleDistributedFlinkAtoms(int parallelism) {
		super(parallelism);
	}

	@Override
	protected DataSet<SerializableEvlResultAtom> getProcessingPipeline(ExecutionEnvironment execEnv) throws Exception {
		return execEnv.createInput(new EvlFlinkInputFormat(createJobs(true))).flatMap(new EvlFlinkAtomFlatMapFunction());
	}
}
