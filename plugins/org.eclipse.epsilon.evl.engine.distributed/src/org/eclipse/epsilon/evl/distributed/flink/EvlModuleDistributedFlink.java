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

import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;

/**
 * Convenience base class for Flink EVL modules.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributedFlink extends EvlModuleDistributedMaster {

	private ExecutionEnvironment executionEnv;
	
	public EvlModuleDistributedFlink() {
		this(-1);
	}
	
	public EvlModuleDistributedFlink(int parallelism) {
		super(parallelism);
	}
	
	@Override
	protected final void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		
		EvlContextDistributedMaster context = getContext();
		executionEnv = ExecutionEnvironment.getExecutionEnvironment();
		int parallelism = context.getDistributedParallelism();
		if (parallelism < 1) {
			context.setDistributedParallelism(parallelism = ExecutionConfig.PARALLELISM_DEFAULT);
		}
		executionEnv.setParallelism(parallelism);
	}
	
	protected abstract DataSet<SerializableEvlResultAtom> getProcessingPipeline(final ExecutionEnvironment execEnv) throws Exception;
	
	@Override
	protected final void checkConstraints() throws EolRuntimeException {
		try {
			Configuration config = getJobConfiguration();
			String outputPath = getContext().getOutputPath();
			executionEnv.getConfig().setGlobalJobParameters(config);
			DataSet<SerializableEvlResultAtom> pipeline = getProcessingPipeline(executionEnv);
			
			if (outputPath != null && !outputPath.isEmpty()) {
				pipeline.writeAsText(outputPath);
				executionEnv.execute();
			}
			else {
				assignDeserializedResults(pipeline.collect().parallelStream());
			}
		}
		catch (Exception ex) {
			EolRuntimeException.propagate(ex);
		}
	}

	private Configuration getJobConfiguration() {
		Configuration configuration = new Configuration();
		
		for (Map.Entry<String, ?> entry : getContext().getJobParameters().entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			
			if (value instanceof Boolean) {
				configuration.setBoolean(key, (boolean) value);
			}
			else if (value instanceof Integer) {
				configuration.setInteger(key, (int) value);
			}
			else if (value instanceof Long) {
				configuration.setLong(key, (long) value);
			}
			else if (value instanceof Float) {
				configuration.setFloat(key, (float) value);
			}
			else if (value instanceof Double) {
				configuration.setDouble(key, (double) value);
			}
			else {
				configuration.setString(key, Objects.toString(value));
			}
		}
		
		return configuration;
	}

}
