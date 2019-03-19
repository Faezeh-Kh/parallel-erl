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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextDistributedMaster extends EvlContextParallel {

	protected Collection<StringProperties> modelProperties;
	protected Collection<Variable> initialVariables;
	protected int distributedParallelism;
	protected String outputDir, basePath;
	
	public EvlContextDistributedMaster(int localParallelism, int distributedParallelism) {
		super(localParallelism);
		this.distributedParallelism = distributedParallelism;
	}

	public void setUnsatisfiedConstraints(Set<UnsatisfiedConstraint> unsatisfiedConstraints) {
		this.unsatisfiedConstraints = unsatisfiedConstraints;
	}

	public void setModelProperties(Collection<StringProperties> modelProperties) {
		this.modelProperties = modelProperties;
	}
	
	public int getDistributedParallelism() {
		return distributedParallelism;
	}
	
	public void setDistributedParallelism(int parallelism) {
		this.distributedParallelism = parallelism;
	}
	
	public String getOutputPath() {
		return outputDir;
	}

	public void setOutputPath(String out) {
		this.outputDir = out;
	}
	
	public void setBasePath(String path) {
		this.basePath = path;
	}

	/**
	 * Saves the frame stack for the benefit of slave nodes.
	 */
	public void storeInitialVariables() {
		initialVariables = getFrameStack()
			.getFrames()
			.stream()
			.flatMap(frame -> frame.getAll().values().stream())
			//.filter(v -> StringUtil.isOneOf(v.getName(), "null", "System"))
			.collect(Collectors.toSet());
	}
	
	protected String removeBasePath(String fullPath) {
		return fullPath.replace(basePath, "");
	}
	
	/**
	 * Converts the program's configuration into serializable key-value pairs which
	 * can then be used by slave modules to re-build an equivalent state. Such information
	 * includes the parallelism, path to the script, models and variables in the frame stack.
	 * 
	 * @return The configuration properties.
	 */
	public HashMap<String, ? extends Serializable> getJobParameters() {
		HashMap<String, Serializable> config = new HashMap<>();
		
		config.put("basePath", basePath);
		config.put("localParallelism", numThreads);
		config.put("distributedParallelism", distributedParallelism);
		config.put("evlScript", removeBasePath(getModule().getFile().toPath().toString()));
		config.put("output", outputDir);
		
		List<IModel> models = getModelRepository().getModels();
		int numModels = models.size();
		config.put("numberOfModels", numModels);
		
		if (modelProperties != null) {
			assert numModels == modelProperties.size();
			
			Iterator<StringProperties> modelPropertiesIter = modelProperties.iterator();
			
			for (int i = 0; i < numModels; i++) {
				config.put("model"+i,
					models.get(i).getClass().getName().replace("org.eclipse.epsilon.emc.", "")+"#"+
					modelPropertiesIter.next().entrySet().stream()
						.map(entry -> entry.getKey()+"="+entry.getValue())
						.map(this::removeBasePath)
						.collect(Collectors.joining(","))
				);
			}
		}
		
		if (initialVariables != null) {
			String variablesFlattened = initialVariables
				.stream()
				.map(v -> v.getName() + "=" + Objects.toString(v.getValue()))
				.collect(Collectors.joining(","));
			
			config.put("scriptParameters", variablesFlattened);
		}
		
		return config;
	}
}
