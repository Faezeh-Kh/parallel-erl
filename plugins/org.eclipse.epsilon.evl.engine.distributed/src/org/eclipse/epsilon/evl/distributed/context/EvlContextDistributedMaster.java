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
import static java.net.URLDecoder.*;
import static java.net.URLEncoder.*;
import java.nio.file.Paths;
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

	static final String
		BASE_PATH = "basePath",
		BASE_PATH_SUBSTITUTE = "//BASEPATH//",
		LOCAL_PARALLELISM = "localParallelism",
		DISTRIBUTED_PARALLELISM = "distributedParallelism",
		EVL_SCRIPT = "evlScript",
		OUTPUT_DIR = "output",
		NUM_MODELS = "numberOfModels",
		MODEL_PREFIX = "model",
		SCRIPT_PARAMS = "scriptParameters";
	
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
		if (path != null) {
			try {
				this.basePath = decode(java.net.URI.create(encode(path)).normalize().toString());
			}
			catch (IllegalArgumentException iax) {
				this.basePath = Paths.get(path).normalize().toString();
			}
		}
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
	
	protected String removeBasePath(Object fullPath) {
		String fpStr = Objects.toString(fullPath);
		try {
			String fpNormal = fpStr
				.replace("\\", "/")
				.replace(basePath, BASE_PATH_SUBSTITUTE)
				.replace(
					java.net.URI.create(encode(basePath)).normalize().toString(), BASE_PATH_SUBSTITUTE
				)
				.replace(basePath.replace(" ", "%20"), BASE_PATH_SUBSTITUTE);
			
			/*try {
				fpNormal = Paths.get(fpStr).normalize().toString();
			}
			catch (InvalidPathException ipx) {
				try {
					fpNormal = java.net.URI.create(fpStr).normalize().toString();
				}
				catch (IllegalArgumentException iax) {
					fpNormal = decode(java.net.URI.create(encode(fpStr)).normalize().toString());
				}
			}*/
			return fpNormal.replace(basePath, BASE_PATH_SUBSTITUTE);
		}
		catch (Exception ex) {
			return fpStr;
		}
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
		
		config.put(BASE_PATH, BASE_PATH_SUBSTITUTE);
		config.put(LOCAL_PARALLELISM, numThreads);
		config.put(DISTRIBUTED_PARALLELISM, distributedParallelism);
		config.put(EVL_SCRIPT, removeBasePath(getModule().getFile().toPath().toString()));
		config.put(OUTPUT_DIR, outputDir);
		
		List<IModel> models = getModelRepository().getModels();
		int numModels = models.size();
		config.put(NUM_MODELS, numModels);
		
		if (modelProperties != null) {
			assert numModels == modelProperties.size();
			
			Iterator<StringProperties> modelPropertiesIter = modelProperties.iterator();
			
			for (int i = 0; i < numModels; i++) {
				config.put(MODEL_PREFIX+i,
					models.get(i).getClass().getName().replace("org.eclipse.epsilon.emc.", "")+"#"+
					modelPropertiesIter.next().entrySet().stream()
						.map(entry -> entry.getKey()+"="+removeBasePath(entry.getValue()))
						.collect(Collectors.joining(","))
				);
			}
		}
		
		if (initialVariables != null) {
			String variablesFlattened = initialVariables
				.stream()
				.map(v -> v.getName() + "=" + Objects.toString(v.getValue()))
				.collect(Collectors.joining(","));
			
			config.put(SCRIPT_PARAMS, variablesFlattened);
		}
		
		return config;
	}
}
