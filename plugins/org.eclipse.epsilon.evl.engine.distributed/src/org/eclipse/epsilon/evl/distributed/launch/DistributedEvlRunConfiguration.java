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

import java.net.URI;
import java.nio.file.Paths;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.evl.launch.EvlRunConfiguration;

/**
 * Run configuration container which holds the program arguments in the slave
 * nodes (i.e. the path to the script, the models, additional
 * parameters and arguments etc.).
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class DistributedEvlRunConfiguration extends EvlRunConfiguration {
	
	public static Builder<? extends DistributedEvlRunConfiguration, ?> Builder() {
		return new Builder<>(DistributedEvlRunConfiguration.class);
	}
	
	@SuppressWarnings("unchecked")
	public static class Builder<D extends DistributedEvlRunConfiguration, B extends Builder<D, B>> extends EvlRunConfiguration.Builder<D, B> {
		public String basePath;
		
		protected Builder(Class<D> runConfigClass) {
			super(runConfigClass);
		}
		
		public B withBasePath(String base) {
			this.basePath = base;
			return (B) this;
		}
		
		@Override
		public D build() {
			for (StringProperties props : modelsAndProperties.values()) {
				props.replaceAll((k, v) -> {
					// TODO better way to determine if there is a path?
					if (v instanceof String && ((String)v).startsWith("file://")) {
						return appendBasePath(basePath, (String) v);
					}
					return v;
				});
			}
			if (script != null) {
				script = Paths.get(basePath, script.toString());
			}
			if (outputFile != null) {
				outputFile = Paths.get(basePath, outputFile.toString());
			}
			
			return super.buildReflective(null);
		}
	}
	
	public static String appendBasePath(String basePath, String relPath) {
		return Paths.get(basePath, URI.create(relPath).getPath()).toUri().toString();
	}
	
	protected final String basePath;
	
	public DistributedEvlRunConfiguration(EvlRunConfiguration other) {
		super(other);
		basePath = "/";
	}
	
	DistributedEvlRunConfiguration(Builder<? extends DistributedEvlRunConfiguration, ?> builder) {
		super(builder);
		this.basePath = builder.basePath;
	}
}
