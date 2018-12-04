/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.epl.launch;

import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.epl.DynamicEplModule;
import org.eclipse.epsilon.epl.IEplModule;
import org.eclipse.epsilon.epl.execute.PatternMatchModel;
import org.eclipse.epsilon.erl.launch.IErlRunConfiguration;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EplRunConfiguration extends IErlRunConfiguration {

	public static Builder<? extends EplRunConfiguration, ?> Builder() {
		return Builder(EplRunConfiguration.class);
	}
	
	public EplRunConfiguration(Builder<EplRunConfiguration, ?> builder) {
		super(builder);
	}
	
	public EplRunConfiguration(EplRunConfiguration other) {
		super(other);
	}
	
	@Override
	protected PatternMatchModel execute() throws EolRuntimeException {
		return (PatternMatchModel) super.execute();
	}
	
	@Override
	public PatternMatchModel getResult() {
		return (PatternMatchModel) super.getResult();
	}
	
	@Override
	public IEplModule getModule() {
		return (IEplModule) super.getModule();
	}
	
	@Override
	protected IEplModule getDefaultModule() {
		return new DynamicEplModule();
	}
}