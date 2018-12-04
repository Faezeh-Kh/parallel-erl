/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.epl.concurrent;

import java.util.Collection;
import java.util.Iterator;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.epl.AbstractEplModule;
import org.eclipse.epsilon.epl.dom.Pattern;
import org.eclipse.epsilon.epl.execute.PatternMatchModel;
import org.eclipse.epsilon.epl.execute.context.concurrent.EplContextParallel;
import org.eclipse.epsilon.erl.execute.context.concurrent.IErlContextParallel;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class ForkJoinEplModule extends AbstractEplModule {

	public ForkJoinEplModule() {
		this.context = new EplContextParallel();
	}

	public ForkJoinEplModule(int parallelism) {
		this.context = new EplContextParallel(parallelism);
	}
	
	@Override
	protected void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
		getContext().goParallel();
	}
	
	@Override
	protected void postExecution() throws EolRuntimeException {
		getContext().endParallel();
		super.postExecution();
	}
	
	@Override
	public IErlContextParallel getContext() {
		return (IErlContextParallel) context;
	}
	
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof EplContextParallel) {
			super.setContext(context);
		}
	}
	
	@Override
	protected PatternMatchModel createModel() throws EolRuntimeException {
		return new PatternMatchModel(true);
	}
	
	@Override
	protected Iterator<? extends Collection<? extends Iterable<?>>> getCandidates(Pattern pattern) throws EolRuntimeException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
