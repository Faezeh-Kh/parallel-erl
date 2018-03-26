/*******************************************************************************
 * Copyright (c) 2008 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Dimitrios Kolovos - initial API and implementation
 ******************************************************************************/
package org.eclipse.epsilon.eol.execute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.concurrent.ConcurrentBaseDelegate;
import org.eclipse.epsilon.common.module.ModuleElement;
import org.eclipse.epsilon.eol.EolModule;
import org.eclipse.epsilon.eol.dom.IExecutableModuleElement;
import org.eclipse.epsilon.eol.exceptions.EolInternalException;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.exceptions.flowcontrol.EolTerminationException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.execute.control.DefaultExecutionController;
import org.eclipse.epsilon.eol.execute.control.ExecutionController;
import org.eclipse.epsilon.eol.execute.control.IExecutionListener;

public class ExecutorFactory implements ConcurrentBaseDelegate<ExecutorFactory> {
	
	protected ExecutionController executionController;
	protected ModuleElement activeModuleElement = null;
	protected Collection<IExecutionListener> executionListeners;
	protected StackTraceManager stackTraceManager;
	protected ExecutorFactory base;
	protected boolean isConcurrent;
	
	public ExecutorFactory() {
		this(null);
	}
	
	public ExecutorFactory(ExecutorFactory parent) {
		this(parent, false);
	}
	
	public ExecutorFactory(ExecutorFactory parent, boolean concurrent) {
		this.base = parent;
		this.isConcurrent = concurrent;
		executionController = new DefaultExecutionController();
		executionListeners = concurrent ? new ConcurrentLinkedQueue<>() : new ArrayList<>(2);
		if (base != null) {
			executionListeners.addAll(base.executionListeners
				.stream()
				.filter(el -> el != base.stackTraceManager)
				.collect(Collectors.toSet())
			);
		}
		setStackTraceManager(new StackTraceManager());
	}
	
	public void addExecutionListener(IExecutionListener listener) {
		executionListeners.add(listener);
	}
	
	public boolean removeExecutionListener(IExecutionListener listener) {
		return executionListeners.remove(listener);
	}
	
	public ExecutionController getExecutionController() {
		return executionController;
	}

	public void setExecutionController(ExecutionController executionController) {
		this.executionController = executionController;
	}
	
	public StackTraceManager getStackTraceManager() {
		return stackTraceManager;
	}
	
	public void setStackTraceManager(StackTraceManager stackTraceManager) {
		if (this.stackTraceManager != null) {
			removeExecutionListener(this.stackTraceManager);
		}
		
		this.stackTraceManager = stackTraceManager;
		addExecutionListener(stackTraceManager);
	}
	
	/**
	 * @deprecated Use {@link ExecutorFactory#execute(ModuleElement, IEolContext)} instead.
	 */
	@Deprecated
	public Object executeAST(ModuleElement moduleElement, IEolContext context) throws EolRuntimeException {
		return execute(moduleElement, context);
	}
	
	public Object execute(ModuleElement moduleElement, IEolContext context) throws EolRuntimeException {
		
		if (moduleElement == null) return null;
		
		activeModuleElement = moduleElement;
		
		if (executionController != null) {
			if (executionController.isTerminated()) throw new EolTerminationException(moduleElement);
			try {
				executionController.control(moduleElement, context);
			}
			catch (Exception ex) {
				throw new EolInternalException(ex);
			} 
		}
		
		for (IExecutionListener listener : executionListeners) {
			listener.aboutToExecute(moduleElement, context);
		}
		
		Object result = null;
		
		try {
			if (moduleElement instanceof IExecutableModuleElement) {
				result = ((IExecutableModuleElement) moduleElement).execute(context);
			}
			else if (moduleElement instanceof EolModule) {
				result = ((EolModule) moduleElement).executeImpl();
			}
			
			for (IExecutionListener listener : executionListeners) {
				listener.finishedExecuting(moduleElement, result, context);
			}
		}
		catch (Exception ex) {
			EolRuntimeException exception = null;
			if (ex instanceof EolRuntimeException) {
				EolRuntimeException eolEx = (EolRuntimeException) ex;
				if (eolEx.getAst() == null) {
					eolEx.setAst(moduleElement);
				}
				exception = eolEx;
			}
			else {
				exception = new EolInternalException(ex, moduleElement);
			}
			for (IExecutionListener listener : executionListeners) {
				listener.finishedExecutingWithException(moduleElement, exception, context);
			}
			throw exception;
		}
		finally {
			if (executionController != null) {
				executionController.done(moduleElement, context);
			}
		}
		
		return result;
	}
	
	public ModuleElement getActiveModuleElement() {
		return activeModuleElement;
	}
	
	@Override
	public ExecutorFactory getBase() {
		return base;
	}
	
	@Override
	public boolean isThreadSafe() {
		return isConcurrent;
	}

	@Override
	public void setThreadSafe(boolean concurrent) {
		if (concurrent != this.isConcurrent) {
			this.isConcurrent = concurrent;
			
			executionListeners = concurrent ?
				new ConcurrentLinkedQueue<>(executionListeners) :
				new ArrayList<>(executionListeners);
		}
	}

	@Override
	public void merge(MergeMode mode) {
		mergeCollectionsUnique(
			ef -> ef.executionListeners
				.stream()
				.filter(el -> el != stackTraceManager)
				.collect(Collectors.toList()),
			ConcurrentLinkedQueue::new,
			ArrayList::new,
			mode
		);
	}
}
