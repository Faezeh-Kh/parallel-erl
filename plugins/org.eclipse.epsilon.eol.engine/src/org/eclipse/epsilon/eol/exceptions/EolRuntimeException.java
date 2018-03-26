/*******************************************************************************
 * Copyright (c) 2017 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Dimitrios Kolovos - initial API and implementation
 *     Sina Madani - findCause, message caching
 ******************************************************************************/
package org.eclipse.epsilon.eol.exceptions;

import java.util.Collection;
import java.util.HashSet;
import org.eclipse.epsilon.common.module.ModuleElement;
import org.eclipse.epsilon.eol.IEolModule;
import org.eclipse.epsilon.eol.execute.context.IEolContext;

public class EolRuntimeException extends Exception {
	
	protected ModuleElement ast = null;
	protected String reason = "", message = null;
	protected IEolContext context = null;
	
	public EolRuntimeException() {
		super();
	}
	
	public EolRuntimeException(String reason) {
		super(reason);
		this.reason = reason;
	}

	public EolRuntimeException(String reason, ModuleElement ast) {
		this(reason);
		this.ast = ast;
	}
	
	public EolRuntimeException(Throwable cause) {
		super(cause);
	}
	
	public EolRuntimeException(String reason, Throwable cause) {
		super(reason, cause);
	}

	public ModuleElement getAst() {
		return ast;
	}

	public void setAst(ModuleElement ast) {
		this.ast = ast;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}
	
	public void setMessage(String message) {
		this.message = message;
	}
	
	public int getLine() {
		if (getAst() != null) {
			return getAst().getRegion().getStart().getLine();
		}
		else return 0;
	}
	
	public int getColumn() {
		if (getAst() != null) {
			return getAst().getRegion().getStart().getColumn();
		}
		else return 0;
	}
	
	@Override
	public String getMessage() {
		if (message == null) {
			message = getReason();
			message = message != null ? message.replace('(','[').replace(')',']') : "Unkown reason";
			
			if (ast != null && ast.getModule() instanceof IEolModule) {
				IEolContext context = ((IEolModule) ast.getModule()).getContext();
				if (context != null) {
					message += "\r\n" + context.getExecutorFactory().getStackTraceManager().getStackTraceAsString();
				}
			}
		}
		return message;
	}
	
	@Override
	public String toString() {
		return getMessage();
	}
	
	public static EolRuntimeException wrap(Throwable t) {
		if (t instanceof EolRuntimeException) return (EolRuntimeException) t;
		else return new EolInternalException(t);
	}
	
	public static void propagate(Throwable t) throws EolRuntimeException {
		throw wrap(t);
	}

	public static void propagateDetailed(Throwable t) throws EolRuntimeException {
		if (t instanceof EolRuntimeException) {
			throw (EolRuntimeException) t;
		}
		else {
			throw new EolRuntimeException(
				t.getMessage(),
				EolRuntimeException.findCause(t).getAst()
			);
		}
	}
	
	/*
	 * Traverses the causes of the (usually unchecked) exception to find an EolRuntimeException.
	 * If not found, it will wrap the root cause into an instance of EolRuntimeException.
	 * This method accounts for any depth of cyclic causes, so it is guaranteed to terminate;
	 * unless there is a @linkplain{StackOverflowError}.
	 */
	public static EolRuntimeException findCause(Throwable runtimeEx) {
		return findCauseImpl(runtimeEx, new HashSet<>());
	}
	
	private static EolRuntimeException findCauseImpl(Throwable ex, Collection<Throwable> causes) {
		if (ex instanceof EolRuntimeException) {
			return (EolRuntimeException) ex;
		}
		else if (ex != null) {
			Throwable currentCause = ex.getCause();
			if (currentCause instanceof EolRuntimeException) {
				return (EolRuntimeException) currentCause;
			}
			else if (currentCause != null) {
				if (causes.contains(currentCause)) {
					return new EolRuntimeException(currentCause);
				}
				else {
					causes.add(currentCause);
					return findCauseImpl(currentCause.getCause(), causes);
				}
			}
			else {
				return new EolRuntimeException(ex);
			}
		}
		else {
			return new EolRuntimeException();
		}
	}
}
