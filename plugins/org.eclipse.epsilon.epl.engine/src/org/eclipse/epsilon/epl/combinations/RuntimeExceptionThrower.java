/*******************************************************************************
 * Copyright (c) 2012 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Dimitrios Kolovos - initial API and implementation
 ******************************************************************************/
package org.eclipse.epsilon.epl.combinations;

import org.eclipse.epsilon.common.function.ExceptionHandler;
import org.eclipse.epsilon.eol.execute.context.IEolContext;

public class RuntimeExceptionThrower<E extends Exception> implements ExceptionHandler<E> {
	
	protected IEolContext context;
	
	public RuntimeExceptionThrower(IEolContext context) {
		this.context = context;
	}
	
	@Override
	public void handleException(E ex) {
		throw new RuntimeException(ex.getMessage());
	}

}
