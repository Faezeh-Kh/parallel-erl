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

import java.util.List;
import org.eclipse.epsilon.common.function.ExceptionHandler;

public interface CompositeCombinationValidator<T, E extends Exception> {
	
	boolean isValid(List<List<T>> combination) throws E;
	
	ExceptionHandler<E> getExceptionHandler();
	
}
