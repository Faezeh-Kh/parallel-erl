/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.performance.eol.imdb;

import java.util.stream.StreamSupport;
import org.eclipse.epsilon.common.util.profiling.BenchmarkUtils;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;

/**
 *
 * @author Sina Madani
 */
public class imdb_atLeastN extends AbstractIMDBQuery {
	
	public imdb_atLeastN(Builder<?, ?> builder) {
		super(builder);
	}

	@Override
	protected Boolean execute() throws EolRuntimeException {
		return BenchmarkUtils.profileExecutionStage(profiledStages, "execute()", () -> {
			return StreamSupport.stream(model.getAllOfKind("Movie").spliterator(), parallel)
				.filter(this::nestedActors)
				.count() <= getN();
		});
	}
}
