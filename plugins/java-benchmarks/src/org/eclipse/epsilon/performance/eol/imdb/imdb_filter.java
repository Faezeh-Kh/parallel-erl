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

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.eclipse.epsilon.common.util.profiling.BenchmarkUtils;

/**
 * 
 * @author Sina Madani
 */
public class imdb_filter extends AbstractIMDBQuery {
	
	public static void main(String... args) throws Exception {
		extensibleMain(imdb_count.class, args);
	}
	
	protected imdb_filter(Builder<?> builder) {
		super(builder);
	}
	
	@Override
	protected Number execute() throws Exception {
		return BenchmarkUtils.profileExecutionStage(profiledStages, "execute()", () -> {
			return StreamSupport.stream(model.getAllOfKind("Person").spliterator(), parallel)
				.filter(a -> coactors(a).stream().anyMatch(co -> areCoupleCoactors(a, co)))
				.collect(Collectors.toList())
				.size();
		});
	}
}
