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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import org.eclipse.epsilon.emc.emf.EmfModel;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.operations.contributors.IterableOperationContributor;
import org.eclipse.epsilon.eol.types.EolSet;
import org.eclipse.epsilon.performance.eol.AbstractBenchmark;

/**
 * 
 * @see imdb_foop.eol
 * @author Sina Madani
 */
public abstract class AbstractIMDBQuery extends AbstractBenchmark {
	
	protected static <B extends AbstractBenchmark> void extensibleMain(Class<B> clazz, String... args) throws Exception {
		extensibleMain(clazz, new EmfModel(), args);
	}
	
	protected AbstractIMDBQuery(Builder<?> builder) {
		super(builder);
	}
	
	protected final int threshold = 3;
	
	protected int getN() {
		return 32;
	}
	
	protected boolean nestedActors(Object self) {
		try {
			var actors = new ArrayList<>(model.getAllOfKind("Person"));
			var n = getN();
			var toIndex = actors.size() / n;
			var subActors = actors.subList(0, toIndex);
			var persons = (Collection<?>) propertyGetter.invoke(self, "persons");
			return persons.stream().anyMatch(ac -> 
				subActors.stream().anyMatch(mp -> {
					if (ac.hashCode() == mp.hashCode()) try {
						var mpMovies = (Collection<?>) propertyGetter.invoke(mp, "movies");
						var acMovies = (Collection<?>) propertyGetter.invoke(ac, "movies");
						return mpMovies.size() == acMovies.size();
					}
					catch (EolRuntimeException ex) {
						throw new RuntimeException(ex);
					}
					else return false;
				})
			);
		}
		catch (EolRuntimeException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	protected boolean hasCoupleCoactors(Object self) {
		return coactors(self).stream().anyMatch(co -> areCoupleCoactors(self, co));
	}
	
	protected Set<?> coactors(Object self) {
		try {
			var movies = (Collection<?>) propertyGetter.invoke(self, "movies");
			var results = new EolSet<>();
			for (var movie : movies) {
				for (var moviePerson : ((Iterable<?>) propertyGetter.invoke(movie, "persons"))) {
					results.add(moviePerson);
				}
			}
			return results;
		}
		catch (EolRuntimeException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	protected boolean areCoupleCoactors(Object self, Object co) {
		try {
			var selfName = (String) propertyGetter.invoke(self, "name");
			var coName = (String) propertyGetter.invoke(co, "name");
			var comparison = selfName.compareTo(coName);
			if (comparison < 0) {
				var bMovies = (Collection<?>) propertyGetter.invoke(co, "movies");
				if (bMovies.size() >= threshold) {
					return areCouple(self, co);
				}
			}
			return false;
		}
		catch (EolRuntimeException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	protected boolean areCouple(Object self, Object p) throws EolRuntimeException {
		var selfMovies = (Collection<?>) propertyGetter.invoke(self, "movies");
		var pMovies = (Collection<?>) propertyGetter.invoke(p, "movies");
		var excludingPMoviesSize = new IterableOperationContributor(selfMovies).excludingAll(pMovies).size();
		var targetSize = selfMovies.size() - threshold;
		return excludingPMoviesSize <= targetSize;
	}
}
