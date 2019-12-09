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

import java.util.*;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.operations.contributors.IterableOperationContributor;
import org.eclipse.epsilon.eol.types.*;
import org.eclipse.epsilon.performance.eol.AbstractBenchmark;

/**
 * 
 * @see imdb_foop.eol
 * @author Sina Madani
 */
public abstract class AbstractIMDBQuery extends AbstractBenchmark {
	
	protected AbstractIMDBQuery(Builder<?, ?> builder) {
		super(builder);
	}
	
	protected final int threshold = 3;
	
	protected int getN() {
		return 32;
	}
	
	protected boolean nestedActors(Object self) {
		try {
			ArrayList<?> actors = new ArrayList<>(model.getAllOfKind("Person"));
			int n = getN();
			int toIndex = actors.size() / n;
			Collection<?> subActors = actors.subList(0, toIndex);
			Collection<?> persons = (Collection<?>) propertyGetter.invoke(self, "persons");
			return persons.stream().anyMatch(ac -> 
				subActors.stream().anyMatch(mp -> {
					if (ac.hashCode() == mp.hashCode()) try {
						Collection<?> mpMovies = (Collection<?>) propertyGetter.invoke(mp, "movies");
						Collection<?> acMovies = (Collection<?>) propertyGetter.invoke(ac, "movies");
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
		return coactors(self).stream().anyMatch(co -> areCoupleCoactors(co, self));
	}
	
	protected Set<?> coactors(Object self) {
		try {
			Collection<?> movies = (Collection<?>) propertyGetter.invoke(self, "movies");
			Collection<Object> persons = EolCollectionType.isOrdered(movies) ?
				new EolSequence<>(movies.size()) : new EolBag<>(movies.size());
			
			for (Object movie : movies) {
				persons.add(propertyGetter.invoke(movie, "persons"));
			}
			Collection<Object> flattened = new IterableOperationContributor(persons).flatten();
			return new IterableOperationContributor(flattened).asSet();
		}
		catch (EolRuntimeException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	protected boolean areCoupleCoactors(Object self, Object co) {
		try {
			String selfName = (String) propertyGetter.invoke(self, "name");
			String coName = (String) propertyGetter.invoke(co, "name");
			if (selfName.compareTo(coName) < 0) {
				Collection<?> bMovies = (Collection<?>) propertyGetter.invoke(co, "movies");
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
		Collection<?> selfMovies = (Collection<?>) propertyGetter.invoke(self, "movies");
		Collection<?> pMovies = (Collection<?>) propertyGetter.invoke(p, "movies");
		int excludingPMoviesSize = new IterableOperationContributor(selfMovies).excludingAll(pMovies).size();
		int targetSize = selfMovies.size() - threshold;
		return excludingPMoviesSize <= targetSize;
	}
}
