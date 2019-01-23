import java.util.Collection;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.epsilon.emc.emf.EmfModel;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.introspection.IPropertyGetter;
import org.eclipse.epsilon.eol.execute.operations.contributors.IterableOperationContributor;
import org.eclipse.epsilon.eol.types.EolSet;

/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
public class ImdbQueryBenchmark {

	static IPropertyGetter propertyGetter;
	static int threshold = 3;
	
	public static void main(String... args) throws Exception {
		if (args.length < 3) throw new IllegalArgumentException("Must include path to model, metamodel and whether to parallelise!");
		var modelPath = args[0];
		var metamodelPath = args[1];
		var parallel = Boolean.valueOf(args[1]);
		
		var model = new EmfModel();
		model.setCachingEnabled(true);
		model.setConcurrent(true);
		model.setModelFile(modelPath);
		model.setMetamodelFile(metamodelPath);
		model.load();
		propertyGetter = model.getPropertyGetter();
		
		var result = StreamSupport.stream(model.getAllOfKind("Person").spliterator(), parallel)
			.filter(a -> coactors(a).stream().anyMatch(co -> areCoupleCoactors(a, co)))
			.count();
		
		System.out.println(result);
	}
	
	@SuppressWarnings("unchecked")
	static Set<? extends EObject> coactors(EObject self) {
		try {
			var movies = (Collection<? extends EObject>) propertyGetter.invoke(self, "movies");
			var results = new EolSet<EObject>();
			for (var movie : movies) {
				for (var moviePerson : ((Iterable<? extends EObject>) propertyGetter.invoke(movie, "persons"))) {
					results.add(moviePerson);
				}
			}
			return results;
		}
		catch (EolRuntimeException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	@SuppressWarnings("unchecked")
	static boolean areCoupleCoactors(EObject self, EObject co) {
		try {
			var selfName = (String) propertyGetter.invoke(self, "name");
			var coName = (String) propertyGetter.invoke(co, "name");
			var comparison = selfName.compareTo(coName);
			if (comparison < 0) {
				var bMovies = (Collection<? extends EObject>) propertyGetter.invoke(co, "movies");
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
	
	@SuppressWarnings("unchecked")
	static boolean areCouple(EObject self, EObject p) throws EolRuntimeException {
		var selfMovies = (Collection<? extends EObject>) propertyGetter.invoke(self, "movies");
		var pMovies = (Collection<? extends EObject>) propertyGetter.invoke(p, "movies");
		var excludingPMoviesSize = new IterableOperationContributor(selfMovies).excludingAll(pMovies).size();
		var targetSize = selfMovies.size() - threshold;
		return excludingPMoviesSize <= targetSize;
	}
}
