import java.util.Collection;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.eclipse.epsilon.common.launch.ProfilableRunConfiguration;
import org.eclipse.epsilon.common.util.profiling.BenchmarkUtils;
import org.eclipse.epsilon.emc.emf.EmfModel;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.introspection.IPropertyGetter;
import org.eclipse.epsilon.eol.execute.operations.contributors.IterableOperationContributor;
import org.eclipse.epsilon.eol.function.CheckedEolRunnable;
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
public class ImdbQueryBenchmark extends ProfilableRunConfiguration {

	static class Builder extends ProfilableRunConfiguration.Builder<ImdbQueryBenchmark, Builder> {
		EmfModel model;
		boolean parallel;
		
		public Builder parallel(boolean p) {
			this.parallel = p;
			return this;
		}
		
		public Builder withModel(String modelPath, String metamodelPath) {
			model = new EmfModel();
			model.setCachingEnabled(true);
			model.setConcurrent(true);
			model.setModelFile(modelPath);
			model.setMetamodelFile(metamodelPath);
			return this;
		}
		
		@Override
		public ImdbQueryBenchmark build() throws IllegalArgumentException, IllegalStateException {
			return new ImdbQueryBenchmark(this);
		}
	}
	
	ImdbQueryBenchmark(Builder builder) {
		super(builder);
		this.propertyGetter = (this.model = builder.model).getPropertyGetter();
		this.parallel = builder.parallel;
	}
	
	final EmfModel model;
	final IPropertyGetter propertyGetter;
	final boolean parallel;
	int threshold = 3;
	
	@Override
	protected void preExecute() throws Exception {
		super.preExecute();
		BenchmarkUtils.profileExecutionStage(profiledStages, "Model loading", (CheckedEolRunnable) model::load);
	}
	
	@Override
	protected Void execute() throws Exception {
		BenchmarkUtils.profileExecutionStage(profiledStages, "execute()", new CheckedEolRunnable() {
			@Override
			public void runThrows() throws EolRuntimeException {
				
				var result = StreamSupport.stream(model.getAllOfKind("Person").spliterator(), parallel)
					.filter(a -> coactors(a).stream().anyMatch(co -> areCoupleCoactors(a, co)))
					.count();
				System.out.println(result);
			}
		});
		
		return null;
	}
	
	public static void main(String... args) throws Exception {
		if (args.length < 4) throw new IllegalArgumentException("Must include path to model, metamodel, whether to parallelise and output file!");
		var modelPath = args[0];
		var metamodelPath = args[1];
		var parallel = Boolean.valueOf(args[2]);
		var resultsFile = args[3];
		
		new Builder().withOutputFile(resultsFile).withProfiling().withModel(modelPath, metamodelPath).parallel(parallel).build().run();
	}
	
	Set<?> coactors(Object self) {
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
	
	boolean areCoupleCoactors(Object self, Object co) {
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
	
	boolean areCouple(Object self, Object p) throws EolRuntimeException {
		var selfMovies = (Collection<?>) propertyGetter.invoke(self, "movies");
		var pMovies = (Collection<?>) propertyGetter.invoke(p, "movies");
		var excludingPMoviesSize = new IterableOperationContributor(selfMovies).excludingAll(pMovies).size();
		var targetSize = selfMovies.size() - threshold;
		return excludingPMoviesSize <= targetSize;
	}
}
