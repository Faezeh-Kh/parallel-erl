/*******************************************************************************
 * Copyright (c) 2017 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Louis Rose - initial API and implementation
 *     Sina Madani - Concurrency tests, refactoring, utilities etc.
 ******************************************************************************
 *
 * $Id$
 */
package org.eclipse.epsilon.evl.engine.test.acceptance;

import static org.junit.Assert.fail;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.common.util.CollectionUtil;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.erl.engine.launch.ErlRunConfiguration;
import org.eclipse.epsilon.evl.*;
import org.eclipse.epsilon.evl.concurrent.*;
import org.eclipse.epsilon.evl.engine.launch.EvlRunConfiguration;
import org.eclipse.epsilon.evl.execute.context.concurrent.EvlContextParallel;
import org.eclipse.epsilon.evl.execute.context.concurrent.IEvlContextParallel;

public class EvlAcceptanceTestUtil {

	private EvlAcceptanceTestUtil() {}
	
	public static <T> void failIfDifferent(boolean condition, T expected, T actual) {
		if (condition) {
			String datatype = expected.getClass().getSimpleName();
			System.err.println();
			System.out.println("Expected "+datatype+": ");
			System.out.println(expected);
			System.out.println(); System.err.println();
			System.out.println("Actual "+datatype+": ");
			System.out.println(actual);
			
			fail(datatype+"s differ!");
		}
	}
	
	public static final String
		//Core
		testsBase = getTestBaseDir(EvlAcceptanceTestSuite.class),
		metamodelsRoot = testsBase+"metamodels/",
		scriptsRoot = testsBase+"scripts/",
		modelsRoot = testsBase+"models/",
		
		//Metamodels and scripts:
		javaMetamodel = "java.ecore",
		javaScripts[] = {
			"java_findbugs",
			"java_1Constraint",
			"java_manyConstraint1Context",
			"java_manyContext1Constraint"
		},
		javaModels[] = {
			"epsilon_profiling_test.xmi",
			"test001_java.xmi",
			"emf_cdo-example.xmi",
		},
		
		thriftMetamodel = "thrift.ecore",
		thriftScripts[] = {"thrift_validator"},
		thriftModels[] = {
			"fb303.xmi",
			"SimpleService.xmi",
			"ThriftTest.xmi"
		},
		
		cookbookMetamodel = "cookbook.ecore",
		cookbookScripts[] = {"cookbook"},
		cookbookModels[],
		
		imdbMetamodel = "movies.ecore",
		imdbScripts[] = {"imdb_validator"},
		imdbModels[] = {"imdb-small.xmi"},
		
		dblpMetamodel = "dblp.ecore",
		dblpScripts[] = {"dblp_isbn"},
		dblpModels[] = {"simpleDBLP.xmi"};
	
	/*Nx3 array where N is number of test inputs;
	 *  0 is the script path,
	 *  1 is the model path,
	 *  2 is the metamodel path.
	 */
	public static final List<String[]>
		allInputs,
		javaInputs,
		thriftInputs,
		cookbookInputs,
		dblpInputs,
		imdbInputs;
	
	static {
		cookbookModels = new String[6];
		cookbookModels[0] = "cookbook_cyclic.model";
		for (int i = 1; i < 6; i++) {
			cookbookModels[i] = "cookbook"+i+".model";
		}
		
		javaInputs = addAllInputs(javaScripts, javaModels, javaMetamodel);
		thriftInputs = addAllInputs(thriftScripts, thriftModels, thriftMetamodel);
		cookbookInputs = addAllInputs(cookbookScripts, cookbookModels, cookbookMetamodel);
		imdbInputs = addAllInputs(imdbScripts, imdbModels, imdbMetamodel);
		dblpInputs = addAllInputs(dblpScripts, dblpModels, dblpMetamodel);
		
		allInputs = CollectionUtil.composeArrayListFrom(
			dblpInputs,
			imdbInputs,
			cookbookInputs,
			javaInputs,
			thriftInputs
		);
	}
	
	static final Collection<Supplier<? extends IEvlContextParallel>> PARALLEL_CONTEXTS = parallelContexts(
		new int[] { // Number of threads
			0, 1, 2, 3, 4,
			(ConcurrencyUtils.DEFAULT_PARALLELISM/2)+1,
			(ConcurrencyUtils.DEFAULT_PARALLELISM*2)-1,
			//0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
			//Short.MAX_VALUE/8
		}
	);
	
	public static List<String[]> addAllInputs(String[] scripts, String[] models, String metamodel, String scriptExt, String scriptRoot, String modelRoot, String metamodelRoot) {
		ArrayList<String[]> inputsCol = new ArrayList<>(scripts.length*models.length);
		for (String script : scripts) {
			for (String model : models) {
				inputsCol.add(new String[] {
					scriptRoot+script+'.'+scriptExt,
					modelRoot+model,
					metamodelRoot+metamodel
				});
			}
		}
		return inputsCol;
	}
	
	public static int getScenarioID(String[] inputs) {
		return Arrays.deepHashCode(inputs);
	}
	
	/*
	 * A list of pre-configured Runnables which will call the execute() method on the provided module.
	 * @param modules A collection of IEvlModules to use in combination with each set of test data.
	 */
	public static Collection<EvlRunConfiguration> getScenarios(List<String[]> testInputs, boolean includeTest, Collection<Supplier<? extends IEvlModule>> moduleGetters, Function<String[], Integer> idCalculator) {
		if (testInputs == null) testInputs = allInputs;
		if (idCalculator == null) idCalculator = EvlAcceptanceTestUtil::getScenarioID;
		if (moduleGetters == null) moduleGetters = modules();
		
		List<EvlRunConfiguration> scenarios = new ArrayList<>(moduleGetters.size()*(testInputs.size()+2));
		boolean showUnsatisfied = false, profileExecution = false;
		
		for (String[] testInput : testInputs) {
			Path evlScript = Paths.get(testInput[0]);
			
			StringProperties testProperties = ErlRunConfiguration.makeProperties(
				testInput[1],				//Model path
				testInput[2],				//Metamodel path
				true,						//Cache model
				true						//Store on disposal
			);
			
			IModel model = ErlRunConfiguration.getIModelFromPath(testInput[2]);
			
			for (Supplier<? extends IEvlModule> moduleGetter : moduleGetters) {
				scenarios.add(new EvlRunConfiguration(
						evlScript,									//Path to the script to run
						testProperties,								//Model and metamodel paths
						model,										//Model object to use
						Optional.of(showUnsatisfied),				//Whether to show results
						Optional.of(profileExecution),				//Whether to measure execution time
						Optional.of(moduleGetter.get()),			//IEvlModule
						Optional.of(idCalculator.apply(testInput)),	//Unique identifier for this configuration
						Optional.empty()							//Output file
					)
				);
				
				if (includeTest) {
					IEvlModule evlStd = moduleGetter.get();
					int evlStdId = testInputs.size()+1;
					
					scenarios.add(
						new EvlRunConfiguration(
							EvlTests.getTestScript(evlStd).toPath(),
							null,
							EvlTests.getTestModel(false),
							Optional.of(showUnsatisfied),
							Optional.of(profileExecution),
							Optional.of(evlStd),
							Optional.of(evlStdId),
							Optional.empty()
						)
					);
				}
			}
		}
		
		return scenarios;
	}
	
	public static Collection<? extends IEvlModule> unwrapModules(Collection<Supplier<? extends IEvlModule>> moduleGetters) {
		return moduleGetters.stream().map(Supplier::get).collect(Collectors.toList());
	}
	
	/*
	 * @param includeStandard Whether to include the default IEvlModule
	 * @param others An array indicating which modules to include. [0] is EvlModuleParallel, [1] is EvlModuleParallelStaged, [2] is EvlModuleParallelGraph.
	 * @param contexts The IEvlContextParallel configurations to use.
	 * @param repeats How many times to duplicate (cycle) the modules in the returned collection.
	 * @return A collection of Suppliers which return the specified IEvlModules.
	 */
	static Collection<Supplier<? extends IEvlModule>> parallelModules(boolean[] moduleIncludes, Collection<Supplier<? extends IEvlContextParallel>> contexts, int repeats) {
		if (contexts == null) contexts = Collections.singleton(EvlContextParallel::new);
		ArrayList<Supplier<? extends IEvlModule>> modules = new ArrayList<>(moduleIncludes.length*repeats*contexts.size());
		
		for (int r = 0; r < repeats; r++) {
			for (Supplier<? extends IEvlContextParallel> contextGetter : contexts) {
				if (moduleIncludes[0])
					modules.add(() -> new EvlModuleParallelStaged(contextGetter.get()));
				if (moduleIncludes[1])
					modules.add(() -> new EvlModuleParallelConstraints(contextGetter.get()));
				if (moduleIncludes[2])
					modules.add(() -> new EvlModuleParallelElements(contextGetter.get()));
			}
		}
		
		return modules;
	}
	
	private static Collection<Supplier<? extends IEvlContextParallel>> parallelContexts(int[] parallelisms) {
		Collection<Supplier<? extends IEvlContextParallel>> contexts = new ArrayList<>(parallelisms.length);
		
		for (int threads : parallelisms) {
			contexts.add(() -> new EvlContextParallel(threads));
		}
		
		return contexts;
	}
	
	/*
	 * @param moduleIncludes [0]=EvlModule, [1]=EvlModuleParallelStaged, [2]=EvlModuleParallelConstraints, [3]=EvlModuleParallelElements.
	 */
	public static Collection<Supplier<? extends IEvlModule>> modules(boolean[] moduleIncludes, int repeats) {
		Collection<Supplier<? extends IEvlModule>> modules = parallelModules(Arrays.copyOfRange(moduleIncludes, 1, moduleIncludes.length), PARALLEL_CONTEXTS, repeats);
		if (moduleIncludes[0]) {
			for (int r = 0; r < repeats; r++) {
				modules.add(EvlModule::new);
			}
		}
		return modules;
	}
	
	/*
	 * Convenience hack for handling exceptions when resolving this class's package source directory.
	 */
	public static String getTestBaseDir(Class<?> clazz) {
		try {
			return Paths.get(clazz.getResource("").toURI()).toString().replace("bin", "src")+'/';
		}
		catch (URISyntaxException urx) {
			System.err.println(urx.getMessage());
			return null;
		}
	}
	
	
	//Boilerplate defaults
	public static List<String[]> addAllInputs(String[] scripts, String[] models, String metamodel) {
		return addAllInputs(scripts, models, metamodel, "evl", scriptsRoot, modelsRoot, metamodelsRoot);
	}
	@SafeVarargs
	public static Collection<? extends EvlRunConfiguration> getScenarios(boolean includeTest, Supplier<? extends IEvlModule>... moduleGetters) {
		return getScenarios(null, includeTest, Arrays.asList(moduleGetters), null);
	}
	@SafeVarargs
	public static Collection<? extends EvlRunConfiguration> getScenarios(Supplier<? extends IEvlModule>... moduleGetters) {
		return getScenarios(null, true, Arrays.asList(moduleGetters), null);
	}
	public static Collection<EvlRunConfiguration> getScenarios(List<String[]> testInputs, boolean includeTest, Collection<Supplier<? extends IEvlModule>> moduleGetters) {
		return getScenarios(testInputs, includeTest, moduleGetters, null);
	}
	
	public static Collection<Supplier<? extends IEvlModule>> modules(int repeats, boolean... moduleIncludes) {
		boolean[] fixedIncludes = new boolean[4];
		for (int i = 0; i < moduleIncludes.length; i++) {
			fixedIncludes[i] = moduleIncludes[i];
		}
		for (int i = fixedIncludes.length-1; i >= moduleIncludes.length; i--) {
			fixedIncludes[i] = true;
		}
		return modules(fixedIncludes, repeats);
	}
	
	public static Collection<Supplier<? extends IEvlModule>> modules(boolean... moduleIncludes) {
		return modules(1, moduleIncludes);
	}
	public static Collection<Supplier<? extends IEvlModule>> modules() {
		return modules(1);
	}
}
