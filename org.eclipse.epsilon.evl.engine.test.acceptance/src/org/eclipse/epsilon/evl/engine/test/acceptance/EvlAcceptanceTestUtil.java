/*******************************************************************************
 * Copyright (c) 2018 The University of York.
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

import static org.eclipse.epsilon.test.util.EpsilonTestUtil.*;
import static org.eclipse.epsilon.erl.engine.test.util.ErlAcceptanceTestUtil.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.eclipse.epsilon.common.util.CollectionUtil;
import org.eclipse.epsilon.erl.engine.test.util.ErlAcceptanceTestUtil;
import org.eclipse.epsilon.evl.*;
import org.eclipse.epsilon.evl.concurrent.*;
import org.eclipse.epsilon.evl.execute.context.concurrent.*;
import org.eclipse.epsilon.evl.engine.launch.EvlRunConfiguration;

public class EvlAcceptanceTestUtil {
	private EvlAcceptanceTestUtil() {}
	
	public static final String
		//Core
		testsBase = getTestBaseDir(EvlAcceptanceTestSuite.class),
		metamodelsRoot = testsBase+"metamodels/",
		scriptsRoot = testsBase+"scripts/",
		modelsRoot = testsBase+"models/",
		
		//Metamodels and scripts
		javaMetamodel = "java.ecore",
		javaModels[] = {
			"epsilon_profiling_test.xmi",
			"test001_java.xmi",
			"emf_cdo-example.xmi",
		},
		javaScripts[] = {
			"java_findbugs",
			"java_1Constraint",
			"java_manyConstraint1Context",
			"java_manyContext1Constraint"
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
	
	static final Collection<Supplier<? extends IEvlContextParallel>> PARALLEL_CONTEXTS = parallelContexts(EvlContextParallel::new);
	
	/*
	 * A list of pre-configured Runnables which will call the execute() method on the provided module.
	 * @param modules A collection of IEvlModules to use in combination with each set of test data.
	 */
	public static Collection<EvlRunConfiguration> getScenarios(List<String[]> testInputs, boolean includeTest, Collection<Supplier<? extends IEvlModule>> moduleGetters, Function<String[], Integer> idCalculator) {
		if (testInputs == null) testInputs = allInputs;
		if (moduleGetters == null) moduleGetters = modules();
		Collection<EvlRunConfiguration> scenarios = ErlAcceptanceTestUtil.getScenarios(EvlRunConfiguration.class, testInputs, moduleGetters, idCalculator);
		
		if (includeTest) {
			for (Supplier<? extends IEvlModule> moduleGetter : moduleGetters) {
				IEvlModule evlStd = moduleGetter.get();
				
				scenarios.add(
					new EvlRunConfiguration(
						EvlTests.getTestScript(evlStd).toPath(),
						null,
						EvlTests.getTestModel(false),
						Optional.of(false),
						Optional.of(false),
						Optional.of(evlStd),
						Optional.of(testInputs.size()+1),
						Optional.empty()
					)
				);
			}
		}
		
		return scenarios;
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
	
	//Boilerplate defaults
	public static List<String[]> addAllInputs(String[] scripts, String[] models, String metamodel) {
		return ErlAcceptanceTestUtil.addAllInputs(scripts, models, metamodel, "evl", scriptsRoot, modelsRoot, metamodelsRoot);
	}
	@SafeVarargs
	public static Collection<EvlRunConfiguration> getScenarios(boolean includeTest, Supplier<? extends IEvlModule>... moduleGetters) {
		return getScenarios(null, includeTest, Arrays.asList(moduleGetters), null);
	}
	@SafeVarargs
	public static Collection<EvlRunConfiguration> getScenarios(Supplier<? extends IEvlModule>... moduleGetters) {
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
