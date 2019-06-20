/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.epl.engine.test.acceptance;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import static org.eclipse.epsilon.eol.engine.test.acceptance.util.EolAcceptanceTestUtil.*;
import org.eclipse.epsilon.common.util.CollectionUtil;
import org.eclipse.epsilon.eol.engine.test.acceptance.util.EolAcceptanceTestUtil;
import org.eclipse.epsilon.eol.launch.IEolRunConfiguration;
import org.eclipse.epsilon.epl.*;
import org.eclipse.epsilon.epl.launch.EplRunConfiguration;

public class EplAcceptanceTestUtil {
	private EplAcceptanceTestUtil() {}
	
	public static final String
		//Core
		testsBase = getTestBaseDir(EplAcceptanceTestSuite.class),
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
		javaScripts[] = {"java_findbugs"};
	
	/*Nx3 array where N is number of test inputs;
	 *  0 is the script path,
	 *  1 is the model path,
	 *  2 is the metamodel path.
	 */
	public static final List<String[]>
		allInputs,
		//imdbInputs,
		javaInputs;
	
	static {
		javaInputs = addAllInputs(javaScripts, javaModels, javaMetamodel);
		//imdbInputs = addAllInputs(imdbScripts, imdbModels, imdbMetamodel);
		
		allInputs = CollectionUtil.composeArrayListFrom(
			//imdbInputs,
			javaInputs
		);
	}
	
	public static Collection<Supplier<? extends IEplModule>> modules(boolean includeStandard) {
		return parallelModules(THREADS,
			includeStandard ? DynamicEplModule::new : null/*,
			ForkJoinEplModule::new*/
		);
	}
	
	public static Collection<EplRunConfiguration> getScenarios(
		List<String[]> testInputs,
		boolean includeTest,
		Collection<Supplier<? extends IEplModule>> moduleGetters
		) {
			Collection<EplRunConfiguration> scenarios = EolAcceptanceTestUtil
				.getScenarios(EplRunConfiguration.class, testInputs, moduleGetters, null);
			
			if (includeTest) {
				for (Supplier<? extends IEplModule> moduleGetter : moduleGetters) {
					IEplModule eplStd = moduleGetter.get();
					
					scenarios.add(IEolRunConfiguration.Builder(EplRunConfiguration.class)
						.withScript(EplTests.getTestScript(eplStd))
						.withModel(EplTests.TEST_MODEL)
						.withModule(eplStd)
						.withId(testInputs.size()+1)
						.build()
					);
				}
			}
			
			return scenarios;
	}
	
	public static List<String[]> addAllInputs(String[] scripts, String[] models, String metamodel) {
		return EolAcceptanceTestUtil.addAllInputs(scripts, models, metamodel, "epl", scriptsRoot, modelsRoot, metamodelsRoot);
	}
}
