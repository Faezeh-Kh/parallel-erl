/*********************************************************************
 * Copyright (c) 2017 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.engine.test.ocl.equivalence;

import static org.junit.Assert.*;
import static org.eclipse.epsilon.evl.engine.test.acceptance.EvlAcceptanceTestUtil.*;
import static org.eclipse.epsilon.common.util.FileUtil.getFileName;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.epsilon.evl.launch.EvlRunConfiguration;
import org.eclipse.ocl.standalone.*;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Evaluates the correctness of EvlModule implementations against
 * Eclipse OCL by comparing their unsatisfied constraints for given
 * model and script combinations (same approach as EvlModuleEquivalenceTests).
 * 
 * The test assumes that the scripts are identical in their semantics,
 * with the same filename (only their extensions being different),
 * and that the models are EMF-based (Ecore, XMI).
 * 
 * The equivalence of unsatisfied constraints is based on the constraint names
 * and element (EObject) combination. This structure is stored in
 * StandaloneOCL.UnsatisfiedOclConstraint.
 * 
 * @see org.eclipse.epsilon.evl.engine.test.acceptance.equivalence.StandaloneOCL
 * @author Sina Madani
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EvlOclTests {
	
	static final String CWD = getTestBaseDir(EvlOclTests.class);
	
	static final List<String[]>
		oclTestInputs = addAllInputs(
			// Exclude the java_noguard script
			javaScripts, new String[]{"ocl_java_codegen.xmi"}, javaMetamodel, "ocl", CWD+"scripts/", CWD+"models/", metamodelsRoot
		),
		evlTestInputs = addAllInputs(
			javaScripts, new String[]{"ocl_java_codegen.xmi"}, javaMetamodel, "evl", scriptsRoot, CWD+"models/", metamodelsRoot
		);
	
	// This is to ensure that the scenarios have same ID because the paths (and file extension for scripts) are different.
	static final Function<String[], Integer> idCalculator = uris -> {
		String scriptNoExt = getFileName(uris[0], false);
		return Objects.hash(scriptNoExt, uris[1], uris[2]);
	};
	
	// The oracle scenario.
	final StandaloneOCL expectedConfig;
	
	// The scenario and module combination under test. This is the parameterised test variable.
	final EvlRunConfiguration actualConfig;
	
	// Used to identify which scenario to compare our results with.
	static final Map<Integer, StandaloneOCL> expectedConfigIDs = new HashMap<>();
	
	// The oracle configurations
	static final Collection<StandaloneOCL> expectedConfigs = getStandaloneOCLConfigsFrom(oclTestInputs);
	
	public EvlOclTests(EvlRunConfiguration configUnderTest) {
		this.actualConfig = configUnderTest;
		expectedConfig = expectedConfigIDs.get(actualConfig.getId());
	}
	
	private static List<StandaloneOCL> getStandaloneOCLConfigsFrom(Collection<String[]> uriss) {
		return uriss
			.stream()
			.map(uris -> new StandaloneOCLBuilder()
				.withId(idCalculator.apply(uris))
				.withURIs(uris)
				.build()
			)
			.collect(Collectors.toList());
	}
	
	@Parameters(name = "{0}")
	public static Iterable<? extends Object> configurations() {
		return getScenarios(
			evlTestInputs,
			false,
			// Original module should be sufficient, since we already do equivalence testing
			// for other implementations in EvlModuleEquivalenceTests. Nevertheless, it's
			// worth running everything as it doesn't take long.
			modules(),
			idCalculator
		);
	}
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		for (StandaloneOCL expectedConfig : expectedConfigs) {
			expectedConfigIDs.put(expectedConfig.getId(), expectedConfig);
			expectedConfig.run();
		}
	}
	
	@Test	// Must be run first as setup
	public void _test0ModuleCanExecute() {
		try {
			actualConfig.run();
		}
		catch (Throwable ex) {
			fail(ex.getMessage());
		}
	}
	
	@Test	// Ensure we're not comparing different scenarios!
	public void _test1ScenarioMatches() {
		assertEquals("Same IDs", expectedConfig.getId(), actualConfig.getId());
	}
	
	@Test
	public void testConstraints() {
		Collection<UnsatisfiedOclConstraint>
			expectedConstraints = expectedConfig.getResult(),
			
			actualConstraints = actualConfig
				.getModule()
				.getContext()
				.getUnsatisfiedConstraints()
				.stream()
				.map(uc -> new UnsatisfiedOclConstraint(uc.getConstraint().getName(), (EObject)uc.getInstance()))
				.collect(Collectors.toList());
		
		assertEquals("Same number of unsatisfied constraints", expectedConstraints.size(), actualConstraints.size());
		
		assertTrue("Effectively same unsatisfied constraints", actualConstraints.containsAll(expectedConstraints));
	}
}
