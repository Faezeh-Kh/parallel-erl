/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.epl.engine.test.acceptance.equivalence;

import static org.eclipse.epsilon.epl.engine.test.acceptance.EplAcceptanceTestUtil.*;
import java.util.Collections;
import org.eclipse.epsilon.eol.engine.test.acceptance.util.EolEquivalenceTests;
import org.eclipse.epsilon.epl.*;
import org.eclipse.epsilon.epl.launch.EplRunConfiguration;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized.Parameters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EplModuleEquivalenceTests extends EolEquivalenceTests<EplRunConfiguration> {
	
	public EplModuleEquivalenceTests(EplRunConfiguration configUnderTest) {
		super(configUnderTest);
	}
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		expectedConfigs = getScenarios(allInputs, true, Collections.singleton(DynamicEplModule::new));
		setUpEquivalenceTest();
	}
	
	/**
	 * @return A collection of pre-configured run configurations, each with their own EpllModule.
	 * @see EvlAcceptanceTestSuite.getScenarios
	 */
	@Parameters(name = "0")	// Don't use this as the Eclipse JUnit view won't show failures!
	public static Iterable<EplRunConfiguration> configurations() {
		// Used to specify which module configurations we'd like to test in our scenarios
		return getScenarios(
			allInputs,		// All scripts & models
			true,			// Include test.epl
			modules(false) 	// Exclude the standard EplModule
		);
	}
	
	@Test
	public void _test0() {
		//super.beforeTests();
	}
	/*
	@Test
	public void testPatternMatchModel() {
		IModel
			expectedModel = (IModel) expectedConfig.getResult(),
			actualModel = (IModel) testConfig.getResult();
		//TODO: implement
		assertEquals(expectedModel.allContents().size(), actualModel.allContents().size());
	}*/
}
