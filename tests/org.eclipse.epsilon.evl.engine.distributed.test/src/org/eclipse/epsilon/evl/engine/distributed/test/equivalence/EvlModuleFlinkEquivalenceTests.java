/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.engine.distributed.test.equivalence;

import java.util.Collection;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.flink.atomic.EvlModuleFlinkAtoms;
import org.eclipse.epsilon.evl.distributed.flink.batch.EvlModuleFlinkSubset;
import org.eclipse.epsilon.evl.engine.test.acceptance.EvlAcceptanceTestUtil;
import org.eclipse.epsilon.evl.engine.test.acceptance.equivalence.EvlModuleEquivalenceTests;
import org.eclipse.epsilon.evl.launch.EvlRunConfiguration;
import org.junit.runners.Parameterized.Parameters;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleFlinkEquivalenceTests extends EvlModuleEquivalenceTests {

	public EvlModuleFlinkEquivalenceTests(EvlRunConfiguration configUnderTest) {
		super(configUnderTest);
	}
	
	@Parameters//(name = "0")	Don't use this as the Eclipse JUnit view won't show failures!
	public static Collection<EvlRunConfiguration> configurations() {
		return EvlAcceptanceTestUtil.getScenarios(
			() -> new EvlModuleFlinkAtoms(4),
			() -> new EvlModuleFlinkSubset(4)
		);
	}
	
	@Override
	public void _test0() {
		((EvlModuleDistributedMaster) testConfig.getModule())
			.getContext()
			.setModelProperties(testConfig.modelsAndProperties.values());
		
		super._test0();
	}
	
	@Override
	public void testFrameStacks() {
		return;	// We don't merge variables to the master (yet!)
	}
	
	@Override
	public void testConstraintTraces() {
		return;	// We don't merge the trace in distributed mode.
	}
	
	@Override
	public void testConstraintsDependedOn() {
		return;	// We don't merge this to the master in distributed mode.
	}
}
