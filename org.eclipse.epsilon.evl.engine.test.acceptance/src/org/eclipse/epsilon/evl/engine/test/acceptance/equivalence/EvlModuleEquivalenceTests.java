package org.eclipse.epsilon.evl.engine.test.acceptance.equivalence;

import static org.junit.Assert.*;
import static org.eclipse.epsilon.evl.engine.test.acceptance.EvlAcceptanceTestUtil.*;
import static org.eclipse.epsilon.test.util.EpsilonTestUtil.*;
import org.eclipse.epsilon.evl.engine.launch.EvlRunConfiguration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.module.ModuleElement;
import org.eclipse.epsilon.eol.execute.context.Frame;
import org.eclipse.epsilon.eol.execute.context.FrameStack;
import org.eclipse.epsilon.eol.execute.operations.contributors.OperationContributor;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.*;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;
import org.eclipse.epsilon.evl.trace.ConstraintTrace;
import org.eclipse.epsilon.evl.trace.ConstraintTraceItem;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/*
 * A series of tests which use the standard EvlModule as an oracle and test the concurrent implementations
 * in various configurations (different number of threads) against it to ensure identical results
 * and behavioural equivalence. The tests are carried out in the context of scenarios. A scenario
 * is a given combination of script (EVL file) and model to execute the script on. Since each scenario
 * is independent, it requires its own IEvlModule. For this reason, each EvlRunConfiguration has an
 * identifier so that each scenario can be uniquely identified and different modules under the same
 * scenario can then be compared.
 * 
 * Regarding test ordering, only the testModuleCanExecute() method is required to be run before the others
 * (for obvious reasons). Note that since the expected configurations are our oracles, they are assumed
 * to pass and are exempt from testing; hence being executed in setUpBeforeClass().
 * 
 * @see EvlAcceptanceTestUtil
 * @author Sina Madani
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EvlModuleEquivalenceTests {
	
	//The oracle configurations
	static final Collection<? extends EvlRunConfiguration> expectedConfigs = getScenarios(EvlModule::new);
	
	//Used to identify which scenario to compare our results with.
	static final Map<Integer, IEvlModule> expectedModuleIDs = new HashMap<>(expectedConfigs.size());
	
	//The scenario and module combination under test. This is the parameterised test variable.
	final EvlRunConfiguration testConfig;
	
	//Convenience variables for the tests
	final IEvlModule expectedModule, actualModule;
	final IEvlContext expectedContext, actualContext;
	
	public EvlModuleEquivalenceTests(EvlRunConfiguration configUnderTest) {
		this.testConfig = configUnderTest;
		expectedModule = expectedModuleIDs.get(testConfig.getId());
		actualModule = testConfig.module;
		expectedContext = expectedModule.getContext();
		actualContext = actualModule.getContext();
	}
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {	
		for (EvlRunConfiguration expectedConfig : expectedConfigs) {
			expectedModuleIDs.put(expectedConfig.getId(), expectedConfig.module);
			expectedConfig.run();
		}
	}
	
	/*
	 * @return A collection of pre-configured run configurations, each with their own IEvlModule.
	 * @see EvlAcceptanceTestSuite.getScenarios
	 */
	@Parameters//(name = "0")	//Don't use this as the Eclipse JUnit view won't show failures!
	public static Iterable<EvlRunConfiguration> configurations() {
		//Used to specify which module configurations we'd like to test in our scenarios
		return getScenarios(
			allInputs,		//All scripts & models
			true,			//Include test.evl
			modules(false) 	//Exclude the standard EvlModule
		);
	}
	
	@Test //Must be run first as setup
	public void _test0ModuleCanExecute() {
		try {
			testConfig.run();
		}
		catch (Throwable ex) {
			fail(ex.getMessage());
		}
	}
	
	@Test	//Ensure we're not comparing different scenarios!
	public void _test1ScenarioMatches() {
		Function<IEvlModule, Collection<String>> modelCollector = module ->
		module.getContext().getModelRepository().getModels()
			.stream()
			.map(IModel::getName)
			.collect(Collectors.toList());
	
		assertEquals("Same script",
			expectedModule.getUri(),
			actualModule.getUri()
		);
			
		assertEquals("Same models",
			modelCollector.apply(expectedModule),
			modelCollector.apply(actualModule)
		);
	}
	
	@Test
	public void testUnsatisfiedConstraints() {
		Collection<UnsatisfiedConstraint>
			expectedUnsatisfiedConstraints = expectedContext.getUnsatisfiedConstraints(),
			actualUnsatisfiedConstraints = actualContext.getUnsatisfiedConstraints();
		
		boolean sizesDiffer = actualUnsatisfiedConstraints.size() != expectedUnsatisfiedConstraints.size();
		boolean contentsDiffer = !actualUnsatisfiedConstraints.containsAll(expectedUnsatisfiedConstraints);
		
		failIfDifferent(sizesDiffer || contentsDiffer, expectedUnsatisfiedConstraints, actualUnsatisfiedConstraints);
	}
	
	@Test
	public void testFrameStacks() {
		FrameStack
			expectedFrameStack = expectedContext.getFrameStack(),
			actualFrameStack = actualContext.getFrameStack();
		
		Function<FrameStack, List<String>> sameContents = fs ->
				fs.getFrames().stream()
					.map(Frame::getAll)
					.map(Map::keySet)
					.flatMap(Set::stream)
					.collect(Collectors.toList());	
		
		boolean sizesDiffer = expectedFrameStack.size(true) != actualFrameStack.size(true);
		boolean contentsDiffer = !sameContents.apply(expectedFrameStack).containsAll(sameContents.apply(actualFrameStack));
		
		failIfDifferent(sizesDiffer || contentsDiffer, expectedFrameStack, actualFrameStack);
	}
	
	@Test
	public void testExecutorFactories() {
		Function<IEvlContext, List<ModuleElement>> stackTraceGetter =
			context -> context.getExecutorFactory().getStackTraceManager().getStackTrace();
		
		assertEquals("Same stack traces",
			stackTraceGetter.apply(expectedContext),
			stackTraceGetter.apply(actualContext)
		);
	}
	
	@Test
	public void testOperationContributorRegistries() {
		Function<IEvlContext, Collection<OperationContributor>> contributors = md ->
			md.getOperationContributorRegistry().stream().collect(Collectors.toSet());
		
		Collection<OperationContributor>
			expectedOCs = contributors.apply(expectedContext),
			actualOCs = contributors.apply(actualContext);

		failIfDifferent(actualOCs.size() < expectedOCs.size(), expectedOCs, actualOCs);
	}
	
	@Test
	public void testConstraintTraces() {
		ConstraintTrace
			expectedConstraintTrace = expectedContext.getConstraintTrace(),
			actualConstraintTrace = actualContext.getConstraintTrace();
		
		//Uses Set instead of List for performance reasons when calling containsAll.
		Function<ConstraintTrace, Collection<ConstraintTraceItem>> ctContents = ct ->
			ct.stream().collect(Collectors.toSet());
		
		boolean sizesDiffer = actualConstraintTrace.stream().count() != expectedConstraintTrace.stream().count();
		boolean contentsDiffer = !ctContents.apply(actualConstraintTrace).containsAll(ctContents.apply(expectedConstraintTrace));
		
		failIfDifferent(sizesDiffer || contentsDiffer, expectedConstraintTrace, actualConstraintTrace);
	}
	
	@Test
	public void testConstraintsDependedOn() {
		Set<Constraint>
			expectedDependents = expectedContext.getConstraintsDependedOn(),
			actualDependents = actualContext.getConstraintsDependedOn();
		
		assertEquals("Same ConstraintsDependedOn", expectedDependents, actualDependents);
	}
}
