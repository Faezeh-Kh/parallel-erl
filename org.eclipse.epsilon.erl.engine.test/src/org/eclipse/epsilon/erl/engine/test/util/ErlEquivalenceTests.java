package org.eclipse.epsilon.erl.engine.test.util;

import static org.eclipse.epsilon.test.util.EpsilonTestUtil.failIfDifferent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.module.ModuleElement;
import org.eclipse.epsilon.eol.execute.context.Frame;
import org.eclipse.epsilon.eol.execute.operations.contributors.OperationContributor;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.erl.IErlModule;
import org.eclipse.epsilon.erl.engine.launch.ErlRunConfiguration;
import org.eclipse.epsilon.test.util.EpsilonTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;

/*
 * A series of tests which use the standard ErlModule as an oracle and test the concurrent implementations
 * in various configurations (different number of threads) against it to ensure identical results
 * and behavioural equivalence. The tests are carried out in the context of scenarios. A scenario
 * is a given combination of script (ERL file) and model to execute the script on. Since each scenario
 * is independent, it requires its own IErlModule. For this reason, each EvlRunConfiguration has an
 * identifier so that each scenario can be uniquely identified and different modules under the same
 * scenario can then be compared.
 * 
 * Regarding test ordering, only the testModuleCanExecute() method is required to be run before the others
 * (for obvious reasons). Note that since the expected configurations are our oracles, they are assumed
 * to pass and are exempt from testing; hence being executed in setUpBeforeClass().
 * 
 * This test class is intended to be extended by tests for extensions of ERL. For a reference
 * implementation/example, please see EvlModuleEquivalenceTests.
 * A basic implementation would need to provide the following:
 * 
 * - A constructor which calls super(C configUnderTest)
 * 
 * - A setUpBeforeClass static method (annotated with @BeforeClass) which assigns
 * 	 expectedConfigs and subsequently calls setUpEquivalenceTest()
 * 
 * - A static method returning an Iterable<C> annotated with @Parameters.
 * 
 * - A @Test method whose name begins with a very low character value (e.g. _test0)
 *   which simply calls the {@linkplain #beforeTests()} method
 *   
 * - The subclass should be annotated with @FixMethodOrder(MethodSorters.NAME_ASCENDING)
 *   so that the aforementioned method which calls beforeTests() is run first.
 * 
 * The complexity of this design is a result of the deficiencies of JUnit. For example, it would
 * be much easier to use a non-static @BeforeAll or even @Before with a flag for running the setup
 * but this doesn't seem to work.
 * 
 * @see ErlAcceptanceTestUtil
 * @author Sina Madani
 */
@RunWith(org.junit.runners.Parameterized.class)
public abstract class ErlEquivalenceTests<M extends IErlModule, C extends ErlRunConfiguration<M>> {

	//The oracle configurations
	protected static Collection<? extends ErlRunConfiguration<? extends IErlModule>> expectedConfigs;
	
	//Used to identify which scenario to compare our results with.
	protected static Map<Integer, IErlModule> expectedModuleIDs;
	
	//The scenario and module combination under test. This is the parameterised test variable.
	protected final C testConfig;
	
	//Convenience variables for the tests
	protected final M expectedModule, actualModule;
	
	@SuppressWarnings("unchecked")
	public ErlEquivalenceTests(C configUnderTest) {
		this.testConfig = configUnderTest;
		expectedModule = (M) expectedModuleIDs.get(testConfig.getId());
		actualModule = testConfig.module;
	}
	
	/*
	 * This should be called after assigning expectedConfigs in
	 * setUpBeforeClass().
	 */
	protected static void setUpEquivalenceTest() {
		expectedModuleIDs = new HashMap<>(expectedConfigs.size());
		
		for (ErlRunConfiguration<?> expectedConfig : expectedConfigs) {
			expectedModuleIDs.put(expectedConfig.getId(), expectedConfig.module);
			expectedConfig.run();
		}
	}
	
	/*
	 * Pre-requisite for testing. This should be called as the first test!
	 */
	protected void beforeTests() {
		testModuleCanExecute();
		testScenariosMatch();
		assert expectedModule != actualModule;
	}
	
	protected void testModuleCanExecute() {
		try {
			testConfig.run();
		}
		catch (Throwable ex) {
			fail(ex.getMessage());
		}
	}
	
	protected void testScenariosMatch() {
		Function<M, Collection<String>> modelCollector = module -> module
			.getContext().getModelRepository().getModels()
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
	public void testFrameStacks() {
		Function<M, List<String>> fsMapper = module -> module
			.getContext().getFrameStack()
			.getFrames().stream()
			.map(Frame::getAll)
			.map(Map::keySet)
			.flatMap(Set::stream)
			.collect(Collectors.toList());	
		
		EpsilonTestUtil.testCollectionsHaveSameElements(
			fsMapper.apply(expectedModule),
			fsMapper.apply(actualModule)
		);
	}
	
	@Test
	public void testExecutorFactories() {
		Function<M, List<ModuleElement>> stackTraceGetter =
			module -> module.getContext().getExecutorFactory().getStackTraceManager().getStackTrace();
		
		assertEquals("Same stack traces",
			stackTraceGetter.apply(expectedModule),
			stackTraceGetter.apply(actualModule)
		);
	}
	
	@Test
	public void testOperationContributorRegistries() {
		Function<M, Collection<OperationContributor>> contributors = module ->
			module.getContext().getOperationContributorRegistry().stream().collect(Collectors.toSet());
	
		Collection<OperationContributor>
			expectedOCs = contributors.apply(expectedModule),
			actualOCs = contributors.apply(actualModule);

		failIfDifferent(actualOCs.size() < expectedOCs.size(), expectedOCs, actualOCs);
	}
	
}
