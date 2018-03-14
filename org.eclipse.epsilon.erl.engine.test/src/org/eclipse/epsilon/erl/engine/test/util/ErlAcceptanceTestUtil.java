package org.eclipse.epsilon.erl.engine.test.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.epsilon.common.concurrent.ConcurrencyUtils;
import org.eclipse.epsilon.common.util.StringProperties;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.erl.engine.launch.ErlRunConfiguration;
import org.eclipse.epsilon.erl.IErlModule;

public class ErlAcceptanceTestUtil {
	private ErlAcceptanceTestUtil() {}
	
	public static final int[] THREADS = new int[] {
		0, 1, 2, 3, 4,
		(ConcurrencyUtils.DEFAULT_PARALLELISM/2)+1,
		(ConcurrencyUtils.DEFAULT_PARALLELISM*2)-1,
		//0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		//Short.MAX_VALUE/8
	};
	
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
	 * @param modules A collection of IErlModules to use in combination with each set of test data.
	 */
	public static <M extends IErlModule, C extends ErlRunConfiguration<M>> Collection<C> getScenarios(
		Class<C> clazz,
		List<String[]> testInputs,
		Collection<Supplier<? extends M>> moduleGetters,
		Function<String[], Integer> idCalculator) {
			if (idCalculator == null) idCalculator = ErlAcceptanceTestUtil::getScenarioID;
			
			List<C> scenarios = new ArrayList<>(moduleGetters.size()*(testInputs.size()+2));
			boolean showUnsatisfied = false, profileExecution = false;
			
			for (String[] testInput : testInputs) {
				Path erlScript = Paths.get(testInput[0]);
				
				StringProperties testProperties = ErlRunConfiguration.makeProperties(
					testInput[1],				// Model path
					testInput[2],				// Metamodel path
					true,						// Cache model
					true						// Store on disposal
				);
				
				IModel model = ErlRunConfiguration.getIModelFromPath(testInput[2]);
				
				for (Supplier<? extends M> moduleGetter : moduleGetters) {
					scenarios.add(ErlRunConfiguration.instantiate(
							clazz,										// The ErlRunConfiguration subclass
							erlScript,									// Path to the script to run
							testProperties,								// Model and metamodel paths
							model,										// Model object to use
							Optional.of(showUnsatisfied),				// Whether to show results
							Optional.of(profileExecution),				// Whether to measure execution time
							Optional.of(moduleGetter.get()),			// IErlModule
							Optional.of(idCalculator.apply(testInput)),	// Unique identifier for this configuration
							Optional.empty()							// Output file
						)
					);
				}
			}
			
			return scenarios;
	}
	
	public static <M extends IErlModule> Collection<? extends M> unwrapModules(Collection<Supplier<? extends M>> moduleGetters) {
		return moduleGetters.stream().map(Supplier::get).collect(Collectors.toList());
	}
}