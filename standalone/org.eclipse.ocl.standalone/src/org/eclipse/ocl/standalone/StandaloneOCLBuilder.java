package org.eclipse.ocl.standalone;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EValidator;
import org.eclipse.epsilon.common.cli.ConfigParser;

public class StandaloneOCLBuilder {

	public static void main(String[] args) throws Exception {
		try {
			newInstance(args).run();
		}
		catch (IllegalArgumentException iax) {
			System.err.println(iax.getMessage());
		}
	}
	
	public static StandaloneOCL newTestInstance(String[] uris, int configID) throws IllegalArgumentException {
		return newInstance(uris[0], uris[1], uris[2], Optional.of(false), Optional.of(false), Optional.of(configID), Optional.empty());
	}
	
	public static StandaloneOCL newInstance(
		String oclPath,
		String modelPath,
		String metamodelPath,
		Optional<Boolean> showUnsatisfied,
		Optional<Boolean> showExecTime,
		Optional<Integer> configID,
		Optional<Path> scratchFile) throws IllegalArgumentException {
			try {
				URI
					modelUri = URI.createFileURI(modelPath),
					metamodelUri = metamodelPath != null && metamodelPath.length() > 4 ? URI.createFileURI(metamodelPath) : null;
				Path oclDoc = oclPath != null && oclPath.length() > 4 ? Paths.get(oclPath) : null;
				
				return new StandaloneOCL(oclDoc, modelUri, metamodelUri, showUnsatisfied, showExecTime, configID, scratchFile);
			}
			catch (IllegalArgumentException iax) {
				throw new IllegalArgumentException("Invalid path: "+iax.getMessage());
			}
	}
	
	public static StandaloneOCL newInstance(String... args) throws IllegalArgumentException {
		return newInstance(true, args);
	}
	
	public static StandaloneOCL newInstance(boolean checkArgs, String... args) throws IllegalArgumentException {
		ConfigParser<StandaloneOCL> parser = new ConfigParser<>() {
			{
				requiredUsage = "Must provide absolute path to "+nL
				  + "  [Complete OCL Document] (if metamodel doesn't contain constraints) "+nL
				  + "  [XMI model file] "+nL
				  + "  [Ecore metamodel file] ";
			}
			@Override
			protected void parseArgs(String[] args) throws Exception {
				if (checkArgs && args.length < 3) {
					throw new IllegalArgumentException();
				}
				super.parseArgs(args);
			}
		};
		
		parser.accept(args);
		
		return newInstance(
			args[0],
			args[1],
			args[2],
			parser.showResults,
			parser.profileExecution,
			parser.id,
			parser.outputFile
		);
	}
	
	public static StandaloneOCL newCompiledInstance(EPackage rootPackage, EValidator customValidator, String... args) {
		if (args.length == 0 ||
			(args.length == 1 && (args[0].length() < 5 || !args[0].endsWith(".xmi"))) ||
			(args.length >= 3 && args[1].length() < 5)
		) {
			throw new IllegalArgumentException("Must provide absolute path to EMF model!");
		}
		if (args.length == 1)
			return compiledOCL(rootPackage, customValidator, URI.createFileURI(args[0]));
		else
			return compiledOCL(newInstance(false, args), rootPackage, customValidator);
	}
	
	public static StandaloneOCL compiledOCL(EPackage rootPackage, EValidator customValidator, URI model) {
		return compiledOCL(new StandaloneOCL(null, model, null, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()), rootPackage, customValidator);
	}
	
	public static StandaloneOCL compiledOCL(StandaloneOCL cocl, EPackage rootPackage, EValidator customValidator) {
		cocl.metamodelPackage = rootPackage;
		cocl.validator = customValidator;
		return cocl;
	}
	
}
