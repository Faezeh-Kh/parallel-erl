/*********************************************************************
 * Copyright (c) 2017-2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.ocl.standalone;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EValidator;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.epsilon.common.launch.ProfilableRunConfiguration;
import static org.eclipse.epsilon.common.util.profiling.BenchmarkUtils.profileExecutionStage;
import static org.eclipse.emf.common.util.URI.createURI;
import org.eclipse.ocl.pivot.ExpressionInOCL;
import org.eclipse.ocl.pivot.resource.ASResource;
import org.eclipse.ocl.pivot.utilities.OCL;
import org.eclipse.ocl.pivot.utilities.ParserException;
import org.eclipse.ocl.xtext.completeocl.validation.CompleteOCLEObjectValidator;
import org.eclipse.ocl.xtext.oclinecore.validation.OCLinEcoreEObjectValidator;

/**
 * A way to run Eclipse OCL (Pivot) without Eclipse.
 * This class essentially makes OCL behave like EVL,
 * allowing you to provide an EMF model, metamodel
 * and Complete OCL document for model validation,
 * rather than having to embed the constraints in an
 * existing metamodel. It allows for programmatically
 * running constraints from an OCL file (aka a
 * "Complete OCL document" against a model.
 * <br/>
 * Alternatively, embedded constraints in the metamodel
 * via OCLinEcore are also supported.
 * <br/>
 * The {@link ConstraintDiagnostician} provides
 * a convenient extension point for optimising execution. In
 * particular, concurrent validation is supported by the
 * ConstraintDiagnostician but may not be by the underlying
 * {@linkplain EValidator}.
 * <br/>
 * In addition, an OCL query can be executed if there exists
 * a no-args operation called QUERY defined in the context of
 * a type which has at least one model element preset.
 * Doing so will skip validation, so it is important to not
 * have such an operation if validation is also desired.
 * <br/>
 * For a command-line interface and initialisation utilities,
 * @see {@link StandaloneOCLBuilder}
 * 
 * @author Sina Madani
 */
public class StandaloneOCL extends ProfilableRunConfiguration {
	
	protected OCL ocl = OCL.newInstance();
	protected ConstraintDiagnostician diagnostician;
	protected EPackage metamodelPackage;
	protected EValidator validator;
	protected Resource modelResource;
	public final URI model, metamodel;
	
	public StandaloneOCL(StandaloneOCLBuilder builder) {
		super(builder);
		this.model = builder.modelUri;
		this.metamodel = builder.metamodelUri;
		this.id = Optional.ofNullable(builder.id).orElseGet(() ->
			Objects.hash(super.id,
				Objects.toString(ocl),
				Objects.toString(model),
				Objects.toString(metamodel)
			)
		);
		this.metamodelPackage = builder.rootPackage;
		this.validator = builder.customValidator;
	}
	

	protected Resource registerAndLoadModel() throws Exception {
		ResourceSet resourceSet = ocl.getResourceSet();
		
		if (metamodelPackage == null) {
			Resource metamodelResource = resourceSet.createResource(metamodel);
			metamodelResource.load(Collections.EMPTY_MAP);
			metamodelPackage = metamodelResource.getContents()
				.stream()
				.filter(eObject -> eObject instanceof EPackage)
				.map(EPackage.class::cast)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
					"Metamodel '"+metamodel.path()+"' does not contain an EPackage!")
				);
		}
		resourceSet.getPackageRegistry().put(metamodelPackage.getNsURI(), metamodelPackage);
		
		Resource modelResource = resourceSet.createResource(model);
		modelResource.load(Collections.EMPTY_MAP);
		return modelResource;
	}
	
	protected void registerValidator() {
		if (validator == null) {
			org.eclipse.ocl.pivot.model.OCLstdlib.install();
			if (script != null) {
				org.eclipse.ocl.xtext.completeocl.CompleteOCLStandaloneSetup.doSetup();
				validator = new CompleteOCLEObjectValidator(
					metamodelPackage,
					createURI(script.toUri().toString()),
					ocl.getEnvironmentFactory()
				);
			}
			else {
				org.eclipse.ocl.xtext.oclinecore.OCLinEcoreStandaloneSetup.doSetup();
				validator = new OCLinEcoreEObjectValidator();
			}
		}
		EValidator.Registry.INSTANCE.put(metamodelPackage, validator);
	}
	
	protected ConstraintDiagnostician createDiagnostician(final Resource modelImpl) {
		return new ConstraintDiagnostician(modelImpl);
	}
	
	protected Optional<Supplier<?>> checkForQuery(ASResource scriptResource) throws ParserException {
		final Function<EObject, Stream<EObject>> flatMapper = e -> e.eContents().stream();
		org.eclipse.ocl.pivot.Operation queryOp = scriptResource
			.getContents().stream()
			.flatMap(flatMapper)
			.filter(e -> e instanceof org.eclipse.ocl.pivot.Package)
			.flatMap(flatMapper)
			.filter(e -> e instanceof org.eclipse.ocl.pivot.Class)
			.flatMap(flatMapper)
			.filter(e -> e instanceof org.eclipse.ocl.pivot.Operation)
			.map(org.eclipse.ocl.pivot.Operation.class::cast)
			.filter(op -> "QUERY".equals(op.getName()))
			.findAny().orElse(null);
		
		if (queryOp != null) {
			String fullyQualifiedType = queryOp.eContainer().toString();
			int pkgIndex = fullyQualifiedType.indexOf("::");
			String typeName = fullyQualifiedType.substring(pkgIndex+2);
			
			EClassifier targetType = metamodelPackage.getEClassifiers()
				.stream().filter(e -> e.getName().equals(typeName)).findAny()
				.orElseThrow(() -> new IllegalStateException("Could not find type "+fullyQualifiedType+" in "+metamodel));
			
			EObject contextElement = modelResource.getContents()
				.stream().filter(targetType::isInstance).findAny()
				.orElseThrow(() -> new IllegalStateException("Could not find a model element of type "+fullyQualifiedType+" in "+model));
			
			ExpressionInOCL asQuery = ocl.createQuery(contextElement.eClass(), queryOp.getBodyExpression().getBody());
			return Optional.of(() -> ocl.evaluate(contextElement, asQuery));
		}
		else return Optional.empty();
	}
	
	@Override
	protected final void preExecute() throws Exception {
		super.preExecute();
		
		modelResource = profileExecution ?
			profileExecutionStage(profiledStages, "Prepare model", this::registerAndLoadModel) :
			registerAndLoadModel();
		
		Runnable validatorPreparation = () -> {
			registerValidator();
			diagnostician = createDiagnostician(modelResource);
			Objects.requireNonNull(diagnostician, "Diagnostician must be set!");
		};
		
		if (profileExecution) {
			profileExecutionStage(profiledStages, "Prepare validator", validatorPreparation);
		}
		else {
			validatorPreparation.run();
		}
	}
	
	@Override
	protected final Object execute() throws ParserException {
		final ASResource scriptResource = profileExecution ?
			profileExecutionStage(profiledStages, "Parse script", () -> ocl.parse(createURI(script.toUri().toString()))) :
			ocl.parse(createURI(script.toUri().toString()));
		
		return result = checkForQuery(scriptResource)
			.map(evaluator -> profileExecution ?
				profileExecutionStage(profiledStages, "execute query", evaluator) :
				evaluator.get()
			)
			.orElseGet(() -> profileExecution ?
				profileExecutionStage(profiledStages, "validate",
					(java.util.function.Supplier<Collection<UnsatisfiedOclConstraint>>) diagnostician::validate
				) :
				diagnostician.validate()
			);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void postExecute() throws Exception {
		if (profileExecution) {
			profileExecutionStage(profiledStages, "dispose", ocl::dispose);
		}
		else {
			ocl.dispose();
		}
		
		super.postExecute();
		
		if (result instanceof Collection && (profileExecution || showResults)) {
			Collection<UnsatisfiedOclConstraint> unsatisfiedConstraints = (Collection<UnsatisfiedOclConstraint>) result;
			
			if (unsatisfiedConstraints.isEmpty()) {
				writeOut("All constraints satisfied.");
			}
			else {
				writeOut(unsatisfiedConstraints.size() + " unsatisfied constraints"+(showResults ? ':' : '.'));
				if (showResults) {
					writeOut(UnsatisfiedOclConstraint.sortUnsatisfiedConstraintsBySize(unsatisfiedConstraints).entrySet());
				}
			}
			writeOut(printMarker);
		}
	}
	
	/**
	 * Copy constructor.
	 */
	public StandaloneOCL(StandaloneOCL other) {
		super(other);
		this.model = other.model;
		this.metamodel = other.metamodel;
		this.ocl = other.ocl;
		this.validator = other.validator;
		this.metamodelPackage = other.metamodelPackage;
		this.result = other.result;
	}
}
