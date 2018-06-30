package org.eclipse.ocl.standalone;

import java.nio.file.Path;
import java.util.*;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EValidator;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.epsilon.common.launch.ProfilableRunConfiguration;
import org.eclipse.ocl.pivot.utilities.OCL;
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
 * For a command-line interface and initialisation utilities,
 * @see {@link StandaloneOCLBuilder}
 * 
 * @author Sina Madani
 */
public class StandaloneOCL extends ProfilableRunConfiguration<Collection<UnsatisfiedOclConstraint>> {
	
	protected Collection<UnsatisfiedOclConstraint> unsatisfiedConstraints;
	protected OCL ocl = OCL.newInstance();
	protected ConstraintDiagnostician diagnostician;
	protected EPackage metamodelPackage;
	protected EValidator validator;
	public final URI model, metamodel;
	
	public StandaloneOCL(
		Path oclScript,
		URI modelUri,
		URI metamodelUri,
		Optional<Boolean> showUnsatisfied,
		Optional<Boolean> profileExecution,
		Optional<Integer> configID,
		Optional<Path> scratchFile) {
			super(oclScript, showUnsatisfied, profileExecution, configID, scratchFile);
			this.model = modelUri;
			this.metamodel = metamodelUri;
			this.id = configID.orElseGet(() ->
				Objects.hash(super.id,
					Objects.toString(ocl),
					Objects.toString(modelUri),
					Objects.toString(metamodelUri)
				)
			);
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
		
		Resource resource = resourceSet.createResource(model);
		resource.load(Collections.EMPTY_MAP);
		return resource;
	}
	
	protected void registerValidator() {
		if (validator == null) {
			org.eclipse.ocl.pivot.model.OCLstdlib.install();
			if (script != null) {
				org.eclipse.ocl.xtext.completeocl.CompleteOCLStandaloneSetup.doSetup();
				validator = new CompleteOCLEObjectValidator(
					metamodelPackage,
					URI.createURI(script.toUri().toString()),
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
	
	@Override
	protected final void preExecute() throws Exception {
		super.preExecute();
		Resource modelResource = registerAndLoadModel();
		registerValidator();
		diagnostician = createDiagnostician(modelResource);
		Objects.requireNonNull(diagnostician, "Diagnostician must be set!");
	}
	
	@Override
	protected final Collection<UnsatisfiedOclConstraint> execute() {
		return unsatisfiedConstraints = diagnostician.validate();
	}
	
	@Override
	protected void postExecute() throws Exception {
		super.postExecute();
		
		if (profileExecution || showResults) {
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
	
	public Collection<UnsatisfiedOclConstraint> getUnsatisfiedConstraints() {
		return unsatisfiedConstraints;
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
		this.unsatisfiedConstraints = other.unsatisfiedConstraints;
	}
}
