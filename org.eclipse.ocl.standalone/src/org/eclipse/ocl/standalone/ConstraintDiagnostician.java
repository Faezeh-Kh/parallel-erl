package org.eclipse.ocl.standalone;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;
import org.eclipse.emf.common.util.BasicDiagnostic;
import org.eclipse.emf.common.util.Diagnostic;
import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EValidator;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.util.Diagnostician;
import org.eclipse.emf.ecore.util.EObjectValidator;
import org.eclipse.emf.edit.ui.EMFEditUIPlugin;
//import org.eclipse.epsilon.common.concurrent.ConcurrentCASList;
import org.eclipse.ocl.pivot.internal.labels.LabelSubstitutionLabelProvider;

//@see org.eclipse.ocl.examples.test.xtext.PivotDocumentationExamples.java
public class ConstraintDiagnostician extends Diagnostician {
	public final Resource resource;
	public final boolean isConcurrent;
	
	public ConstraintDiagnostician(Resource model) {
		this(model, false);
	}
	
	protected ConstraintDiagnostician(Resource model, boolean concurrent) {
		this.resource = model;
		this.isConcurrent = concurrent;
	}
	
	@Override
	public Map<Object, Object> createDefaultContext() {
		Map<Object, Object> context = super.createDefaultContext();
		context.put(
			EValidator.SubstitutionLabelProvider.class,
			new LabelSubstitutionLabelProvider()
		);
		if (isConcurrent) {
			context = new ConcurrentHashMap<>(context);
		}
		return context;
	}

	protected BasicDiagnostic createDefaultDiagnostic() {
		return new BasicDiagnostic(
			EObjectValidator.DIAGNOSTIC_SOURCE,
			0,
			EMFEditUIPlugin.INSTANCE.getString("_UI_DiagnosisOfNObjects_message", new String[]{"1"}),
			new Object[]{resource}
		) {
			@Override
			public void add(Diagnostic diagnostic) {
				/*if (isConcurrent && children == null) {
					children = new ConcurrentCASList<>();
			    }*/
				super.add(diagnostic);
			}
		};
	}
	
	@Override
	protected boolean doValidateContents(EObject eObject, DiagnosticChain diagnostics, Map<Object, Object> context) {
		return StreamSupport.stream(eObject.eContents().spliterator(), isConcurrent)
			.allMatch(eObj -> validate(eObj, diagnostics, context) || diagnostics != null);
	}
	
	public void validate(DiagnosticChain diagnostics, Map<Object, Object> context) {
		for (EObject eObject : resource.getContents()) {
			validate(eObject, diagnostics, context);
		}
	}
	
	public final Collection<UnsatisfiedOclConstraint> validate() {
		BasicDiagnostic diagnostics = createDefaultDiagnostic();
		Map<Object, Object> context = createDefaultContext();
		validate(diagnostics, context);
		return UnsatisfiedOclConstraint.getUnsatisfiedConstraintsFromDiagnostic(diagnostics);
	}
}