/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.IEvlModule;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;

/**
 * Data unit to be used as inputs in distributed processing. No additional
 * information over the base {@linkplain SerializableEvlAtom} is required.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlInputAtom extends SerializableEvlAtom {

	private static final long serialVersionUID = 4132510437749978354L;

	@Override
	protected SerializableEvlInputAtom clone() {
		return (SerializableEvlInputAtom) super.clone();
	}
	
	/**
	 * Deserializes this element, executes it and transforms the results into a collection of
	 * serialized {@linkplain UnsatisfiedConstraint}s.
	 * @param module
	 * @return
	 * @throws EolRuntimeException
	 */
	public Collection<SerializableEvlResultAtom> evaluate(IEvlModule module) throws EolRuntimeException {
		IEvlContext context = module.getContext();
		Object modelElement = findElement(context);
		
		if (modelElement == null) {
			throw new EolRuntimeException(
				"Could not find model element with ID "+modelElementID+
				(modelName != null && modelName.trim().length() > 0 ? 
					" in model "+modelName : ""
				)
				+" in context of "+contextName
			);
		}
		
		ConstraintContext constraintContext = module.getConstraintContext(contextName);
		if (!constraintContext.shouldBeChecked(modelElement, context)) {
			return Collections.emptyList();
		}
		
		Collection<Constraint> constraintsToCheck = constraintContext.getConstraints();
		Collection<SerializableEvlResultAtom> unsatisfied = new ArrayList<>(constraintsToCheck.size());
		
		for (Constraint constraint : constraintsToCheck) {
			serializeUnsatisfiedConstraintIfPresent(
				constraint.execute(modelElement, context)
			)
			.ifPresent(unsatisfied::add);
		}
		
		return unsatisfied;
	}
	
	protected Optional<SerializableEvlResultAtom> serializeUnsatisfiedConstraintIfPresent(Optional<UnsatisfiedConstraint> result) {
		return result.map(unsatisfiedConstraint -> {
			SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
			outputAtom.contextName = this.contextName;
			outputAtom.modelName = this.modelName;
			outputAtom.constraintName = unsatisfiedConstraint.getConstraint().getName();
			outputAtom.modelElementID = this.modelElementID;
			outputAtom.message = unsatisfiedConstraint.getMessage();
			return outputAtom;
		});
	}
	
	/**
	 * Splits the workload into a collection of model elements. The order can be randomised
	 * (shuffled) to ensure a balanced workload. Subclasses may override this method to
	 * define an optimal split based on static analysis.
	 * 
	 * @param constraintContexts
	 * @param context
	 * @param shuffle Whether to randomise the list order.
	 * @return The data to be distributed.
	 * @throws EolRuntimeException
	 */
	public static ArrayList<SerializableEvlInputAtom> createJobs(Iterable<ConstraintContext> constraintContexts, IEvlContext context, boolean shuffle) throws EolRuntimeException {
		ArrayList<SerializableEvlInputAtom> problems = new ArrayList<>();
		
		for (ConstraintContext constraintContext : constraintContexts) {
			EolModelElementType modelElementType = constraintContext.getType(context);
			IModel model = modelElementType.getModel();
			Collection<?> allOfKind = model.getAllOfKind(modelElementType.getTypeName());
			
			problems.ensureCapacity(problems.size()+allOfKind.size());
			
			for (Object modelElement : allOfKind) {
				SerializableEvlInputAtom problem = new SerializableEvlInputAtom();
				problem.modelElementID = model.getElementId(modelElement);
				problem.modelName = model.getName(); //modelElementType.getModelName();
				problem.contextName = constraintContext.getTypeName();
				problems.add(problem);
			}
		}
		
		if (shuffle)
			Collections.shuffle(problems);
		
		return problems;
	}
}
