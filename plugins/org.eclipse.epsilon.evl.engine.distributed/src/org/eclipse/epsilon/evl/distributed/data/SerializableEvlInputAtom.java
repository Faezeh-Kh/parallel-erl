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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.IEvlModule;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;
import org.eclipse.epsilon.evl.execute.context.concurrent.IEvlContextParallel;

/**
 * Data unit to be used as inputs in distributed processing. No additional
 * information over the base {@linkplain SerializableEvlAtom} is required.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlInputAtom extends SerializableEvlAtom {

	private static final long serialVersionUID = -3214236078336249582L;

	@Override
	protected SerializableEvlInputAtom clone() {
		return (SerializableEvlInputAtom) super.clone();
	}
	
	/**
	 * Deserializes this element, executes it and transforms the results into a collection of
	 * serialized {@linkplain UnsatisfiedConstraint}s.
	 * 
	 * @param module The IEvlModule to use for resolution and its context for execution. Note
	 * that if the module is an instanceof {@linkplain EvlModuleParallel}, then execution is
	 * performed in parallel using Parallel Streams.
	 * @return A Serializable Collection of UnsatisfiedConstraint instances.
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
		
		return StreamSupport.stream(
			constraintContext.getConstraints().spliterator(), module instanceof EvlModuleParallel)
			.map(constraint -> {
				try {
					return constraint.execute(modelElement, context);
				}
				catch (EolRuntimeException ex) {
					((IEvlContextParallel) context).handleException(ex, null);
					return Optional.<UnsatisfiedConstraint> empty();
				}
			})
			.map(this::serializeUnsatisfiedConstraintIfPresent)
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.toCollection(ArrayList::new));
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
	 * Splits the workload into a Collection of model elements. The order can be randomised
	 * (shuffled) to ensure a balanced workload. Subclasses may override this method to
	 * define an optimal split based on static analysis.
	 * 
	 * @param module
	 * @param shuffle Whether to randomise the list order.
	 * @return The data to be distributed.
	 * @throws EolRuntimeException
	 */
	public static ArrayList<SerializableEvlInputAtom> createJobs(IEvlModule module, boolean shuffle) throws EolRuntimeException {
		IEvlContext context = module.getContext();
		ArrayList<SerializableEvlInputAtom> problems = new ArrayList<>();
		
		for (ConstraintContext constraintContext : module.getConstraintContexts()) {
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
