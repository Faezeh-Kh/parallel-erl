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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.stream.Stream;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelElementTypeNotFoundException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelNotFoundException;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.IEvlModule;
import org.eclipse.epsilon.evl.concurrent.EvlModuleParallel;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;
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

	private static final long serialVersionUID = -4229068854180769590L;
	
	protected transient Stream<Constraint> constraintStream;
	protected transient Object modelElement;
	protected transient IEvlContext context;
	
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
	public Collection<SerializableEvlResultAtom> execute(IEvlModule module) throws EolRuntimeException {
		if (!preExecute(module)) return new ArrayList<>(0);	
		return constraintStream
			.map(constraint -> {
				try {
					return constraint.execute(modelElement, context)
						.map(this::serializeUnsatisfiedConstraint);
				}
				catch (EolRuntimeException ex) {
					if (context instanceof IEvlContextParallel) {
						((IEvlContextParallel) context).handleException(ex, null);
					}
					return Optional.<SerializableEvlResultAtom> empty();
				}
			})
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.toCollection(ArrayList::new));
	}
	
	/**
	 * Executes this atom without any results.
	 * 
	 * @param module
	 * @throws EolRuntimeException
	 */
	public void executeLocal(IEvlModule module) throws EolRuntimeException {
		if (!preExecute(module)) return;
		constraintStream.forEach(constraint -> {
			try {
				constraint.execute(modelElement, context);
			}
			catch (EolRuntimeException ex) {
				if (context instanceof IEvlContextParallel) {
					((IEvlContextParallel) context).handleException(ex, null);
				}
				else ex.printStackTrace();
			}
		});
	}
	
	/**
	 * Internal method to be called prior to execution for convenience. Resolves
	 * the actual data required for evaluation.
	 * @param module
	 * @return Whether the ConstraintContext's guard was satisfied.
	 * @throws EolRuntimeException
	 */
	protected boolean preExecute(IEvlModule module) throws EolRuntimeException {
		modelElement = findElement(context = module.getContext());
		
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
		constraintStream = StreamSupport.stream(
			constraintContext.getConstraints().spliterator(), module instanceof EvlModuleParallel
		);
		return constraintContext.shouldBeChecked(modelElement, context);
	}
	
	public SerializableEvlResultAtom serializeUnsatisfiedConstraint(UnsatisfiedConstraint unsatisfiedConstraint) {
		SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
		outputAtom.contextName = this.contextName;
		outputAtom.modelName = this.modelName;
		outputAtom.constraintName = unsatisfiedConstraint.getConstraint().getName();
		outputAtom.modelElementID = this.modelElementID;
		outputAtom.message = unsatisfiedConstraint.getMessage();
		return outputAtom;
	}
	
	/**
	 * Transforms the given non-serializable jobs into their serializable forms.
	 * 
	 * @param atoms The ConstraintContext and element pairs.
	 * @return A Serializable List of {@link SerializableEvlInputAtom}, in deterministic order.
	 * @throws EolModelElementTypeNotFoundException If resolving any of the model elements fails.
	 * @throws EolModelNotFoundException 
	 */
	public static ArrayList<SerializableEvlInputAtom> serializeJobs(Collection<ConstraintContextAtom> atoms) throws EolModelElementTypeNotFoundException, EolModelNotFoundException {
		ArrayList<SerializableEvlInputAtom> serAtoms = new ArrayList<>(atoms.size());
		
		for (ConstraintContextAtom cca : atoms) {
			EolModelElementType modelType = cca.unit.getType(cca.context);
			SerializableEvlInputAtom sa = new SerializableEvlInputAtom();
			sa.modelName = modelType.getModelName();
			sa.modelElementID = modelType.getModel().getElementId(cca.element);
			sa.contextName = cca.unit.getTypeName();
			serAtoms.add(sa);
		}
		
		return serAtoms;
	}
}
