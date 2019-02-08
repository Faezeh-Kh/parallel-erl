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
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
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

	private static final long serialVersionUID = -8561698072322884841L;

	@Override
	protected SerializableEvlInputAtom clone() {
		SerializableEvlInputAtom clone = new SerializableEvlInputAtom();
		clone.modelElementID = ""+this.modelElementID;
		clone.modelName = ""+this.modelName;
		clone.contextName = ""+this.contextName;
		return clone;
	}
	
	/**
	 * Deserializes this element, executes it and transforms the results into a collection of
	 * serialized {@linkplain UnsatisfiedConstraint}s.
	 * @param context
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
		
		ConstraintContext constraintContext = module.getConstraintContextByTypeName(contextName);
		
		if (!constraintContext.shouldBeChecked(modelElement, context)) {
			return Collections.emptyList();
		}
		
		Collection<Constraint> constraintsToCheck = constraintContext.getConstraints();
		Collection<SerializableEvlResultAtom> unsatisfied = new ArrayList<>(constraintsToCheck.size());
		
		for (Constraint constraint : constraintsToCheck) {
			constraint.execute(modelElement, context)
				.map(unsatisfiedConstraint -> {
					SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
					outputAtom.contextName = this.contextName;
					outputAtom.modelName = this.modelName;
					outputAtom.constraintName = unsatisfiedConstraint.getConstraint().getName();
					outputAtom.modelElementID = this.modelElementID;
					outputAtom.message = unsatisfiedConstraint.getMessage();
					return outputAtom;
				})
				.ifPresent(unsatisfied::add);
		}
		
		return unsatisfied;
	}
}
