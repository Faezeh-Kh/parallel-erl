/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.epl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.eclipse.epsilon.common.function.ExceptionHandler;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.epl.combinations.*;
import org.eclipse.epsilon.epl.dom.NoMatch;
import org.eclipse.epsilon.epl.dom.Pattern;
import org.eclipse.epsilon.epl.dom.Role;
import org.eclipse.epsilon.epl.execute.RoleExecutor;

/**
 * Generator-based sequential implementation of EPL.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DynamicEplModule extends AbstractEplModule {
	
	@SuppressWarnings({ "unchecked" })
	private static List<Object> wrapGetValues(Collection<?> superReturnInstances) {
		if (superReturnInstances instanceof ArrayList || superReturnInstances instanceof DynamicList)
			return (List<Object>) superReturnInstances;
		else
			return new ArrayList<>(superReturnInstances);
	}
	
	
	protected final RoleExecutor<DynamicList<Object>> roleExecutor = new RoleExecutor<DynamicList<Object>>(context) {
		
		@Override
		protected DynamicList<Object> wrapBasicRoleInstances(Role role, String roleName, LazyBasicRoleInstancesInitializer initializer) throws EolRuntimeException {
			DynamicList<Object> dynamicInstances = new DynamicList<Object>() {
				
				@Override
				protected List<Object> getValues() throws EolRuntimeException {
					return wrapGetValues(initializer.get(role, roleName));
				}
			};
			
			dynamicInstances.setResetable(role.hasActiveAst() || (role.getDomain() != null && role.getDomain().isDynamic()));
			dynamicInstances.setExceptionHandler(new RuntimeExceptionThrower<EolRuntimeException>(context));
			return dynamicInstances;
		}
		
		@Override
		protected DynamicList<Object> wrapAdvancedRoleInstances(Role role, String roleName, DynamicList<Object> currentInstances, LazyAdvancedRoleInstancesInitializer<DynamicList<Object>> initializer) throws EolRuntimeException {
			DynamicList<Object> dynamicInstances = new DynamicList<Object>() {
				
				@Override
				protected List<Object> getValues() throws EolRuntimeException {
					return wrapGetValues(initializer.get(role, roleName, currentInstances));
				}
				
				@Override
				public void reset() {
					super.reset();
					currentInstances.reset();
				}
			};
			
			dynamicInstances.setResetable(currentInstances.isResetable());
			dynamicInstances.setExceptionHandler(currentInstances.getExceptionHandler());
			return dynamicInstances;
		}
	};
	
	
	@Override
	protected final Iterator<List<List<Object>>> getCandidates(Pattern pattern) throws EolRuntimeException {
		return initGenerator(pattern);
	}
	
	
	/**
	 * Create a new CompositeCombinationGenerator, add the generator for each role and attach the validator.
	 * 
	 * @param pattern the pattern being executed
	 * @return the created generator
	 * @throws EolRuntimeException
	 */
	protected CompositeCombinationGenerator<Object> initGenerator(Pattern pattern) throws EolRuntimeException {
		CompositeCombinationGenerator<Object> generator = new CompositeCombinationGenerator<>();
		
		for (Role role : pattern.getRoles()) {
			generator.addCombinationGenerator(createCombinationGenerator(role));
		}

		generator.setValidator(new CompositeCombinationValidator<Object, EolRuntimeException>() {
			
			@Override
			public boolean isValid(List<List<Object>> combination) throws EolRuntimeException {
				return isValidCombination(pattern, combination);
			}

			@Override
			public ExceptionHandler<EolRuntimeException> getExceptionHandler() {
				return new RuntimeExceptionThrower<>(context);
			}
		});
		
		return generator;
	}
	
	
	protected CombinationGenerator<Object> createCombinationGenerator(Role role) throws EolRuntimeException {
		DynamicListCombinationGenerator<Object> combinationGenerator = new DynamicListCombinationGenerator<Object>(
			roleExecutor.getRoleInstances(role, null), role.getNames().size()) {
			
			@Override
			public boolean checkOptional() {
				try {
					return role.isOptional(context);
				}
				catch (EolRuntimeException ex) {
					throw new RuntimeException(ex);
				}
			}
		};
	
		combinationGenerator.addListener(new CombinationGeneratorListener<Object>() {
			
			@Override
			public void generated(Collection<Object> next) {
				if (next == null) {
					for (String name : role.getNames()) {
						context.getFrameStack().put(Variable.createReadOnlyVariable(name, NoMatch.INSTANCE));
					}
				}
				else {
					context.getFrameStack().put(getVariables(next, role));
				}
			}
			
			@Override
			public void reset() {
				context.getFrameStack().remove(role.getNames());
			}
		});
		
		return combinationGenerator;
	}
	
}
