package org.eclipse.epsilon.epl.execute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.eclipse.epsilon.eol.dom.ExecutableBlock;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.eol.types.EolType;
import org.eclipse.epsilon.epl.dom.NoMatch;
import org.eclipse.epsilon.epl.dom.Role;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class RoleExecutor<C extends Collection<?>> {

	protected final IEolContext context;
	
	public RoleExecutor(IEolContext context) {
		this.context = context;
	}
	
	/**
	 * Executes the role, returning applicable model elements which satisfy the conditions specified
	 * in the role. Since roles may depend on other roles, which in turn depend on the combination of
	 * elements currently bound to those roles, the implementation of this method is non-trivial.
	 * <br/>
	 * This method structures the execution of a role and for consistency,
	 * it cannot be overriden. Subclasses should override the {@link #negativeGuard(ExecutableBlock, Collection)}
	 * and/or {@link #filterElements(ExecutableBlock, Collection)} methods as these involve executing guard
	 * blocks for each element. If even greater control is desired (e.g. to change the return type of the
	 * collection), subclasses can override the intermediate methods, which are:
	 * <br/><ul>
	 * <li> {@link #getRoleInstances(Role, String)} <br/>
	 * <li> {@link #getNegativeRoleInstances(Role, String, Collection)} <br/>
	 * <li> {@link #getAllRoleInstances(Role, String, Collection)} <br/>
	 * 
	 * @param role
	 * @param roleName The name to which instances will be bound when executing the guard block. This
	 * will always come from <code>role.getNames()</code>
	 * @return All objects satisfying the constraints of the role.
	 * @throws EolRuntimeException
	 */
	public final C getRoleInstances(final Role role, final String roleName) throws EolRuntimeException {
		C currentInstances = wrapBasicRoleInstances(role, roleName, this::preprocessRoleInstances);

		if (role.isNegative()) {
			return wrapAdvancedRoleInstances(role, roleName, currentInstances, this::getNegativeRoleInstances);
		}
		else if (role.getCardinality().isMany()) {
			return wrapAdvancedRoleInstances(role, roleName, currentInstances, this::getAllRoleInstances);
		}
		else {
			return currentInstances;
		}
	}
	
	@FunctionalInterface
	protected interface LazyBasicRoleInstancesInitializer {
		Collection<?> get(Role role, String roleName) throws EolRuntimeException;
	}
	
	/**
	 * 
	 * @param <C> The type of Collection returned from {@link RoleExecutor#wrapBasicRoleInstances(Role, String, LazyBasicRoleInstancesInitializer)}.
	 */
	@FunctionalInterface
	protected interface LazyAdvancedRoleInstancesInitializer<C extends Collection<?>> {
		Collection<?> get(Role role, String roleName, C currentInstances) throws EolRuntimeException;
	}
	
	/**
	 * Wraps the result of {@link #preprocessRoleInstances(Role, String)} into the specified collection.
	 * @param role
	 * @param roleName
	 * @param initializer Method reference to {@link #preprocessRoleInstances(Role, String)}.
	 * @return The result of {@linkplain LazyBasicRoleInstancesInitializer#get(Role, String)};
	 * either directly or wrapped into a custom collection.
	 * @throws EolRuntimeException
	 */
	protected abstract C wrapBasicRoleInstances(Role role, String roleName, LazyBasicRoleInstancesInitializer initializer) throws EolRuntimeException;
	
	/**
	 * 
	 * @param role
	 * @param roleName
	 * @param currentInstances The values returned from {@link #wrapBasicRoleInstances(Role, String, LazyBasicRoleInstancesInitializer)}.
	 * @param initializer Method reference to {@link #getNegativeRoleInstances(Role, String, Collection)}
	 * or {@link #getAllRoleInstances(Role, String, Collection)}.
	 * @return The result of {@linkplain LazyAdvancedRoleInstancesInitializer#initialize(Role, String)};
	 * either directly or wrapped into a custom collection.
	 * @throws EolRuntimeException
	 */
	protected abstract C wrapAdvancedRoleInstances(Role role, String roleName, C currentInstances, LazyAdvancedRoleInstancesInitializer<C> initializer) throws EolRuntimeException;
	
	/**
	 * Executes the role's type expression and domain. This is the first method
	 * to be invoked by {@link #getRoleInstances(Role, String)}.
	 * @param role
	 * @param roleName
	 * @return The model elements.
	 * @throws EolRuntimeException
	 */
	private final Collection<?> preprocessRoleInstances(Role role, String roleName) throws EolRuntimeException {
		EolType type = role.getType(context);

		if (!role.isActive(context, true)) {
			return NoMatch.asList();
		}
		else if (role.getDomain() != null) {
			return role.getDomain().getValues(context, type);
		}
		else {
			return ((EolModelElementType)type).getAllOfKind();
		}
	}
	
	/**
	 * Filters the role's instances.
	 * @param role
	 * @param roleName
	 * @param currentInstances
	 * @return The model elements which satisfy the guard.
	 * @throws EolRuntimeException
	 */
	private final Collection<?> getAllRoleInstances(Role role, String roleName, Collection<?> currentInstances) throws EolRuntimeException {
		ExecutableBlock<Boolean> guard = role.getGuard();
		
		Collection<?> filtered = guard == null ?
			currentInstances :
			filterElements(guard, roleName, currentInstances);
		
		if (role.getCardinality().isInBounds(filtered.size()))
			return Collections.singletonList(filtered);
		else
			return Collections.emptyList();
	}
	
	/**
	 * Negates the role.
	 * @param role
	 * @param roleName
	 * @param currentInstances
	 * @return
	 * @throws EolRuntimeException
	 */
	private final Collection<?> getNegativeRoleInstances(Role role, String roleName, Collection<?> currentInstances) throws EolRuntimeException {
		ExecutableBlock<Boolean> guard = role.getGuard();
		
		if (guard != null) {
			if (negativeGuard(guard, roleName, currentInstances)) {
				return Collections.emptyList();
			}
		}
		else if (!currentInstances.isEmpty()) {
			return Collections.emptyList();
		}
		
		return Collections.singletonList(NoMatch.INSTANCE);
	}
	
	/**
	 * Executes the guard block when called by {@link #getNegativeRoleInstances(Role, String, Collection)}
	 * @param guard
	 * @param currentInstances The model elements of the negative role.
	 * @return Whether any of the elements in currentInstances satisfy the guard.
	 * @throws EolRuntimeException
	 */
	protected boolean negativeGuard(ExecutableBlock<Boolean> guard, String roleName, Collection<?> currentInstances) throws EolRuntimeException {
		for (Object element : currentInstances) {
			if (guard.execute(context, true, Variable.createReadOnlyVariable(roleName, element))) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Executes the guard block when called by {@link #getAllRoleInstances(Role, String, Collection)}
	 * @param guard
	 * @param currentInstances The model elements of the role.
	 * @return The subset of model elements satisfying the guard.
	 * @throws EolRuntimeException
	 */
	protected Collection<?> filterElements(ExecutableBlock<Boolean> guard, String roleName, Collection<?> currentInstances) throws EolRuntimeException {
		Collection<Object> filtered = new ArrayList<>();
		for (Object element : currentInstances) {
			if (guard.execute(context, true, Variable.createReadOnlyVariable(roleName, element))) {
				filtered.add(element);
			}
		}
		return filtered;
	}
	
}
