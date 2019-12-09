/**
 */
package javaMM.impl;

import java.lang.reflect.InvocationTargetException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javaMM.AbstractMethodDeclaration;
import javaMM.AbstractMethodInvocation;
import javaMM.Expression;
import javaMM.JavaMMPackage;
import javaMM.JavaMMTables;
import javaMM.MethodInvocation;
import javaMM.SingleVariableDeclaration;
import javaMM.Type;
import javaMM.TypeAccess;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

import org.eclipse.ocl.pivot.evaluation.Executor;

import org.eclipse.ocl.pivot.ids.IdResolver;
import org.eclipse.ocl.pivot.ids.TypeId;

import org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal;

import org.eclipse.ocl.pivot.library.collection.CollectionIsEmptyOperation;
import org.eclipse.ocl.pivot.library.collection.CollectionSizeOperation;
import org.eclipse.ocl.pivot.library.collection.OrderedCollectionFirstOperation;

import org.eclipse.ocl.pivot.library.logical.BooleanAndOperation;
import org.eclipse.ocl.pivot.library.logical.BooleanNotOperation;

import org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation;
import org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation;

import org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation;
import org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation;

import org.eclipse.ocl.pivot.utilities.ValueUtil;

import org.eclipse.ocl.pivot.values.IntegerValue;
import org.eclipse.ocl.pivot.values.InvalidValueException;
import org.eclipse.ocl.pivot.values.OrderedSetValue;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Method Invocation</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.MethodInvocationImpl#getMethod <em>Method</em>}</li>
 *   <li>{@link javaMM.impl.MethodInvocationImpl#getArguments <em>Arguments</em>}</li>
 *   <li>{@link javaMM.impl.MethodInvocationImpl#getTypeArguments <em>Type Arguments</em>}</li>
 *   <li>{@link javaMM.impl.MethodInvocationImpl#getExpression <em>Expression</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MethodInvocationImpl extends ExpressionImpl implements MethodInvocation {
	/**
	 * The cached value of the '{@link #getMethod() <em>Method</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMethod()
	 * @generated
	 * @ordered
	 */
	protected AbstractMethodDeclaration method;

	/**
	 * The cached value of the '{@link #getArguments() <em>Arguments</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getArguments()
	 * @generated
	 * @ordered
	 */
	protected EList<Expression> arguments;

	/**
	 * The cached value of the '{@link #getTypeArguments() <em>Type Arguments</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTypeArguments()
	 * @generated
	 * @ordered
	 */
	protected EList<TypeAccess> typeArguments;

	/**
	 * The cached value of the '{@link #getExpression() <em>Expression</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpression()
	 * @generated
	 * @ordered
	 */
	protected Expression expression;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MethodInvocationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.METHOD_INVOCATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AbstractMethodDeclaration getMethod() {
		if (method != null && method.eIsProxy()) {
			InternalEObject oldMethod = (InternalEObject)method;
			method = (AbstractMethodDeclaration)eResolveProxy(oldMethod);
			if (method != oldMethod) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, JavaMMPackage.METHOD_INVOCATION__METHOD, oldMethod, method));
			}
		}
		return method;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AbstractMethodDeclaration basicGetMethod() {
		return method;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetMethod(AbstractMethodDeclaration newMethod, NotificationChain msgs) {
		AbstractMethodDeclaration oldMethod = method;
		method = newMethod;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_INVOCATION__METHOD, oldMethod, newMethod);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMethod(AbstractMethodDeclaration newMethod) {
		if (newMethod != method) {
			NotificationChain msgs = null;
			if (method != null)
				msgs = ((InternalEObject)method).eInverseRemove(this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES, AbstractMethodDeclaration.class, msgs);
			if (newMethod != null)
				msgs = ((InternalEObject)newMethod).eInverseAdd(this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES, AbstractMethodDeclaration.class, msgs);
			msgs = basicSetMethod(newMethod, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_INVOCATION__METHOD, newMethod, newMethod));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Expression> getArguments() {
		if (arguments == null) {
			arguments = new EObjectContainmentEList<Expression>(Expression.class, this, JavaMMPackage.METHOD_INVOCATION__ARGUMENTS);
		}
		return arguments;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TypeAccess> getTypeArguments() {
		if (typeArguments == null) {
			typeArguments = new EObjectContainmentEList<TypeAccess>(TypeAccess.class, this, JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS);
		}
		return typeArguments;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Expression getExpression() {
		return expression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetExpression(Expression newExpression, NotificationChain msgs) {
		Expression oldExpression = expression;
		expression = newExpression;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_INVOCATION__EXPRESSION, oldExpression, newExpression);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setExpression(Expression newExpression) {
		if (newExpression != expression) {
			NotificationChain msgs = null;
			if (expression != null)
				msgs = ((InternalEObject)expression).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.METHOD_INVOCATION__EXPRESSION, null, msgs);
			if (newExpression != null)
				msgs = ((InternalEObject)newExpression).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.METHOD_INVOCATION__EXPRESSION, null, msgs);
			msgs = basicSetExpression(newExpression, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_INVOCATION__EXPRESSION, newExpression, newExpression));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean doesNotCallRunFinalizers(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv doesNotCallRunFinalizers:
		 *   let
		 *     severity : Integer[1] = 'MethodInvocation::doesNotCallRunFinalizers'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = self.method <> null implies
		 *         not (self.method.name = 'runFinalizersOnExit' and
		 *           self.method.parameters->size() = 1 and
		 *           self.method.parameters->first()
		 *           .type.type.oclIsTypeOf(PrimitiveTypeBoolean)
		 *         )
		 *       in
		 *         'MethodInvocation::doesNotCallRunFinalizers'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_MethodInvocation_c_c_doesNotCallRunFinalizers);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ AbstractMethodDeclaration method = this.getMethod();
			final /*@NonInvalid*/ boolean ne = method != null;
			/*@NonInvalid*/ Boolean result;
			if (ne) {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_0;
				try {
					/*@Caught*/ /*@Nullable*/ Object CAUGHT_and;
					try {
						/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq;
						try {
							if (method == null) {
								throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::NamedElement::name\'");
							}
							final /*@Thrown*/ String name = method.getName();
							final /*@Thrown*/ boolean eq = JavaMMTables.STR_runFinalizersOnExit.equals(name);
							CAUGHT_eq = eq;
						}
						catch (Exception e) {
							CAUGHT_eq = ValueUtil.createInvalidValue(e);
						}
						/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq_0;
						try {
							if (method == null) {
								throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::AbstractMethodDeclaration::parameters\'");
							}
							final /*@Thrown*/ List<SingleVariableDeclaration> parameters = method.getParameters();
							final /*@Thrown*/ OrderedSetValue BOXED_parameters = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters);
							final /*@Thrown*/ IntegerValue size = CollectionSizeOperation.INSTANCE.evaluate(BOXED_parameters);
							final /*@Thrown*/ boolean eq_0 = size.equals(JavaMMTables.INT_1);
							CAUGHT_eq_0 = eq_0;
						}
						catch (Exception e) {
							CAUGHT_eq_0 = ValueUtil.createInvalidValue(e);
						}
						final /*@Thrown*/ Boolean and = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_eq, CAUGHT_eq_0);
						CAUGHT_and = and;
					}
					catch (Exception e) {
						CAUGHT_and = ValueUtil.createInvalidValue(e);
					}
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
					try {
						final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeBoolean_0 = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeBoolean, null);
						if (method == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::AbstractMethodDeclaration::parameters\'");
						}
						final /*@Thrown*/ List<SingleVariableDeclaration> parameters_0 = method.getParameters();
						final /*@Thrown*/ OrderedSetValue BOXED_parameters_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters_0);
						final /*@Thrown*/ SingleVariableDeclaration first = (SingleVariableDeclaration)OrderedCollectionFirstOperation.INSTANCE.evaluate(BOXED_parameters_0);
						if (first == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::SingleVariableDeclaration::type\'");
						}
						final /*@Thrown*/ TypeAccess type_0 = first.getType();
						if (type_0 == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
						}
						final /*@Thrown*/ Type type_1 = type_0.getType();
						final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_1, TYP_javaMM_c_c_PrimitiveTypeBoolean_0).booleanValue();
						CAUGHT_oclIsTypeOf = oclIsTypeOf;
					}
					catch (Exception e) {
						CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
					}
					final /*@Thrown*/ Boolean and_0 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and, CAUGHT_oclIsTypeOf);
					CAUGHT_and_0 = and_0;
				}
				catch (Exception e) {
					CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
				}
				final /*@NonInvalid*/ Boolean not = BooleanNotOperation.INSTANCE.evaluate(CAUGHT_and_0);
				result = not;
			}
			else {
				result = ValueUtil.TRUE_VALUE;
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_MethodInvocation_c_c_doesNotCallRunFinalizers, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean doesNotCallFinalize(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv doesNotCallFinalize:
		 *   let
		 *     severity : Integer[1] = 'MethodInvocation::doesNotCallFinalize'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = self.method <> null implies
		 *         not (self.method.name = 'finalize' and
		 *           self.method.parameters->isEmpty()
		 *         )
		 *       in
		 *         'MethodInvocation::doesNotCallFinalize'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_MethodInvocation_c_c_doesNotCallFinalize);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ AbstractMethodDeclaration method = this.getMethod();
			final /*@NonInvalid*/ boolean ne = method != null;
			/*@NonInvalid*/ Boolean result;
			if (ne) {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_and;
				try {
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq;
					try {
						if (method == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::NamedElement::name\'");
						}
						final /*@Thrown*/ String name = method.getName();
						final /*@Thrown*/ boolean eq = JavaMMTables.STR_finalize.equals(name);
						CAUGHT_eq = eq;
					}
					catch (Exception e) {
						CAUGHT_eq = ValueUtil.createInvalidValue(e);
					}
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_isEmpty;
					try {
						if (method == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::AbstractMethodDeclaration::parameters\'");
						}
						final /*@Thrown*/ List<SingleVariableDeclaration> parameters = method.getParameters();
						final /*@Thrown*/ OrderedSetValue BOXED_parameters = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters);
						final /*@Thrown*/ boolean isEmpty = CollectionIsEmptyOperation.INSTANCE.evaluate(BOXED_parameters).booleanValue();
						CAUGHT_isEmpty = isEmpty;
					}
					catch (Exception e) {
						CAUGHT_isEmpty = ValueUtil.createInvalidValue(e);
					}
					final /*@Thrown*/ Boolean and = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_eq, CAUGHT_isEmpty);
					CAUGHT_and = and;
				}
				catch (Exception e) {
					CAUGHT_and = ValueUtil.createInvalidValue(e);
				}
				final /*@NonInvalid*/ Boolean not = BooleanNotOperation.INSTANCE.evaluate(CAUGHT_and);
				result = not;
			}
			else {
				result = ValueUtil.TRUE_VALUE;
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_MethodInvocation_c_c_doesNotCallFinalize, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean doesNotCallExit(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv doesNotCallExit:
		 *   let
		 *     severity : Integer[1] = 'MethodInvocation::doesNotCallExit'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = self.method <> null implies
		 *         not (self.method.name = 'exit' and
		 *           self.method.parameters->size() = 1 and
		 *           self.method.parameters->first()
		 *           .type.type.oclIsTypeOf(PrimitiveTypeInt)
		 *         )
		 *       in
		 *         'MethodInvocation::doesNotCallExit'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_MethodInvocation_c_c_doesNotCallExit);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ AbstractMethodDeclaration method = this.getMethod();
			final /*@NonInvalid*/ boolean ne = method != null;
			/*@NonInvalid*/ Boolean result;
			if (ne) {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_0;
				try {
					/*@Caught*/ /*@Nullable*/ Object CAUGHT_and;
					try {
						/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq;
						try {
							if (method == null) {
								throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::NamedElement::name\'");
							}
							final /*@Thrown*/ String name = method.getName();
							final /*@Thrown*/ boolean eq = JavaMMTables.STR_exit.equals(name);
							CAUGHT_eq = eq;
						}
						catch (Exception e) {
							CAUGHT_eq = ValueUtil.createInvalidValue(e);
						}
						/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq_0;
						try {
							if (method == null) {
								throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::AbstractMethodDeclaration::parameters\'");
							}
							final /*@Thrown*/ List<SingleVariableDeclaration> parameters = method.getParameters();
							final /*@Thrown*/ OrderedSetValue BOXED_parameters = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters);
							final /*@Thrown*/ IntegerValue size = CollectionSizeOperation.INSTANCE.evaluate(BOXED_parameters);
							final /*@Thrown*/ boolean eq_0 = size.equals(JavaMMTables.INT_1);
							CAUGHT_eq_0 = eq_0;
						}
						catch (Exception e) {
							CAUGHT_eq_0 = ValueUtil.createInvalidValue(e);
						}
						final /*@Thrown*/ Boolean and = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_eq, CAUGHT_eq_0);
						CAUGHT_and = and;
					}
					catch (Exception e) {
						CAUGHT_and = ValueUtil.createInvalidValue(e);
					}
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
					try {
						final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_PrimitiveTypeInt_0 = idResolver.getClass(JavaMMTables.CLSSid_PrimitiveTypeInt, null);
						if (method == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::AbstractMethodDeclaration::parameters\'");
						}
						final /*@Thrown*/ List<SingleVariableDeclaration> parameters_0 = method.getParameters();
						final /*@Thrown*/ OrderedSetValue BOXED_parameters_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters_0);
						final /*@Thrown*/ SingleVariableDeclaration first = (SingleVariableDeclaration)OrderedCollectionFirstOperation.INSTANCE.evaluate(BOXED_parameters_0);
						if (first == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::SingleVariableDeclaration::type\'");
						}
						final /*@Thrown*/ TypeAccess type_0 = first.getType();
						if (type_0 == null) {
							throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::TypeAccess::type\'");
						}
						final /*@Thrown*/ Type type_1 = type_0.getType();
						final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, type_1, TYP_javaMM_c_c_PrimitiveTypeInt_0).booleanValue();
						CAUGHT_oclIsTypeOf = oclIsTypeOf;
					}
					catch (Exception e) {
						CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
					}
					final /*@Thrown*/ Boolean and_0 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and, CAUGHT_oclIsTypeOf);
					CAUGHT_and_0 = and_0;
				}
				catch (Exception e) {
					CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
				}
				final /*@NonInvalid*/ Boolean not = BooleanNotOperation.INSTANCE.evaluate(CAUGHT_and_0);
				result = not;
			}
			else {
				result = ValueUtil.TRUE_VALUE;
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_MethodInvocation_c_c_doesNotCallExit, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.METHOD_INVOCATION__METHOD:
				if (method != null)
					msgs = ((InternalEObject)method).eInverseRemove(this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES, AbstractMethodDeclaration.class, msgs);
				return basicSetMethod((AbstractMethodDeclaration)otherEnd, msgs);
		}
		return super.eInverseAdd(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.METHOD_INVOCATION__METHOD:
				return basicSetMethod(null, msgs);
			case JavaMMPackage.METHOD_INVOCATION__ARGUMENTS:
				return ((InternalEList<?>)getArguments()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS:
				return ((InternalEList<?>)getTypeArguments()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.METHOD_INVOCATION__EXPRESSION:
				return basicSetExpression(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case JavaMMPackage.METHOD_INVOCATION__METHOD:
				if (resolve) return getMethod();
				return basicGetMethod();
			case JavaMMPackage.METHOD_INVOCATION__ARGUMENTS:
				return getArguments();
			case JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS:
				return getTypeArguments();
			case JavaMMPackage.METHOD_INVOCATION__EXPRESSION:
				return getExpression();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case JavaMMPackage.METHOD_INVOCATION__METHOD:
				setMethod((AbstractMethodDeclaration)newValue);
				return;
			case JavaMMPackage.METHOD_INVOCATION__ARGUMENTS:
				getArguments().clear();
				getArguments().addAll((Collection<? extends Expression>)newValue);
				return;
			case JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS:
				getTypeArguments().clear();
				getTypeArguments().addAll((Collection<? extends TypeAccess>)newValue);
				return;
			case JavaMMPackage.METHOD_INVOCATION__EXPRESSION:
				setExpression((Expression)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case JavaMMPackage.METHOD_INVOCATION__METHOD:
				setMethod((AbstractMethodDeclaration)null);
				return;
			case JavaMMPackage.METHOD_INVOCATION__ARGUMENTS:
				getArguments().clear();
				return;
			case JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS:
				getTypeArguments().clear();
				return;
			case JavaMMPackage.METHOD_INVOCATION__EXPRESSION:
				setExpression((Expression)null);
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case JavaMMPackage.METHOD_INVOCATION__METHOD:
				return method != null;
			case JavaMMPackage.METHOD_INVOCATION__ARGUMENTS:
				return arguments != null && !arguments.isEmpty();
			case JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS:
				return typeArguments != null && !typeArguments.isEmpty();
			case JavaMMPackage.METHOD_INVOCATION__EXPRESSION:
				return expression != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int eBaseStructuralFeatureID(int derivedFeatureID, Class<?> baseClass) {
		if (baseClass == AbstractMethodInvocation.class) {
			switch (derivedFeatureID) {
				case JavaMMPackage.METHOD_INVOCATION__METHOD: return JavaMMPackage.ABSTRACT_METHOD_INVOCATION__METHOD;
				case JavaMMPackage.METHOD_INVOCATION__ARGUMENTS: return JavaMMPackage.ABSTRACT_METHOD_INVOCATION__ARGUMENTS;
				case JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS: return JavaMMPackage.ABSTRACT_METHOD_INVOCATION__TYPE_ARGUMENTS;
				default: return -1;
			}
		}
		return super.eBaseStructuralFeatureID(derivedFeatureID, baseClass);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int eDerivedStructuralFeatureID(int baseFeatureID, Class<?> baseClass) {
		if (baseClass == AbstractMethodInvocation.class) {
			switch (baseFeatureID) {
				case JavaMMPackage.ABSTRACT_METHOD_INVOCATION__METHOD: return JavaMMPackage.METHOD_INVOCATION__METHOD;
				case JavaMMPackage.ABSTRACT_METHOD_INVOCATION__ARGUMENTS: return JavaMMPackage.METHOD_INVOCATION__ARGUMENTS;
				case JavaMMPackage.ABSTRACT_METHOD_INVOCATION__TYPE_ARGUMENTS: return JavaMMPackage.METHOD_INVOCATION__TYPE_ARGUMENTS;
				default: return -1;
			}
		}
		return super.eDerivedStructuralFeatureID(baseFeatureID, baseClass);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Object eInvoke(int operationID, EList<?> arguments) throws InvocationTargetException {
		switch (operationID) {
			case JavaMMPackage.METHOD_INVOCATION___DOES_NOT_CALL_RUN_FINALIZERS__DIAGNOSTICCHAIN_MAP:
				return doesNotCallRunFinalizers((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.METHOD_INVOCATION___DOES_NOT_CALL_FINALIZE__DIAGNOSTICCHAIN_MAP:
				return doesNotCallFinalize((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.METHOD_INVOCATION___DOES_NOT_CALL_EXIT__DIAGNOSTICCHAIN_MAP:
				return doesNotCallExit((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
		}
		return super.eInvoke(operationID, arguments);
	}

} //MethodInvocationImpl
