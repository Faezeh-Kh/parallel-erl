/**
 */
package javaMM.impl;

import java.lang.reflect.InvocationTargetException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javaMM.AbstractMethodDeclaration;
import javaMM.AbstractMethodInvocation;
import javaMM.Assignment;
import javaMM.AssignmentKind;
import javaMM.Block;
import javaMM.Expression;
import javaMM.ExpressionStatement;
import javaMM.JavaMMPackage;
import javaMM.JavaMMTables;
import javaMM.MethodRef;
import javaMM.Modifier;
import javaMM.SingleVariableAccess;
import javaMM.SingleVariableDeclaration;
import javaMM.Statement;
import javaMM.TypeAccess;
import javaMM.TypeParameter;
import javaMM.VariableDeclaration;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectContainmentWithInverseEList;
import org.eclipse.emf.ecore.util.EObjectWithInverseResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

import org.eclipse.ocl.pivot.evaluation.Executor;

import org.eclipse.ocl.pivot.ids.EnumerationLiteralId;
import org.eclipse.ocl.pivot.ids.IdResolver;
import org.eclipse.ocl.pivot.ids.TypeId;

import org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal;

import org.eclipse.ocl.pivot.library.collection.CollectionExcludesOperation;
import org.eclipse.ocl.pivot.library.collection.CollectionIsEmptyOperation;
import org.eclipse.ocl.pivot.library.collection.CollectionNotEmptyOperation;
import org.eclipse.ocl.pivot.library.collection.CollectionSelectByKindOperation;

import org.eclipse.ocl.pivot.library.logical.BooleanAndOperation;
import org.eclipse.ocl.pivot.library.logical.BooleanImpliesOperation;

import org.eclipse.ocl.pivot.library.oclany.OclAnyOclAsTypeOperation;
import org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsKindOfOperation;
import org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation;

import org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation;
import org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation;

import org.eclipse.ocl.pivot.utilities.ClassUtil;
import org.eclipse.ocl.pivot.utilities.ValueUtil;

import org.eclipse.ocl.pivot.values.IntegerValue;
import org.eclipse.ocl.pivot.values.InvalidValueException;
import org.eclipse.ocl.pivot.values.OrderedSetValue;
import org.eclipse.ocl.pivot.values.SequenceValue;
import org.eclipse.ocl.pivot.values.SetValue;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Abstract Method Declaration</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.AbstractMethodDeclarationImpl#getBody <em>Body</em>}</li>
 *   <li>{@link javaMM.impl.AbstractMethodDeclarationImpl#getParameters <em>Parameters</em>}</li>
 *   <li>{@link javaMM.impl.AbstractMethodDeclarationImpl#getThrownExceptions <em>Thrown Exceptions</em>}</li>
 *   <li>{@link javaMM.impl.AbstractMethodDeclarationImpl#getTypeParameters <em>Type Parameters</em>}</li>
 *   <li>{@link javaMM.impl.AbstractMethodDeclarationImpl#getUsagesInDocComments <em>Usages In Doc Comments</em>}</li>
 *   <li>{@link javaMM.impl.AbstractMethodDeclarationImpl#getUsages <em>Usages</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class AbstractMethodDeclarationImpl extends BodyDeclarationImpl implements AbstractMethodDeclaration {
	/**
	 * The cached value of the '{@link #getBody() <em>Body</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBody()
	 * @generated
	 * @ordered
	 */
	protected Block body;

	/**
	 * The cached value of the '{@link #getParameters() <em>Parameters</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParameters()
	 * @generated
	 * @ordered
	 */
	protected EList<SingleVariableDeclaration> parameters;

	/**
	 * The cached value of the '{@link #getThrownExceptions() <em>Thrown Exceptions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getThrownExceptions()
	 * @generated
	 * @ordered
	 */
	protected EList<TypeAccess> thrownExceptions;

	/**
	 * The cached value of the '{@link #getTypeParameters() <em>Type Parameters</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTypeParameters()
	 * @generated
	 * @ordered
	 */
	protected EList<TypeParameter> typeParameters;

	/**
	 * The cached value of the '{@link #getUsagesInDocComments() <em>Usages In Doc Comments</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUsagesInDocComments()
	 * @generated
	 * @ordered
	 */
	protected EList<MethodRef> usagesInDocComments;

	/**
	 * The cached value of the '{@link #getUsages() <em>Usages</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUsages()
	 * @generated
	 * @ordered
	 */
	protected EList<AbstractMethodInvocation> usages;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AbstractMethodDeclarationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.ABSTRACT_METHOD_DECLARATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Block getBody() {
		return body;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetBody(Block newBody, NotificationChain msgs) {
		Block oldBody = body;
		body = newBody;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY, oldBody, newBody);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setBody(Block newBody) {
		if (newBody != body) {
			NotificationChain msgs = null;
			if (body != null)
				msgs = ((InternalEObject)body).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY, null, msgs);
			if (newBody != null)
				msgs = ((InternalEObject)newBody).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY, null, msgs);
			msgs = basicSetBody(newBody, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY, newBody, newBody));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SingleVariableDeclaration> getParameters() {
		if (parameters == null) {
			parameters = new EObjectContainmentWithInverseEList<SingleVariableDeclaration>(SingleVariableDeclaration.class, this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION);
		}
		return parameters;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TypeAccess> getThrownExceptions() {
		if (thrownExceptions == null) {
			thrownExceptions = new EObjectContainmentEList<TypeAccess>(TypeAccess.class, this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS);
		}
		return thrownExceptions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TypeParameter> getTypeParameters() {
		if (typeParameters == null) {
			typeParameters = new EObjectContainmentEList<TypeParameter>(TypeParameter.class, this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS);
		}
		return typeParameters;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<MethodRef> getUsagesInDocComments() {
		if (usagesInDocComments == null) {
			usagesInDocComments = new EObjectWithInverseResolvingEList<MethodRef>(MethodRef.class, this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS, JavaMMPackage.METHOD_REF__METHOD);
		}
		return usagesInDocComments;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<AbstractMethodInvocation> getUsages() {
		if (usages == null) {
			usages = new EObjectWithInverseResolvingEList<AbstractMethodInvocation>(AbstractMethodInvocation.class, this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES, JavaMMPackage.ABSTRACT_METHOD_INVOCATION__METHOD);
		}
		return usages;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean parametersEffectivelyFinal(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv parametersEffectivelyFinal:
		 *   let
		 *     severity : Integer[1] = 'AbstractMethodDeclaration::parametersEffectivelyFinal'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = self.body <> null and
		 *         self.parameters->notEmpty() implies
		 *         self.body.statements->selectByKind(ExpressionStatement)
		 *         ->collect(s | s.expression)
		 *         ->selectByKind(Assignment)
		 *         ->select(assignment |
		 *           assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and assignment.operator = AssignmentKind::ASSIGN and
		 *           assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and
		 *           self.parameters->excludes(
		 *             assignment.leftHandSide.oclAsType(SingleVariableAccess).variable))
		 *         ->isEmpty()
		 *       in
		 *         'AbstractMethodDeclaration::parametersEffectivelyFinal'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_AbstractMethodDeclaration_c_c_parametersEffectivelyFinal);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@NonNull*/ Object CAUGHT_result;
			try {
				final /*@NonInvalid*/ Block body = this.getBody();
				final /*@NonInvalid*/ boolean ne = body != null;
				/*@NonInvalid*/ boolean and;
				if (ne) {
					final /*@NonInvalid*/ List<SingleVariableDeclaration> parameters = this.getParameters();
					final /*@NonInvalid*/ OrderedSetValue BOXED_parameters = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters);
					final /*@NonInvalid*/ boolean notEmpty = CollectionNotEmptyOperation.INSTANCE.evaluate(BOXED_parameters).booleanValue();
					and = notEmpty;
				}
				else {
					and = ValueUtil.FALSE_VALUE;
				}
				/*@Thrown*/ boolean result;
				if (and) {
					final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_Assignment = idResolver.getClass(JavaMMTables.CLSSid_Assignment, null);
					final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_ExpressionStatement = idResolver.getClass(JavaMMTables.CLSSid_ExpressionStatement, null);
					if (body == null) {
						throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Block::statements\'");
					}
					final /*@Thrown*/ List<Statement> statements = body.getStatements();
					final /*@Thrown*/ OrderedSetValue BOXED_statements = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_Statement, statements);
					final /*@Thrown*/ OrderedSetValue selectByKind = (OrderedSetValue)CollectionSelectByKindOperation.INSTANCE.evaluate(executor, BOXED_statements, TYP_javaMM_c_c_ExpressionStatement);
					/*@Thrown*/ SequenceValue.Accumulator accumulator = ValueUtil.createSequenceAccumulatorValue(JavaMMTables.SEQ_CLSSid_Expression);
					/*@NonNull*/ Iterator<Object> ITERATOR_s = selectByKind.iterator();
					/*@Thrown*/ SequenceValue collect;
					while (true) {
						if (!ITERATOR_s.hasNext()) {
							collect = accumulator;
							break;
						}
						/*@NonInvalid*/ ExpressionStatement s = (ExpressionStatement)ITERATOR_s.next();
						/**
						 * s.expression
						 */
						final /*@NonInvalid*/ Expression expression = s.getExpression();
						//
						accumulator.add(expression);
					}
					final /*@Thrown*/ SequenceValue selectByKind_0 = (SequenceValue)CollectionSelectByKindOperation.INSTANCE.evaluate(executor, collect, TYP_javaMM_c_c_Assignment);
					/*@Thrown*/ SequenceValue.Accumulator accumulator_0 = ValueUtil.createSequenceAccumulatorValue(JavaMMTables.SEQ_CLSSid_Assignment);
					/*@NonNull*/ Iterator<Object> ITERATOR_assignment = selectByKind_0.iterator();
					/*@Thrown*/ SequenceValue select;
					while (true) {
						if (!ITERATOR_assignment.hasNext()) {
							select = accumulator_0;
							break;
						}
						/*@NonInvalid*/ Assignment assignment = (Assignment)ITERATOR_assignment.next();
						/**
						 *
						 * assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and assignment.operator = AssignmentKind::ASSIGN and
						 * assignment.leftHandSide.oclIsKindOf(SingleVariableAccess) and
						 * self.parameters->excludes(
						 *   assignment.leftHandSide.oclAsType(SingleVariableAccess).variable)
						 */
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_1;
						try {
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsKindOf_0;
							try {
								final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_SingleVariableAccess_0 = idResolver.getClass(JavaMMTables.CLSSid_SingleVariableAccess, null);
								final /*@NonInvalid*/ Expression leftHandSide_0 = assignment.getLeftHandSide();
								final /*@Thrown*/ boolean oclIsKindOf_0 = OclAnyOclIsKindOfOperation.INSTANCE.evaluate(executor, leftHandSide_0, TYP_javaMM_c_c_SingleVariableAccess_0).booleanValue();
								CAUGHT_oclIsKindOf_0 = oclIsKindOf_0;
							}
							catch (Exception e) {
								CAUGHT_oclIsKindOf_0 = ValueUtil.createInvalidValue(e);
							}
							/*@Caught*/ /*@Nullable*/ Object CAUGHT_and_0;
							try {
								final /*@NonInvalid*/ AssignmentKind operator = assignment.getOperator();
								final /*@NonInvalid*/ EnumerationLiteralId BOXED_operator = operator == null ? null : JavaMMTables.ENUMid_AssignmentKind.getEnumerationLiteralId(ClassUtil.nonNullState(operator.getName()));
								final /*@NonInvalid*/ boolean eq = BOXED_operator == JavaMMTables.ELITid_ASSIGN;
								final /*@Thrown*/ Boolean and_0 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_oclIsKindOf_0, eq);
								CAUGHT_and_0 = and_0;
							}
							catch (Exception e) {
								CAUGHT_and_0 = ValueUtil.createInvalidValue(e);
							}
							final /*@Thrown*/ Boolean and_1 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_0, CAUGHT_oclIsKindOf_0);
							CAUGHT_and_1 = and_1;
						}
						catch (Exception e) {
							CAUGHT_and_1 = ValueUtil.createInvalidValue(e);
						}
						/*@Caught*/ /*@NonNull*/ Object CAUGHT_excludes;
						try {
							final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_SingleVariableAccess_1 = idResolver.getClass(JavaMMTables.CLSSid_SingleVariableAccess, null);
							final /*@NonInvalid*/ List<SingleVariableDeclaration> parameters_0 = this.getParameters();
							final /*@NonInvalid*/ OrderedSetValue BOXED_parameters_0 = idResolver.createOrderedSetOfAll(JavaMMTables.ORD_CLSSid_SingleVariableDeclaration, parameters_0);
							final /*@NonInvalid*/ Expression leftHandSide_1 = assignment.getLeftHandSide();
							final /*@Thrown*/ SingleVariableAccess oclAsType = ClassUtil.nonNullState((SingleVariableAccess)OclAnyOclAsTypeOperation.INSTANCE.evaluate(executor, leftHandSide_1, TYP_javaMM_c_c_SingleVariableAccess_1));
							final /*@Thrown*/ VariableDeclaration variable = oclAsType.getVariable();
							final /*@Thrown*/ boolean excludes = CollectionExcludesOperation.INSTANCE.evaluate(BOXED_parameters_0, variable).booleanValue();
							CAUGHT_excludes = excludes;
						}
						catch (Exception e) {
							CAUGHT_excludes = ValueUtil.createInvalidValue(e);
						}
						final /*@Thrown*/ Boolean and_2 = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_and_1, CAUGHT_excludes);
						if (and_2 == null) {
							throw new InvalidValueException("Null body for \'Sequence(T).select(Sequence.T[?] | Lambda T() : Boolean[1]) : Sequence(T)\'");
						}
						//
						if (and_2 == ValueUtil.TRUE_VALUE) {
							accumulator_0.add(assignment);
						}
					}
					final /*@Thrown*/ boolean isEmpty = CollectionIsEmptyOperation.INSTANCE.evaluate(select).booleanValue();
					result = isEmpty;
				}
				else {
					result = ValueUtil.TRUE_VALUE;
				}
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_AbstractMethodDeclaration_c_c_parametersEffectivelyFinal, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean localMethodIsUsed(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv localMethodIsUsed:
		 *   let
		 *     severity : Integer[1] = 'AbstractMethodDeclaration::localMethodIsUsed'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = self.modifier <> null and
		 *         self.modifier.isLocal() implies
		 *         self.usages->notEmpty()
		 *       in
		 *         'AbstractMethodDeclaration::localMethodIsUsed'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_AbstractMethodDeclaration_c_c_localMethodIsUsed);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				/*@Caught*/ /*@NonNull*/ Object CAUGHT_and;
				try {
					final /*@NonInvalid*/ Modifier modifier = this.getModifier();
					final /*@NonInvalid*/ boolean ne = modifier != null;
					/*@Thrown*/ boolean and;
					if (ne) {
						if (modifier == null) {
							throw new InvalidValueException("Null source for \'javaMM::Modifier::isLocal() : Boolean[?]\'");
						}
						final /*@Thrown*/ boolean isLocal = modifier.isLocal();
						and = isLocal;
					}
					else {
						and = ValueUtil.FALSE_VALUE;
					}
					CAUGHT_and = and;
				}
				catch (Exception e) {
					CAUGHT_and = ValueUtil.createInvalidValue(e);
				}
				final /*@NonInvalid*/ List<AbstractMethodInvocation> usages = this.getUsages();
				final /*@NonInvalid*/ SetValue BOXED_usages = idResolver.createSetOfAll(JavaMMTables.SET_CLSSid_AbstractMethodInvocation, usages);
				final /*@NonInvalid*/ boolean notEmpty = CollectionNotEmptyOperation.INSTANCE.evaluate(BOXED_usages).booleanValue();
				final /*@Thrown*/ Boolean result = BooleanImpliesOperation.INSTANCE.evaluate(CAUGHT_and, notEmpty);
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_AbstractMethodDeclaration_c_c_localMethodIsUsed, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getParameters()).basicAdd(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getUsagesInDocComments()).basicAdd(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getUsages()).basicAdd(otherEnd, msgs);
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
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY:
				return basicSetBody(null, msgs);
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS:
				return ((InternalEList<?>)getParameters()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS:
				return ((InternalEList<?>)getThrownExceptions()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS:
				return ((InternalEList<?>)getTypeParameters()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS:
				return ((InternalEList<?>)getUsagesInDocComments()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES:
				return ((InternalEList<?>)getUsages()).basicRemove(otherEnd, msgs);
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
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY:
				return getBody();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS:
				return getParameters();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS:
				return getThrownExceptions();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS:
				return getTypeParameters();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS:
				return getUsagesInDocComments();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES:
				return getUsages();
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
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY:
				setBody((Block)newValue);
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS:
				getParameters().clear();
				getParameters().addAll((Collection<? extends SingleVariableDeclaration>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS:
				getThrownExceptions().clear();
				getThrownExceptions().addAll((Collection<? extends TypeAccess>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS:
				getTypeParameters().clear();
				getTypeParameters().addAll((Collection<? extends TypeParameter>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS:
				getUsagesInDocComments().clear();
				getUsagesInDocComments().addAll((Collection<? extends MethodRef>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES:
				getUsages().clear();
				getUsages().addAll((Collection<? extends AbstractMethodInvocation>)newValue);
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
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY:
				setBody((Block)null);
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS:
				getParameters().clear();
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS:
				getThrownExceptions().clear();
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS:
				getTypeParameters().clear();
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS:
				getUsagesInDocComments().clear();
				return;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES:
				getUsages().clear();
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
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__BODY:
				return body != null;
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS:
				return parameters != null && !parameters.isEmpty();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__THROWN_EXCEPTIONS:
				return thrownExceptions != null && !thrownExceptions.isEmpty();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__TYPE_PARAMETERS:
				return typeParameters != null && !typeParameters.isEmpty();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES_IN_DOC_COMMENTS:
				return usagesInDocComments != null && !usagesInDocComments.isEmpty();
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION__USAGES:
				return usages != null && !usages.isEmpty();
		}
		return super.eIsSet(featureID);
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
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION___PARAMETERS_EFFECTIVELY_FINAL__DIAGNOSTICCHAIN_MAP:
				return parametersEffectivelyFinal((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.ABSTRACT_METHOD_DECLARATION___LOCAL_METHOD_IS_USED__DIAGNOSTICCHAIN_MAP:
				return localMethodIsUsed((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
		}
		return super.eInvoke(operationID, arguments);
	}

} //AbstractMethodDeclarationImpl
