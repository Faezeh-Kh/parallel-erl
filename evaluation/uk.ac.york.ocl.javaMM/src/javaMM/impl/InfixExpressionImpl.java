/**
 */
package javaMM.impl;

import java.lang.reflect.InvocationTargetException;

import java.util.Collection;
import java.util.Map;

import javaMM.Expression;
import javaMM.InfixExpression;
import javaMM.InfixExpressionKind;
import javaMM.JavaMMPackage;
import javaMM.JavaMMTables;

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

import org.eclipse.ocl.pivot.ids.EnumerationLiteralId;
import org.eclipse.ocl.pivot.ids.IdResolver;
import org.eclipse.ocl.pivot.ids.TypeId;

import org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal;

import org.eclipse.ocl.pivot.library.logical.BooleanAndOperation;
import org.eclipse.ocl.pivot.library.logical.BooleanNotOperation;
import org.eclipse.ocl.pivot.library.logical.BooleanOrOperation;

import org.eclipse.ocl.pivot.library.oclany.OclAnyOclIsTypeOfOperation;
import org.eclipse.ocl.pivot.library.oclany.OclAnyOclTypeOperation;
import org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation;

import org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation;
import org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation;

import org.eclipse.ocl.pivot.utilities.ClassUtil;
import org.eclipse.ocl.pivot.utilities.ValueUtil;

import org.eclipse.ocl.pivot.values.IntegerValue;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Infix Expression</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.InfixExpressionImpl#getOperator <em>Operator</em>}</li>
 *   <li>{@link javaMM.impl.InfixExpressionImpl#getRightOperand <em>Right Operand</em>}</li>
 *   <li>{@link javaMM.impl.InfixExpressionImpl#getLeftOperand <em>Left Operand</em>}</li>
 *   <li>{@link javaMM.impl.InfixExpressionImpl#getExtendedOperands <em>Extended Operands</em>}</li>
 * </ul>
 *
 * @generated
 */
public class InfixExpressionImpl extends ExpressionImpl implements InfixExpression {
	/**
	 * The default value of the '{@link #getOperator() <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOperator()
	 * @generated
	 * @ordered
	 */
	protected static final InfixExpressionKind OPERATOR_EDEFAULT = InfixExpressionKind.TIMES;

	/**
	 * The cached value of the '{@link #getOperator() <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOperator()
	 * @generated
	 * @ordered
	 */
	protected InfixExpressionKind operator = OPERATOR_EDEFAULT;

	/**
	 * The cached value of the '{@link #getRightOperand() <em>Right Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRightOperand()
	 * @generated
	 * @ordered
	 */
	protected Expression rightOperand;

	/**
	 * The cached value of the '{@link #getLeftOperand() <em>Left Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLeftOperand()
	 * @generated
	 * @ordered
	 */
	protected Expression leftOperand;

	/**
	 * The cached value of the '{@link #getExtendedOperands() <em>Extended Operands</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExtendedOperands()
	 * @generated
	 * @ordered
	 */
	protected EList<Expression> extendedOperands;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected InfixExpressionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.INFIX_EXPRESSION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InfixExpressionKind getOperator() {
		return operator;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOperator(InfixExpressionKind newOperator) {
		InfixExpressionKind oldOperator = operator;
		operator = newOperator == null ? OPERATOR_EDEFAULT : newOperator;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.INFIX_EXPRESSION__OPERATOR, oldOperator, operator));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Expression getRightOperand() {
		return rightOperand;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRightOperand(Expression newRightOperand, NotificationChain msgs) {
		Expression oldRightOperand = rightOperand;
		rightOperand = newRightOperand;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND, oldRightOperand, newRightOperand);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setRightOperand(Expression newRightOperand) {
		if (newRightOperand != rightOperand) {
			NotificationChain msgs = null;
			if (rightOperand != null)
				msgs = ((InternalEObject)rightOperand).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND, null, msgs);
			if (newRightOperand != null)
				msgs = ((InternalEObject)newRightOperand).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND, null, msgs);
			msgs = basicSetRightOperand(newRightOperand, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND, newRightOperand, newRightOperand));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Expression getLeftOperand() {
		return leftOperand;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetLeftOperand(Expression newLeftOperand, NotificationChain msgs) {
		Expression oldLeftOperand = leftOperand;
		leftOperand = newLeftOperand;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND, oldLeftOperand, newLeftOperand);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setLeftOperand(Expression newLeftOperand) {
		if (newLeftOperand != leftOperand) {
			NotificationChain msgs = null;
			if (leftOperand != null)
				msgs = ((InternalEObject)leftOperand).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND, null, msgs);
			if (newLeftOperand != null)
				msgs = ((InternalEObject)newLeftOperand).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND, null, msgs);
			msgs = basicSetLeftOperand(newLeftOperand, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND, newLeftOperand, newLeftOperand));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Expression> getExtendedOperands() {
		if (extendedOperands == null) {
			extendedOperands = new EObjectContainmentEList<Expression>(Expression.class, this, JavaMMPackage.INFIX_EXPRESSION__EXTENDED_OPERANDS);
		}
		return extendedOperands;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Boolean operatorIsEquality() {
		/**
		 * self.operator = InfixExpressionKind::EQUALS or self.operator = InfixExpressionKind::NOT_EQUALS
		 */
		final /*@NonInvalid*/ InfixExpressionKind operator_0 = this.getOperator();
		final /*@NonInvalid*/ EnumerationLiteralId BOXED_operator_0 = operator_0 == null ? null : JavaMMTables.ENUMid_InfixExpressionKind.getEnumerationLiteralId(ClassUtil.nonNullState(operator_0.getName()));
		final /*@NonInvalid*/ boolean eq = BOXED_operator_0 == JavaMMTables.ELITid_EQUALS;
		/*@NonInvalid*/ boolean or;
		if (eq) {
			or = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ boolean eq_0 = BOXED_operator_0 == JavaMMTables.ELITid_NOT_EQUALS;
			or = eq_0;
		}
		return or;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean equalsNotOnLiterals(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv equalsNotOnLiterals:
		 *   let
		 *     severity : Integer[1] = 'InfixExpression::equalsNotOnLiterals'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = operatorIsEquality() implies
		 *         self.leftOperand.oclIsTypeOf(self.rightOperand.oclType()) and
		 *         not (
		 *           self.leftOperand.oclIsTypeOf(StringLiteral) or
		 *           self.leftOperand.oclIsTypeOf(NumberLiteral) or
		 *           self.leftOperand.oclIsTypeOf(CharacterLiteral) or
		 *           self.leftOperand.oclIsTypeOf(BooleanLiteral) or
		 *           self.leftOperand.oclIsTypeOf(NullLiteral) or
		 *           self.leftOperand.oclIsTypeOf(TypeLiteral)
		 *         )
		 *       in
		 *         'InfixExpression::equalsNotOnLiterals'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_InfixExpression_c_c_equalsNotOnLiterals);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				final /*@NonInvalid*/ boolean operatorIsEquality = this.operatorIsEquality();
				/*@Thrown*/ Boolean result;
				if (operatorIsEquality) {
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
					try {
						final /*@NonInvalid*/ Expression leftOperand = this.getLeftOperand();
						final /*@NonInvalid*/ Expression rightOperand = this.getRightOperand();
						final /*@Thrown*/ org.eclipse.ocl.pivot.Class oclType = (org.eclipse.ocl.pivot.Class)OclAnyOclTypeOperation.INSTANCE.evaluate(executor, rightOperand);
						final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand, oclType).booleanValue();
						CAUGHT_oclIsTypeOf = oclIsTypeOf;
					}
					catch (Exception e) {
						CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
					}
					/*@Caught*/ /*@Nullable*/ Object CAUGHT_or_3;
					try {
						/*@Caught*/ /*@Nullable*/ Object CAUGHT_or_2;
						try {
							/*@Caught*/ /*@Nullable*/ Object CAUGHT_or_1;
							try {
								/*@Caught*/ /*@Nullable*/ Object CAUGHT_or_0;
								try {
									/*@Caught*/ /*@Nullable*/ Object CAUGHT_or;
									try {
										/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf_0;
										try {
											final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_StringLiteral = idResolver.getClass(JavaMMTables.CLSSid_StringLiteral, null);
											final /*@NonInvalid*/ Expression leftOperand_0 = this.getLeftOperand();
											final /*@Thrown*/ boolean oclIsTypeOf_0 = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand_0, TYP_javaMM_c_c_StringLiteral).booleanValue();
											CAUGHT_oclIsTypeOf_0 = oclIsTypeOf_0;
										}
										catch (Exception e) {
											CAUGHT_oclIsTypeOf_0 = ValueUtil.createInvalidValue(e);
										}
										/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf_1;
										try {
											final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_NumberLiteral = idResolver.getClass(JavaMMTables.CLSSid_NumberLiteral, null);
											final /*@NonInvalid*/ Expression leftOperand_1 = this.getLeftOperand();
											final /*@Thrown*/ boolean oclIsTypeOf_1 = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand_1, TYP_javaMM_c_c_NumberLiteral).booleanValue();
											CAUGHT_oclIsTypeOf_1 = oclIsTypeOf_1;
										}
										catch (Exception e) {
											CAUGHT_oclIsTypeOf_1 = ValueUtil.createInvalidValue(e);
										}
										final /*@Thrown*/ Boolean or = BooleanOrOperation.INSTANCE.evaluate(CAUGHT_oclIsTypeOf_0, CAUGHT_oclIsTypeOf_1);
										CAUGHT_or = or;
									}
									catch (Exception e) {
										CAUGHT_or = ValueUtil.createInvalidValue(e);
									}
									/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf_2;
									try {
										final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_CharacterLiteral = idResolver.getClass(JavaMMTables.CLSSid_CharacterLiteral, null);
										final /*@NonInvalid*/ Expression leftOperand_2 = this.getLeftOperand();
										final /*@Thrown*/ boolean oclIsTypeOf_2 = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand_2, TYP_javaMM_c_c_CharacterLiteral).booleanValue();
										CAUGHT_oclIsTypeOf_2 = oclIsTypeOf_2;
									}
									catch (Exception e) {
										CAUGHT_oclIsTypeOf_2 = ValueUtil.createInvalidValue(e);
									}
									final /*@Thrown*/ Boolean or_0 = BooleanOrOperation.INSTANCE.evaluate(CAUGHT_or, CAUGHT_oclIsTypeOf_2);
									CAUGHT_or_0 = or_0;
								}
								catch (Exception e) {
									CAUGHT_or_0 = ValueUtil.createInvalidValue(e);
								}
								/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf_3;
								try {
									final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_BooleanLiteral = idResolver.getClass(JavaMMTables.CLSSid_BooleanLiteral, null);
									final /*@NonInvalid*/ Expression leftOperand_3 = this.getLeftOperand();
									final /*@Thrown*/ boolean oclIsTypeOf_3 = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand_3, TYP_javaMM_c_c_BooleanLiteral).booleanValue();
									CAUGHT_oclIsTypeOf_3 = oclIsTypeOf_3;
								}
								catch (Exception e) {
									CAUGHT_oclIsTypeOf_3 = ValueUtil.createInvalidValue(e);
								}
								final /*@Thrown*/ Boolean or_1 = BooleanOrOperation.INSTANCE.evaluate(CAUGHT_or_0, CAUGHT_oclIsTypeOf_3);
								CAUGHT_or_1 = or_1;
							}
							catch (Exception e) {
								CAUGHT_or_1 = ValueUtil.createInvalidValue(e);
							}
							/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf_4;
							try {
								final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_NullLiteral = idResolver.getClass(JavaMMTables.CLSSid_NullLiteral, null);
								final /*@NonInvalid*/ Expression leftOperand_4 = this.getLeftOperand();
								final /*@Thrown*/ boolean oclIsTypeOf_4 = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand_4, TYP_javaMM_c_c_NullLiteral).booleanValue();
								CAUGHT_oclIsTypeOf_4 = oclIsTypeOf_4;
							}
							catch (Exception e) {
								CAUGHT_oclIsTypeOf_4 = ValueUtil.createInvalidValue(e);
							}
							final /*@Thrown*/ Boolean or_2 = BooleanOrOperation.INSTANCE.evaluate(CAUGHT_or_1, CAUGHT_oclIsTypeOf_4);
							CAUGHT_or_2 = or_2;
						}
						catch (Exception e) {
							CAUGHT_or_2 = ValueUtil.createInvalidValue(e);
						}
						/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf_5;
						try {
							final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_TypeLiteral_0 = idResolver.getClass(JavaMMTables.CLSSid_TypeLiteral, null);
							final /*@NonInvalid*/ Expression leftOperand_5 = this.getLeftOperand();
							final /*@Thrown*/ boolean oclIsTypeOf_5 = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand_5, TYP_javaMM_c_c_TypeLiteral_0).booleanValue();
							CAUGHT_oclIsTypeOf_5 = oclIsTypeOf_5;
						}
						catch (Exception e) {
							CAUGHT_oclIsTypeOf_5 = ValueUtil.createInvalidValue(e);
						}
						final /*@Thrown*/ Boolean or_3 = BooleanOrOperation.INSTANCE.evaluate(CAUGHT_or_2, CAUGHT_oclIsTypeOf_5);
						CAUGHT_or_3 = or_3;
					}
					catch (Exception e) {
						CAUGHT_or_3 = ValueUtil.createInvalidValue(e);
					}
					final /*@NonInvalid*/ Boolean not = BooleanNotOperation.INSTANCE.evaluate(CAUGHT_or_3);
					final /*@Thrown*/ Boolean and = BooleanAndOperation.INSTANCE.evaluate(CAUGHT_oclIsTypeOf, not);
					result = and;
				}
				else {
					result = ValueUtil.TRUE_VALUE;
				}
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_InfixExpression_c_c_equalsNotOnLiterals, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean noRedundantComparison(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv noRedundantComparison:
		 *   let
		 *     severity : Integer[1] = 'InfixExpression::noRedundantComparison'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[1] = self.leftOperand <> self.rightOperand
		 *       in
		 *         'InfixExpression::noRedundantComparison'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_InfixExpression_c_c_noRedundantComparison);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ Expression leftOperand = this.getLeftOperand();
			final /*@NonInvalid*/ Expression rightOperand = this.getRightOperand();
			final /*@NonInvalid*/ boolean result = (leftOperand != null) ? !leftOperand.equals(rightOperand) : (rightOperand != null);
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_InfixExpression_c_c_noRedundantComparison, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, JavaMMTables.INT_0).booleanValue();
			symbol_0 = logDiagnostic;
		}
		return Boolean.TRUE == symbol_0;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean equalsNotOnStrings(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv equalsNotOnStrings:
		 *   let
		 *     severity : Integer[1] = 'InfixExpression::equalsNotOnStrings'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let
		 *         result : Boolean[?] = operatorIsEquality() implies
		 *         not (
		 *           self.leftOperand.oclIsTypeOf(StringLiteral) or
		 *           self.rightOperand.oclIsTypeOf(StringLiteral)
		 *         )
		 *       in
		 *         'InfixExpression::equalsNotOnStrings'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IdResolver idResolver = executor.getIdResolver();
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_InfixExpression_c_c_equalsNotOnStrings);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			final /*@NonInvalid*/ boolean operatorIsEquality = this.operatorIsEquality();
			/*@NonInvalid*/ Boolean result;
			if (operatorIsEquality) {
				/*@Caught*/ /*@Nullable*/ Object CAUGHT_or;
				try {
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf;
					try {
						final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_StringLiteral_0 = idResolver.getClass(JavaMMTables.CLSSid_StringLiteral, null);
						final /*@NonInvalid*/ Expression leftOperand = this.getLeftOperand();
						final /*@Thrown*/ boolean oclIsTypeOf = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, leftOperand, TYP_javaMM_c_c_StringLiteral_0).booleanValue();
						CAUGHT_oclIsTypeOf = oclIsTypeOf;
					}
					catch (Exception e) {
						CAUGHT_oclIsTypeOf = ValueUtil.createInvalidValue(e);
					}
					/*@Caught*/ /*@NonNull*/ Object CAUGHT_oclIsTypeOf_0;
					try {
						final /*@NonInvalid*/ org.eclipse.ocl.pivot.Class TYP_javaMM_c_c_StringLiteral_1 = idResolver.getClass(JavaMMTables.CLSSid_StringLiteral, null);
						final /*@NonInvalid*/ Expression rightOperand = this.getRightOperand();
						final /*@Thrown*/ boolean oclIsTypeOf_0 = OclAnyOclIsTypeOfOperation.INSTANCE.evaluate(executor, rightOperand, TYP_javaMM_c_c_StringLiteral_1).booleanValue();
						CAUGHT_oclIsTypeOf_0 = oclIsTypeOf_0;
					}
					catch (Exception e) {
						CAUGHT_oclIsTypeOf_0 = ValueUtil.createInvalidValue(e);
					}
					final /*@Thrown*/ Boolean or = BooleanOrOperation.INSTANCE.evaluate(CAUGHT_oclIsTypeOf, CAUGHT_oclIsTypeOf_0);
					CAUGHT_or = or;
				}
				catch (Exception e) {
					CAUGHT_or = ValueUtil.createInvalidValue(e);
				}
				final /*@NonInvalid*/ Boolean not = BooleanNotOperation.INSTANCE.evaluate(CAUGHT_or);
				result = not;
			}
			else {
				result = ValueUtil.TRUE_VALUE;
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_InfixExpression_c_c_equalsNotOnStrings, this, (Object)null, diagnostics, context, (Object)null, severity_0, result, JavaMMTables.INT_0).booleanValue();
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
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND:
				return basicSetRightOperand(null, msgs);
			case JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND:
				return basicSetLeftOperand(null, msgs);
			case JavaMMPackage.INFIX_EXPRESSION__EXTENDED_OPERANDS:
				return ((InternalEList<?>)getExtendedOperands()).basicRemove(otherEnd, msgs);
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
			case JavaMMPackage.INFIX_EXPRESSION__OPERATOR:
				return getOperator();
			case JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND:
				return getRightOperand();
			case JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND:
				return getLeftOperand();
			case JavaMMPackage.INFIX_EXPRESSION__EXTENDED_OPERANDS:
				return getExtendedOperands();
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
			case JavaMMPackage.INFIX_EXPRESSION__OPERATOR:
				setOperator((InfixExpressionKind)newValue);
				return;
			case JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND:
				setRightOperand((Expression)newValue);
				return;
			case JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND:
				setLeftOperand((Expression)newValue);
				return;
			case JavaMMPackage.INFIX_EXPRESSION__EXTENDED_OPERANDS:
				getExtendedOperands().clear();
				getExtendedOperands().addAll((Collection<? extends Expression>)newValue);
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
			case JavaMMPackage.INFIX_EXPRESSION__OPERATOR:
				setOperator(OPERATOR_EDEFAULT);
				return;
			case JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND:
				setRightOperand((Expression)null);
				return;
			case JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND:
				setLeftOperand((Expression)null);
				return;
			case JavaMMPackage.INFIX_EXPRESSION__EXTENDED_OPERANDS:
				getExtendedOperands().clear();
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
			case JavaMMPackage.INFIX_EXPRESSION__OPERATOR:
				return operator != OPERATOR_EDEFAULT;
			case JavaMMPackage.INFIX_EXPRESSION__RIGHT_OPERAND:
				return rightOperand != null;
			case JavaMMPackage.INFIX_EXPRESSION__LEFT_OPERAND:
				return leftOperand != null;
			case JavaMMPackage.INFIX_EXPRESSION__EXTENDED_OPERANDS:
				return extendedOperands != null && !extendedOperands.isEmpty();
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
			case JavaMMPackage.INFIX_EXPRESSION___OPERATOR_IS_EQUALITY:
				return operatorIsEquality();
			case JavaMMPackage.INFIX_EXPRESSION___EQUALS_NOT_ON_LITERALS__DIAGNOSTICCHAIN_MAP:
				return equalsNotOnLiterals((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.INFIX_EXPRESSION___NO_REDUNDANT_COMPARISON__DIAGNOSTICCHAIN_MAP:
				return noRedundantComparison((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
			case JavaMMPackage.INFIX_EXPRESSION___EQUALS_NOT_ON_STRINGS__DIAGNOSTICCHAIN_MAP:
				return equalsNotOnStrings((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
		}
		return super.eInvoke(operationID, arguments);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (operator: ");
		result.append(operator);
		result.append(')');
		return result.toString();
	}

} //InfixExpressionImpl
