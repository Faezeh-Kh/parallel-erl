/**
 */
package javaMM.impl;

import java.lang.reflect.InvocationTargetException;

import java.util.Collection;
import java.util.Map;

import javaMM.AbstractVariablesContainer;
import javaMM.Annotation;
import javaMM.InheritanceKind;
import javaMM.JavaMMPackage;
import javaMM.JavaMMTables;
import javaMM.Modifier;
import javaMM.TypeAccess;
import javaMM.VariableDeclarationExpression;
import javaMM.VariableDeclarationFragment;
import javaMM.VisibilityKind;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.DiagnosticChain;
import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectContainmentWithInverseEList;
import org.eclipse.emf.ecore.util.InternalEList;

import org.eclipse.ocl.pivot.evaluation.Executor;

import org.eclipse.ocl.pivot.ids.EnumerationLiteralId;
import org.eclipse.ocl.pivot.ids.TypeId;

import org.eclipse.ocl.pivot.internal.utilities.PivotUtilInternal;

import org.eclipse.ocl.pivot.library.logical.BooleanImpliesOperation;

import org.eclipse.ocl.pivot.library.oclany.OclComparableLessThanEqualOperation;

import org.eclipse.ocl.pivot.library.string.CGStringGetSeverityOperation;
import org.eclipse.ocl.pivot.library.string.CGStringLogDiagnosticOperation;

import org.eclipse.ocl.pivot.utilities.ClassUtil;
import org.eclipse.ocl.pivot.utilities.ValueUtil;

import org.eclipse.ocl.pivot.values.IntegerValue;
import org.eclipse.ocl.pivot.values.InvalidValueException;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Variable Declaration Expression</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.VariableDeclarationExpressionImpl#getType <em>Type</em>}</li>
 *   <li>{@link javaMM.impl.VariableDeclarationExpressionImpl#getFragments <em>Fragments</em>}</li>
 *   <li>{@link javaMM.impl.VariableDeclarationExpressionImpl#getModifier <em>Modifier</em>}</li>
 *   <li>{@link javaMM.impl.VariableDeclarationExpressionImpl#getAnnotations <em>Annotations</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VariableDeclarationExpressionImpl extends ExpressionImpl implements VariableDeclarationExpression {
	/**
	 * The cached value of the '{@link #getType() <em>Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getType()
	 * @generated
	 * @ordered
	 */
	protected TypeAccess type;

	/**
	 * The cached value of the '{@link #getFragments() <em>Fragments</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFragments()
	 * @generated
	 * @ordered
	 */
	protected EList<VariableDeclarationFragment> fragments;

	/**
	 * The cached value of the '{@link #getModifier() <em>Modifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getModifier()
	 * @generated
	 * @ordered
	 */
	protected Modifier modifier;

	/**
	 * The cached value of the '{@link #getAnnotations() <em>Annotations</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAnnotations()
	 * @generated
	 * @ordered
	 */
	protected EList<Annotation> annotations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VariableDeclarationExpressionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.VARIABLE_DECLARATION_EXPRESSION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TypeAccess getType() {
		return type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetType(TypeAccess newType, NotificationChain msgs) {
		TypeAccess oldType = type;
		type = newType;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE, oldType, newType);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setType(TypeAccess newType) {
		if (newType != type) {
			NotificationChain msgs = null;
			if (type != null)
				msgs = ((InternalEObject)type).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE, null, msgs);
			if (newType != null)
				msgs = ((InternalEObject)newType).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE, null, msgs);
			msgs = basicSetType(newType, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE, newType, newType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VariableDeclarationFragment> getFragments() {
		if (fragments == null) {
			fragments = new EObjectContainmentWithInverseEList<VariableDeclarationFragment>(VariableDeclarationFragment.class, this, JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS, JavaMMPackage.VARIABLE_DECLARATION_FRAGMENT__VARIABLES_CONTAINER);
		}
		return fragments;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Modifier getModifier() {
		return modifier;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetModifier(Modifier newModifier, NotificationChain msgs) {
		Modifier oldModifier = modifier;
		modifier = newModifier;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER, oldModifier, newModifier);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setModifier(Modifier newModifier) {
		if (newModifier != modifier) {
			NotificationChain msgs = null;
			if (modifier != null)
				msgs = ((InternalEObject)modifier).eInverseRemove(this, JavaMMPackage.MODIFIER__VARIABLE_DECLARATION_EXPRESSION, Modifier.class, msgs);
			if (newModifier != null)
				msgs = ((InternalEObject)newModifier).eInverseAdd(this, JavaMMPackage.MODIFIER__VARIABLE_DECLARATION_EXPRESSION, Modifier.class, msgs);
			msgs = basicSetModifier(newModifier, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER, newModifier, newModifier));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Annotation> getAnnotations() {
		if (annotations == null) {
			annotations = new EObjectContainmentEList<Annotation>(Annotation.class, this, JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS);
		}
		return annotations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean publicVariableIsFinal(final DiagnosticChain diagnostics, final Map<Object, Object> context) {
		/**
		 *
		 * inv publicVariableIsFinal:
		 *   let
		 *     severity : Integer[1] = 'VariableDeclarationExpression::publicVariableIsFinal'.getSeverity()
		 *   in
		 *     if severity <= 0
		 *     then true
		 *     else
		 *       let result : Boolean[?] = self.modifier.visibility = VisibilityKind::public implies self.modifier.inheritance = InheritanceKind::final
		 *       in
		 *         'VariableDeclarationExpression::publicVariableIsFinal'.logDiagnostic(self, null, diagnostics, context, null, severity, result, 0)
		 *     endif
		 */
		final /*@NonInvalid*/ Executor executor = PivotUtilInternal.getExecutor(this);
		final /*@NonInvalid*/ IntegerValue severity_0 = CGStringGetSeverityOperation.INSTANCE.evaluate(executor, JavaMMTables.STR_VariableDeclarationExpression_c_c_publicVariableIsFinal);
		final /*@NonInvalid*/ boolean le = OclComparableLessThanEqualOperation.INSTANCE.evaluate(executor, severity_0, JavaMMTables.INT_0).booleanValue();
		/*@NonInvalid*/ boolean symbol_0;
		if (le) {
			symbol_0 = ValueUtil.TRUE_VALUE;
		}
		else {
			/*@Caught*/ /*@Nullable*/ Object CAUGHT_result;
			try {
				/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq;
				try {
					final /*@NonInvalid*/ Modifier modifier = this.getModifier();
					if (modifier == null) {
						throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::visibility\'");
					}
					final /*@Thrown*/ VisibilityKind visibility = modifier.getVisibility();
					final /*@Thrown*/ EnumerationLiteralId BOXED_visibility = visibility == null ? null : JavaMMTables.ENUMid_VisibilityKind.getEnumerationLiteralId(ClassUtil.nonNullState(visibility.getName()));
					final /*@Thrown*/ boolean eq = BOXED_visibility == JavaMMTables.ELITid_public;
					CAUGHT_eq = eq;
				}
				catch (Exception e) {
					CAUGHT_eq = ValueUtil.createInvalidValue(e);
				}
				/*@Caught*/ /*@NonNull*/ Object CAUGHT_eq_0;
				try {
					final /*@NonInvalid*/ Modifier modifier_0 = this.getModifier();
					if (modifier_0 == null) {
						throw new InvalidValueException("Null source for \'\'http://www.eclipse.org/MoDisco/Java/0.2.incubation/java\'::Modifier::inheritance\'");
					}
					final /*@Thrown*/ InheritanceKind inheritance = modifier_0.getInheritance();
					final /*@Thrown*/ EnumerationLiteralId BOXED_inheritance = inheritance == null ? null : JavaMMTables.ENUMid_InheritanceKind.getEnumerationLiteralId(ClassUtil.nonNullState(inheritance.getName()));
					final /*@Thrown*/ boolean eq_0 = BOXED_inheritance == JavaMMTables.ELITid_final;
					CAUGHT_eq_0 = eq_0;
				}
				catch (Exception e) {
					CAUGHT_eq_0 = ValueUtil.createInvalidValue(e);
				}
				final /*@Thrown*/ Boolean result = BooleanImpliesOperation.INSTANCE.evaluate(CAUGHT_eq, CAUGHT_eq_0);
				CAUGHT_result = result;
			}
			catch (Exception e) {
				CAUGHT_result = ValueUtil.createInvalidValue(e);
			}
			final /*@NonInvalid*/ boolean logDiagnostic = CGStringLogDiagnosticOperation.INSTANCE.evaluate(executor, TypeId.BOOLEAN, JavaMMTables.STR_VariableDeclarationExpression_c_c_publicVariableIsFinal, this, (Object)null, diagnostics, context, (Object)null, severity_0, CAUGHT_result, JavaMMTables.INT_0).booleanValue();
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
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getFragments()).basicAdd(otherEnd, msgs);
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER:
				if (modifier != null)
					msgs = ((InternalEObject)modifier).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER, null, msgs);
				return basicSetModifier((Modifier)otherEnd, msgs);
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
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE:
				return basicSetType(null, msgs);
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS:
				return ((InternalEList<?>)getFragments()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER:
				return basicSetModifier(null, msgs);
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS:
				return ((InternalEList<?>)getAnnotations()).basicRemove(otherEnd, msgs);
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
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE:
				return getType();
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS:
				return getFragments();
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER:
				return getModifier();
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS:
				return getAnnotations();
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
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE:
				setType((TypeAccess)newValue);
				return;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS:
				getFragments().clear();
				getFragments().addAll((Collection<? extends VariableDeclarationFragment>)newValue);
				return;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER:
				setModifier((Modifier)newValue);
				return;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS:
				getAnnotations().clear();
				getAnnotations().addAll((Collection<? extends Annotation>)newValue);
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
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE:
				setType((TypeAccess)null);
				return;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS:
				getFragments().clear();
				return;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER:
				setModifier((Modifier)null);
				return;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS:
				getAnnotations().clear();
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
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE:
				return type != null;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS:
				return fragments != null && !fragments.isEmpty();
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__MODIFIER:
				return modifier != null;
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__ANNOTATIONS:
				return annotations != null && !annotations.isEmpty();
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
		if (baseClass == AbstractVariablesContainer.class) {
			switch (derivedFeatureID) {
				case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE: return JavaMMPackage.ABSTRACT_VARIABLES_CONTAINER__TYPE;
				case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS: return JavaMMPackage.ABSTRACT_VARIABLES_CONTAINER__FRAGMENTS;
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
		if (baseClass == AbstractVariablesContainer.class) {
			switch (baseFeatureID) {
				case JavaMMPackage.ABSTRACT_VARIABLES_CONTAINER__TYPE: return JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__TYPE;
				case JavaMMPackage.ABSTRACT_VARIABLES_CONTAINER__FRAGMENTS: return JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION__FRAGMENTS;
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
			case JavaMMPackage.VARIABLE_DECLARATION_EXPRESSION___PUBLIC_VARIABLE_IS_FINAL__DIAGNOSTICCHAIN_MAP:
				return publicVariableIsFinal((DiagnosticChain)arguments.get(0), (Map<Object, Object>)arguments.get(1));
		}
		return super.eInvoke(operationID, arguments);
	}

} //VariableDeclarationExpressionImpl
