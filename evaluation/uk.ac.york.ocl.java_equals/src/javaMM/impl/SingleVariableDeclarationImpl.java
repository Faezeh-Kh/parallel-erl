/**
 */
package javaMM.impl;

import java.util.Collection;

import javaMM.AbstractMethodDeclaration;
import javaMM.Annotation;
import javaMM.CatchClause;
import javaMM.EnhancedForStatement;
import javaMM.JavaMMPackage;
import javaMM.Modifier;
import javaMM.SingleVariableDeclaration;
import javaMM.TypeAccess;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Single Variable Declaration</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.SingleVariableDeclarationImpl#getModifier <em>Modifier</em>}</li>
 *   <li>{@link javaMM.impl.SingleVariableDeclarationImpl#isVarargs <em>Varargs</em>}</li>
 *   <li>{@link javaMM.impl.SingleVariableDeclarationImpl#getType <em>Type</em>}</li>
 *   <li>{@link javaMM.impl.SingleVariableDeclarationImpl#getAnnotations <em>Annotations</em>}</li>
 *   <li>{@link javaMM.impl.SingleVariableDeclarationImpl#getMethodDeclaration <em>Method Declaration</em>}</li>
 *   <li>{@link javaMM.impl.SingleVariableDeclarationImpl#getCatchClause <em>Catch Clause</em>}</li>
 *   <li>{@link javaMM.impl.SingleVariableDeclarationImpl#getEnhancedForStatement <em>Enhanced For Statement</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SingleVariableDeclarationImpl extends VariableDeclarationImpl implements SingleVariableDeclaration {
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
	 * The default value of the '{@link #isVarargs() <em>Varargs</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isVarargs()
	 * @generated
	 * @ordered
	 */
	protected static final boolean VARARGS_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isVarargs() <em>Varargs</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isVarargs()
	 * @generated
	 * @ordered
	 */
	protected boolean varargs = VARARGS_EDEFAULT;

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
	protected SingleVariableDeclarationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.SINGLE_VARIABLE_DECLARATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
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
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER, oldModifier, newModifier);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setModifier(Modifier newModifier) {
		if (newModifier != modifier) {
			NotificationChain msgs = null;
			if (modifier != null)
				msgs = ((InternalEObject)modifier).eInverseRemove(this, JavaMMPackage.MODIFIER__SINGLE_VARIABLE_DECLARATION, Modifier.class, msgs);
			if (newModifier != null)
				msgs = ((InternalEObject)newModifier).eInverseAdd(this, JavaMMPackage.MODIFIER__SINGLE_VARIABLE_DECLARATION, Modifier.class, msgs);
			msgs = basicSetModifier(newModifier, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER, newModifier, newModifier));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isVarargs() {
		return varargs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVarargs(boolean newVarargs) {
		boolean oldVarargs = varargs;
		varargs = newVarargs;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__VARARGS, oldVarargs, varargs));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
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
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE, oldType, newType);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setType(TypeAccess newType) {
		if (newType != type) {
			NotificationChain msgs = null;
			if (type != null)
				msgs = ((InternalEObject)type).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE, null, msgs);
			if (newType != null)
				msgs = ((InternalEObject)newType).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE, null, msgs);
			msgs = basicSetType(newType, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE, newType, newType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Annotation> getAnnotations() {
		if (annotations == null) {
			annotations = new EObjectContainmentEList<Annotation>(Annotation.class, this, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ANNOTATIONS);
		}
		return annotations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AbstractMethodDeclaration getMethodDeclaration() {
		if (eContainerFeatureID() != JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION) return null;
		return (AbstractMethodDeclaration)eInternalContainer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetMethodDeclaration(AbstractMethodDeclaration newMethodDeclaration, NotificationChain msgs) {
		msgs = eBasicSetContainer((InternalEObject)newMethodDeclaration, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION, msgs);
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMethodDeclaration(AbstractMethodDeclaration newMethodDeclaration) {
		if (newMethodDeclaration != eInternalContainer() || (eContainerFeatureID() != JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION && newMethodDeclaration != null)) {
			if (EcoreUtil.isAncestor(this, newMethodDeclaration))
				throw new IllegalArgumentException("Recursive containment not allowed for " + toString());
			NotificationChain msgs = null;
			if (eInternalContainer() != null)
				msgs = eBasicRemoveFromContainer(msgs);
			if (newMethodDeclaration != null)
				msgs = ((InternalEObject)newMethodDeclaration).eInverseAdd(this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS, AbstractMethodDeclaration.class, msgs);
			msgs = basicSetMethodDeclaration(newMethodDeclaration, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION, newMethodDeclaration, newMethodDeclaration));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CatchClause getCatchClause() {
		if (eContainerFeatureID() != JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE) return null;
		return (CatchClause)eInternalContainer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetCatchClause(CatchClause newCatchClause, NotificationChain msgs) {
		msgs = eBasicSetContainer((InternalEObject)newCatchClause, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE, msgs);
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCatchClause(CatchClause newCatchClause) {
		if (newCatchClause != eInternalContainer() || (eContainerFeatureID() != JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE && newCatchClause != null)) {
			if (EcoreUtil.isAncestor(this, newCatchClause))
				throw new IllegalArgumentException("Recursive containment not allowed for " + toString());
			NotificationChain msgs = null;
			if (eInternalContainer() != null)
				msgs = eBasicRemoveFromContainer(msgs);
			if (newCatchClause != null)
				msgs = ((InternalEObject)newCatchClause).eInverseAdd(this, JavaMMPackage.CATCH_CLAUSE__EXCEPTION, CatchClause.class, msgs);
			msgs = basicSetCatchClause(newCatchClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE, newCatchClause, newCatchClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EnhancedForStatement getEnhancedForStatement() {
		if (eContainerFeatureID() != JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT) return null;
		return (EnhancedForStatement)eInternalContainer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetEnhancedForStatement(EnhancedForStatement newEnhancedForStatement, NotificationChain msgs) {
		msgs = eBasicSetContainer((InternalEObject)newEnhancedForStatement, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT, msgs);
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEnhancedForStatement(EnhancedForStatement newEnhancedForStatement) {
		if (newEnhancedForStatement != eInternalContainer() || (eContainerFeatureID() != JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT && newEnhancedForStatement != null)) {
			if (EcoreUtil.isAncestor(this, newEnhancedForStatement))
				throw new IllegalArgumentException("Recursive containment not allowed for " + toString());
			NotificationChain msgs = null;
			if (eInternalContainer() != null)
				msgs = eBasicRemoveFromContainer(msgs);
			if (newEnhancedForStatement != null)
				msgs = ((InternalEObject)newEnhancedForStatement).eInverseAdd(this, JavaMMPackage.ENHANCED_FOR_STATEMENT__PARAMETER, EnhancedForStatement.class, msgs);
			msgs = basicSetEnhancedForStatement(newEnhancedForStatement, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT, newEnhancedForStatement, newEnhancedForStatement));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER:
				if (modifier != null)
					msgs = ((InternalEObject)modifier).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER, null, msgs);
				return basicSetModifier((Modifier)otherEnd, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION:
				if (eInternalContainer() != null)
					msgs = eBasicRemoveFromContainer(msgs);
				return basicSetMethodDeclaration((AbstractMethodDeclaration)otherEnd, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE:
				if (eInternalContainer() != null)
					msgs = eBasicRemoveFromContainer(msgs);
				return basicSetCatchClause((CatchClause)otherEnd, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT:
				if (eInternalContainer() != null)
					msgs = eBasicRemoveFromContainer(msgs);
				return basicSetEnhancedForStatement((EnhancedForStatement)otherEnd, msgs);
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
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER:
				return basicSetModifier(null, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE:
				return basicSetType(null, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ANNOTATIONS:
				return ((InternalEList<?>)getAnnotations()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION:
				return basicSetMethodDeclaration(null, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE:
				return basicSetCatchClause(null, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT:
				return basicSetEnhancedForStatement(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eBasicRemoveFromContainerFeature(NotificationChain msgs) {
		switch (eContainerFeatureID()) {
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION:
				return eInternalContainer().eInverseRemove(this, JavaMMPackage.ABSTRACT_METHOD_DECLARATION__PARAMETERS, AbstractMethodDeclaration.class, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE:
				return eInternalContainer().eInverseRemove(this, JavaMMPackage.CATCH_CLAUSE__EXCEPTION, CatchClause.class, msgs);
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT:
				return eInternalContainer().eInverseRemove(this, JavaMMPackage.ENHANCED_FOR_STATEMENT__PARAMETER, EnhancedForStatement.class, msgs);
		}
		return super.eBasicRemoveFromContainerFeature(msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER:
				return getModifier();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__VARARGS:
				return isVarargs();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE:
				return getType();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ANNOTATIONS:
				return getAnnotations();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION:
				return getMethodDeclaration();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE:
				return getCatchClause();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT:
				return getEnhancedForStatement();
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
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER:
				setModifier((Modifier)newValue);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__VARARGS:
				setVarargs((Boolean)newValue);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE:
				setType((TypeAccess)newValue);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ANNOTATIONS:
				getAnnotations().clear();
				getAnnotations().addAll((Collection<? extends Annotation>)newValue);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION:
				setMethodDeclaration((AbstractMethodDeclaration)newValue);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE:
				setCatchClause((CatchClause)newValue);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT:
				setEnhancedForStatement((EnhancedForStatement)newValue);
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
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER:
				setModifier((Modifier)null);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__VARARGS:
				setVarargs(VARARGS_EDEFAULT);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE:
				setType((TypeAccess)null);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ANNOTATIONS:
				getAnnotations().clear();
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION:
				setMethodDeclaration((AbstractMethodDeclaration)null);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE:
				setCatchClause((CatchClause)null);
				return;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT:
				setEnhancedForStatement((EnhancedForStatement)null);
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
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__MODIFIER:
				return modifier != null;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__VARARGS:
				return varargs != VARARGS_EDEFAULT;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__TYPE:
				return type != null;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ANNOTATIONS:
				return annotations != null && !annotations.isEmpty();
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__METHOD_DECLARATION:
				return getMethodDeclaration() != null;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__CATCH_CLAUSE:
				return getCatchClause() != null;
			case JavaMMPackage.SINGLE_VARIABLE_DECLARATION__ENHANCED_FOR_STATEMENT:
				return getEnhancedForStatement() != null;
		}
		return super.eIsSet(featureID);
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
		result.append(" (varargs: ");
		result.append(varargs);
		result.append(')');
		return result.toString();
	}

} //SingleVariableDeclarationImpl
