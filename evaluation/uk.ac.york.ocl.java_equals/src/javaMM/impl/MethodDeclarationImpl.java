/**
 */
package javaMM.impl;

import java.util.Collection;

import javaMM.JavaMMPackage;
import javaMM.MethodDeclaration;
import javaMM.TypeAccess;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectWithInverseResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Method Declaration</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.MethodDeclarationImpl#getExtraArrayDimensions <em>Extra Array Dimensions</em>}</li>
 *   <li>{@link javaMM.impl.MethodDeclarationImpl#getReturnType <em>Return Type</em>}</li>
 *   <li>{@link javaMM.impl.MethodDeclarationImpl#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}</li>
 *   <li>{@link javaMM.impl.MethodDeclarationImpl#getRedefinitions <em>Redefinitions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MethodDeclarationImpl extends AbstractMethodDeclarationImpl implements MethodDeclaration {
	/**
	 * The default value of the '{@link #getExtraArrayDimensions() <em>Extra Array Dimensions</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExtraArrayDimensions()
	 * @generated
	 * @ordered
	 */
	protected static final int EXTRA_ARRAY_DIMENSIONS_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getExtraArrayDimensions() <em>Extra Array Dimensions</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExtraArrayDimensions()
	 * @generated
	 * @ordered
	 */
	protected int extraArrayDimensions = EXTRA_ARRAY_DIMENSIONS_EDEFAULT;

	/**
	 * The cached value of the '{@link #getReturnType() <em>Return Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReturnType()
	 * @generated
	 * @ordered
	 */
	protected TypeAccess returnType;

	/**
	 * The cached value of the '{@link #getRedefinedMethodDeclaration() <em>Redefined Method Declaration</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRedefinedMethodDeclaration()
	 * @generated
	 * @ordered
	 */
	protected MethodDeclaration redefinedMethodDeclaration;

	/**
	 * The cached value of the '{@link #getRedefinitions() <em>Redefinitions</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRedefinitions()
	 * @generated
	 * @ordered
	 */
	protected EList<MethodDeclaration> redefinitions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MethodDeclarationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.METHOD_DECLARATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int getExtraArrayDimensions() {
		return extraArrayDimensions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setExtraArrayDimensions(int newExtraArrayDimensions) {
		int oldExtraArrayDimensions = extraArrayDimensions;
		extraArrayDimensions = newExtraArrayDimensions;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS, oldExtraArrayDimensions, extraArrayDimensions));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TypeAccess getReturnType() {
		return returnType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetReturnType(TypeAccess newReturnType, NotificationChain msgs) {
		TypeAccess oldReturnType = returnType;
		returnType = newReturnType;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE, oldReturnType, newReturnType);
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
	public void setReturnType(TypeAccess newReturnType) {
		if (newReturnType != returnType) {
			NotificationChain msgs = null;
			if (returnType != null)
				msgs = ((InternalEObject)returnType).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE, null, msgs);
			if (newReturnType != null)
				msgs = ((InternalEObject)newReturnType).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE, null, msgs);
			msgs = basicSetReturnType(newReturnType, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE, newReturnType, newReturnType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MethodDeclaration getRedefinedMethodDeclaration() {
		if (redefinedMethodDeclaration != null && redefinedMethodDeclaration.eIsProxy()) {
			InternalEObject oldRedefinedMethodDeclaration = (InternalEObject)redefinedMethodDeclaration;
			redefinedMethodDeclaration = (MethodDeclaration)eResolveProxy(oldRedefinedMethodDeclaration);
			if (redefinedMethodDeclaration != oldRedefinedMethodDeclaration) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION, oldRedefinedMethodDeclaration, redefinedMethodDeclaration));
			}
		}
		return redefinedMethodDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MethodDeclaration basicGetRedefinedMethodDeclaration() {
		return redefinedMethodDeclaration;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRedefinedMethodDeclaration(MethodDeclaration newRedefinedMethodDeclaration, NotificationChain msgs) {
		MethodDeclaration oldRedefinedMethodDeclaration = redefinedMethodDeclaration;
		redefinedMethodDeclaration = newRedefinedMethodDeclaration;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION, oldRedefinedMethodDeclaration, newRedefinedMethodDeclaration);
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
	public void setRedefinedMethodDeclaration(MethodDeclaration newRedefinedMethodDeclaration) {
		if (newRedefinedMethodDeclaration != redefinedMethodDeclaration) {
			NotificationChain msgs = null;
			if (redefinedMethodDeclaration != null)
				msgs = ((InternalEObject)redefinedMethodDeclaration).eInverseRemove(this, JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS, MethodDeclaration.class, msgs);
			if (newRedefinedMethodDeclaration != null)
				msgs = ((InternalEObject)newRedefinedMethodDeclaration).eInverseAdd(this, JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS, MethodDeclaration.class, msgs);
			msgs = basicSetRedefinedMethodDeclaration(newRedefinedMethodDeclaration, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION, newRedefinedMethodDeclaration, newRedefinedMethodDeclaration));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MethodDeclaration> getRedefinitions() {
		if (redefinitions == null) {
			redefinitions = new EObjectWithInverseResolvingEList<MethodDeclaration>(MethodDeclaration.class, this, JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS, JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION);
		}
		return redefinitions;
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
			case JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION:
				if (redefinedMethodDeclaration != null)
					msgs = ((InternalEObject)redefinedMethodDeclaration).eInverseRemove(this, JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS, MethodDeclaration.class, msgs);
				return basicSetRedefinedMethodDeclaration((MethodDeclaration)otherEnd, msgs);
			case JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getRedefinitions()).basicAdd(otherEnd, msgs);
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
			case JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE:
				return basicSetReturnType(null, msgs);
			case JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION:
				return basicSetRedefinedMethodDeclaration(null, msgs);
			case JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS:
				return ((InternalEList<?>)getRedefinitions()).basicRemove(otherEnd, msgs);
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
			case JavaMMPackage.METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS:
				return getExtraArrayDimensions();
			case JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE:
				return getReturnType();
			case JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION:
				if (resolve) return getRedefinedMethodDeclaration();
				return basicGetRedefinedMethodDeclaration();
			case JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS:
				return getRedefinitions();
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
			case JavaMMPackage.METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS:
				setExtraArrayDimensions((Integer)newValue);
				return;
			case JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE:
				setReturnType((TypeAccess)newValue);
				return;
			case JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION:
				setRedefinedMethodDeclaration((MethodDeclaration)newValue);
				return;
			case JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS:
				getRedefinitions().clear();
				getRedefinitions().addAll((Collection<? extends MethodDeclaration>)newValue);
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
			case JavaMMPackage.METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS:
				setExtraArrayDimensions(EXTRA_ARRAY_DIMENSIONS_EDEFAULT);
				return;
			case JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE:
				setReturnType((TypeAccess)null);
				return;
			case JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION:
				setRedefinedMethodDeclaration((MethodDeclaration)null);
				return;
			case JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS:
				getRedefinitions().clear();
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
			case JavaMMPackage.METHOD_DECLARATION__EXTRA_ARRAY_DIMENSIONS:
				return extraArrayDimensions != EXTRA_ARRAY_DIMENSIONS_EDEFAULT;
			case JavaMMPackage.METHOD_DECLARATION__RETURN_TYPE:
				return returnType != null;
			case JavaMMPackage.METHOD_DECLARATION__REDEFINED_METHOD_DECLARATION:
				return redefinedMethodDeclaration != null;
			case JavaMMPackage.METHOD_DECLARATION__REDEFINITIONS:
				return redefinitions != null && !redefinitions.isEmpty();
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
		result.append(" (extraArrayDimensions: ");
		result.append(extraArrayDimensions);
		result.append(')');
		return result.toString();
	}

} //MethodDeclarationImpl
