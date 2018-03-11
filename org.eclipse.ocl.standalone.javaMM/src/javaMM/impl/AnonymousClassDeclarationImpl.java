/**
 */
package javaMM.impl;

import java.util.Collection;

import javaMM.AnonymousClassDeclaration;
import javaMM.BodyDeclaration;
import javaMM.ClassInstanceCreation;
import javaMM.JavaMMPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentWithInverseEList;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Anonymous Class Declaration</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.AnonymousClassDeclarationImpl#getBodyDeclarations <em>Body Declarations</em>}</li>
 *   <li>{@link javaMM.impl.AnonymousClassDeclarationImpl#getClassInstanceCreation <em>Class Instance Creation</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AnonymousClassDeclarationImpl extends ASTNodeImpl implements AnonymousClassDeclaration {
	/**
	 * The cached value of the '{@link #getBodyDeclarations() <em>Body Declarations</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBodyDeclarations()
	 * @generated
	 * @ordered
	 */
	protected EList<BodyDeclaration> bodyDeclarations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AnonymousClassDeclarationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.ANONYMOUS_CLASS_DECLARATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BodyDeclaration> getBodyDeclarations() {
		if (bodyDeclarations == null) {
			bodyDeclarations = new EObjectContainmentWithInverseEList<BodyDeclaration>(BodyDeclaration.class, this, JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS, JavaMMPackage.BODY_DECLARATION__ANONYMOUS_CLASS_DECLARATION_OWNER);
		}
		return bodyDeclarations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ClassInstanceCreation getClassInstanceCreation() {
		if (eContainerFeatureID() != JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION) return null;
		return (ClassInstanceCreation)eInternalContainer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetClassInstanceCreation(ClassInstanceCreation newClassInstanceCreation, NotificationChain msgs) {
		msgs = eBasicSetContainer((InternalEObject)newClassInstanceCreation, JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION, msgs);
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setClassInstanceCreation(ClassInstanceCreation newClassInstanceCreation) {
		if (newClassInstanceCreation != eInternalContainer() || (eContainerFeatureID() != JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION && newClassInstanceCreation != null)) {
			if (EcoreUtil.isAncestor(this, newClassInstanceCreation))
				throw new IllegalArgumentException("Recursive containment not allowed for " + toString());
			NotificationChain msgs = null;
			if (eInternalContainer() != null)
				msgs = eBasicRemoveFromContainer(msgs);
			if (newClassInstanceCreation != null)
				msgs = ((InternalEObject)newClassInstanceCreation).eInverseAdd(this, JavaMMPackage.CLASS_INSTANCE_CREATION__ANONYMOUS_CLASS_DECLARATION, ClassInstanceCreation.class, msgs);
			msgs = basicSetClassInstanceCreation(newClassInstanceCreation, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION, newClassInstanceCreation, newClassInstanceCreation));
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
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getBodyDeclarations()).basicAdd(otherEnd, msgs);
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION:
				if (eInternalContainer() != null)
					msgs = eBasicRemoveFromContainer(msgs);
				return basicSetClassInstanceCreation((ClassInstanceCreation)otherEnd, msgs);
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
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS:
				return ((InternalEList<?>)getBodyDeclarations()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION:
				return basicSetClassInstanceCreation(null, msgs);
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
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION:
				return eInternalContainer().eInverseRemove(this, JavaMMPackage.CLASS_INSTANCE_CREATION__ANONYMOUS_CLASS_DECLARATION, ClassInstanceCreation.class, msgs);
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
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS:
				return getBodyDeclarations();
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION:
				return getClassInstanceCreation();
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
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS:
				getBodyDeclarations().clear();
				getBodyDeclarations().addAll((Collection<? extends BodyDeclaration>)newValue);
				return;
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION:
				setClassInstanceCreation((ClassInstanceCreation)newValue);
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
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS:
				getBodyDeclarations().clear();
				return;
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION:
				setClassInstanceCreation((ClassInstanceCreation)null);
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
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__BODY_DECLARATIONS:
				return bodyDeclarations != null && !bodyDeclarations.isEmpty();
			case JavaMMPackage.ANONYMOUS_CLASS_DECLARATION__CLASS_INSTANCE_CREATION:
				return getClassInstanceCreation() != null;
		}
		return super.eIsSet(featureID);
	}

} //AnonymousClassDeclarationImpl
