/**
 */
package javaMM.impl;

import java.util.Collection;

import javaMM.AbstractTypeDeclaration;
import javaMM.BodyDeclaration;
import javaMM.Comment;
import javaMM.JavaMMPackage;
import javaMM.Type;
import javaMM.TypeAccess;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectContainmentWithInverseEList;
import org.eclipse.emf.ecore.util.EObjectWithInverseResolvingEList;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Abstract Type Declaration</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.AbstractTypeDeclarationImpl#getUsagesInTypeAccess <em>Usages In Type Access</em>}</li>
 *   <li>{@link javaMM.impl.AbstractTypeDeclarationImpl#getBodyDeclarations <em>Body Declarations</em>}</li>
 *   <li>{@link javaMM.impl.AbstractTypeDeclarationImpl#getCommentsBeforeBody <em>Comments Before Body</em>}</li>
 *   <li>{@link javaMM.impl.AbstractTypeDeclarationImpl#getCommentsAfterBody <em>Comments After Body</em>}</li>
 *   <li>{@link javaMM.impl.AbstractTypeDeclarationImpl#getPackage <em>Package</em>}</li>
 *   <li>{@link javaMM.impl.AbstractTypeDeclarationImpl#getSuperInterfaces <em>Super Interfaces</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class AbstractTypeDeclarationImpl extends BodyDeclarationImpl implements AbstractTypeDeclaration {
	/**
	 * The cached value of the '{@link #getUsagesInTypeAccess() <em>Usages In Type Access</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUsagesInTypeAccess()
	 * @generated
	 * @ordered
	 */
	protected EList<TypeAccess> usagesInTypeAccess;

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
	 * The cached value of the '{@link #getCommentsBeforeBody() <em>Comments Before Body</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCommentsBeforeBody()
	 * @generated
	 * @ordered
	 */
	protected EList<Comment> commentsBeforeBody;

	/**
	 * The cached value of the '{@link #getCommentsAfterBody() <em>Comments After Body</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCommentsAfterBody()
	 * @generated
	 * @ordered
	 */
	protected EList<Comment> commentsAfterBody;

	/**
	 * The cached value of the '{@link #getSuperInterfaces() <em>Super Interfaces</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSuperInterfaces()
	 * @generated
	 * @ordered
	 */
	protected EList<TypeAccess> superInterfaces;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AbstractTypeDeclarationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.ABSTRACT_TYPE_DECLARATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TypeAccess> getUsagesInTypeAccess() {
		if (usagesInTypeAccess == null) {
			usagesInTypeAccess = new EObjectWithInverseResolvingEList<TypeAccess>(TypeAccess.class, this, JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS, JavaMMPackage.TYPE_ACCESS__TYPE);
		}
		return usagesInTypeAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BodyDeclaration> getBodyDeclarations() {
		if (bodyDeclarations == null) {
			bodyDeclarations = new EObjectContainmentWithInverseEList<BodyDeclaration>(BodyDeclaration.class, this, JavaMMPackage.ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS, JavaMMPackage.BODY_DECLARATION__ABSTRACT_TYPE_DECLARATION);
		}
		return bodyDeclarations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Comment> getCommentsBeforeBody() {
		if (commentsBeforeBody == null) {
			commentsBeforeBody = new EObjectContainmentEList<Comment>(Comment.class, this, JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY);
		}
		return commentsBeforeBody;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Comment> getCommentsAfterBody() {
		if (commentsAfterBody == null) {
			commentsAfterBody = new EObjectContainmentEList<Comment>(Comment.class, this, JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY);
		}
		return commentsAfterBody;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public javaMM.Package getPackage() {
		if (eContainerFeatureID() != JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE) return null;
		return (javaMM.Package)eInternalContainer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetPackage(javaMM.Package newPackage, NotificationChain msgs) {
		msgs = eBasicSetContainer((InternalEObject)newPackage, JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE, msgs);
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setPackage(javaMM.Package newPackage) {
		if (newPackage != eInternalContainer() || (eContainerFeatureID() != JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE && newPackage != null)) {
			if (EcoreUtil.isAncestor(this, newPackage))
				throw new IllegalArgumentException("Recursive containment not allowed for " + toString());
			NotificationChain msgs = null;
			if (eInternalContainer() != null)
				msgs = eBasicRemoveFromContainer(msgs);
			if (newPackage != null)
				msgs = ((InternalEObject)newPackage).eInverseAdd(this, JavaMMPackage.PACKAGE__OWNED_ELEMENTS, javaMM.Package.class, msgs);
			msgs = basicSetPackage(newPackage, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE, newPackage, newPackage));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TypeAccess> getSuperInterfaces() {
		if (superInterfaces == null) {
			superInterfaces = new EObjectContainmentEList<TypeAccess>(TypeAccess.class, this, JavaMMPackage.ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES);
		}
		return superInterfaces;
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
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getUsagesInTypeAccess()).basicAdd(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getBodyDeclarations()).basicAdd(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE:
				if (eInternalContainer() != null)
					msgs = eBasicRemoveFromContainer(msgs);
				return basicSetPackage((javaMM.Package)otherEnd, msgs);
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
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS:
				return ((InternalEList<?>)getUsagesInTypeAccess()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS:
				return ((InternalEList<?>)getBodyDeclarations()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY:
				return ((InternalEList<?>)getCommentsBeforeBody()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY:
				return ((InternalEList<?>)getCommentsAfterBody()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE:
				return basicSetPackage(null, msgs);
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES:
				return ((InternalEList<?>)getSuperInterfaces()).basicRemove(otherEnd, msgs);
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
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE:
				return eInternalContainer().eInverseRemove(this, JavaMMPackage.PACKAGE__OWNED_ELEMENTS, javaMM.Package.class, msgs);
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
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS:
				return getUsagesInTypeAccess();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS:
				return getBodyDeclarations();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY:
				return getCommentsBeforeBody();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY:
				return getCommentsAfterBody();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE:
				return getPackage();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES:
				return getSuperInterfaces();
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
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS:
				getUsagesInTypeAccess().clear();
				getUsagesInTypeAccess().addAll((Collection<? extends TypeAccess>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS:
				getBodyDeclarations().clear();
				getBodyDeclarations().addAll((Collection<? extends BodyDeclaration>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY:
				getCommentsBeforeBody().clear();
				getCommentsBeforeBody().addAll((Collection<? extends Comment>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY:
				getCommentsAfterBody().clear();
				getCommentsAfterBody().addAll((Collection<? extends Comment>)newValue);
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE:
				setPackage((javaMM.Package)newValue);
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES:
				getSuperInterfaces().clear();
				getSuperInterfaces().addAll((Collection<? extends TypeAccess>)newValue);
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
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS:
				getUsagesInTypeAccess().clear();
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS:
				getBodyDeclarations().clear();
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY:
				getCommentsBeforeBody().clear();
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY:
				getCommentsAfterBody().clear();
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE:
				setPackage((javaMM.Package)null);
				return;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES:
				getSuperInterfaces().clear();
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
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS:
				return usagesInTypeAccess != null && !usagesInTypeAccess.isEmpty();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__BODY_DECLARATIONS:
				return bodyDeclarations != null && !bodyDeclarations.isEmpty();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_BEFORE_BODY:
				return commentsBeforeBody != null && !commentsBeforeBody.isEmpty();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__COMMENTS_AFTER_BODY:
				return commentsAfterBody != null && !commentsAfterBody.isEmpty();
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__PACKAGE:
				return getPackage() != null;
			case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__SUPER_INTERFACES:
				return superInterfaces != null && !superInterfaces.isEmpty();
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
		if (baseClass == Type.class) {
			switch (derivedFeatureID) {
				case JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS: return JavaMMPackage.TYPE__USAGES_IN_TYPE_ACCESS;
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
		if (baseClass == Type.class) {
			switch (baseFeatureID) {
				case JavaMMPackage.TYPE__USAGES_IN_TYPE_ACCESS: return JavaMMPackage.ABSTRACT_TYPE_DECLARATION__USAGES_IN_TYPE_ACCESS;
				default: return -1;
			}
		}
		return super.eDerivedStructuralFeatureID(baseFeatureID, baseClass);
	}

} //AbstractTypeDeclarationImpl
