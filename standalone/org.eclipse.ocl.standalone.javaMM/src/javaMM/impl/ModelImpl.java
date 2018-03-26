/**
 */
package javaMM.impl;

import java.util.Collection;

import javaMM.Archive;
import javaMM.ClassFile;
import javaMM.CompilationUnit;
import javaMM.JavaMMPackage;
import javaMM.Model;
import javaMM.Type;
import javaMM.UnresolvedItem;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectContainmentWithInverseEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link javaMM.impl.ModelImpl#getName <em>Name</em>}</li>
 *   <li>{@link javaMM.impl.ModelImpl#getOwnedElements <em>Owned Elements</em>}</li>
 *   <li>{@link javaMM.impl.ModelImpl#getOrphanTypes <em>Orphan Types</em>}</li>
 *   <li>{@link javaMM.impl.ModelImpl#getUnresolvedItems <em>Unresolved Items</em>}</li>
 *   <li>{@link javaMM.impl.ModelImpl#getCompilationUnits <em>Compilation Units</em>}</li>
 *   <li>{@link javaMM.impl.ModelImpl#getClassFiles <em>Class Files</em>}</li>
 *   <li>{@link javaMM.impl.ModelImpl#getArchives <em>Archives</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ModelImpl extends MinimalEObjectImpl.Container implements Model {
	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The cached value of the '{@link #getOwnedElements() <em>Owned Elements</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOwnedElements()
	 * @generated
	 * @ordered
	 */
	protected EList<javaMM.Package> ownedElements;

	/**
	 * The cached value of the '{@link #getOrphanTypes() <em>Orphan Types</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrphanTypes()
	 * @generated
	 * @ordered
	 */
	protected EList<Type> orphanTypes;

	/**
	 * The cached value of the '{@link #getUnresolvedItems() <em>Unresolved Items</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUnresolvedItems()
	 * @generated
	 * @ordered
	 */
	protected EList<UnresolvedItem> unresolvedItems;

	/**
	 * The cached value of the '{@link #getCompilationUnits() <em>Compilation Units</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCompilationUnits()
	 * @generated
	 * @ordered
	 */
	protected EList<CompilationUnit> compilationUnits;

	/**
	 * The cached value of the '{@link #getClassFiles() <em>Class Files</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getClassFiles()
	 * @generated
	 * @ordered
	 */
	protected EList<ClassFile> classFiles;

	/**
	 * The cached value of the '{@link #getArchives() <em>Archives</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getArchives()
	 * @generated
	 * @ordered
	 */
	protected EList<Archive> archives;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ModelImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return JavaMMPackage.Literals.MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, JavaMMPackage.MODEL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<javaMM.Package> getOwnedElements() {
		if (ownedElements == null) {
			ownedElements = new EObjectContainmentWithInverseEList<javaMM.Package>(javaMM.Package.class, this, JavaMMPackage.MODEL__OWNED_ELEMENTS, JavaMMPackage.PACKAGE__MODEL);
		}
		return ownedElements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Type> getOrphanTypes() {
		if (orphanTypes == null) {
			orphanTypes = new EObjectContainmentEList<Type>(Type.class, this, JavaMMPackage.MODEL__ORPHAN_TYPES);
		}
		return orphanTypes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<UnresolvedItem> getUnresolvedItems() {
		if (unresolvedItems == null) {
			unresolvedItems = new EObjectContainmentEList<UnresolvedItem>(UnresolvedItem.class, this, JavaMMPackage.MODEL__UNRESOLVED_ITEMS);
		}
		return unresolvedItems;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<CompilationUnit> getCompilationUnits() {
		if (compilationUnits == null) {
			compilationUnits = new EObjectContainmentEList<CompilationUnit>(CompilationUnit.class, this, JavaMMPackage.MODEL__COMPILATION_UNITS);
		}
		return compilationUnits;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ClassFile> getClassFiles() {
		if (classFiles == null) {
			classFiles = new EObjectContainmentEList<ClassFile>(ClassFile.class, this, JavaMMPackage.MODEL__CLASS_FILES);
		}
		return classFiles;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Archive> getArchives() {
		if (archives == null) {
			archives = new EObjectContainmentEList<Archive>(Archive.class, this, JavaMMPackage.MODEL__ARCHIVES);
		}
		return archives;
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
			case JavaMMPackage.MODEL__OWNED_ELEMENTS:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getOwnedElements()).basicAdd(otherEnd, msgs);
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
			case JavaMMPackage.MODEL__OWNED_ELEMENTS:
				return ((InternalEList<?>)getOwnedElements()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.MODEL__ORPHAN_TYPES:
				return ((InternalEList<?>)getOrphanTypes()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.MODEL__UNRESOLVED_ITEMS:
				return ((InternalEList<?>)getUnresolvedItems()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.MODEL__COMPILATION_UNITS:
				return ((InternalEList<?>)getCompilationUnits()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.MODEL__CLASS_FILES:
				return ((InternalEList<?>)getClassFiles()).basicRemove(otherEnd, msgs);
			case JavaMMPackage.MODEL__ARCHIVES:
				return ((InternalEList<?>)getArchives()).basicRemove(otherEnd, msgs);
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
			case JavaMMPackage.MODEL__NAME:
				return getName();
			case JavaMMPackage.MODEL__OWNED_ELEMENTS:
				return getOwnedElements();
			case JavaMMPackage.MODEL__ORPHAN_TYPES:
				return getOrphanTypes();
			case JavaMMPackage.MODEL__UNRESOLVED_ITEMS:
				return getUnresolvedItems();
			case JavaMMPackage.MODEL__COMPILATION_UNITS:
				return getCompilationUnits();
			case JavaMMPackage.MODEL__CLASS_FILES:
				return getClassFiles();
			case JavaMMPackage.MODEL__ARCHIVES:
				return getArchives();
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
			case JavaMMPackage.MODEL__NAME:
				setName((String)newValue);
				return;
			case JavaMMPackage.MODEL__OWNED_ELEMENTS:
				getOwnedElements().clear();
				getOwnedElements().addAll((Collection<? extends javaMM.Package>)newValue);
				return;
			case JavaMMPackage.MODEL__ORPHAN_TYPES:
				getOrphanTypes().clear();
				getOrphanTypes().addAll((Collection<? extends Type>)newValue);
				return;
			case JavaMMPackage.MODEL__UNRESOLVED_ITEMS:
				getUnresolvedItems().clear();
				getUnresolvedItems().addAll((Collection<? extends UnresolvedItem>)newValue);
				return;
			case JavaMMPackage.MODEL__COMPILATION_UNITS:
				getCompilationUnits().clear();
				getCompilationUnits().addAll((Collection<? extends CompilationUnit>)newValue);
				return;
			case JavaMMPackage.MODEL__CLASS_FILES:
				getClassFiles().clear();
				getClassFiles().addAll((Collection<? extends ClassFile>)newValue);
				return;
			case JavaMMPackage.MODEL__ARCHIVES:
				getArchives().clear();
				getArchives().addAll((Collection<? extends Archive>)newValue);
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
			case JavaMMPackage.MODEL__NAME:
				setName(NAME_EDEFAULT);
				return;
			case JavaMMPackage.MODEL__OWNED_ELEMENTS:
				getOwnedElements().clear();
				return;
			case JavaMMPackage.MODEL__ORPHAN_TYPES:
				getOrphanTypes().clear();
				return;
			case JavaMMPackage.MODEL__UNRESOLVED_ITEMS:
				getUnresolvedItems().clear();
				return;
			case JavaMMPackage.MODEL__COMPILATION_UNITS:
				getCompilationUnits().clear();
				return;
			case JavaMMPackage.MODEL__CLASS_FILES:
				getClassFiles().clear();
				return;
			case JavaMMPackage.MODEL__ARCHIVES:
				getArchives().clear();
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
			case JavaMMPackage.MODEL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case JavaMMPackage.MODEL__OWNED_ELEMENTS:
				return ownedElements != null && !ownedElements.isEmpty();
			case JavaMMPackage.MODEL__ORPHAN_TYPES:
				return orphanTypes != null && !orphanTypes.isEmpty();
			case JavaMMPackage.MODEL__UNRESOLVED_ITEMS:
				return unresolvedItems != null && !unresolvedItems.isEmpty();
			case JavaMMPackage.MODEL__COMPILATION_UNITS:
				return compilationUnits != null && !compilationUnits.isEmpty();
			case JavaMMPackage.MODEL__CLASS_FILES:
				return classFiles != null && !classFiles.isEmpty();
			case JavaMMPackage.MODEL__ARCHIVES:
				return archives != null && !archives.isEmpty();
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
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //ModelImpl
