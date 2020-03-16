/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.Model#getName <em>Name</em>}</li>
 *   <li>{@link javaMM.Model#getOwnedElements <em>Owned Elements</em>}</li>
 *   <li>{@link javaMM.Model#getOrphanTypes <em>Orphan Types</em>}</li>
 *   <li>{@link javaMM.Model#getUnresolvedItems <em>Unresolved Items</em>}</li>
 *   <li>{@link javaMM.Model#getCompilationUnits <em>Compilation Units</em>}</li>
 *   <li>{@link javaMM.Model#getClassFiles <em>Class Files</em>}</li>
 *   <li>{@link javaMM.Model#getArchives <em>Archives</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getModel()
 * @model
 * @generated
 */
public interface Model extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see javaMM.JavaMMPackage#getModel_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link javaMM.Model#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Owned Elements</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Package}.
	 * It is bidirectional and its opposite is '{@link javaMM.Package#getModel <em>Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Owned Elements</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getModel_OwnedElements()
	 * @see javaMM.Package#getModel
	 * @model opposite="model" containment="true" ordered="false"
	 * @generated
	 */
	EList<javaMM.Package> getOwnedElements();

	/**
	 * Returns the value of the '<em><b>Orphan Types</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Type}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Orphan Types</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getModel_OrphanTypes()
	 * @model containment="true" ordered="false"
	 * @generated
	 */
	EList<Type> getOrphanTypes();

	/**
	 * Returns the value of the '<em><b>Unresolved Items</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.UnresolvedItem}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Unresolved Items</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getModel_UnresolvedItems()
	 * @model containment="true" ordered="false"
	 * @generated
	 */
	EList<UnresolvedItem> getUnresolvedItems();

	/**
	 * Returns the value of the '<em><b>Compilation Units</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.CompilationUnit}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Compilation Units</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getModel_CompilationUnits()
	 * @model containment="true" ordered="false"
	 * @generated
	 */
	EList<CompilationUnit> getCompilationUnits();

	/**
	 * Returns the value of the '<em><b>Class Files</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.ClassFile}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Class Files</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getModel_ClassFiles()
	 * @model containment="true" ordered="false"
	 * @generated
	 */
	EList<ClassFile> getClassFiles();

	/**
	 * Returns the value of the '<em><b>Archives</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Archive}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Archives</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getModel_Archives()
	 * @model containment="true" ordered="false"
	 * @generated
	 */
	EList<Archive> getArchives();

} // Model
