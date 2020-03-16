/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Named Element</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.NamedElement#getName <em>Name</em>}</li>
 *   <li>{@link javaMM.NamedElement#isProxy <em>Proxy</em>}</li>
 *   <li>{@link javaMM.NamedElement#getUsagesInImports <em>Usages In Imports</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getNamedElement()
 * @model abstract="true"
 * @generated
 */
public interface NamedElement extends ASTNode {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see javaMM.JavaMMPackage#getNamedElement_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link javaMM.NamedElement#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Proxy</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Proxy</em>' attribute.
	 * @see #setProxy(boolean)
	 * @see javaMM.JavaMMPackage#getNamedElement_Proxy()
	 * @model required="true"
	 * @generated
	 */
	boolean isProxy();

	/**
	 * Sets the value of the '{@link javaMM.NamedElement#isProxy <em>Proxy</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Proxy</em>' attribute.
	 * @see #isProxy()
	 * @generated
	 */
	void setProxy(boolean value);

	/**
	 * Returns the value of the '<em><b>Usages In Imports</b></em>' reference list.
	 * The list contents are of type {@link javaMM.ImportDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.ImportDeclaration#getImportedElement <em>Imported Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages In Imports</em>' reference list.
	 * @see javaMM.JavaMMPackage#getNamedElement_UsagesInImports()
	 * @see javaMM.ImportDeclaration#getImportedElement
	 * @model opposite="importedElement" ordered="false"
	 * @generated
	 */
	EList<ImportDeclaration> getUsagesInImports();

} // NamedElement
