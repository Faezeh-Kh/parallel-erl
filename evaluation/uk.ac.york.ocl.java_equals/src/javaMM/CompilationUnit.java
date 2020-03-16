/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Compilation Unit</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.CompilationUnit#getOriginalFilePath <em>Original File Path</em>}</li>
 *   <li>{@link javaMM.CompilationUnit#getCommentList <em>Comment List</em>}</li>
 *   <li>{@link javaMM.CompilationUnit#getImports <em>Imports</em>}</li>
 *   <li>{@link javaMM.CompilationUnit#getPackage <em>Package</em>}</li>
 *   <li>{@link javaMM.CompilationUnit#getTypes <em>Types</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getCompilationUnit()
 * @model
 * @generated
 */
public interface CompilationUnit extends NamedElement {
	/**
	 * Returns the value of the '<em><b>Original File Path</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Original File Path</em>' attribute.
	 * @see #setOriginalFilePath(String)
	 * @see javaMM.JavaMMPackage#getCompilationUnit_OriginalFilePath()
	 * @model required="true"
	 * @generated
	 */
	String getOriginalFilePath();

	/**
	 * Sets the value of the '{@link javaMM.CompilationUnit#getOriginalFilePath <em>Original File Path</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Original File Path</em>' attribute.
	 * @see #getOriginalFilePath()
	 * @generated
	 */
	void setOriginalFilePath(String value);

	/**
	 * Returns the value of the '<em><b>Comment List</b></em>' reference list.
	 * The list contents are of type {@link javaMM.Comment}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comment List</em>' reference list.
	 * @see javaMM.JavaMMPackage#getCompilationUnit_CommentList()
	 * @model
	 * @generated
	 */
	EList<Comment> getCommentList();

	/**
	 * Returns the value of the '<em><b>Imports</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.ImportDeclaration}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Imports</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getCompilationUnit_Imports()
	 * @model containment="true"
	 * @generated
	 */
	EList<ImportDeclaration> getImports();

	/**
	 * Returns the value of the '<em><b>Package</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Package</em>' reference.
	 * @see #setPackage(javaMM.Package)
	 * @see javaMM.JavaMMPackage#getCompilationUnit_Package()
	 * @model
	 * @generated
	 */
	javaMM.Package getPackage();

	/**
	 * Sets the value of the '{@link javaMM.CompilationUnit#getPackage <em>Package</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Package</em>' reference.
	 * @see #getPackage()
	 * @generated
	 */
	void setPackage(javaMM.Package value);

	/**
	 * Returns the value of the '<em><b>Types</b></em>' reference list.
	 * The list contents are of type {@link javaMM.AbstractTypeDeclaration}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Types</em>' reference list.
	 * @see javaMM.JavaMMPackage#getCompilationUnit_Types()
	 * @model
	 * @generated
	 */
	EList<AbstractTypeDeclaration> getTypes();

} // CompilationUnit
