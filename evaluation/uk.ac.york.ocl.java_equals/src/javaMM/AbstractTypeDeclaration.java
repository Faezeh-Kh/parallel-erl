/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Abstract Type Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getBodyDeclarations <em>Body Declarations</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getCommentsBeforeBody <em>Comments Before Body</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getCommentsAfterBody <em>Comments After Body</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getPackage <em>Package</em>}</li>
 *   <li>{@link javaMM.AbstractTypeDeclaration#getSuperInterfaces <em>Super Interfaces</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration()
 * @model abstract="true"
 * @generated
 */
public interface AbstractTypeDeclaration extends BodyDeclaration, Type {
	/**
	 * Returns the value of the '<em><b>Body Declarations</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.BodyDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.BodyDeclaration#getAbstractTypeDeclaration <em>Abstract Type Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body Declarations</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_BodyDeclarations()
	 * @see javaMM.BodyDeclaration#getAbstractTypeDeclaration
	 * @model opposite="abstractTypeDeclaration" containment="true"
	 * @generated
	 */
	EList<BodyDeclaration> getBodyDeclarations();

	/**
	 * Returns the value of the '<em><b>Comments Before Body</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Comment}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comments Before Body</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_CommentsBeforeBody()
	 * @model containment="true"
	 * @generated
	 */
	EList<Comment> getCommentsBeforeBody();

	/**
	 * Returns the value of the '<em><b>Comments After Body</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Comment}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comments After Body</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_CommentsAfterBody()
	 * @model containment="true"
	 * @generated
	 */
	EList<Comment> getCommentsAfterBody();

	/**
	 * Returns the value of the '<em><b>Package</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.Package#getOwnedElements <em>Owned Elements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Package</em>' container reference.
	 * @see #setPackage(javaMM.Package)
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_Package()
	 * @see javaMM.Package#getOwnedElements
	 * @model opposite="ownedElements" transient="false"
	 * @generated
	 */
	javaMM.Package getPackage();

	/**
	 * Sets the value of the '{@link javaMM.AbstractTypeDeclaration#getPackage <em>Package</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Package</em>' container reference.
	 * @see #getPackage()
	 * @generated
	 */
	void setPackage(javaMM.Package value);

	/**
	 * Returns the value of the '<em><b>Super Interfaces</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.TypeAccess}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Super Interfaces</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractTypeDeclaration_SuperInterfaces()
	 * @model containment="true"
	 * @generated
	 */
	EList<TypeAccess> getSuperInterfaces();

} // AbstractTypeDeclaration
