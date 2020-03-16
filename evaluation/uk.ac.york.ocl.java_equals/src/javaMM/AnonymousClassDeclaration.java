/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Anonymous Class Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AnonymousClassDeclaration#getBodyDeclarations <em>Body Declarations</em>}</li>
 *   <li>{@link javaMM.AnonymousClassDeclaration#getClassInstanceCreation <em>Class Instance Creation</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAnonymousClassDeclaration()
 * @model
 * @generated
 */
public interface AnonymousClassDeclaration extends ASTNode {
	/**
	 * Returns the value of the '<em><b>Body Declarations</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.BodyDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.BodyDeclaration#getAnonymousClassDeclarationOwner <em>Anonymous Class Declaration Owner</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body Declarations</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAnonymousClassDeclaration_BodyDeclarations()
	 * @see javaMM.BodyDeclaration#getAnonymousClassDeclarationOwner
	 * @model opposite="anonymousClassDeclarationOwner" containment="true"
	 * @generated
	 */
	EList<BodyDeclaration> getBodyDeclarations();

	/**
	 * Returns the value of the '<em><b>Class Instance Creation</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.ClassInstanceCreation#getAnonymousClassDeclaration <em>Anonymous Class Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Class Instance Creation</em>' container reference.
	 * @see #setClassInstanceCreation(ClassInstanceCreation)
	 * @see javaMM.JavaMMPackage#getAnonymousClassDeclaration_ClassInstanceCreation()
	 * @see javaMM.ClassInstanceCreation#getAnonymousClassDeclaration
	 * @model opposite="anonymousClassDeclaration" transient="false"
	 * @generated
	 */
	ClassInstanceCreation getClassInstanceCreation();

	/**
	 * Sets the value of the '{@link javaMM.AnonymousClassDeclaration#getClassInstanceCreation <em>Class Instance Creation</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Class Instance Creation</em>' container reference.
	 * @see #getClassInstanceCreation()
	 * @generated
	 */
	void setClassInstanceCreation(ClassInstanceCreation value);

} // AnonymousClassDeclaration
