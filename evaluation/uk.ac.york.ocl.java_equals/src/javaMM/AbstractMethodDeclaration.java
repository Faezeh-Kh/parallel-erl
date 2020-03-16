/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Abstract Method Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getBody <em>Body</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getParameters <em>Parameters</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getThrownExceptions <em>Thrown Exceptions</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getTypeParameters <em>Type Parameters</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getUsagesInDocComments <em>Usages In Doc Comments</em>}</li>
 *   <li>{@link javaMM.AbstractMethodDeclaration#getUsages <em>Usages</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration()
 * @model abstract="true"
 * @generated
 */
public interface AbstractMethodDeclaration extends BodyDeclaration {
	/**
	 * Returns the value of the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body</em>' containment reference.
	 * @see #setBody(Block)
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_Body()
	 * @model containment="true"
	 * @generated
	 */
	Block getBody();

	/**
	 * Sets the value of the '{@link javaMM.AbstractMethodDeclaration#getBody <em>Body</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Body</em>' containment reference.
	 * @see #getBody()
	 * @generated
	 */
	void setBody(Block value);

	/**
	 * Returns the value of the '<em><b>Parameters</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.SingleVariableDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.SingleVariableDeclaration#getMethodDeclaration <em>Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parameters</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_Parameters()
	 * @see javaMM.SingleVariableDeclaration#getMethodDeclaration
	 * @model opposite="methodDeclaration" containment="true"
	 * @generated
	 */
	EList<SingleVariableDeclaration> getParameters();

	/**
	 * Returns the value of the '<em><b>Thrown Exceptions</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.TypeAccess}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Thrown Exceptions</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_ThrownExceptions()
	 * @model containment="true"
	 * @generated
	 */
	EList<TypeAccess> getThrownExceptions();

	/**
	 * Returns the value of the '<em><b>Type Parameters</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.TypeParameter}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type Parameters</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_TypeParameters()
	 * @model containment="true"
	 * @generated
	 */
	EList<TypeParameter> getTypeParameters();

	/**
	 * Returns the value of the '<em><b>Usages In Doc Comments</b></em>' reference list.
	 * The list contents are of type {@link javaMM.MethodRef}.
	 * It is bidirectional and its opposite is '{@link javaMM.MethodRef#getMethod <em>Method</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages In Doc Comments</em>' reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_UsagesInDocComments()
	 * @see javaMM.MethodRef#getMethod
	 * @model opposite="method" ordered="false"
	 * @generated
	 */
	EList<MethodRef> getUsagesInDocComments();

	/**
	 * Returns the value of the '<em><b>Usages</b></em>' reference list.
	 * The list contents are of type {@link javaMM.AbstractMethodInvocation}.
	 * It is bidirectional and its opposite is '{@link javaMM.AbstractMethodInvocation#getMethod <em>Method</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages</em>' reference list.
	 * @see javaMM.JavaMMPackage#getAbstractMethodDeclaration_Usages()
	 * @see javaMM.AbstractMethodInvocation#getMethod
	 * @model opposite="method" ordered="false"
	 * @generated
	 */
	EList<AbstractMethodInvocation> getUsages();

} // AbstractMethodDeclaration
