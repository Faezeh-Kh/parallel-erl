/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Method Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.MethodDeclaration#getExtraArrayDimensions <em>Extra Array Dimensions</em>}</li>
 *   <li>{@link javaMM.MethodDeclaration#getReturnType <em>Return Type</em>}</li>
 *   <li>{@link javaMM.MethodDeclaration#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}</li>
 *   <li>{@link javaMM.MethodDeclaration#getRedefinitions <em>Redefinitions</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getMethodDeclaration()
 * @model
 * @generated
 */
public interface MethodDeclaration extends AbstractMethodDeclaration {
	/**
	 * Returns the value of the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Extra Array Dimensions</em>' attribute.
	 * @see #setExtraArrayDimensions(int)
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_ExtraArrayDimensions()
	 * @model required="true"
	 * @generated
	 */
	int getExtraArrayDimensions();

	/**
	 * Sets the value of the '{@link javaMM.MethodDeclaration#getExtraArrayDimensions <em>Extra Array Dimensions</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Extra Array Dimensions</em>' attribute.
	 * @see #getExtraArrayDimensions()
	 * @generated
	 */
	void setExtraArrayDimensions(int value);

	/**
	 * Returns the value of the '<em><b>Return Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Return Type</em>' containment reference.
	 * @see #setReturnType(TypeAccess)
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_ReturnType()
	 * @model containment="true"
	 * @generated
	 */
	TypeAccess getReturnType();

	/**
	 * Sets the value of the '{@link javaMM.MethodDeclaration#getReturnType <em>Return Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Return Type</em>' containment reference.
	 * @see #getReturnType()
	 * @generated
	 */
	void setReturnType(TypeAccess value);

	/**
	 * Returns the value of the '<em><b>Redefined Method Declaration</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link javaMM.MethodDeclaration#getRedefinitions <em>Redefinitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Redefined Method Declaration</em>' reference.
	 * @see #setRedefinedMethodDeclaration(MethodDeclaration)
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_RedefinedMethodDeclaration()
	 * @see javaMM.MethodDeclaration#getRedefinitions
	 * @model opposite="redefinitions"
	 * @generated
	 */
	MethodDeclaration getRedefinedMethodDeclaration();

	/**
	 * Sets the value of the '{@link javaMM.MethodDeclaration#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Redefined Method Declaration</em>' reference.
	 * @see #getRedefinedMethodDeclaration()
	 * @generated
	 */
	void setRedefinedMethodDeclaration(MethodDeclaration value);

	/**
	 * Returns the value of the '<em><b>Redefinitions</b></em>' reference list.
	 * The list contents are of type {@link javaMM.MethodDeclaration}.
	 * It is bidirectional and its opposite is '{@link javaMM.MethodDeclaration#getRedefinedMethodDeclaration <em>Redefined Method Declaration</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Redefinitions</em>' reference list.
	 * @see javaMM.JavaMMPackage#getMethodDeclaration_Redefinitions()
	 * @see javaMM.MethodDeclaration#getRedefinedMethodDeclaration
	 * @model opposite="redefinedMethodDeclaration" ordered="false"
	 * @generated
	 */
	EList<MethodDeclaration> getRedefinitions();

} // MethodDeclaration
