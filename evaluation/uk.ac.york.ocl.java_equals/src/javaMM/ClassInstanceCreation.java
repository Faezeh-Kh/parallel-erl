/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Class Instance Creation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.ClassInstanceCreation#getAnonymousClassDeclaration <em>Anonymous Class Declaration</em>}</li>
 *   <li>{@link javaMM.ClassInstanceCreation#getExpression <em>Expression</em>}</li>
 *   <li>{@link javaMM.ClassInstanceCreation#getType <em>Type</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getClassInstanceCreation()
 * @model
 * @generated
 */
public interface ClassInstanceCreation extends Expression, AbstractMethodInvocation {
	/**
	 * Returns the value of the '<em><b>Anonymous Class Declaration</b></em>' containment reference.
	 * It is bidirectional and its opposite is '{@link javaMM.AnonymousClassDeclaration#getClassInstanceCreation <em>Class Instance Creation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Anonymous Class Declaration</em>' containment reference.
	 * @see #setAnonymousClassDeclaration(AnonymousClassDeclaration)
	 * @see javaMM.JavaMMPackage#getClassInstanceCreation_AnonymousClassDeclaration()
	 * @see javaMM.AnonymousClassDeclaration#getClassInstanceCreation
	 * @model opposite="classInstanceCreation" containment="true"
	 * @generated
	 */
	AnonymousClassDeclaration getAnonymousClassDeclaration();

	/**
	 * Sets the value of the '{@link javaMM.ClassInstanceCreation#getAnonymousClassDeclaration <em>Anonymous Class Declaration</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Anonymous Class Declaration</em>' containment reference.
	 * @see #getAnonymousClassDeclaration()
	 * @generated
	 */
	void setAnonymousClassDeclaration(AnonymousClassDeclaration value);

	/**
	 * Returns the value of the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Expression</em>' containment reference.
	 * @see #setExpression(Expression)
	 * @see javaMM.JavaMMPackage#getClassInstanceCreation_Expression()
	 * @model containment="true"
	 * @generated
	 */
	Expression getExpression();

	/**
	 * Sets the value of the '{@link javaMM.ClassInstanceCreation#getExpression <em>Expression</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Expression</em>' containment reference.
	 * @see #getExpression()
	 * @generated
	 */
	void setExpression(Expression value);

	/**
	 * Returns the value of the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type</em>' containment reference.
	 * @see #setType(TypeAccess)
	 * @see javaMM.JavaMMPackage#getClassInstanceCreation_Type()
	 * @model containment="true" required="true"
	 * @generated
	 */
	TypeAccess getType();

	/**
	 * Sets the value of the '{@link javaMM.ClassInstanceCreation#getType <em>Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Type</em>' containment reference.
	 * @see #getType()
	 * @generated
	 */
	void setType(TypeAccess value);

} // ClassInstanceCreation
