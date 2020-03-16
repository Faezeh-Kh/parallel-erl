/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Single Variable Access</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.SingleVariableAccess#getVariable <em>Variable</em>}</li>
 *   <li>{@link javaMM.SingleVariableAccess#getQualifier <em>Qualifier</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getSingleVariableAccess()
 * @model
 * @generated
 */
public interface SingleVariableAccess extends Expression {
	/**
	 * Returns the value of the '<em><b>Variable</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link javaMM.VariableDeclaration#getUsageInVariableAccess <em>Usage In Variable Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variable</em>' reference.
	 * @see #setVariable(VariableDeclaration)
	 * @see javaMM.JavaMMPackage#getSingleVariableAccess_Variable()
	 * @see javaMM.VariableDeclaration#getUsageInVariableAccess
	 * @model opposite="usageInVariableAccess" required="true"
	 * @generated
	 */
	VariableDeclaration getVariable();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableAccess#getVariable <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable</em>' reference.
	 * @see #getVariable()
	 * @generated
	 */
	void setVariable(VariableDeclaration value);

	/**
	 * Returns the value of the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Qualifier</em>' containment reference.
	 * @see #setQualifier(Expression)
	 * @see javaMM.JavaMMPackage#getSingleVariableAccess_Qualifier()
	 * @model containment="true"
	 * @generated
	 */
	Expression getQualifier();

	/**
	 * Sets the value of the '{@link javaMM.SingleVariableAccess#getQualifier <em>Qualifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Qualifier</em>' containment reference.
	 * @see #getQualifier()
	 * @generated
	 */
	void setQualifier(Expression value);

} // SingleVariableAccess
