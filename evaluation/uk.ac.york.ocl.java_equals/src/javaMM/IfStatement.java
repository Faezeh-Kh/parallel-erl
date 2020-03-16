/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>If Statement</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.IfStatement#getExpression <em>Expression</em>}</li>
 *   <li>{@link javaMM.IfStatement#getThenStatement <em>Then Statement</em>}</li>
 *   <li>{@link javaMM.IfStatement#getElseStatement <em>Else Statement</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getIfStatement()
 * @model
 * @generated
 */
public interface IfStatement extends Statement {
	/**
	 * Returns the value of the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Expression</em>' containment reference.
	 * @see #setExpression(Expression)
	 * @see javaMM.JavaMMPackage#getIfStatement_Expression()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Expression getExpression();

	/**
	 * Sets the value of the '{@link javaMM.IfStatement#getExpression <em>Expression</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Expression</em>' containment reference.
	 * @see #getExpression()
	 * @generated
	 */
	void setExpression(Expression value);

	/**
	 * Returns the value of the '<em><b>Then Statement</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Then Statement</em>' containment reference.
	 * @see #setThenStatement(Statement)
	 * @see javaMM.JavaMMPackage#getIfStatement_ThenStatement()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Statement getThenStatement();

	/**
	 * Sets the value of the '{@link javaMM.IfStatement#getThenStatement <em>Then Statement</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Then Statement</em>' containment reference.
	 * @see #getThenStatement()
	 * @generated
	 */
	void setThenStatement(Statement value);

	/**
	 * Returns the value of the '<em><b>Else Statement</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Else Statement</em>' containment reference.
	 * @see #setElseStatement(Statement)
	 * @see javaMM.JavaMMPackage#getIfStatement_ElseStatement()
	 * @model containment="true"
	 * @generated
	 */
	Statement getElseStatement();

	/**
	 * Sets the value of the '{@link javaMM.IfStatement#getElseStatement <em>Else Statement</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Else Statement</em>' containment reference.
	 * @see #getElseStatement()
	 * @generated
	 */
	void setElseStatement(Statement value);

} // IfStatement
