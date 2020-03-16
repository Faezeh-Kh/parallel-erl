/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Enhanced For Statement</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.EnhancedForStatement#getBody <em>Body</em>}</li>
 *   <li>{@link javaMM.EnhancedForStatement#getExpression <em>Expression</em>}</li>
 *   <li>{@link javaMM.EnhancedForStatement#getParameter <em>Parameter</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getEnhancedForStatement()
 * @model
 * @generated
 */
public interface EnhancedForStatement extends Statement {
	/**
	 * Returns the value of the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body</em>' containment reference.
	 * @see #setBody(Statement)
	 * @see javaMM.JavaMMPackage#getEnhancedForStatement_Body()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Statement getBody();

	/**
	 * Sets the value of the '{@link javaMM.EnhancedForStatement#getBody <em>Body</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Body</em>' containment reference.
	 * @see #getBody()
	 * @generated
	 */
	void setBody(Statement value);

	/**
	 * Returns the value of the '<em><b>Expression</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Expression</em>' containment reference.
	 * @see #setExpression(Expression)
	 * @see javaMM.JavaMMPackage#getEnhancedForStatement_Expression()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Expression getExpression();

	/**
	 * Sets the value of the '{@link javaMM.EnhancedForStatement#getExpression <em>Expression</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Expression</em>' containment reference.
	 * @see #getExpression()
	 * @generated
	 */
	void setExpression(Expression value);

	/**
	 * Returns the value of the '<em><b>Parameter</b></em>' containment reference.
	 * It is bidirectional and its opposite is '{@link javaMM.SingleVariableDeclaration#getEnhancedForStatement <em>Enhanced For Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Parameter</em>' containment reference.
	 * @see #setParameter(SingleVariableDeclaration)
	 * @see javaMM.JavaMMPackage#getEnhancedForStatement_Parameter()
	 * @see javaMM.SingleVariableDeclaration#getEnhancedForStatement
	 * @model opposite="enhancedForStatement" containment="true" required="true"
	 * @generated
	 */
	SingleVariableDeclaration getParameter();

	/**
	 * Sets the value of the '{@link javaMM.EnhancedForStatement#getParameter <em>Parameter</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Parameter</em>' containment reference.
	 * @see #getParameter()
	 * @generated
	 */
	void setParameter(SingleVariableDeclaration value);

} // EnhancedForStatement
