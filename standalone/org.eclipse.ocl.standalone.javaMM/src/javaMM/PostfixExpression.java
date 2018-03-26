/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Postfix Expression</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.PostfixExpression#getOperator <em>Operator</em>}</li>
 *   <li>{@link javaMM.PostfixExpression#getOperand <em>Operand</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getPostfixExpression()
 * @model
 * @generated
 */
public interface PostfixExpression extends Expression {
	/**
	 * Returns the value of the '<em><b>Operator</b></em>' attribute.
	 * The literals are from the enumeration {@link javaMM.PostfixExpressionKind}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Operator</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operator</em>' attribute.
	 * @see javaMM.PostfixExpressionKind
	 * @see #setOperator(PostfixExpressionKind)
	 * @see javaMM.JavaMMPackage#getPostfixExpression_Operator()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!PostfixExpression!operator'"
	 * @generated
	 */
	PostfixExpressionKind getOperator();

	/**
	 * Sets the value of the '{@link javaMM.PostfixExpression#getOperator <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operator</em>' attribute.
	 * @see javaMM.PostfixExpressionKind
	 * @see #getOperator()
	 * @generated
	 */
	void setOperator(PostfixExpressionKind value);

	/**
	 * Returns the value of the '<em><b>Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Operand</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operand</em>' containment reference.
	 * @see #setOperand(Expression)
	 * @see javaMM.JavaMMPackage#getPostfixExpression_Operand()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!PostfixExpression!operand'"
	 * @generated
	 */
	Expression getOperand();

	/**
	 * Sets the value of the '{@link javaMM.PostfixExpression#getOperand <em>Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operand</em>' containment reference.
	 * @see #getOperand()
	 * @generated
	 */
	void setOperand(Expression value);

} // PostfixExpression
