/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Prefix Expression</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.PrefixExpression#getOperator <em>Operator</em>}</li>
 *   <li>{@link javaMM.PrefixExpression#getOperand <em>Operand</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getPrefixExpression()
 * @model
 * @generated
 */
public interface PrefixExpression extends Expression {
	/**
	 * Returns the value of the '<em><b>Operator</b></em>' attribute.
	 * The literals are from the enumeration {@link javaMM.PrefixExpressionKind}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operator</em>' attribute.
	 * @see javaMM.PrefixExpressionKind
	 * @see #setOperator(PrefixExpressionKind)
	 * @see javaMM.JavaMMPackage#getPrefixExpression_Operator()
	 * @model required="true"
	 * @generated
	 */
	PrefixExpressionKind getOperator();

	/**
	 * Sets the value of the '{@link javaMM.PrefixExpression#getOperator <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operator</em>' attribute.
	 * @see javaMM.PrefixExpressionKind
	 * @see #getOperator()
	 * @generated
	 */
	void setOperator(PrefixExpressionKind value);

	/**
	 * Returns the value of the '<em><b>Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operand</em>' containment reference.
	 * @see #setOperand(Expression)
	 * @see javaMM.JavaMMPackage#getPrefixExpression_Operand()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Expression getOperand();

	/**
	 * Sets the value of the '{@link javaMM.PrefixExpression#getOperand <em>Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operand</em>' containment reference.
	 * @see #getOperand()
	 * @generated
	 */
	void setOperand(Expression value);

} // PrefixExpression
