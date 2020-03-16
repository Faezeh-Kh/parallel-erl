/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Infix Expression</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.InfixExpression#getOperator <em>Operator</em>}</li>
 *   <li>{@link javaMM.InfixExpression#getRightOperand <em>Right Operand</em>}</li>
 *   <li>{@link javaMM.InfixExpression#getLeftOperand <em>Left Operand</em>}</li>
 *   <li>{@link javaMM.InfixExpression#getExtendedOperands <em>Extended Operands</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getInfixExpression()
 * @model
 * @generated
 */
public interface InfixExpression extends Expression {
	/**
	 * Returns the value of the '<em><b>Operator</b></em>' attribute.
	 * The literals are from the enumeration {@link javaMM.InfixExpressionKind}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Operator</em>' attribute.
	 * @see javaMM.InfixExpressionKind
	 * @see #setOperator(InfixExpressionKind)
	 * @see javaMM.JavaMMPackage#getInfixExpression_Operator()
	 * @model required="true"
	 * @generated
	 */
	InfixExpressionKind getOperator();

	/**
	 * Sets the value of the '{@link javaMM.InfixExpression#getOperator <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Operator</em>' attribute.
	 * @see javaMM.InfixExpressionKind
	 * @see #getOperator()
	 * @generated
	 */
	void setOperator(InfixExpressionKind value);

	/**
	 * Returns the value of the '<em><b>Right Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Right Operand</em>' containment reference.
	 * @see #setRightOperand(Expression)
	 * @see javaMM.JavaMMPackage#getInfixExpression_RightOperand()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Expression getRightOperand();

	/**
	 * Sets the value of the '{@link javaMM.InfixExpression#getRightOperand <em>Right Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Right Operand</em>' containment reference.
	 * @see #getRightOperand()
	 * @generated
	 */
	void setRightOperand(Expression value);

	/**
	 * Returns the value of the '<em><b>Left Operand</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Left Operand</em>' containment reference.
	 * @see #setLeftOperand(Expression)
	 * @see javaMM.JavaMMPackage#getInfixExpression_LeftOperand()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Expression getLeftOperand();

	/**
	 * Sets the value of the '{@link javaMM.InfixExpression#getLeftOperand <em>Left Operand</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Left Operand</em>' containment reference.
	 * @see #getLeftOperand()
	 * @generated
	 */
	void setLeftOperand(Expression value);

	/**
	 * Returns the value of the '<em><b>Extended Operands</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Expression}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Extended Operands</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getInfixExpression_ExtendedOperands()
	 * @model containment="true"
	 * @generated
	 */
	EList<Expression> getExtendedOperands();

} // InfixExpression
