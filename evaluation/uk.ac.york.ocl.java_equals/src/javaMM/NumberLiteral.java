/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Number Literal</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.NumberLiteral#getTokenValue <em>Token Value</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getNumberLiteral()
 * @model
 * @generated
 */
public interface NumberLiteral extends Expression {
	/**
	 * Returns the value of the '<em><b>Token Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Token Value</em>' attribute.
	 * @see #setTokenValue(String)
	 * @see javaMM.JavaMMPackage#getNumberLiteral_TokenValue()
	 * @model required="true"
	 * @generated
	 */
	String getTokenValue();

	/**
	 * Sets the value of the '{@link javaMM.NumberLiteral#getTokenValue <em>Token Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Token Value</em>' attribute.
	 * @see #getTokenValue()
	 * @generated
	 */
	void setTokenValue(String value);

} // NumberLiteral
