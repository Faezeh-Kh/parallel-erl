/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Abstract Type Qualified Expression</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AbstractTypeQualifiedExpression#getQualifier <em>Qualifier</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAbstractTypeQualifiedExpression()
 * @model abstract="true"
 * @generated
 */
public interface AbstractTypeQualifiedExpression extends Expression {
	/**
	 * Returns the value of the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Qualifier</em>' containment reference.
	 * @see #setQualifier(TypeAccess)
	 * @see javaMM.JavaMMPackage#getAbstractTypeQualifiedExpression_Qualifier()
	 * @model containment="true"
	 * @generated
	 */
	TypeAccess getQualifier();

	/**
	 * Sets the value of the '{@link javaMM.AbstractTypeQualifiedExpression#getQualifier <em>Qualifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Qualifier</em>' containment reference.
	 * @see #getQualifier()
	 * @generated
	 */
	void setQualifier(TypeAccess value);

} // AbstractTypeQualifiedExpression
