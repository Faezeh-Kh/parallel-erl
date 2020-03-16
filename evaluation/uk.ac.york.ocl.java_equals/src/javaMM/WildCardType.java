/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Wild Card Type</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.WildCardType#isUpperBound <em>Upper Bound</em>}</li>
 *   <li>{@link javaMM.WildCardType#getBound <em>Bound</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getWildCardType()
 * @model
 * @generated
 */
public interface WildCardType extends Type {
	/**
	 * Returns the value of the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Upper Bound</em>' attribute.
	 * @see #setUpperBound(boolean)
	 * @see javaMM.JavaMMPackage#getWildCardType_UpperBound()
	 * @model required="true"
	 * @generated
	 */
	boolean isUpperBound();

	/**
	 * Sets the value of the '{@link javaMM.WildCardType#isUpperBound <em>Upper Bound</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Upper Bound</em>' attribute.
	 * @see #isUpperBound()
	 * @generated
	 */
	void setUpperBound(boolean value);

	/**
	 * Returns the value of the '<em><b>Bound</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Bound</em>' containment reference.
	 * @see #setBound(TypeAccess)
	 * @see javaMM.JavaMMPackage#getWildCardType_Bound()
	 * @model containment="true"
	 * @generated
	 */
	TypeAccess getBound();

	/**
	 * Sets the value of the '{@link javaMM.WildCardType#getBound <em>Bound</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Bound</em>' containment reference.
	 * @see #getBound()
	 * @generated
	 */
	void setBound(TypeAccess value);

} // WildCardType
