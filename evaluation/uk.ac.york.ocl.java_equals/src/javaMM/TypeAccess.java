/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Type Access</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.TypeAccess#getType <em>Type</em>}</li>
 *   <li>{@link javaMM.TypeAccess#getQualifier <em>Qualifier</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getTypeAccess()
 * @model
 * @generated
 */
public interface TypeAccess extends Expression, NamespaceAccess {
	/**
	 * Returns the value of the '<em><b>Type</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link javaMM.Type#getUsagesInTypeAccess <em>Usages In Type Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type</em>' reference.
	 * @see #setType(Type)
	 * @see javaMM.JavaMMPackage#getTypeAccess_Type()
	 * @see javaMM.Type#getUsagesInTypeAccess
	 * @model opposite="usagesInTypeAccess" required="true"
	 * @generated
	 */
	Type getType();

	/**
	 * Sets the value of the '{@link javaMM.TypeAccess#getType <em>Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Type</em>' reference.
	 * @see #getType()
	 * @generated
	 */
	void setType(Type value);

	/**
	 * Returns the value of the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Qualifier</em>' containment reference.
	 * @see #setQualifier(NamespaceAccess)
	 * @see javaMM.JavaMMPackage#getTypeAccess_Qualifier()
	 * @model containment="true"
	 * @generated
	 */
	NamespaceAccess getQualifier();

	/**
	 * Sets the value of the '{@link javaMM.TypeAccess#getQualifier <em>Qualifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Qualifier</em>' containment reference.
	 * @see #getQualifier()
	 * @generated
	 */
	void setQualifier(NamespaceAccess value);

} // TypeAccess
