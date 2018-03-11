/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Unresolved Item Access</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.UnresolvedItemAccess#getElement <em>Element</em>}</li>
 *   <li>{@link javaMM.UnresolvedItemAccess#getQualifier <em>Qualifier</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getUnresolvedItemAccess()
 * @model
 * @generated
 */
public interface UnresolvedItemAccess extends Expression, NamespaceAccess {
	/**
	 * Returns the value of the '<em><b>Element</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Element</em>' reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Element</em>' reference.
	 * @see #setElement(UnresolvedItem)
	 * @see javaMM.JavaMMPackage#getUnresolvedItemAccess_Element()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!UnresolvedItemAccess!element'"
	 * @generated
	 */
	UnresolvedItem getElement();

	/**
	 * Sets the value of the '{@link javaMM.UnresolvedItemAccess#getElement <em>Element</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Element</em>' reference.
	 * @see #getElement()
	 * @generated
	 */
	void setElement(UnresolvedItem value);

	/**
	 * Returns the value of the '<em><b>Qualifier</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Qualifier</em>' containment reference isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Qualifier</em>' containment reference.
	 * @see #setQualifier(ASTNode)
	 * @see javaMM.JavaMMPackage#getUnresolvedItemAccess_Qualifier()
	 * @model containment="true"
	 *        annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!UnresolvedItemAccess!qualifier'"
	 * @generated
	 */
	ASTNode getQualifier();

	/**
	 * Sets the value of the '{@link javaMM.UnresolvedItemAccess#getQualifier <em>Qualifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Qualifier</em>' containment reference.
	 * @see #getQualifier()
	 * @generated
	 */
	void setQualifier(ASTNode value);

} // UnresolvedItemAccess
