/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Comment</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.Comment#getContent <em>Content</em>}</li>
 *   <li>{@link javaMM.Comment#isEnclosedByParent <em>Enclosed By Parent</em>}</li>
 *   <li>{@link javaMM.Comment#isPrefixOfParent <em>Prefix Of Parent</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getComment()
 * @model abstract="true"
 * @generated
 */
public interface Comment extends ASTNode {
	/**
	 * Returns the value of the '<em><b>Content</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Content</em>' attribute.
	 * @see #setContent(String)
	 * @see javaMM.JavaMMPackage#getComment_Content()
	 * @model required="true"
	 * @generated
	 */
	String getContent();

	/**
	 * Sets the value of the '{@link javaMM.Comment#getContent <em>Content</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Content</em>' attribute.
	 * @see #getContent()
	 * @generated
	 */
	void setContent(String value);

	/**
	 * Returns the value of the '<em><b>Enclosed By Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enclosed By Parent</em>' attribute.
	 * @see #setEnclosedByParent(boolean)
	 * @see javaMM.JavaMMPackage#getComment_EnclosedByParent()
	 * @model required="true"
	 * @generated
	 */
	boolean isEnclosedByParent();

	/**
	 * Sets the value of the '{@link javaMM.Comment#isEnclosedByParent <em>Enclosed By Parent</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Enclosed By Parent</em>' attribute.
	 * @see #isEnclosedByParent()
	 * @generated
	 */
	void setEnclosedByParent(boolean value);

	/**
	 * Returns the value of the '<em><b>Prefix Of Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Prefix Of Parent</em>' attribute.
	 * @see #setPrefixOfParent(boolean)
	 * @see javaMM.JavaMMPackage#getComment_PrefixOfParent()
	 * @model required="true"
	 * @generated
	 */
	boolean isPrefixOfParent();

	/**
	 * Sets the value of the '{@link javaMM.Comment#isPrefixOfParent <em>Prefix Of Parent</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Prefix Of Parent</em>' attribute.
	 * @see #isPrefixOfParent()
	 * @generated
	 */
	void setPrefixOfParent(boolean value);

} // Comment
