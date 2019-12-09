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
 *   <li>{@link javaMM.Comment#getEnclosedByParent <em>Enclosed By Parent</em>}</li>
 *   <li>{@link javaMM.Comment#getPrefixOfParent <em>Prefix Of Parent</em>}</li>
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
	 * <p>
	 * If the meaning of the '<em>Content</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Content</em>' attribute.
	 * @see #setContent(String)
	 * @see javaMM.JavaMMPackage#getComment_Content()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!Comment!content'"
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
	 * <p>
	 * If the meaning of the '<em>Enclosed By Parent</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enclosed By Parent</em>' attribute.
	 * @see #setEnclosedByParent(Boolean)
	 * @see javaMM.JavaMMPackage#getComment_EnclosedByParent()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!Comment!enclosedByParent'"
	 * @generated
	 */
	Boolean getEnclosedByParent();

	/**
	 * Sets the value of the '{@link javaMM.Comment#getEnclosedByParent <em>Enclosed By Parent</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Enclosed By Parent</em>' attribute.
	 * @see #getEnclosedByParent()
	 * @generated
	 */
	void setEnclosedByParent(Boolean value);

	/**
	 * Returns the value of the '<em><b>Prefix Of Parent</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Prefix Of Parent</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Prefix Of Parent</em>' attribute.
	 * @see #setPrefixOfParent(Boolean)
	 * @see javaMM.JavaMMPackage#getComment_PrefixOfParent()
	 * @model annotation="http://www.eclipse.org/emf/2002/GenModel get='throw new UnsupportedOperationException();  // FIXME Unimplemented http://www.eclipse.org/MoDisco/Java/0.2.incubation/java!Comment!prefixOfParent'"
	 * @generated
	 */
	Boolean getPrefixOfParent();

	/**
	 * Sets the value of the '{@link javaMM.Comment#getPrefixOfParent <em>Prefix Of Parent</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Prefix Of Parent</em>' attribute.
	 * @see #getPrefixOfParent()
	 * @generated
	 */
	void setPrefixOfParent(Boolean value);

} // Comment
