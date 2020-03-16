/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Annotation Member Value Pair</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AnnotationMemberValuePair#getMember <em>Member</em>}</li>
 *   <li>{@link javaMM.AnnotationMemberValuePair#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAnnotationMemberValuePair()
 * @model
 * @generated
 */
public interface AnnotationMemberValuePair extends NamedElement {
	/**
	 * Returns the value of the '<em><b>Member</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link javaMM.AnnotationTypeMemberDeclaration#getUsages <em>Usages</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Member</em>' reference.
	 * @see #setMember(AnnotationTypeMemberDeclaration)
	 * @see javaMM.JavaMMPackage#getAnnotationMemberValuePair_Member()
	 * @see javaMM.AnnotationTypeMemberDeclaration#getUsages
	 * @model opposite="usages"
	 * @generated
	 */
	AnnotationTypeMemberDeclaration getMember();

	/**
	 * Sets the value of the '{@link javaMM.AnnotationMemberValuePair#getMember <em>Member</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Member</em>' reference.
	 * @see #getMember()
	 * @generated
	 */
	void setMember(AnnotationTypeMemberDeclaration value);

	/**
	 * Returns the value of the '<em><b>Value</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Value</em>' containment reference.
	 * @see #setValue(Expression)
	 * @see javaMM.JavaMMPackage#getAnnotationMemberValuePair_Value()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Expression getValue();

	/**
	 * Sets the value of the '{@link javaMM.AnnotationMemberValuePair#getValue <em>Value</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Value</em>' containment reference.
	 * @see #getValue()
	 * @generated
	 */
	void setValue(Expression value);

} // AnnotationMemberValuePair
